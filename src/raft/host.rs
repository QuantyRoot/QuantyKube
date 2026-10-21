//! The host around a Raft node (ADR-034): one thread that owns the node,
//! keeps its clock, writes what it must keep, sends its messages, applies
//! what it commits, and hands answers back to the callers that waited.

use super::{Body, Config, Entry, HardState, Message, Node, NodeId, Role, SnapshotMeta};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Where a node keeps its term, vote and log across restarts.
pub trait LogStore: Send {
    fn load(&mut self) -> io::Result<(HardState, SnapshotMeta, Vec<Entry>)>;
    /// Durable when it returns: replaces the log from the first entry's
    /// index on.
    fn save(&mut self, hard: Option<HardState>, entries: &[Entry]) -> io::Result<()>;
    /// The state up to `snap` is kept elsewhere now; the log before it can
    /// go.
    fn compact(&mut self, snap: SnapshotMeta) -> io::Result<()>;
    /// The state is a snapshot taken from the leader: the whole log goes,
    /// and what follows is saved anew.
    fn reset(&mut self, snap: SnapshotMeta) -> io::Result<()>;
}

/// A log kept in memory, for tests and for a node that may forget.
#[derive(Default)]
pub struct MemLog {
    hard: HardState,
    snap: SnapshotMeta,
    log: Vec<Entry>,
}

impl LogStore for MemLog {
    fn load(&mut self) -> io::Result<(HardState, SnapshotMeta, Vec<Entry>)> {
        Ok((self.hard, self.snap, self.log.clone()))
    }

    fn save(&mut self, hard: Option<HardState>, entries: &[Entry]) -> io::Result<()> {
        if let Some(h) = hard {
            self.hard = h;
        }
        if let Some(first) = entries.first() {
            self.log.retain(|e| e.index < first.index);
            self.log.extend_from_slice(entries);
        }
        Ok(())
    }

    fn compact(&mut self, snap: SnapshotMeta) -> io::Result<()> {
        self.snap = snap;
        self.log.retain(|e| e.index > snap.index);
        Ok(())
    }

    fn reset(&mut self, snap: SnapshotMeta) -> io::Result<()> {
        self.snap = snap;
        self.log.clear();
        Ok(())
    }
}

/// How messages leave this node.
pub trait Wire: Send {
    fn send(&self, m: Message);
}

impl Wire for super::transport::Transport {
    fn send(&self, m: Message) {
        super::transport::Transport::send(self, m)
    }
}

/// What the host does with a committed command: applies it, and says
/// which caller it answers, if one on this node waits for it.
pub trait Machine: Send + Sync {
    /// Applies the command at log index `index`. The machine must keep
    /// the index of what it applied with what it applied, atomically, so
    /// that after a restart nothing is applied twice.
    fn apply(&self, index: u64, data: &[u8]) -> Option<(u64, Vec<u8>)>;
    /// The last index applied, as kept by `apply`.
    fn applied(&self) -> u64;
    /// The whole state as of the last command applied, for a follower
    /// that needs more than the log still holds.
    fn snapshot(&self) -> io::Result<Vec<u8>>;
    /// Replaces the state with `data`, taken from the leader as of
    /// `index`, and keeps `index` as applied.
    fn restore(&self, index: u64, data: &[u8]) -> io::Result<()>;
}

enum Input {
    Step(Message),
    Propose(Vec<u8>, u64, Sender<Vec<u8>>, Instant),
    Read(Sender<u64>),
    Status(Sender<Status>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub role: Role,
    pub commit: u64,
    pub applied: u64,
    /// Where the log starts: what is before it is only in the store.
    pub log_start: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// No answer within the time given: the command may still be applied.
    Timeout,
    Stopped,
}

/// The handle callers use; the node itself lives on the host's thread.
pub struct Host {
    input: Sender<Input>,
    next: Mutex<u64>,
}

pub const TICK: Duration = Duration::from_millis(100);

/// When the log is cut: once this many entries are applied beyond the
/// last cut, all but the last `keep` go, as etcd's snapshot-count does.
#[derive(Debug, Clone, Copy)]
pub struct Compaction {
    pub after: u64,
    pub keep: u64,
}

impl Default for Compaction {
    fn default() -> Self {
        Compaction {
            after: 10_000,
            keep: 5_000,
        }
    }
}

impl Host {
    /// Starts the node of `id` among `peers` from what `log` holds.
    /// Messages for it go in through [`Host::step`]; `inbound` is drained
    /// into it as well when given.
    pub fn start(
        id: NodeId,
        peers: &[NodeId],
        log: Box<dyn LogStore>,
        wire: Box<dyn Wire>,
        machine: Arc<dyn Machine>,
        inbound: Option<Receiver<Message>>,
    ) -> io::Result<Arc<Host>> {
        Host::start_with(
            id,
            peers,
            log,
            wire,
            machine,
            inbound,
            Compaction::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_with(
        id: NodeId,
        peers: &[NodeId],
        mut log: Box<dyn LogStore>,
        wire: Box<dyn Wire>,
        machine: Arc<dyn Machine>,
        inbound: Option<Receiver<Message>>,
        compaction: Compaction,
    ) -> io::Result<Arc<Host>> {
        let (hard, snap, entries) = log.load()?;
        let seed = id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ now_nanos();
        // what the machine already holds is not applied again
        let applied = machine.applied().min(hard.commit.max(snap.index));
        let node = Node::new(
            id,
            peers,
            Config::default(),
            hard,
            snap,
            entries,
            applied,
            seed,
        );
        let (input, rx) = mpsc::channel();
        if let Some(inbound) = inbound {
            let input = input.clone();
            std::thread::spawn(move || {
                for m in inbound {
                    if input.send(Input::Step(m)).is_err() {
                        return;
                    }
                }
            });
        }
        std::thread::spawn(move || run(node, applied, compaction, log, wire, machine, rx));
        Ok(Arc::new(Host {
            input,
            next: Mutex::new(0),
        }))
    }

    pub fn step(&self, m: Message) {
        let _ = self.input.send(Input::Step(m));
    }

    /// Proposes `data`, made by `wrap` from a request id unique on this
    /// node, and waits up to `wait` for the answer the machine gives when
    /// it applies it here.
    pub fn propose(
        &self,
        wrap: impl FnOnce(u64) -> Vec<u8>,
        wait: Duration,
    ) -> Result<Vec<u8>, Error> {
        let req = {
            let mut n = self.next.lock().unwrap_or_else(|e| e.into_inner());
            *n += 1;
            *n
        };
        let (tx, rx) = mpsc::channel();
        self.input
            .send(Input::Propose(wrap(req), req, tx, Instant::now() + wait))
            .map_err(|_| Error::Stopped)?;
        rx.recv_timeout(wait).map_err(|_| Error::Timeout)
    }

    /// Waits until this node has applied everything committed before the
    /// call, so a read that follows is linearizable.
    pub fn read_barrier(&self, wait: Duration) -> Result<u64, Error> {
        let (tx, rx) = mpsc::channel();
        self.input
            .send(Input::Read(tx))
            .map_err(|_| Error::Stopped)?;
        rx.recv_timeout(wait).map_err(|_| Error::Timeout)
    }

    pub fn status(&self) -> Result<Status, Error> {
        let (tx, rx) = mpsc::channel();
        self.input
            .send(Input::Status(tx))
            .map_err(|_| Error::Stopped)?;
        rx.recv_timeout(Duration::from_secs(5))
            .map_err(|_| Error::Timeout)
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

struct State {
    node: Node,
    log: Box<dyn LogStore>,
    wire: Box<dyn Wire>,
    machine: Arc<dyn Machine>,
    applied: u64,
    /// Callers waiting for their command, until the time they gave.
    proposals: HashMap<u64, (Sender<Vec<u8>>, Instant)>,
    reads: HashMap<u64, Sender<u64>>,
    /// Reads whose index is known, waiting for this node to apply it.
    ready_reads: BTreeMap<u64, Vec<Sender<u64>>>,
    next_read: u64,
    /// Commands and reads that came while no leader was known; they go
    /// out once one is.
    unsent: Vec<Vec<u8>>,
    unsent_reads: Vec<u64>,
    compaction: Compaction,
    /// The last state sent as a snapshot, and the index it was taken at.
    snapshot: Option<(u64, Arc<Vec<u8>>)>,
}

impl State {
    /// Does what the node's Ready asks, in the order Raft needs. A disk
    /// that fails stops the host: what it acknowledged would not hold.
    fn ready(&mut self) {
        let r = self.node.take_ready();
        if let Some(snap) = r.snapshot {
            // the machine took the state before the node was told
            self.applied = snap.index;
            if let Err(e) = self.log.reset(snap) {
                eprintln!("quantykube: raft log: {e}; stopping");
                std::process::exit(1);
            }
        }
        if r.hard_state.is_some() || !r.entries.is_empty() {
            if let Err(e) = self.log.save(r.hard_state, &r.entries) {
                eprintln!("quantykube: raft log: {e}; stopping");
                std::process::exit(1);
            }
        }
        for mut m in r.messages {
            if let Body::Snapshot { index, term, data } = &mut m.body {
                // the state as it is now, which is the state at the last
                // index applied: newer than the node's snapshot, and as
                // good, since it is committed too
                let at = self.applied;
                let Some(at_term) = self.node.term_of(at) else {
                    continue;
                };
                let state = match &self.snapshot {
                    Some((i, d)) if *i == at => d.clone(),
                    _ => match self.machine.snapshot() {
                        Ok(d) => {
                            let d = Arc::new(d);
                            self.snapshot = Some((at, d.clone()));
                            d
                        }
                        Err(e) => {
                            eprintln!("quantykube: raft: no snapshot to send: {e}");
                            continue;
                        }
                    },
                };
                (*index, *term) = (at, at_term);
                *data = state.to_vec();
            }
            self.wire.send(m);
        }
        for e in r.committed {
            self.applied = e.index;
            if e.data.is_empty() {
                continue;
            }
            if let Some((req, answer)) = self.machine.apply(e.index, &e.data) {
                if let Some((tx, _)) = self.proposals.remove(&req) {
                    let _ = tx.send(answer);
                }
            }
        }
        // what the store holds need not stay in the log; a follower that
        // needs more than is left gets a snapshot instead
        let c = self.compaction;
        let snap = self.node.snapshot().index;
        if self.applied >= snap + c.after + c.keep {
            self.node.compact(self.applied - c.keep);
            if let Err(e) = self.log.compact(self.node.snapshot()) {
                eprintln!("quantykube: raft log: {e}; stopping");
                std::process::exit(1);
            }
        }
        for read in r.reads {
            if let Some(tx) = self.reads.remove(&read.req) {
                self.ready_reads.entry(read.index).or_default().push(tx);
            }
        }
        let applied = self.applied;
        let done: Vec<u64> = self
            .ready_reads
            .range(..=applied)
            .map(|(i, _)| *i)
            .collect();
        for i in done {
            for tx in self.ready_reads.remove(&i).unwrap_or_default() {
                let _ = tx.send(applied);
            }
        }
    }
}

fn run(
    node: Node,
    applied: u64,
    compaction: Compaction,
    log: Box<dyn LogStore>,
    wire: Box<dyn Wire>,
    machine: Arc<dyn Machine>,
    rx: Receiver<Input>,
) {
    let applied = applied.max(node.snapshot().index);
    let mut s = State {
        node,
        log,
        wire,
        machine,
        applied,
        proposals: HashMap::new(),
        reads: HashMap::new(),
        ready_reads: BTreeMap::new(),
        next_read: 0,
        unsent: Vec::new(),
        unsent_reads: Vec::new(),
        compaction,
        snapshot: None,
    };
    let mut next_tick = Instant::now() + TICK;
    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(Input::Step(mut m)) => {
                if let Body::Snapshot { index, data, .. } = &mut m.body {
                    if *index > s.node.commit() {
                        // the state first; the node learns of it only once
                        // it is in place
                        if let Err(e) = s.machine.restore(*index, data) {
                            eprintln!("quantykube: raft: a snapshot at {index}: {e}");
                            continue;
                        }
                    }
                    *data = Vec::new();
                }
                s.node.step(m)
            }
            Ok(Input::Propose(data, req, tx, until)) => {
                s.proposals.insert(req, (tx, until));
                s.unsent.push(data);
            }
            Ok(Input::Read(tx)) => {
                s.next_read += 1;
                s.reads.insert(s.next_read, tx);
                s.unsent_reads.push(s.next_read);
            }
            Ok(Input::Status(tx)) => {
                let _ = tx.send(Status {
                    id: s.node.id(),
                    term: s.node.term(),
                    leader: s.node.leader(),
                    role: s.node.role(),
                    commit: s.node.commit(),
                    applied: s.applied,
                    log_start: s.node.snapshot().index + 1,
                });
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        if s.node.role() == Role::Leader || s.node.leader().is_some() {
            // a follower passes commands on and learns of them when it
            // applies them; the index is of no use here
            for data in std::mem::take(&mut s.unsent) {
                let _ = s.node.propose(data);
            }
            for req in std::mem::take(&mut s.unsent_reads) {
                s.node.read_index(req);
            }
        }
        if Instant::now() >= next_tick {
            next_tick += TICK;
            s.node.tick();
            // callers that gave up are forgotten
            let now = Instant::now();
            s.proposals.retain(|_, (_, until)| *until > now);
        }
        s.ready();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Weak;

    type Hosts = Arc<Mutex<BTreeMap<NodeId, Weak<Host>>>>;
    type Down = Arc<Mutex<BTreeSet<NodeId>>>;

    /// Messages handed straight to the other hosts, except to and from
    /// nodes that are down.
    struct Loopback {
        hosts: Hosts,
        down: Down,
    }

    impl Wire for Loopback {
        fn send(&self, m: Message) {
            let down = self.down.lock().unwrap();
            if down.contains(&m.from) || down.contains(&m.to) {
                return;
            }
            drop(down);
            let to = self
                .hosts
                .lock()
                .unwrap()
                .get(&m.to)
                .and_then(Weak::upgrade);
            if let Some(h) = to {
                h.step(m);
            }
        }
    }

    /// Commands are `node, req, value`; the machine keeps every value
    /// in order and answers with how many it holds.
    struct Listing {
        id: NodeId,
        values: Mutex<Vec<u64>>,
    }

    impl Machine for Listing {
        fn applied(&self) -> u64 {
            0
        }

        fn snapshot(&self) -> io::Result<Vec<u8>> {
            let v = self.values.lock().unwrap();
            Ok(v.iter().flat_map(|x| x.to_be_bytes()).collect())
        }

        fn restore(&self, _: u64, data: &[u8]) -> io::Result<()> {
            *self.values.lock().unwrap() = data
                .chunks(8)
                .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
                .collect();
            Ok(())
        }

        fn apply(&self, _: u64, data: &[u8]) -> Option<(u64, Vec<u8>)> {
            let f = |i: usize| u64::from_be_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
            let mut v = self.values.lock().unwrap();
            v.push(f(2));
            let n = v.len() as u64;
            (f(0) == self.id).then(|| (f(1), n.to_be_bytes().to_vec()))
        }
    }

    fn cluster() -> (Vec<Arc<Host>>, Vec<Arc<Listing>>, Down) {
        cluster_with(Compaction::default())
    }

    fn cluster_with(compaction: Compaction) -> (Vec<Arc<Host>>, Vec<Arc<Listing>>, Down) {
        let hosts: Hosts = Arc::default();
        let down: Down = Arc::default();
        let ids = [1, 2, 3];
        let mut out = Vec::new();
        let mut machines = Vec::new();
        for id in ids {
            let m = Arc::new(Listing {
                id,
                values: Mutex::new(Vec::new()),
            });
            let wire = Loopback {
                hosts: hosts.clone(),
                down: down.clone(),
            };
            let log = Box::<MemLog>::default();
            let h = Host::start_with(id, &ids, log, Box::new(wire), m.clone(), None, compaction)
                .unwrap();
            hosts.lock().unwrap().insert(id, Arc::downgrade(&h));
            out.push(h);
            machines.push(m);
        }
        (out, machines, down)
    }

    /// As a client does: a command passed to a leader that just died is
    /// lost, and the client sends it again. It may then be applied twice,
    /// as with etcd.
    fn put(h: &Host, id: NodeId, value: u64) {
        let wrap = |req: u64| {
            [id, req, value]
                .iter()
                .flat_map(|x| x.to_be_bytes())
                .collect()
        };
        for _ in 0..10 {
            if h.propose(wrap, Duration::from_secs(2)).is_ok() {
                return;
            }
        }
        panic!("{value} was never applied");
    }

    #[test]
    fn three_hosts_agree_and_survive_losing_one() {
        let (hosts, machines, down) = cluster();
        // any node takes writes: followers pass them to the leader
        for v in 0..30u64 {
            let i = (v % 3) as usize;
            put(&hosts[i], i as NodeId + 1, v);
        }
        let leader = hosts[0].status().unwrap().leader.unwrap();
        down.lock().unwrap().insert(leader);
        let alive: Vec<usize> = (0..3).filter(|i| *i as NodeId + 1 != leader).collect();
        for v in 30..40u64 {
            let i = alive[(v % 2) as usize];
            put(&hosts[i], i as NodeId + 1, v);
        }
        // a read on a follower sees what was written through another
        let applied = hosts[alive[1]]
            .read_barrier(Duration::from_secs(10))
            .unwrap();
        assert!(applied >= 40);
        // and the other one, before its list is taken
        hosts[alive[0]]
            .read_barrier(Duration::from_secs(10))
            .unwrap();
        let want: Vec<u64> = machines[alive[0]].values.lock().unwrap().clone();
        assert!((0..40).all(|v| want.contains(&v)), "{want:?}");
        assert_eq!(*machines[alive[1]].values.lock().unwrap(), want);
        down.lock().unwrap().clear();
        let lagging = (leader - 1) as usize;
        for _ in 0..100 {
            if machines[lagging].values.lock().unwrap().len() == want.len() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(*machines[lagging].values.lock().unwrap(), want);
    }

    // the log is cut behind what was applied, and the cluster goes on
    #[test]
    fn the_log_is_cut_behind_what_was_applied() {
        let (hosts, machines, _) = cluster_with(Compaction { after: 20, keep: 5 });
        for v in 0..80u64 {
            put(&hosts[(v % 3) as usize], (v % 3) + 1, v);
        }
        for h in &hosts {
            h.read_barrier(Duration::from_secs(10)).unwrap();
            assert!(h.status().unwrap().log_start > 20);
        }
        let want = machines[0].values.lock().unwrap().clone();
        assert!((0..80).all(|v| want.contains(&v)));
        for m in &machines {
            assert_eq!(*m.values.lock().unwrap(), want);
        }
    }

    // a follower away for longer than the log reaches back is brought up
    // by a snapshot of the leader's state
    #[test]
    fn a_follower_too_far_behind_gets_a_snapshot() {
        let (hosts, machines, down) = cluster_with(Compaction { after: 20, keep: 5 });
        put(&hosts[0], 1, 0);
        let leader = hosts[0].status().unwrap().leader.unwrap();
        let away = (leader % 3) as usize; // a follower
        down.lock().unwrap().insert(away as NodeId + 1);
        let up: Vec<usize> = (0..3).filter(|i| *i != away).collect();
        for v in 1..100u64 {
            let i = up[(v % 2) as usize];
            put(&hosts[i], i as NodeId + 1, v);
        }
        assert!(hosts[up[0]].status().unwrap().log_start > 10);
        down.lock().unwrap().clear();
        hosts[up[0]].read_barrier(Duration::from_secs(10)).unwrap();
        let want = machines[up[0]].values.lock().unwrap().clone();
        for _ in 0..100 {
            if *machines[away].values.lock().unwrap() == want {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(*machines[away].values.lock().unwrap(), want);
        // and it goes on from there
        put(&hosts[away], away as NodeId + 1, 1000);
        hosts[up[0]].read_barrier(Duration::from_secs(10)).unwrap();
        assert_eq!(machines[away].values.lock().unwrap().last(), Some(&1000));
    }
}
