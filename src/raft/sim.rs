//! The measuring instrument for Raft (ADR-008): several nodes in one
//! process against a simulated network and simulated disks, with time,
//! delays, loss, duplication, partitions and crashes all drawn from one
//! seed, so that any failure is reproduced exactly from its seed.
//!
//! It checks what Raft promises: at most one leader per term, every node
//! applies the same entry at the same index, an acknowledged command is
//! never lost, and a read sees every command acknowledged before it began. It must find the mistakes planted in [`Bug`] and
//! [`HostBug`] before the protocol counts as tested by it.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostBug {
    None,
    /// Sends messages before the disk has the votes and entries they
    /// promise, so a crash in between forgets them.
    SendBeforePersist,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Aimed {
    Crash,
    Isolate,
    Pause,
}

#[derive(Default, Clone)]
struct Disk {
    hard: HardState,
    log: Vec<Entry>,
}

struct Sim {
    rng: u64,
    ids: Vec<NodeId>,
    nodes: Vec<Option<Node>>,
    disks: Vec<Disk>,
    /// What a crash may still lose with HostBug::SendBeforePersist.
    unsynced: Vec<Option<(Option<HardState>, Vec<Entry>)>>,
    net: Vec<(u64, Message)>,
    group: Vec<u8>,
    now: u64,
    leaders: BTreeMap<u64, NodeId>,
    /// The entry each index was first applied as, anywhere.
    applied: BTreeMap<u64, (u64, Vec<u8>)>,
    /// How far each node has applied since it last started.
    applied_to: Vec<u64>,
    pending: Vec<(usize, u64, u64, Vec<u8>)>,
    acked: Vec<(u64, u64, Vec<u8>)>,
    next_value: u64,
    bug: Bug,
    host_bug: HostBug,
    /// Small batches, so that followers often hold only part of what the
    /// leader has, which is where commit rules go wrong.
    max_batch: usize,
    /// Per mille per tick: a crash, a new partition, a heal; per message:
    /// a loss. Drawn per seed, so that some runs are calm and some are
    /// violent, as the rare interleavings need.
    rates: [u64; 4],
    /// Faults aimed at the moments that matter, as FoundationDB's
    /// buggify does: a new leader cut off before it replicates anything,
    /// a leader dying right after it commits, a leader that stops for a
    /// while, as in a long pause of its process. (tick, node, fault)
    aimed: Vec<(u64, usize, Aimed)>,
    /// A paused node gets no ticks and loses what is sent to it.
    paused_until: Vec<u64>,
    faults: bool,
    /// Reads asked for and not yet answered: the highest index
    /// acknowledged when each began.
    reads: BTreeMap<u64, u64>,
    /// A client cut off with a leader keeps reading from it: (tick, node)
    stuck_reads: Vec<(u64, usize)>,
    next_read: u64,
    reads_served: u64,
}

impl Sim {
    fn new(seed: u64, n: usize, bug: Bug, host_bug: HostBug) -> Sim {
        let ids: Vec<NodeId> = (1..=n as u64).collect();
        let mut s = Sim {
            rng: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            ids: ids.clone(),
            nodes: Vec::new(),
            disks: vec![Disk::default(); n],
            unsynced: vec![None; n],
            net: Vec::new(),
            group: vec![0; n],
            now: 0,
            leaders: BTreeMap::new(),
            applied: BTreeMap::new(),
            applied_to: vec![0; n],
            pending: Vec::new(),
            acked: Vec::new(),
            next_value: 0,
            bug,
            host_bug,
            max_batch: 0,
            rates: [0; 4],
            aimed: Vec::new(),
            paused_until: vec![0; n],
            faults: true,
            reads: BTreeMap::new(),
            stuck_reads: Vec::new(),
            next_read: 0,
            reads_served: 0,
        };
        s.max_batch = 1 + (s.random() % 4) as usize;
        s.rates = match s.random() % 3 {
            0 => [2, 5, 20, 5],
            1 => [8, 15, 40, 20],
            _ => [30, 60, 60, 100],
        };
        for i in 0..n {
            let node = s.boot(i);
            s.nodes.push(Some(node));
        }
        s
    }

    fn random(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.random() % 1000 < per_mille
    }

    fn boot(&mut self, i: usize) -> Node {
        let d = self.disks[i].clone();
        let seed = self.random();
        // an election timeout well above the time a message takes, as
        // Raft needs to elect anyone at all
        let config = Config {
            max_batch: self.max_batch,
            election_ticks: 20,
            ..Config::default()
        };
        let mut n = Node::new(self.ids[i], &self.ids, config, d.hard, d.log, 0, seed);
        n.bug = self.bug;
        self.applied_to[i] = 0;
        n
    }

    fn index_of(&self, id: NodeId) -> usize {
        (id - 1) as usize
    }

    /// Does what the node's Ready asks, checking what Raft promises.
    fn ready(&mut self, i: usize) -> Result<(), String> {
        let commit_before = self.applied_to[i];
        let Some(node) = self.nodes[i].as_mut() else {
            return Ok(());
        };
        let r = node.take_ready();
        let (role, term, id) = (node.role(), node.term(), node.id());
        if role == Role::Leader {
            match self.leaders.get(&term) {
                Some(l) if *l != id => {
                    return Err(format!("two leaders in term {term}: {l} and {id}"));
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(term, id);
                    if self.faults && self.chance(300) {
                        let at = self.now + self.random() % 3;
                        let kind = if self.random().is_multiple_of(2) {
                            Aimed::Crash
                        } else {
                            Aimed::Isolate
                        };
                        self.aimed.push((at, i, kind));
                    } else if self.faults && self.chance(150) {
                        // later in its reign, cut off or stopped, with
                        // reads to answer
                        let at = self.now + 5 + self.random() % 40;
                        let kind = if self.random().is_multiple_of(2) {
                            Aimed::Pause
                        } else {
                            Aimed::Isolate
                        };
                        self.aimed.push((at, i, kind));
                    }
                }
            }
            if self.faults && !r.committed.is_empty() && commit_before > 0 && self.chance(150) {
                let at = self.now + self.random() % 2;
                self.aimed.push((at, i, Aimed::Crash));
            }
        }
        // a node that just voted or took entries dies before it acts on
        // them, which is when what it promised must be on its disk
        if self.faults && (r.hard_state.is_some() || !r.entries.is_empty()) && self.chance(30) {
            let at = self.now + 1;
            self.aimed.push((at, i, Aimed::Crash));
        }
        let persist = |disk: &mut Disk, hard: Option<HardState>, entries: &[Entry]| {
            if let Some(h) = hard {
                disk.hard = h;
            }
            if let Some(first) = entries.first() {
                disk.log.truncate(first.index as usize - 1);
                disk.log.extend_from_slice(entries);
            }
        };
        match self.host_bug {
            HostBug::None => persist(&mut self.disks[i], r.hard_state, &r.entries),
            HostBug::SendBeforePersist => {
                // what the last call left reaches the disk only now
                if let Some((h, e)) = self.unsynced[i].take() {
                    persist(&mut self.disks[i], h, &e);
                }
                if r.hard_state.is_some() || !r.entries.is_empty() {
                    self.unsynced[i] = Some((r.hard_state, r.entries.clone()));
                }
            }
        }
        for m in r.messages {
            let delay = 1 + self.random() % 3;
            self.net.push((self.now + delay, m));
        }
        for read in r.reads {
            if let Some(seen) = self.reads.remove(&read.req) {
                if read.index < seen {
                    return Err(format!(
                        "node {id} read at index {} after index {seen} was acknowledged",
                        read.index
                    ));
                }
                self.reads_served += 1;
            }
        }
        for e in r.committed {
            if e.index != self.applied_to[i] + 1 {
                return Err(format!(
                    "node {id} applied {} after {}",
                    e.index, self.applied_to[i]
                ));
            }
            self.applied_to[i] = e.index;
            match self.applied.get(&e.index) {
                Some((t, d)) if (*t, d) != (e.term, &e.data) => {
                    return Err(format!(
                        "node {id} applied index {} as term {}, elsewhere it was term {t}",
                        e.index, e.term
                    ));
                }
                Some(_) => {}
                None => {
                    self.applied.insert(e.index, (e.term, e.data.clone()));
                }
            }
            if let Some(k) = self.pending.iter().position(|p| p.0 == i && p.1 == e.index) {
                let (_, index, term, data) = self.pending.remove(k);
                if term == e.term && data == e.data {
                    self.acked.push((index, term, data));
                }
            }
        }
        Ok(())
    }

    fn crash(&mut self, i: usize) {
        self.nodes[i] = None;
        self.unsynced[i] = None;
        self.pending.retain(|p| p.0 != i);
    }

    fn step(&mut self, propose: bool, faults: bool) -> Result<(), String> {
        self.now += 1;
        self.faults = faults;
        let n = self.ids.len();
        let now = self.now;
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.aimed)
            .into_iter()
            .partition(|a| a.0 <= now);
        self.aimed = later;
        for (_, i, kind) in due {
            if !faults {
                continue;
            }
            if kind == Aimed::Pause {
                let until = now + 30 + self.random() % 30;
                self.paused_until[i] = until;
                for k in 0..10 {
                    self.stuck_reads.push((until + k, i));
                }
            } else if kind == Aimed::Crash {
                self.crash(i);
            } else {
                self.group = vec![0; n];
                self.group[i] = 1;
                // the others' timers happened to be about to run out, so
                // a new leader may come up while the old one still thinks
                // it leads, which is when stale reads would show
                for j in (0..n).filter(|j| *j != i) {
                    if let Some(node) = self.nodes[j].as_mut() {
                        node.elapsed = node.timeout.saturating_sub(1);
                    }
                }
                for k in 0..20 {
                    self.stuck_reads.push((now + 2 * k, i));
                }
            }
        }
        if faults {
            if self.chance(self.rates[0]) {
                let i = (self.random() % n as u64) as usize;
                self.crash(i);
            }
            for i in 0..n {
                if self.nodes[i].is_none() && self.chance(40) {
                    let node = self.boot(i);
                    self.nodes[i] = Some(node);
                }
            }
            if self.chance(10) {
                let leader = (0..n).find(|i| {
                    self.nodes[*i]
                        .as_ref()
                        .is_some_and(|x| x.role() == Role::Leader)
                });
                if let Some(i) = leader {
                    self.aimed.push((now, i, Aimed::Pause));
                }
            }
            if self.chance(self.rates[1]) {
                // one node cut off, or the cluster in up to three parts
                if self.random().is_multiple_of(2) {
                    let lone = (self.random() % n as u64) as usize;
                    self.group = vec![0; n];
                    self.group[lone] = 1;
                } else {
                    for i in 0..n {
                        self.group[i] = (self.random() % 3) as u8;
                    }
                }
            }
            if self.chance(self.rates[2]) {
                self.group = vec![0; n];
            }
        }
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.stuck_reads)
            .into_iter()
            .partition(|r| r.0 <= now);
        self.stuck_reads = later;
        for (_, i) in due {
            if !(propose && faults) {
                continue;
            }
            self.next_read += 1;
            let req = self.next_read;
            let seen = self.acked.iter().map(|a| a.0).max().unwrap_or(0);
            if let Some(node) = self.nodes[i]
                .as_mut()
                .filter(|_| self.paused_until[i] <= now)
            {
                node.read_index(req);
                self.reads.insert(req, seen);
            }
            self.ready(i)?;
        }
        if propose && self.chance(400) {
            // a client stays with the node it knows, which may be a leader
            // that was cut off from the others with it
            let leaders: Vec<usize> = (0..n)
                .filter(|i| {
                    self.paused_until[*i] <= now
                        && self.nodes[*i]
                            .as_ref()
                            .is_some_and(|x| x.role() == Role::Leader)
                })
                .collect();
            let i = match leaders.len() {
                k if k > 0 && self.random().is_multiple_of(2) => {
                    leaders[(self.random() % k as u64) as usize]
                }
                _ => (self.random() % n as u64) as usize,
            };
            self.next_read += 1;
            let req = self.next_read;
            let seen = self.acked.iter().map(|a| a.0).max().unwrap_or(0);
            if let Some(node) = self.nodes[i]
                .as_mut()
                .filter(|_| self.paused_until[i] <= now)
            {
                node.read_index(req);
                self.reads.insert(req, seen);
            }
            self.ready(i)?;
        }
        if propose && self.chance(400) {
            // clients find a leader, as followers forward to theirs; with
            // two that both think they lead, either may get the command
            let leaders: Vec<usize> = (0..n)
                .filter(|i| {
                    self.paused_until[*i] <= now
                        && self.nodes[*i]
                            .as_ref()
                            .is_some_and(|x| x.role() == Role::Leader)
                })
                .collect();
            let i = match leaders.len() {
                0 => (self.random() % n as u64) as usize,
                k => leaders[(self.random() % k as u64) as usize],
            };
            self.next_value += 1;
            let data = self.next_value.to_be_bytes().to_vec();
            if let Some(node) = self.nodes[i]
                .as_mut()
                .filter(|_| self.paused_until[i] <= now)
            {
                if let Some((index, term)) = node.propose(data.clone()) {
                    self.pending.push((i, index, term, data));
                }
            }
            self.ready(i)?;
        }
        for i in 0..n {
            if self.paused_until[i] > now {
                continue;
            }
            if let Some(node) = self.nodes[i].as_mut() {
                node.tick();
            }
            self.ready(i)?;
        }
        let mut due = Vec::new();
        let mut later = Vec::new();
        for (at, m) in std::mem::take(&mut self.net) {
            if at <= self.now {
                due.push(m);
            } else {
                later.push((at, m));
            }
        }
        self.net = later;
        // shuffle what arrives in the same tick
        for k in (1..due.len()).rev() {
            let j = (self.random() % (k as u64 + 1)) as usize;
            due.swap(k, j);
        }
        for m in due {
            let (from, to) = (self.index_of(m.from), self.index_of(m.to));
            if self.group[from] != self.group[to]
                || self.paused_until[to] > now
                || (faults && self.chance(self.rates[3]))
            {
                continue;
            }
            let twice = faults && self.chance(10);
            if twice {
                let delay = 1 + self.random() % 3;
                self.net.push((self.now + delay, m.clone()));
            }
            if let Some(node) = self.nodes[to].as_mut() {
                node.step(m);
            }
            self.ready(to)?;
        }
        Ok(())
    }

    /// Faults for `ticks`, then a healed cluster that must make progress
    /// and agree on everything that was acknowledged.
    fn run(&mut self, ticks: u64) -> Result<(), String> {
        for _ in 0..ticks {
            self.step(true, true)?;
        }
        let n = self.ids.len();
        self.group = vec![0; n];
        self.paused_until = vec![0; n];
        for i in 0..n {
            if self.nodes[i].is_none() {
                let node = self.boot(i);
                self.nodes[i] = Some(node);
            }
        }
        for _ in 0..300 {
            self.step(false, false)?;
        }
        // a long log behind a small batch takes a while to catch up
        let before = self.acked.len();
        for _ in 0..1500 {
            self.step(true, false)?;
            if self.acked.len() > before + 10 {
                break;
            }
        }
        if self.acked.len() <= before {
            return Err("a healed cluster acknowledged nothing in 1500 ticks".into());
        }
        if self.reads_served == 0 {
            return Err("no read was ever answered".into());
        }
        let top = self.acked.iter().map(|a| a.0).max().unwrap_or(0);
        for _ in 0..2000 {
            if self.applied_to.iter().all(|a| *a >= top) {
                break;
            }
            self.step(false, false)?;
        }
        for (index, term, data) in &self.acked {
            if self.applied.get(index) != Some(&(*term, data.clone())) {
                return Err(format!(
                    "acknowledged entry {index} of term {term} was lost"
                ));
            }
        }
        for i in 0..n {
            if self.applied_to[i] < top {
                return Err(format!(
                    "node {} applied {} of {top} after healing",
                    self.ids[i], self.applied_to[i]
                ));
            }
        }
        Ok(())
    }
}

fn run(seed: u64, nodes: usize, bug: Bug, host_bug: HostBug) -> Result<(), String> {
    Sim::new(seed, nodes, bug, host_bug).run(2000)
}

fn seeds() -> u64 {
    std::env::var("QUANTY_RAFT_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
}

#[test]
fn raft_keeps_its_promises_under_faults() {
    for nodes in [3, 5] {
        for seed in 0..seeds() {
            if let Err(e) = run(seed, nodes, Bug::None, HostBug::None) {
                panic!("{nodes} nodes, seed {seed}: {e}");
            }
        }
    }
}

#[test]
fn one_node_is_a_cluster_too() {
    run(1, 1, Bug::None, HostBug::None).unwrap();
}

/// The simulator counts only if it catches each planted mistake.
#[test]
fn the_simulator_finds_planted_bugs() {
    let planted = [
        (Bug::VoteTwice, HostBug::None),
        (Bug::IgnoreLogInVote, HostBug::None),
        (Bug::CommitOldTerms, HostBug::None),
        (Bug::KeepConflicts, HostBug::None),
        (Bug::ReadWithoutQuorum, HostBug::None),
        (Bug::ReadBeforeOwnCommit, HostBug::None),
        (Bug::None, HostBug::SendBeforePersist),
    ];
    for (bug, host_bug) in planted {
        let found = (0..300).find_map(|seed| {
            run(seed, 3, bug, host_bug)
                .err()
                .or_else(|| run(seed, 5, bug, host_bug).err())
                .map(|e| (seed, e))
        });
        match found {
            Some((seed, e)) => eprintln!("{bug:?} {host_bug:?}: seed {seed}: {e}"),
            None => panic!("{bug:?} {host_bug:?} went unnoticed in 300 seeds"),
        }
    }
}
