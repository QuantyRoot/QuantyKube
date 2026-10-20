//! The measuring instrument for Raft (ADR-008): several nodes in one
//! process against a simulated network and simulated disks, with time,
//! delays, loss, duplication, partitions and crashes all drawn from one
//! seed, so that any failure is reproduced exactly from its seed.
//!
//! It checks what Raft promises: at most one leader per term, every node
//! applies the same entry at the same index, and an acknowledged command
//! is never lost. It must find the mistakes planted in [`Bug`] and
//! [`HostBug`] before the protocol counts as tested by it.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostBug {
    None,
    /// Sends messages before the disk has the votes and entries they
    /// promise, so a crash in between forgets them.
    SendBeforePersist,
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
    /// a leader that dies right after it commits. (tick, node, crash)
    aimed: Vec<(u64, usize, bool)>,
    faults: bool,
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
            faults: true,
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
        let config = Config {
            max_batch: self.max_batch,
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
                        let crash = self.random().is_multiple_of(2);
                        self.aimed.push((at, i, crash));
                    }
                }
            }
            if self.faults && !r.committed.is_empty() && commit_before > 0 && self.chance(50) {
                let at = self.now + self.random() % 2;
                self.aimed.push((at, i, true));
            }
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
            let delay = 1 + self.random() % 4;
            self.net.push((self.now + delay, m));
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
        for (_, i, crash) in due {
            if !faults {
                continue;
            }
            if crash {
                self.crash(i);
            } else {
                self.group = vec![0; n];
                self.group[i] = 1;
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
        if propose && self.chance(300) {
            let i = (self.random() % n as u64) as usize;
            self.next_value += 1;
            let data = self.next_value.to_be_bytes().to_vec();
            if let Some(node) = self.nodes[i].as_mut() {
                if let Some((index, term)) = node.propose(data.clone()) {
                    self.pending.push((i, index, term, data));
                }
            }
            self.ready(i)?;
        }
        for i in 0..n {
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
            if self.group[from] != self.group[to] || (faults && self.chance(self.rates[3])) {
                continue;
            }
            let twice = faults && self.chance(10);
            if twice {
                let delay = 1 + self.random() % 4;
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
        for i in 0..n {
            if self.nodes[i].is_none() {
                let node = self.boot(i);
                self.nodes[i] = Some(node);
            }
        }
        for _ in 0..300 {
            self.step(false, false)?;
        }
        let before = self.acked.len();
        for _ in 0..300 {
            self.step(true, false)?;
        }
        if self.acked.len() == before {
            return Err("a healed cluster acknowledged nothing in 300 ticks".into());
        }
        for _ in 0..100 {
            self.step(false, false)?;
        }
        for (index, term, data) in &self.acked {
            if self.applied.get(index) != Some(&(*term, data.clone())) {
                return Err(format!(
                    "acknowledged entry {index} of term {term} was lost"
                ));
            }
        }
        let top = self.acked.iter().map(|a| a.0).max().unwrap_or(0);
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
