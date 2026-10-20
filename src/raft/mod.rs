//! Raft, as a state machine without I/O (ADR-027, ADR-032).
//!
//! A [`Node`] is driven by its host: `tick` on a clock, `step` with each
//! message that arrives, `propose` with a command. It never touches a disk
//! or a network itself. After each call the host takes a [`Ready`] and
//! must, in this order: install the `snapshot` it names, make its
//! `hard_state` and `entries` durable, then send its `messages`, then
//! apply its `committed` entries. Sending before
//! the disk has them is what loses votes and acknowledged writes, so the
//! order is the contract.
//!
//! What it does: leader election with pre-vote, so a node cut off from the
//! others cannot force elections when it comes back; check quorum, so a
//! leader that cannot reach a majority steps down; log replication with
//! fast backtracking; linearizable reads through ReadIndex; a log the host
//! compacts once its state holds what the entries said, and snapshots for
//! followers too far behind for the log. Membership is fixed at start.

use std::collections::{BTreeMap, BTreeSet};

pub mod codec;
pub mod host;
#[cfg(test)]
mod sim;
pub mod transport;

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    /// Empty for the entry a new leader appends to commit its term.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    pub term: u64,
    pub vote: Option<NodeId>,
    pub commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Vote {
        pre: bool,
        last_index: u64,
        last_term: u64,
    },
    VoteReply {
        pre: bool,
        granted: bool,
    },
    Append {
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    },
    AppendReply {
        /// On success the last index the follower now matches; on a
        /// rejection where the leader should look again from.
        index: u64,
        success: bool,
    },
    /// A follower asks the leader for a read index on a client's behalf.
    ReadIndex {
        req: u64,
    },
    ReadIndexReply {
        req: u64,
        index: u64,
    },
    /// The leader asks whether it still leads, for the reads up to `seq`.
    ReadCheck {
        seq: u64,
    },
    ReadCheckReply {
        seq: u64,
    },
    /// The leader's log no longer holds what the follower needs: it is to
    /// take the leader's state as of `index`. The host carries the state
    /// itself alongside, and steps this message once it is installed; the
    /// answer is an `AppendReply`.
    Snapshot {
        index: u64,
        term: u64,
    },
    /// A command a follower received, for its leader to append. The
    /// follower learns of it by applying it like any other entry.
    Propose {
        data: Vec<u8>,
    },
}

/// Where the log starts: everything up to `index` is in the host's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
}

/// A read the host may serve once it has applied up to `index`: by then
/// its state includes every write acknowledged before the read began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadState {
    pub req: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    /// For a pre-vote request, the term the candidate would campaign in.
    pub term: u64,
    pub body: Body,
}

/// What the host must do after a call, in field order.
#[derive(Debug, Default)]
pub struct Ready {
    /// The snapshot this node took from its leader: the host's state is
    /// now that one, and its log starts after it.
    pub snapshot: Option<SnapshotMeta>,
    pub hard_state: Option<HardState>,
    /// Entries to write, replacing whatever the log holds from the first
    /// one's index on.
    pub entries: Vec<Entry>,
    pub messages: Vec<Message>,
    pub committed: Vec<Entry>,
    pub reads: Vec<ReadState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Ticks without hearing from a leader before a node campaigns; the
    /// actual timeout is drawn from [n, 2n) so that nodes rarely tie.
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    /// Entries per append message.
    pub max_batch: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 1,
            max_batch: 256,
        }
    }
}

/// Mistakes a Raft can make, planted on purpose so that the simulator
/// shows it finds them (ADR-008).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    None,
    /// Grants a second vote in the same term.
    VoteTwice,
    /// Grants votes to candidates whose log is behind.
    IgnoreLogInVote,
    /// Commits entries of an earlier term by counting replicas.
    CommitOldTerms,
    /// Keeps entries that conflict with the leader's instead of dropping
    /// them.
    KeepConflicts,
    /// Answers a read with its commit index without asking whether it
    /// still leads.
    ReadWithoutQuorum,
    /// Answers reads before it has committed an entry of its own term,
    /// when its commit index may be behind its predecessor's.
    ReadBeforeOwnCommit,
    /// Puts entries in a snapshot that are not applied yet, so that a
    /// follower takes in what may never be committed.
    CompactPastApplied,
}

struct PendingRead {
    seq: u64,
    index: u64,
    origin: NodeId,
    req: u64,
    acks: BTreeSet<NodeId>,
}

struct Progress {
    next: u64,
    matched: u64,
    /// Heard from since the last quorum check.
    active: bool,
}

pub struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    config: Config,
    term: u64,
    vote: Option<NodeId>,
    /// The log after the snapshot; index i is `log[i - snap.index - 1]`.
    log: Vec<Entry>,
    snap: SnapshotMeta,
    commit: u64,
    applied: u64,
    role: Role,
    leader: Option<NodeId>,
    elapsed: u32,
    timeout: u32,
    heartbeat: u32,
    votes: BTreeMap<NodeId, bool>,
    progress: BTreeMap<NodeId, Progress>,
    rng: u64,
    ready: Ready,
    /// The lowest log index changed since the last `Ready`.
    dirty_from: Option<u64>,
    hard_dirty: bool,
    read_seq: u64,
    reads: Vec<PendingRead>,
    /// Reads that came before this leader committed an entry of its own
    /// term, when its commit index might still be behind.
    early_reads: Vec<(NodeId, u64)>,
    #[cfg(test)]
    pub bug: Bug,
}

impl Node {
    /// A node of `peers` (itself included) starting from what its disk
    /// holds: nothing for a new cluster.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        peers: &[NodeId],
        config: Config,
        hard: HardState,
        snap: SnapshotMeta,
        log: Vec<Entry>,
        applied: u64,
        seed: u64,
    ) -> Node {
        let mut n = Node {
            id,
            peers: peers.to_vec(),
            config,
            term: hard.term,
            vote: hard.vote,
            log,
            snap,
            commit: hard.commit.max(snap.index),
            applied: applied.max(snap.index),
            role: Role::Follower,
            leader: None,
            elapsed: 0,
            timeout: 0,
            heartbeat: 0,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            rng: seed | 1,
            ready: Ready::default(),
            dirty_from: None,
            hard_dirty: false,
            read_seq: 0,
            reads: Vec::new(),
            early_reads: Vec::new(),
            #[cfg(test)]
            bug: Bug::None,
        };
        n.reset_timeout();
        n
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn last_index(&self) -> u64 {
        self.snap.index + self.log.len() as u64
    }

    /// The position of `index` in `log`; `index` is after the snapshot.
    fn at(&self, index: u64) -> usize {
        (index - self.snap.index - 1) as usize
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snap.index {
            return Some(self.snap.term);
        }
        if index < self.snap.index {
            return None;
        }
        self.log.get(self.at(index)).map(|e| e.term)
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(self.snap.term, |e| e.term)
    }

    /// Drops the log up to `index`, which the host's state now holds; the
    /// host keeps that state as the snapshot it sends followers that need
    /// more than the log has.
    pub fn compact(&mut self, index: u64) {
        #[cfg(test)]
        let index = if self.bug == Bug::CompactPastApplied {
            self.last_index()
        } else {
            index
        };
        #[cfg(test)]
        let applied = if self.bug == Bug::CompactPastApplied {
            self.last_index()
        } else {
            self.applied
        };
        #[cfg(not(test))]
        let applied = self.applied;
        if index <= self.snap.index || index > applied {
            return;
        }
        let term = self.term_at(index).expect("applied entries are in the log");
        let drop = self.at(index) + 1;
        self.log.drain(..drop);
        self.snap = SnapshotMeta { index, term };
    }

    pub fn snapshot(&self) -> SnapshotMeta {
        self.snap
    }

    fn quorum(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    fn random(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn reset_timeout(&mut self) {
        self.elapsed = 0;
        let n = u64::from(self.config.election_ticks);
        self.timeout = (n + self.random() % n) as u32;
    }

    fn send(&mut self, to: NodeId, term: u64, body: Body) {
        self.ready.messages.push(Message {
            from: self.id,
            to,
            term,
            body,
        });
    }

    fn others(&self) -> Vec<NodeId> {
        self.peers
            .iter()
            .copied()
            .filter(|p| *p != self.id)
            .collect()
    }

    fn set_term(&mut self, term: u64) {
        if term != self.term {
            self.term = term;
            self.vote = None;
            self.hard_dirty = true;
        }
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        self.set_term(term);
        self.role = Role::Follower;
        self.leader = leader;
        self.reads.clear();
        self.early_reads.clear();
        self.reset_timeout();
    }

    /// Asks for a read index for the host's request `req`; it comes back
    /// in `Ready::reads`, or not at all if leadership changes first, and
    /// then the host asks again.
    pub fn read_index(&mut self, req: u64) {
        match (self.role, self.leader) {
            (Role::Leader, _) => {
                let id = self.id;
                self.leader_read(id, req);
            }
            (_, Some(leader)) => {
                let term = self.term;
                self.send(leader, term, Body::ReadIndex { req });
            }
            _ => {}
        }
    }

    fn leader_read(&mut self, origin: NodeId, req: u64) {
        let own = self.term_at(self.commit) == Some(self.term);
        #[cfg(test)]
        let own = own || self.bug == Bug::ReadBeforeOwnCommit;
        if !own {
            self.early_reads.push((origin, req));
            return;
        }
        let index = self.commit;
        #[cfg(test)]
        if self.bug == Bug::ReadWithoutQuorum {
            return self.read_done(origin, req, index);
        }
        if self.peers.len() == 1 {
            return self.read_done(origin, req, index);
        }
        self.read_seq += 1;
        let seq = self.read_seq;
        self.reads.push(PendingRead {
            seq,
            index,
            origin,
            req,
            acks: BTreeSet::from([self.id]),
        });
        let term = self.term;
        for p in self.others() {
            self.send(p, term, Body::ReadCheck { seq });
        }
    }

    fn read_done(&mut self, origin: NodeId, req: u64, index: u64) {
        if origin == self.id {
            self.ready.reads.push(ReadState { req, index });
        } else {
            let term = self.term;
            self.send(origin, term, Body::ReadIndexReply { req, index });
        }
    }

    fn truncate_and_append(&mut self, from: u64, entries: &[Entry]) {
        let keep = self.at(from);
        self.log.truncate(keep);
        self.log.extend_from_slice(entries);
        self.dirty_from = Some(self.dirty_from.map_or(from, |d| d.min(from)));
    }

    /// Hands over what the host must do now.
    pub fn take_ready(&mut self) -> Ready {
        let mut r = std::mem::take(&mut self.ready);
        if self.hard_dirty {
            r.hard_state = Some(HardState {
                term: self.term,
                vote: self.vote,
                commit: self.commit,
            });
            self.hard_dirty = false;
        }
        if let Some(from) = self.dirty_from.take() {
            r.entries = self.log[self.at(from)..].to_vec();
        }
        if self.commit > self.applied {
            r.committed = self.log[self.at(self.applied + 1)..=self.at(self.commit)].to_vec();
            self.applied = self.commit;
        }
        r
    }

    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat += 1;
                self.elapsed += 1;
                if self.heartbeat >= self.config.heartbeat_ticks {
                    self.heartbeat = 0;
                    for p in self.others() {
                        self.replicate(p);
                    }
                }
                // check quorum: a leader cut off from a majority steps
                // down rather than serve a stale view
                if self.elapsed >= self.config.election_ticks {
                    self.elapsed = 0;
                    let id = self.id;
                    let active = self
                        .progress
                        .iter()
                        .filter(|(p, pr)| **p == id || pr.active)
                        .count();
                    for pr in self.progress.values_mut() {
                        pr.active = false;
                    }
                    if active < self.quorum() {
                        let term = self.term;
                        self.become_follower(term, None);
                    }
                }
            }
            _ => {
                self.elapsed += 1;
                if self.elapsed >= self.timeout {
                    self.campaign(true);
                }
            }
        }
    }

    fn campaign(&mut self, pre: bool) {
        self.reset_timeout();
        self.votes.clear();
        self.votes.insert(self.id, true);
        let term = if pre {
            self.role = Role::PreCandidate;
            self.term + 1
        } else {
            self.role = Role::Candidate;
            let t = self.term + 1;
            self.set_term(t);
            self.vote = Some(self.id);
            self.hard_dirty = true;
            t
        };
        self.leader = None;
        if self.peers.len() == 1 {
            return self.won(pre);
        }
        let (last_index, last_term) = (self.last_index(), self.last_term());
        for p in self.others() {
            self.send(
                p,
                term,
                Body::Vote {
                    pre,
                    last_index,
                    last_term,
                },
            );
        }
    }

    fn won(&mut self, pre: bool) {
        if pre {
            return self.campaign(false);
        }
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.elapsed = 0;
        self.heartbeat = 0;
        let next = self.last_index() + 1;
        self.progress = self
            .peers
            .iter()
            .map(|p| {
                (
                    *p,
                    Progress {
                        next,
                        matched: 0,
                        active: true,
                    },
                )
            })
            .collect();
        // an entry of its own term, so that it can commit what earlier
        // leaders left (Raft 5.4.2)
        self.append(Vec::new());
    }

    fn append(&mut self, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        let e = Entry {
            term: self.term,
            index,
            data,
        };
        self.truncate_and_append(index, &[e]);
        let id = self.id;
        if let Some(pr) = self.progress.get_mut(&id) {
            pr.matched = index;
            pr.next = index + 1;
        }
        for p in self.others() {
            self.replicate(p);
        }
        self.advance_commit();
        index
    }

    /// Appends `data` if this node leads, and returns the index and term
    /// it was given; it is committed once an entry with that index and
    /// term is in `committed`, and lost if another one is. A follower
    /// passes it to its leader and returns None; the host recognizes the
    /// command when it applies it, and gives up on it after a while.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<(u64, u64)> {
        match (self.role, self.leader) {
            (Role::Leader, _) => {
                let i = self.append(data);
                Some((i, self.term))
            }
            (_, Some(leader)) => {
                let term = self.term;
                self.send(leader, term, Body::Propose { data });
                None
            }
            _ => None,
        }
    }

    fn replicate(&mut self, to: NodeId) {
        let Some(pr) = self.progress.get(&to) else {
            return;
        };
        let next = pr.next.max(1);
        if next <= self.snap.index {
            // the log no longer reaches back that far
            let (term, snap) = (self.term, self.snap);
            return self.send(
                to,
                term,
                Body::Snapshot {
                    index: snap.index,
                    term: snap.term,
                },
            );
        }
        let prev_index = next - 1;
        let prev_term = self.term_at(prev_index).unwrap_or(0);
        let end = self
            .last_index()
            .min(prev_index + self.config.max_batch as u64);
        let entries = if end < next {
            Vec::new()
        } else {
            self.log[self.at(next)..self.at(end) + 1].to_vec()
        };
        let (term, commit) = (self.term, self.commit);
        self.send(
            to,
            term,
            Body::Append {
                prev_index,
                prev_term,
                entries,
                commit,
            },
        );
    }

    fn advance_commit(&mut self) {
        let mut matched: Vec<u64> = self.progress.values().map(|p| p.matched).collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.quorum() - 1];
        if n <= self.commit {
            return;
        }
        let current = self.term_at(n) == Some(self.term);
        #[cfg(test)]
        let current = current || self.bug == Bug::CommitOldTerms;
        // only an entry of the leader's own term is committed by counting;
        // earlier ones follow it (Raft 5.4.2)
        if current {
            self.commit = n;
            self.hard_dirty = true;
            for (origin, req) in std::mem::take(&mut self.early_reads) {
                self.leader_read(origin, req);
            }
        }
    }

    fn log_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        last_term > self.last_term()
            || (last_term == self.last_term() && last_index >= self.last_index())
    }

    /// True while this node has heard from a leader within the minimum
    /// election timeout: it then refuses to help unseat that leader.
    fn in_lease(&self) -> bool {
        self.leader.is_some() && self.elapsed < self.config.election_ticks
    }

    pub fn step(&mut self, m: Message) {
        if !self.peers.contains(&m.from) || m.to != self.id {
            return;
        }
        if m.term > self.term {
            match &m.body {
                Body::Vote { .. } if self.in_lease() => return,
                // a pre-vote does not change anyone's term
                Body::Vote { pre: true, .. } => {}
                Body::VoteReply {
                    pre: true,
                    granted: true,
                } => {}
                Body::Append { .. } | Body::ReadCheck { .. } | Body::Snapshot { .. } => {
                    self.become_follower(m.term, Some(m.from))
                }
                _ => self.become_follower(m.term, None),
            }
        } else if m.term < self.term {
            match m.body {
                // a stale leader learns the term and steps down
                Body::Append { .. } | Body::ReadCheck { .. } | Body::Snapshot { .. } => {
                    let term = self.term;
                    self.send(
                        m.from,
                        term,
                        Body::AppendReply {
                            index: 0,
                            success: false,
                        },
                    );
                }
                Body::Vote { pre, .. } => {
                    let term = self.term;
                    self.send(
                        m.from,
                        term,
                        Body::VoteReply {
                            pre,
                            granted: false,
                        },
                    );
                }
                _ => {}
            }
            return;
        }
        match m.body {
            Body::Vote {
                pre,
                last_index,
                last_term,
            } => {
                let free = if pre {
                    m.term > self.term
                } else {
                    self.vote.is_none() || self.vote == Some(m.from)
                };
                #[cfg(test)]
                let free = free || self.bug == Bug::VoteTwice;
                let current = self.log_up_to_date(last_index, last_term);
                #[cfg(test)]
                let current = current || self.bug == Bug::IgnoreLogInVote;
                let granted = free && current;
                if granted && !pre {
                    self.vote = Some(m.from);
                    self.hard_dirty = true;
                    self.reset_timeout();
                }
                self.send(m.from, m.term, Body::VoteReply { pre, granted });
            }
            Body::VoteReply { pre, granted } => {
                let campaigning = if pre {
                    self.role == Role::PreCandidate
                } else {
                    self.role == Role::Candidate
                };
                if !campaigning {
                    return;
                }
                self.votes.insert(m.from, granted);
                let yes = self.votes.values().filter(|v| **v).count();
                let no = self.votes.len() - yes;
                if yes >= self.quorum() {
                    self.won(pre);
                } else if no >= self.quorum() {
                    let term = self.term;
                    self.become_follower(term, None);
                }
            }
            Body::Append {
                prev_index,
                prev_term,
                entries,
                commit,
            } => {
                self.role = Role::Follower;
                self.leader = Some(m.from);
                self.reset_timeout();
                self.handle_append(m.from, prev_index, prev_term, entries, commit);
            }
            Body::Snapshot { index, term } => {
                self.role = Role::Follower;
                self.leader = Some(m.from);
                self.reset_timeout();
                self.install(m.from, index, term);
            }
            Body::Propose { data } => {
                if self.role == Role::Leader {
                    self.append(data);
                }
            }
            Body::ReadIndex { req } => {
                if self.role == Role::Leader {
                    self.leader_read(m.from, req);
                }
            }
            Body::ReadIndexReply { req, index } => {
                self.ready.reads.push(ReadState { req, index });
            }
            Body::ReadCheck { seq } => {
                self.role = Role::Follower;
                self.leader = Some(m.from);
                self.reset_timeout();
                let term = self.term;
                self.send(m.from, term, Body::ReadCheckReply { seq });
            }
            Body::ReadCheckReply { seq } => {
                if self.role != Role::Leader {
                    return;
                }
                if let Some(pr) = self.progress.get_mut(&m.from) {
                    pr.active = true;
                }
                let quorum = self.quorum();
                let mut done = Vec::new();
                self.reads.retain_mut(|r| {
                    if r.seq <= seq {
                        r.acks.insert(m.from);
                    }
                    if r.acks.len() >= quorum {
                        done.push((r.origin, r.req, r.index));
                        return false;
                    }
                    true
                });
                for (origin, req, index) in done {
                    self.read_done(origin, req, index);
                }
            }
            Body::AppendReply { index, success } => {
                // a follower cannot match more than this leader has
                if self.role != Role::Leader || index > self.last_index() {
                    return;
                }
                let Some(pr) = self.progress.get_mut(&m.from) else {
                    return;
                };
                pr.active = true;
                if success {
                    if index > pr.matched {
                        pr.matched = index;
                        pr.next = index + 1;
                        self.advance_commit();
                    }
                    if self.progress[&m.from].next <= self.last_index() {
                        self.replicate(m.from);
                    }
                } else {
                    // look again from where the follower says, never below
                    // what it is known to match
                    let next = (index + 1)
                        .min(pr.next.saturating_sub(1))
                        .max(pr.matched + 1);
                    pr.next = next.max(1);
                    self.replicate(m.from);
                }
            }
        }
    }

    fn install(&mut self, leader: NodeId, index: u64, term: u64) {
        let reply = self.term;
        if index <= self.commit {
            let commit = self.commit;
            return self.send(
                leader,
                reply,
                Body::AppendReply {
                    index: commit,
                    success: true,
                },
            );
        }
        if self.term_at(index) == Some(term) {
            // the log already has it: what follows may stay
            let drop = self.at(index) + 1;
            self.log.drain(..drop);
        } else {
            self.log.clear();
        }
        self.snap = SnapshotMeta { index, term };
        self.commit = index;
        self.applied = index;
        self.hard_dirty = true;
        self.ready.snapshot = Some(self.snap);
        // what was kept after the snapshot is written again from the start
        if !self.log.is_empty() {
            self.dirty_from = Some(index + 1);
        }
        self.send(
            leader,
            reply,
            Body::AppendReply {
                index,
                success: true,
            },
        );
    }

    fn handle_append(
        &mut self,
        leader: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    ) {
        let term = self.term;
        if prev_index < self.commit {
            // what is committed is never rewritten: only what follows it
            // is looked at
            let mine = self.commit;
            let skip = (mine - prev_index) as usize;
            if skip >= entries.len() {
                return self.send(
                    leader,
                    term,
                    Body::AppendReply {
                        index: mine,
                        success: true,
                    },
                );
            }
            let prev_term = entries[skip - 1].term;
            return self.handle_append(leader, mine, prev_term, entries[skip..].to_vec(), commit);
        }
        if self.term_at(prev_index) != Some(prev_term) {
            // the follower's log ends earlier or disagrees there: point the
            // leader at the first index of the disagreeing term
            let hint = if prev_index > self.last_index() {
                self.last_index()
            } else {
                let bad = self.term_at(prev_index).unwrap_or(0);
                let mut i = prev_index;
                while i > self.commit && self.term_at(i - 1) == Some(bad) {
                    i -= 1;
                }
                i - 1
            };
            return self.send(
                leader,
                term,
                Body::AppendReply {
                    index: hint,
                    success: false,
                },
            );
        }
        for (k, e) in entries.iter().enumerate() {
            match self.term_at(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    #[cfg(test)]
                    if self.bug == Bug::KeepConflicts {
                        let extra: Vec<Entry> = entries[k..]
                            .iter()
                            .filter(|x| x.index > self.last_index())
                            .cloned()
                            .collect();
                        if !extra.is_empty() {
                            let from = extra[0].index;
                            self.truncate_and_append(from, &extra);
                        }
                        break;
                    }
                    self.truncate_and_append(e.index, &entries[k..]);
                    break;
                }
                None => {
                    self.truncate_and_append(e.index, &entries[k..]);
                    break;
                }
            }
        }
        let last_new = prev_index + entries.len() as u64;
        let c = commit.min(last_new);
        if c > self.commit {
            self.commit = c;
            self.hard_dirty = true;
        }
        self.send(
            leader,
            term,
            Body::AppendReply {
                index: last_new,
                success: true,
            },
        );
    }
}
