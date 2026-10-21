//! Versioned key value storage under the etcd layer (ADR-006).
//!
//! A revision is a QuantyDB commit id, one for one: every write commits,
//! every commit is a write. Revision 1 is the empty store, committed when
//! the file is created, so that reading at any revision a client was ever
//! told about finds a commit.
//!
//! Three keyspaces share the data tree, told apart by the first byte:
//! `k` holds the live keys, `e` the change log a watch replays from, and
//! `m` the store's own bookkeeping. The change log keeps both sides of
//! every change, because rebuilding them from snapshots would walk the
//! commit chain once per event.

use quantydb_core::{Db, FileStorage, MemStorage, Storage, SuspendedTx};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, RwLock};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub create_rev: i64,
    pub mod_rev: i64,
    pub version: i64,
    pub lease: i64,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Put,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub kind: Kind,
    /// For a delete, the key and the deleting revision, the rest zero, as
    /// etcd reports it.
    pub kv: Record,
    pub prev: Option<Record>,
}

#[derive(Debug)]
pub enum Error {
    Compacted(i64),
    FutureRevision,
    KeyTooLong,
    Engine(quantydb_core::Error),
    Corrupt(&'static str),
    /// The caller's transaction gave up; nothing was committed.
    Aborted,
    /// The group commit this write was part of failed.
    Commit(String),
    /// The revision is older than the retained history.
    NotRetained(i64),
}

impl From<quantydb_core::Error> for Error {
    fn from(e: quantydb_core::Error) -> Self {
        Error::Engine(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // the API server matches on these two texts
            Error::Compacted(_) => f.write_str("mvcc: required revision has been compacted"),
            Error::FutureRevision => f.write_str("mvcc: required revision is a future revision"),
            Error::KeyTooLong => f.write_str("key is too long"),
            Error::Engine(e) => write!(f, "storage: {e}"),
            Error::Corrupt(what) => write!(f, "storage: corrupt {what}"),
            Error::Aborted => f.write_str("transaction aborted"),
            Error::Commit(e) => write!(f, "storage: commit failed: {e}"),
            Error::NotRetained(from) => {
                write!(
                    f,
                    "revision is no longer retained, history starts at {from}"
                )
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait View {
    fn get(&self, key: &[u8]) -> Result<Option<Record>>;
    /// Visits `[start, end)` in key order, `end` None meaning no bound,
    /// until `f` returns false.
    fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut dyn FnMut(Record) -> bool,
    ) -> Result<()>;
}

pub trait Tx: View {
    /// The revision this transaction commits as, if it writes.
    fn revision(&self) -> i64;
    fn put(&mut self, key: &[u8], value: &[u8], lease: i64) -> Result<Option<Record>>;
    fn delete(&mut self, key: &[u8]) -> Result<Option<Record>>;
    fn meta_get(&self, name: &[u8]) -> Result<Option<Vec<u8>>>;
    fn meta_put(&mut self, name: &[u8], value: &[u8]) -> Result<()>;
    fn meta_delete(&mut self, name: &[u8]) -> Result<()>;
    fn meta_scan(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<()>;
}

pub type Publisher = Box<dyn Fn(i64, &[Change]) + Send + Sync>;

pub trait Backend: Send + Sync + 'static {
    fn head(&self) -> i64;
    fn compacted(&self) -> i64;
    /// `rev` 0 reads the newest state. Returns the revision read at, which
    /// for `rev` 0 is the head when the view was taken, not the head now.
    fn read(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<i64>;
    /// Runs `f` as one atomic write. If it changed a key, it gets the next
    /// revision; once that is durable, the publisher has seen it, in
    /// revision order, and this returns. `f` failing changes nothing.
    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
    ) -> Result<Option<(i64, Vec<Change>)>>;
    /// Called with every committed revision and its changes, in order.
    fn set_publisher(&self, p: Publisher);
    /// Replays the change log for revisions in `[from, to]` until `f`
    /// returns false.
    fn changes(&self, from: i64, to: i64, f: &mut dyn FnMut(i64, Change) -> bool) -> Result<()>;
    /// Marks everything before `rev` as compacted for etcd clients. What
    /// is physically kept is the retention policy's business, not this.
    fn compact(&self, rev: i64) -> Result<()>;
    /// Bytes on disk, and of those the bytes in use.
    fn size(&self) -> Result<(i64, i64)>;
    /// Reads at any retained revision, compacted or not (ADR-011).
    fn read_retained(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<()>;
    /// The newest revision committed at or before `unix_ms`.
    fn revision_at(&self, unix_ms: i64) -> Result<i64>;
    /// Every retained change to keys in `[start, end)`, oldest first;
    /// `end` None means the single key `start`.
    fn history(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<HistoryEntry>>;
    /// The live keys and the metadata as of the head, without history:
    /// what a store node that fell too far behind takes from its leader.
    fn export(&self) -> Result<Vec<u8>>;
    /// Replaces everything with what `export` gave. History before it is
    /// gone on this node, and reads before its head are refused as
    /// compacted.
    fn import(&self, data: &[u8]) -> Result<()>;
}

const LIVE: u8 = b'k';
const LOG: u8 = b'e';
const META: u8 = b'm';
const COMPACTED: &[u8] = b"compacted";
const RETAINED_FROM: &[u8] = b"retained";
const COMMITS: &[u8] = b"c/";

/// QuantyDB keys are capped at an eighth of a page, 512 bytes at the
/// default size, and every key here carries a one byte prefix.
pub const MAX_KEY: usize = 511;

#[derive(Debug, Clone, Copy)]
struct CommitEntry {
    last_rev: i64,
    id: u64,
    unix_ms: i64,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Writes gathered into one QuantyDB commit (ADR-012). Writes that arrive
/// while a commit is on its way to disk wait, apply one after another into
/// a new transaction, and the last of them commits it for all.
struct Batch {
    tx: Option<SuspendedTx>,
    /// writes in `tx` that changed something, keys or metadata
    members: usize,
    changes: Vec<(i64, Vec<Change>)>,
    next_rev: i64,
    epoch: u64,
    committed: u64,
    /// The first commit that failed, by epoch. After it the store takes no
    /// more writes (ADR-030): what is on disk is then known only to a
    /// restart, which recovers it.
    failed: Option<(u64, String)>,
    committing: bool,
    waiting: usize,
}

pub struct Quanty<S: Storage + 'static> {
    db: RwLock<Db<S>>,
    batch: Mutex<Batch>,
    done: Condvar,
    publisher: RwLock<Option<Publisher>>,
    head: AtomicI64,
    compacted: AtomicI64,
    /// the oldest revision history can still be read at
    retained_from: AtomicI64,
    /// every commit that carries revisions and whose history is retained,
    /// oldest first; a commit holds the revisions after the previous
    /// entry's up to its own
    commits: RwLock<Vec<CommitEntry>>,
    /// the oldest commit QuantyDB still keeps a snapshot of (ADR-014)
    snap_floor: AtomicU64,
    /// Told once when a commit fails; the server exits from it.
    on_stop: RwLock<Option<StopHook>>,
}

pub type StopHook = Box<dyn Fn(&str) + Send + Sync>;

impl Quanty<FileStorage> {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        if path.exists() {
            Self::start(Db::open_file(path)?)
        } else {
            Self::start(Db::create_file(path)?)
        }
    }
}

impl Quanty<MemStorage> {
    pub fn in_memory() -> Result<Self> {
        Self::start(Db::in_memory()?)
    }
}

/// One entry of a key's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    pub rev: i64,
    pub unix_ms: i64,
    pub change: Change,
}

fn commit_key(id: u64) -> Vec<u8> {
    let mut k = vec![META];
    k.extend_from_slice(COMMITS);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn commit_value(last_rev: i64, unix_ms: i64) -> Vec<u8> {
    let mut out = varint(last_rev);
    put_varint(&mut out, unix_ms as u64);
    out
}

fn varint(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, v as u64);
    out
}

impl<S: Storage + 'static> Quanty<S> {
    fn start(db: Db<S>) -> Result<Self> {
        if db.head_commit() == 0 {
            let mut tx = db.begin();
            let id = tx.pending_commit_id();
            tx.put(&commit_key(id), &commit_value(1, now_ms()))?;
            tx.commit()?;
        }
        let snap_floor = db.log()?.last().map_or(0, |c| c.commit_id);
        let (compacted, retained_from, commits, head) = {
            let snap = db.snapshot();
            let meta = |name: &[u8], default: i64| -> Result<i64> {
                match snap.get(&prefixed(META, name))? {
                    Some(v) => decode_i64(&v),
                    None => Ok(default),
                }
            };
            let compacted = meta(COMPACTED, 0)?;
            let retained_from = meta(RETAINED_FROM, 1)?;
            let from = prefixed(META, COMMITS);
            let to = prefix_end(&from);
            let mut commits = Vec::new();
            for item in snap.scan(Some(&from), to.as_deref())? {
                let (k, v) = item?;
                let id = u64::from_be_bytes(
                    k[from.len()..]
                        .try_into()
                        .map_err(|_| Error::Corrupt("commit index"))?,
                );
                let mut b = &v[..];
                commits.push(CommitEntry {
                    last_rev: take_varint(&mut b)? as i64,
                    id,
                    unix_ms: take_varint(&mut b).unwrap_or(0) as i64,
                });
            }
            let head = commits
                .last()
                .ok_or(Error::Corrupt("commit index"))?
                .last_rev;
            (compacted, retained_from, commits, head)
        };
        Ok(Quanty {
            db: RwLock::new(db),
            batch: Mutex::new(Batch {
                tx: None,
                members: 0,
                changes: Vec::new(),
                next_rev: head + 1,
                epoch: 1,
                committed: 0,
                failed: None,
                committing: false,
                waiting: 0,
            }),
            done: Condvar::new(),
            on_stop: RwLock::new(None),
            publisher: RwLock::new(None),
            head: head.into(),
            compacted: compacted.into(),
            retained_from: retained_from.into(),
            commits: RwLock::new(commits),
            snap_floor: snap_floor.into(),
        })
    }

    fn lock_batch(&self) -> MutexGuard<'_, Batch> {
        self.batch.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Commits the open batch, `tx` holding its writes. The batch lock is
    /// let go during the fsync; `committing` keeps everyone else out.
    fn commit_batch<'g>(
        &'g self,
        mut g: MutexGuard<'g, Batch>,
        mut tx: quantydb_core::WriteTx<'_, S>,
    ) -> MutexGuard<'g, Batch> {
        g.committing = true;
        let epoch = g.epoch;
        g.epoch += 1;
        g.members = 0;
        let changes = std::mem::take(&mut g.changes);
        drop(g);
        let last = changes.last().map(|c| c.0);
        let id = tx.pending_commit_id();
        let at = now_ms();
        let result = match last {
            Some(l) => tx.put(&commit_key(id), &commit_value(l, at)).map(|()| tx),
            None => Ok(tx),
        }
        .and_then(|tx| tx.commit());
        let mut g = self.lock_batch();
        match result {
            Ok(id) => {
                if let Some(l) = last {
                    self.commits
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(CommitEntry {
                            last_rev: l,
                            id,
                            unix_ms: at,
                        });
                    self.head.store(l, Ordering::Release);
                }
                if let Some(p) = &*self.publisher.read().unwrap_or_else(|e| e.into_inner()) {
                    for (rev, c) in &changes {
                        p(*rev, c);
                    }
                }
            }
            Err(e) => self.stop(&mut g, epoch, &e.to_string()),
        }
        g.committed = epoch;
        g.committing = false;
        self.done.notify_all();
        g
    }

    /// Called with what to do when a commit fails, instead of only
    /// refusing every write after it.
    pub fn on_stop(&self, hook: StopHook) {
        *self.on_stop.write().unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    fn stop(&self, g: &mut Batch, epoch: u64, msg: &str) {
        if g.failed.is_none() {
            g.failed = Some((epoch, msg.to_string()));
            if let Some(hook) = &*self.on_stop.read().unwrap_or_else(|e| e.into_inner()) {
                hook(msg);
            }
        }
    }

    /// The oldest commit with a snapshot that holds `rev` or comes after
    /// it, and the last revision in that commit. Everything between `rev`
    /// and that revision is undone from the log.
    fn commit_of(&self, rev: i64) -> Result<(i64, u64)> {
        let from = self.retained_from.load(Ordering::Acquire);
        if rev < from {
            return Err(Error::NotRetained(from));
        }
        let floor = self.snap_floor.load(Ordering::Acquire);
        let commits = self.commits.read().unwrap_or_else(|e| e.into_inner());
        let i = commits.partition_point(|c| c.last_rev < rev);
        let held = commits.get(i).ok_or(Error::FutureRevision)?;
        if held.id >= floor {
            return Ok((held.last_rev, held.id));
        }
        // the snapshot is gone: rebuild from the oldest one left, which
        // may be a bookkeeping commit after the newest revision
        let j = commits.partition_point(|c| c.id < floor);
        Ok(match commits.get(j) {
            Some(c) => (c.last_rev, c.id),
            None => (commits.last().map_or(rev, |c| c.last_rev), floor),
        })
    }

    /// The store as it was right after `rev`.
    fn read_at(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<()> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let (last, id) = self.commit_of(rev)?;
        let snap = db.snapshot_at(id)?;
        let base = Reader { get: &snap };
        if last == rev {
            return f(&base);
        }
        // the commit also holds later revisions; undo them with the log
        let mut overlay = BTreeMap::new();
        for item in snap.scan(Some(&log_key(rev + 1, 0)), Some(&log_key(last + 1, 0)))? {
            let (_, v) = item?;
            let c = decode_change(&v)?;
            overlay.entry(c.kv.key).or_insert(c.prev);
        }
        f(&Overlay {
            base: &base,
            overlay,
        })
    }

    /// Keeps the change log, and so the history, for `keep_ms`, and
    /// QuantyDB's snapshots for `snapshot_ms` (ADR-014). The log never
    /// loses a revision an etcd client may still read, that is anything
    /// from the compaction revision on. Returns how many snapshots were
    /// collected.
    pub fn enforce_retention(&self, keep_ms: i64, snapshot_ms: i64, now_ms: i64) -> Result<usize> {
        let mut g = self.lock_batch();
        while g.committing || g.tx.is_some() {
            g = self.done.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        let retained = self.retained_from.load(Ordering::Acquire);
        // the newest revision at or before the cutoff stays readable,
        // since `get --ago` asks for exactly that state
        let (from, dropped) = {
            let commits = self.commits.read().unwrap_or_else(|e| e.into_inner());
            let n = commits.partition_point(|c| c.unix_ms <= now_ms - keep_ms);
            let by_time = if n == 0 {
                retained
            } else {
                commits[n - 1].last_rev
            };
            let from = by_time.min(self.compacted().max(1)).max(retained);
            let d = commits.partition_point(|c| c.last_rev < from);
            (from, commits[..d].to_vec())
        };
        if let Some((_, msg)) = &g.failed {
            return Err(Error::Commit(format!(
                "stopped after a failed commit: {msg}"
            )));
        }
        let moved = from > retained;
        if moved {
            let db = self.db.read().unwrap_or_else(|e| e.into_inner());
            let mut tx = db.begin();
            let old: Vec<Vec<u8>> = tx
                // the changes at `from` itself stay: a watch may start
                // at the compaction revision and must get them
                .scan(Some(&[LOG]), Some(&log_key(from, 0)))?
                .map(|r| r.map(|(k, _)| k))
                .collect::<std::result::Result<_, _>>()?;
            for k in &old {
                tx.delete(k)?;
            }
            for c in &dropped {
                tx.delete(&commit_key(c.id))?;
            }
            tx.put(&prefixed(META, RETAINED_FROM), &varint(from))?;
            if let Err(e) = tx.commit() {
                let epoch = g.epoch;
                self.stop(&mut g, epoch, &e.to_string());
                return Err(e.into());
            }
            self.commits
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .drain(..dropped.len());
            self.retained_from.store(from, Ordering::Release);
        }
        let mut db = self.db.write().unwrap_or_else(|e| e.into_inner());
        let log = db.log()?;
        let keep = log
            .iter()
            .position(|c| (c.unix_ts_ms as i64) < now_ms - snapshot_ms)
            .map_or(log.len(), |i| i + 1)
            .max(1);
        if keep >= log.len() {
            return Ok(0);
        }
        let report = db.gc(keep)?;
        let floor = db.log()?.last().map_or(0, |c| c.commit_id);
        self.snap_floor.store(floor, Ordering::Release);
        drop(g);
        Ok(report.pruned_commits as usize)
    }
}

/// A snapshot with some keys set back to earlier states.
struct Overlay<'a> {
    base: &'a dyn View,
    overlay: BTreeMap<Vec<u8>, Option<Record>>,
}

impl View for Overlay<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        match self.overlay.get(key) {
            Some(r) => Ok(r.clone()),
            None => self.base.get(key),
        }
    }

    fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut dyn FnMut(Record) -> bool,
    ) -> Result<()> {
        use std::ops::Bound;
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        let mut extra = self
            .overlay
            .range::<[u8], _>((Bound::Included(start), hi))
            .peekable();
        let mut go = true;
        self.base.scan(start, end, &mut |rec| {
            while let Some((k, v)) = extra.next_if(|(k, _)| k.as_slice() < rec.key.as_slice()) {
                let _ = k;
                if let Some(r) = v {
                    go = f(r.clone());
                    if !go {
                        return false;
                    }
                }
            }
            go = match extra.next_if(|(k, _)| k.as_slice() == rec.key.as_slice()) {
                Some((_, Some(r))) => f(r.clone()),
                Some((_, None)) => true,
                None => f(rec),
            };
            go
        })?;
        if go {
            for (_, v) in extra {
                if let Some(r) = v {
                    if !f(r.clone()) {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

fn prefixed(p: u8, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(key.len() + 1);
    k.push(p);
    k.extend_from_slice(key);
    k
}

fn put_varint(out: &mut Vec<u8>, v: u64) {
    quantycore::proto::encode_varint(v, out);
}

fn take_varint(b: &mut &[u8]) -> Result<u64> {
    let (v, n) = quantycore::proto::decode_varint(b).map_err(|_| Error::Corrupt("record"))?;
    *b = &b[n..];
    Ok(v)
}

fn decode_i64(b: &[u8]) -> Result<i64> {
    let mut b = b;
    Ok(take_varint(&mut b)? as i64)
}

fn encode_record(r: &Record, with_key: bool, out: &mut Vec<u8>) {
    if with_key {
        put_varint(out, r.key.len() as u64);
        out.extend_from_slice(&r.key);
    }
    for v in [r.create_rev, r.mod_rev, r.version, r.lease] {
        put_varint(out, v as u64);
    }
    put_varint(out, r.value.len() as u64);
    out.extend_from_slice(&r.value);
}

fn decode_record(key: Option<&[u8]>, b: &mut &[u8]) -> Result<Record> {
    let key = match key {
        Some(k) => k.to_vec(),
        None => take(b)?,
    };
    let mut n = [0i64; 4];
    for v in &mut n {
        *v = take_varint(b)? as i64;
    }
    Ok(Record {
        key,
        create_rev: n[0],
        mod_rev: n[1],
        version: n[2],
        lease: n[3],
        value: take(b)?,
    })
}

fn take(b: &mut &[u8]) -> Result<Vec<u8>> {
    let len = take_varint(b)? as usize;
    let v = b.get(..len).ok_or(Error::Corrupt("record"))?.to_vec();
    *b = &b[len..];
    Ok(v)
}

fn log_key(rev: i64, seq: u32) -> Vec<u8> {
    let mut k = vec![LOG];
    k.extend_from_slice(&(rev as u64).to_be_bytes());
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

fn encode_change(c: &Change) -> Vec<u8> {
    let mut out = vec![match (c.kind, c.prev.is_some()) {
        (Kind::Put, false) => 0,
        (Kind::Put, true) => 1,
        (Kind::Delete, false) => 2,
        (Kind::Delete, true) => 3,
    }];
    encode_record(&c.kv, true, &mut out);
    if let Some(p) = &c.prev {
        encode_record(p, true, &mut out);
    }
    out
}

fn decode_change(mut b: &[u8]) -> Result<Change> {
    let tag = *b.first().ok_or(Error::Corrupt("change"))?;
    b = &b[1..];
    let kind = if tag < 2 { Kind::Put } else { Kind::Delete };
    let kv = decode_record(None, &mut b)?;
    let prev = if tag % 2 == 1 {
        Some(decode_record(None, &mut b)?)
    } else {
        None
    };
    Ok(Change { kind, kv, prev })
}

/// The smallest byte string greater than every string starting with
/// `prefix`, or None when there is none.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

struct Reader<'a, F> {
    get: &'a F,
}

type Visit<'a> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'a;

trait RawRead {
    fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn raw_scan(&self, start: &[u8], end: Option<&[u8]>, f: &mut Visit) -> Result<()>;
}

impl<S: Storage + 'static> RawRead for quantydb_core::Snapshot<'_, S> {
    fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get(key)?)
    }
    fn raw_scan(&self, start: &[u8], end: Option<&[u8]>, f: &mut Visit) -> Result<()> {
        for item in self.scan(Some(start), end)? {
            let (k, v) = item?;
            if !f(&k, &v)? {
                break;
            }
        }
        Ok(())
    }
}

impl<S: Storage + 'static> RawRead for quantydb_core::WriteTx<'_, S> {
    fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get(key)?)
    }
    fn raw_scan(&self, start: &[u8], end: Option<&[u8]>, f: &mut Visit) -> Result<()> {
        for item in self.scan(Some(start), end)? {
            let (k, v) = item?;
            if !f(&k, &v)? {
                break;
            }
        }
        Ok(())
    }
}

impl<F: RawRead> View for Reader<'_, F> {
    fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        live_get(self.get, key)
    }
    fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut dyn FnMut(Record) -> bool,
    ) -> Result<()> {
        live_scan(self.get, start, end, f)
    }
}

fn live_get(r: &dyn RawRead, key: &[u8]) -> Result<Option<Record>> {
    if key.len() > MAX_KEY {
        return Ok(None);
    }
    match r.raw_get(&prefixed(LIVE, key))? {
        Some(v) => Ok(Some(decode_record(Some(key), &mut v.as_slice())?)),
        None => Ok(None),
    }
}

fn live_scan(
    r: &dyn RawRead,
    start: &[u8],
    end: Option<&[u8]>,
    f: &mut dyn FnMut(Record) -> bool,
) -> Result<()> {
    let from = prefixed(LIVE, start);
    let to = match end {
        Some(e) => prefixed(LIVE, e),
        None => vec![LIVE + 1],
    };
    r.raw_scan(&from, Some(&to), &mut |k, v| {
        let rec = decode_record(Some(&k[1..]), &mut &v[..])?;
        Ok(f(rec))
    })
}

struct Writing<'a, 'db, S: Storage + 'static> {
    tx: &'a mut quantydb_core::WriteTx<'db, S>,
    rev: i64,
    changes: Vec<Change>,
    meta_dirty: bool,
}

impl<S: Storage + 'static> View for Writing<'_, '_, S> {
    fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        live_get(&*self.tx, key)
    }
    fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut dyn FnMut(Record) -> bool,
    ) -> Result<()> {
        live_scan(&*self.tx, start, end, f)
    }
}

impl<S: Storage + 'static> Tx for Writing<'_, '_, S> {
    fn revision(&self) -> i64 {
        self.rev
    }

    fn put(&mut self, key: &[u8], value: &[u8], lease: i64) -> Result<Option<Record>> {
        if key.len() > MAX_KEY {
            return Err(Error::KeyTooLong);
        }
        let prev = self.get(key)?;
        let rec = Record {
            key: key.to_vec(),
            create_rev: prev.as_ref().map_or(self.rev, |p| p.create_rev),
            mod_rev: self.rev,
            version: prev.as_ref().map_or(1, |p| p.version + 1),
            lease,
            value: value.to_vec(),
        };
        let mut v = Vec::new();
        encode_record(&rec, false, &mut v);
        self.tx.put(&prefixed(LIVE, key), &v)?;
        self.changes.push(Change {
            kind: Kind::Put,
            kv: rec,
            prev: prev.clone(),
        });
        Ok(prev)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<Record>> {
        let prev = self.get(key)?;
        if prev.is_some() {
            self.tx.delete(&prefixed(LIVE, key))?;
            self.changes.push(Change {
                kind: Kind::Delete,
                kv: Record {
                    key: key.to_vec(),
                    mod_rev: self.rev,
                    ..Record::default()
                },
                prev: prev.clone(),
            });
        }
        Ok(prev)
    }

    fn meta_get(&self, name: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.tx.get(&prefixed(META, name))?)
    }

    fn meta_put(&mut self, name: &[u8], value: &[u8]) -> Result<()> {
        self.meta_dirty = true;
        Ok(self.tx.put(&prefixed(META, name), value)?)
    }

    fn meta_delete(&mut self, name: &[u8]) -> Result<()> {
        self.meta_dirty = true;
        self.tx.delete(&prefixed(META, name))?;
        Ok(())
    }

    fn meta_scan(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<()> {
        let from = prefixed(META, prefix);
        let to = prefix_end(&from);
        for item in self.tx.scan(Some(&from), to.as_deref())? {
            let (k, v) = item?;
            if !f(&k[1..], &v) {
                break;
            }
        }
        Ok(())
    }
}

impl<S: Storage + 'static> Backend for Quanty<S> {
    fn head(&self) -> i64 {
        self.head.load(Ordering::Acquire)
    }

    fn compacted(&self) -> i64 {
        self.compacted.load(Ordering::Acquire)
    }

    fn read(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<i64> {
        let head = self.head();
        if rev > head {
            return Err(Error::FutureRevision);
        }
        if rev > 0 && rev < self.compacted() {
            return Err(Error::Compacted(self.compacted()));
        }
        // the newest state is read at the head revision too, so that the
        // data and the revision reported with it always agree
        let at = if rev == 0 { head } else { rev };
        self.read_at(at, f)?;
        Ok(at)
    }

    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
    ) -> Result<Option<(i64, Vec<Change>)>> {
        let mut g = self.lock_batch();
        g.waiting += 1;
        while g.committing {
            g = self.done.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        g.waiting -= 1;
        if let Some((_, msg)) = &g.failed {
            return Err(Error::Commit(format!(
                "stopped after a failed commit: {msg}"
            )));
        }
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        // what this write reads includes the batch's writes so far, which
        // are not on disk until the batch commits
        let pending = g.members > 0;
        let mut tx = match g.tx.take() {
            Some(t) => match db.resume(t) {
                Ok(t) => t,
                Err(e) => {
                    // the batch's writes are lost: its writers must hear so
                    let epoch = g.epoch;
                    self.stop(&mut g, epoch, &e.to_string());
                    g.epoch += 1;
                    g.members = 0;
                    g.changes.clear();
                    g.committed = epoch;
                    self.done.notify_all();
                    return Err(e.into());
                }
            },
            None => db.begin(),
        };
        let rev = g.next_rev;
        let roots = tx.roots();
        tx.savepoint();
        let mut w = Writing {
            tx: &mut tx,
            rev,
            changes: Vec::new(),
            meta_dirty: false,
        };
        let res = f(&mut w);
        let (changes, meta_dirty) = (std::mem::take(&mut w.changes), w.meta_dirty);
        let res = res.and_then(|()| {
            for (i, c) in changes.iter().enumerate() {
                tx.put(&log_key(rev, i as u32), &encode_change(c))?;
            }
            Ok(())
        });
        let wrote = res.is_ok() && (!changes.is_empty() || meta_dirty);
        if wrote {
            tx.release_savepoint();
            g.members += 1;
            if !changes.is_empty() {
                g.next_rev += 1;
                g.changes.push((rev, changes.clone()));
            }
        } else {
            tx.rollback_savepoint(roots);
            tx.release_savepoint();
        }
        let epoch = g.epoch;
        // whoever applies last while nobody else waits commits the batch,
        // even if its own write changed nothing
        g = if g.members == 0 {
            drop(tx);
            g
        } else if g.waiting == 0 {
            self.commit_batch(g, tx)
        } else {
            g.tx = Some(tx.suspend());
            g
        };
        drop(db);
        res?;
        if !wrote && !pending {
            return Ok(None);
        }
        // a write, or a read of writes that are not yet durable, answers
        // only once they are: etcd never shows a client what it might lose
        while g.committed < epoch {
            g = self.done.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        if let Some((e, msg)) = &g.failed {
            if *e == epoch {
                return Err(Error::Commit(msg.clone()));
            }
        }
        Ok((wrote && !changes.is_empty()).then_some((rev, changes)))
    }

    fn set_publisher(&self, p: Publisher) {
        *self.publisher.write().unwrap_or_else(|e| e.into_inner()) = Some(p);
    }

    fn changes(&self, from: i64, to: i64, f: &mut dyn FnMut(i64, Change) -> bool) -> Result<()> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let snap = db.snapshot();
        let start = log_key(from.max(0), 0);
        let end = log_key(to.saturating_add(1), 0);
        for item in snap.scan(Some(&start), Some(&end))? {
            let (k, v) = item?;
            let rev = u64::from_be_bytes(k[1..9].try_into().unwrap()) as i64;
            if !f(rev, decode_change(&v)?) {
                break;
            }
        }
        Ok(())
    }

    fn read_retained(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<()> {
        if rev > self.head() {
            return Err(Error::FutureRevision);
        }
        self.read_at(rev, f)
    }

    fn revision_at(&self, unix_ms: i64) -> Result<i64> {
        let from = self.retained_from.load(Ordering::Acquire);
        let commits = self.commits.read().unwrap_or_else(|e| e.into_inner());
        match commits.partition_point(|c| c.unix_ms <= unix_ms) {
            0 => Err(Error::NotRetained(from)),
            i => Ok(commits[i - 1].last_rev.max(from)),
        }
    }

    fn history(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<HistoryEntry>> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let commits = self
            .commits
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let snap = db.snapshot();
        let mut out = Vec::new();
        for item in snap.scan(Some(&[LOG]), Some(&[LOG + 1]))? {
            let (k, v) = item?;
            let change = decode_change(&v)?;
            let key = change.kv.key.as_slice();
            let hit = match end {
                None => key == start,
                Some(e) => key >= start && key < e,
            };
            if hit {
                let rev = u64::from_be_bytes(k[1..9].try_into().unwrap()) as i64;
                let commit = commits.get(commits.partition_point(|c| c.last_rev < rev));
                out.push(HistoryEntry {
                    rev,
                    unix_ms: commit.map_or(0, |c| c.unix_ms),
                    change,
                });
            }
        }
        Ok(out)
    }

    fn size(&self) -> Result<(i64, i64)> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let st = db.stats()?;
        let page = quantydb_core::PagerOptions::default().page_size as i64;
        Ok((
            st.page_count as i64 * page,
            (st.page_count - st.free_pages) as i64 * page,
        ))
    }

    fn export(&self) -> Result<Vec<u8>> {
        let mut g = self.lock_batch();
        while g.committing || g.tx.is_some() {
            g = self.done.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let snap = db.snapshot();
        let mut out = Vec::new();
        put_varint(&mut out, self.head() as u64);
        put_varint(&mut out, self.compacted() as u64);
        let commits = prefixed(META, COMMITS);
        let retained = prefixed(META, RETAINED_FROM);
        for p in [LIVE, META] {
            for item in snap.scan(Some(&[p]), Some(&[p + 1]))? {
                let (k, v) = item?;
                // where history and snapshots are kept is this node's own
                if k.starts_with(&commits) || k == retained {
                    continue;
                }
                put_varint(&mut out, k.len() as u64);
                out.extend_from_slice(&k);
                put_varint(&mut out, v.len() as u64);
                out.extend_from_slice(&v);
            }
        }
        drop(g);
        Ok(out)
    }

    fn import(&self, data: &[u8]) -> Result<()> {
        let mut b = data;
        let head = take_varint(&mut b)? as i64;
        let compacted = take_varint(&mut b)? as i64;
        let mut pairs = Vec::new();
        while !b.is_empty() {
            let n = take_varint(&mut b)? as usize;
            let k = b.get(..n).ok_or(Error::Corrupt("import"))?.to_vec();
            b = &b[n..];
            let n = take_varint(&mut b)? as usize;
            let v = b.get(..n).ok_or(Error::Corrupt("import"))?.to_vec();
            b = &b[n..];
            if !matches!(k.first(), Some(&LIVE) | Some(&META)) {
                return Err(Error::Corrupt("import"));
            }
            pairs.push((k, v));
        }
        let mut g = self.lock_batch();
        while g.committing || g.tx.is_some() {
            g = self.done.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        let at = now_ms();
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let mut tx = db.begin();
        for p in [LIVE, LOG, META] {
            let old: Vec<Vec<u8>> = tx
                .scan(Some(&[p]), Some(&[p + 1]))?
                .map(|r| r.map(|(k, _)| k))
                .collect::<std::result::Result<_, _>>()?;
            for k in old {
                tx.delete(&k)?;
            }
        }
        for (k, v) in &pairs {
            tx.put(k, v)?;
        }
        // nothing before the head can be read here any more
        let floor = compacted.max(head);
        tx.put(&prefixed(META, COMPACTED), &varint(floor))?;
        tx.put(&prefixed(META, RETAINED_FROM), &varint(head))?;
        let id = tx.pending_commit_id();
        tx.put(&commit_key(id), &commit_value(head, at))?;
        let id = tx.commit()?;
        drop(db);
        *self.commits.write().unwrap_or_else(|e| e.into_inner()) = vec![CommitEntry {
            last_rev: head,
            id,
            unix_ms: at,
        }];
        self.head.store(head, Ordering::Release);
        self.compacted.store(floor, Ordering::Release);
        self.retained_from.store(head, Ordering::Release);
        g.next_rev = head + 1;
        Ok(())
    }

    fn compact(&self, rev: i64) -> Result<()> {
        if rev <= self.compacted() {
            return Ok(());
        }
        if rev > self.head() {
            return Err(Error::FutureRevision);
        }
        self.write(&mut |tx| tx.meta_put(COMPACTED, &varint(rev)))?;
        self.compacted.fetch_max(rev, Ordering::AcqRel);
        Ok(())
    }
}

#[cfg(test)]
fn tests_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn put(s: &Quanty<MemStorage>, k: &[u8], v: &[u8]) -> i64 {
        s.write(&mut |tx| tx.put(k, v, 0).map(|_| ()))
            .unwrap()
            .unwrap()
            .0
    }

    fn get(s: &Quanty<MemStorage>, rev: i64, k: &[u8]) -> Option<Record> {
        let mut out = None;
        s.read(rev, &mut |v| {
            out = v.get(k)?;
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn revisions_are_commits_and_history_stays_readable() {
        let s = Quanty::in_memory().unwrap();
        assert_eq!(s.head(), 1);
        let r1 = put(&s, b"/a", b"one");
        let r2 = put(&s, b"/a", b"two");
        assert_eq!((r1, r2), (2, 3));
        let now = get(&s, 0, b"/a").unwrap();
        assert_eq!((now.create_rev, now.mod_rev, now.version), (2, 3, 2));
        assert_eq!(get(&s, r1, b"/a").unwrap().value, b"one");
        assert!(get(&s, 1, b"/a").is_none());
        assert!(matches!(
            s.read(9, &mut |_| Ok(())),
            Err(Error::FutureRevision)
        ));
    }

    #[test]
    fn concurrent_writes_share_commits_and_every_revision_reads_back_exactly() {
        let s = std::sync::Arc::new(Quanty::in_memory().unwrap());
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let s = s.clone();
                std::thread::spawn(move || {
                    let mut x = t * 7919 + 1;
                    let mut seen = Vec::new();
                    for _ in 0..60 {
                        x = x
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let key = format!("/k/{}", (x >> 33) % 12).into_bytes();
                        let r = if (x >> 20) % 4 == 0 {
                            s.write(&mut |tx| tx.delete(&key).map(|_| ()))
                        } else {
                            let v = format!("{t}-{x}").into_bytes();
                            s.write(&mut |tx| tx.put(&key, &v, 0).map(|_| ()))
                        };
                        seen.extend(r.unwrap());
                    }
                    seen
                })
            })
            .collect();
        let mut log: Vec<(i64, Vec<Change>)> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        log.sort_by_key(|e| e.0);
        let revs: Vec<i64> = log.iter().map(|e| e.0).collect();
        let dense: Vec<i64> = (2..=s.head()).collect();
        assert_eq!(revs, dense, "every revision used once, none skipped");
        let commits = s.commits.read().unwrap().len();
        assert!(
            commits < revs.len(),
            "{commits} commits for {} writes",
            revs.len()
        );

        let check = |when: &str| {
            let mut model = BTreeMap::new();
            for (rev, changes) in &log {
                for c in changes {
                    match c.kind {
                        Kind::Put => model.insert(c.kv.key.clone(), c.kv.value.clone()),
                        Kind::Delete => model.remove(&c.kv.key),
                    };
                }
                let mut got = BTreeMap::new();
                s.read(*rev, &mut |v| {
                    v.scan(b"/", None, &mut |r| {
                        got.insert(r.key, r.value);
                        true
                    })
                })
                .unwrap();
                assert_eq!(got, model, "{when}: scan at revision {rev}");
                for k in 0..12 {
                    let key = format!("/k/{k}").into_bytes();
                    let mut one = None;
                    s.read(*rev, &mut |v| {
                        one = v.get(&key)?.map(|r| r.value);
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(
                        one.as_ref(),
                        model.get(&key),
                        "{when}: get at revision {rev}"
                    );
                }
            }
        };
        check("with every snapshot");
        s.enforce_retention(3_600_000, 0, super::tests_now() + 1000)
            .unwrap();
        assert_eq!(s.db.read().unwrap().log().unwrap().len(), 1);
        check("rebuilt from the log");
    }

    #[test]
    fn a_write_that_changes_nothing_takes_no_revision() {
        let s = Quanty::in_memory().unwrap();
        let r = s
            .write(&mut |tx| tx.delete(b"/missing").map(|_| ()))
            .unwrap();
        assert!(r.is_none());
        assert_eq!(s.head(), 1);
    }

    #[test]
    fn the_change_log_replays_both_sides() {
        let s = Quanty::in_memory().unwrap();
        put(&s, b"/a", b"1");
        put(&s, b"/a", b"2");
        let del = s
            .write(&mut |tx| tx.delete(b"/a").map(|_| ()))
            .unwrap()
            .unwrap()
            .0;
        let mut seen = Vec::new();
        s.changes(3, del, &mut |rev, c| {
            seen.push((rev, c));
            true
        })
        .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].1.prev.as_ref().unwrap().value, b"1");
        assert_eq!(seen[1].1.kind, Kind::Delete);
        assert_eq!(seen[1].1.kv.mod_rev, del);
        assert_eq!(seen[1].1.prev.as_ref().unwrap().value, b"2");
    }

    #[test]
    fn retention_drops_snapshots_and_old_history_but_not_what_etcd_may_read() {
        let s = Quanty::in_memory().unwrap();
        for v in [b"1", b"2", b"3", b"4", b"5"] {
            put(&s, b"/k", v);
        }
        s.compact(4).unwrap();
        let later = super::tests_now() + 1000;
        assert!(s.enforce_retention(0, 0, later).unwrap() > 0);
        assert_eq!(
            s.db.read().unwrap().log().unwrap().len(),
            1,
            "one snapshot left"
        );
        // revision 4 survives the loss of its snapshot: it is rebuilt from
        // the newest one and the changes after 4
        assert_eq!(get(&s, 4, b"/k").unwrap().value, b"3");
        assert_eq!(get(&s, 5, b"/k").unwrap().value, b"4");
        assert_eq!(get(&s, 0, b"/k").unwrap().value, b"5");
        assert!(matches!(
            s.read_retained(3, &mut |_| Ok(())),
            Err(Error::NotRetained(4))
        ));
        let kept: Vec<i64> = s
            .history(b"/k", None)
            .unwrap()
            .iter()
            .map(|h| h.rev)
            .collect();
        assert_eq!(kept, [4, 5, 6]);
        // a watch from the compaction revision gets that revision's change
        let mut at_compaction = Vec::new();
        s.changes(4, 4, &mut |rev, _| {
            at_compaction.push(rev);
            true
        })
        .unwrap();
        assert_eq!(at_compaction, [4]);
    }

    #[test]
    fn history_outlives_snapshots_within_its_window() {
        let s = Quanty::in_memory().unwrap();
        for v in [b"1", b"2", b"3"] {
            put(&s, b"/k", v);
        }
        let later = super::tests_now() + 1000;
        // keep an hour of history but no snapshots beyond the newest
        s.enforce_retention(3_600_000, 0, later).unwrap();
        assert_eq!(s.db.read().unwrap().log().unwrap().len(), 1);
        let mut first = None;
        s.read_retained(2, &mut |v| {
            first = v.get(b"/k")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(first.unwrap().value, b"1");
        assert_eq!(s.history(b"/k", None).unwrap().len(), 3);
        assert_eq!(s.revision_at(later).unwrap(), 4);
    }

    #[test]
    fn ranges_respect_bounds_and_compaction_is_remembered() {
        let dir = std::env::temp_dir().join(format!("qk-store-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        {
            let s = Quanty::open(&dir).unwrap();
            for k in [&b"/a"[..], b"/b", b"/b/x", b"/c"] {
                s.write(&mut |tx| tx.put(k, b"v", 0).map(|_| ())).unwrap();
            }
            let mut keys = Vec::new();
            s.read(0, &mut |v| {
                v.scan(b"/b", Some(b"/c"), &mut |r| {
                    keys.push(r.key);
                    true
                })
            })
            .unwrap();
            assert_eq!(keys, [b"/b".to_vec(), b"/b/x".to_vec()]);
            s.compact(3).unwrap();
            assert!(matches!(
                s.read(2, &mut |_| Ok(())),
                Err(Error::Compacted(3))
            ));
        }
        let s = Quanty::open(&dir).unwrap();
        assert_eq!(s.compacted(), 3);
        assert_eq!(s.head(), 5, "compaction is bookkeeping, not a revision");
        std::fs::remove_file(&dir).unwrap();
    }

    /// Memory whose syncs fail while `fail` is set, as a full or broken
    /// disk does.
    struct Flaky {
        mem: MemStorage,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Storage for Flaky {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> quantydb_core::Result<()> {
            self.mem.read_at(offset, buf)
        }
        fn write_at(&self, offset: u64, buf: &[u8]) -> quantydb_core::Result<()> {
            self.mem.write_at(offset, buf)
        }
        fn sync(&self) -> quantydb_core::Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("no space left on device").into());
            }
            self.mem.sync()
        }
        fn len(&self) -> quantydb_core::Result<u64> {
            self.mem.len()
        }
    }

    // as etcd: a store whose commit failed knows its disk only after a
    // restart, so it takes nothing more, and nothing it said was written
    // is later found missing
    #[test]
    fn after_a_failed_commit_the_store_takes_no_more_writes() {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let storage = Flaky {
            mem: MemStorage::new(),
            fail: fail.clone(),
        };
        let s = Quanty::start(Db::create(storage, Default::default()).unwrap()).unwrap();
        let stops = Arc::new(Mutex::new(Vec::new()));
        let seen = stops.clone();
        s.on_stop(Box::new(move |m| seen.lock().unwrap().push(m.to_string())));
        let w = |s: &Quanty<Flaky>| s.write(&mut |tx| tx.put(b"k", b"v", 0).map(|_| ()));
        w(&s).unwrap();
        let head = s.head();
        fail.store(true, Ordering::SeqCst);
        assert!(matches!(w(&s), Err(Error::Commit(_))));
        fail.store(false, Ordering::SeqCst);
        assert!(matches!(w(&s), Err(Error::Commit(_))));
        assert!(s.enforce_retention(0, 0, now_ms()).is_err());
        assert_eq!(s.head(), head);
        assert_eq!(stops.lock().unwrap().len(), 1);
    }

    // a read inside a batch sees the batch's writes; if their commit then
    // fails, the reader must not have been told about them
    #[test]
    fn a_read_of_writes_that_fail_to_commit_fails_too() {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::new(
            Quanty::start(
                Db::create(
                    Flaky {
                        mem: MemStorage::new(),
                        fail: fail.clone(),
                    },
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        s.write(&mut |tx| tx.put(b"k", b"old", 0).map(|_| ()))
            .unwrap();
        // someone else is about to join: the writer below leaves its
        // batch open for them
        s.lock_batch().waiting += 1;
        let s2 = s.clone();
        let writer =
            std::thread::spawn(move || s2.write(&mut |tx| tx.put(b"k", b"new", 0).map(|_| ())));
        while s.lock_batch().tx.is_none() {
            std::thread::yield_now();
        }
        s.lock_batch().waiting -= 1;
        fail.store(true, Ordering::SeqCst);
        let mut seen = None;
        let read = s.write(&mut |tx| {
            seen = tx.get(b"k")?.map(|r| r.value);
            Ok(())
        });
        assert_eq!(seen.as_deref(), Some(&b"new"[..]), "read inside the batch");
        assert!(read.is_err(), "told about a write that was lost");
        assert!(writer.join().unwrap().is_err());
    }

    // a node that fell behind takes the leader's state: the same keys at
    // the same revisions, and from there on the same revisions too
    #[test]
    fn a_store_taken_from_another_continues_like_it() {
        let a = Quanty::in_memory().unwrap();
        for i in 0..5 {
            put(&a, format!("/k/{i}").as_bytes(), b"v");
        }
        a.write(&mut |tx| tx.delete(b"/k/1").map(|_| ())).unwrap();
        let b = Quanty::in_memory().unwrap();
        put(&b, b"/old", b"gone after the import");
        b.import(&a.export().unwrap()).unwrap();
        assert_eq!(b.head(), a.head());
        for k in ["/k/0", "/k/1", "/k/4", "/old"] {
            assert_eq!(get(&b, 0, k.as_bytes()), get(&a, 0, k.as_bytes()), "{k}");
        }
        assert_eq!(put(&b, b"/next", b"v"), put(&a, b"/next", b"v"));
        assert!(matches!(
            b.read(a.head() - 2, &mut |_| Ok(())),
            Err(Error::Compacted(_))
        ));
    }
}
