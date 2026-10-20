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

use quantydb_core::{Db, FileStorage, MemStorage, Storage};
use std::sync::{Mutex, RwLock};

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

pub trait Backend: Send + Sync + 'static {
    fn head(&self) -> i64;
    fn compacted(&self) -> i64;
    /// `rev` 0 reads the newest state.
    fn read(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<()>;
    /// Runs `f` in one transaction. If it changed anything, commits and
    /// returns the new revision and the changes, after `publish` saw them
    /// while no later write could have been committed yet.
    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
        publish: &mut dyn FnMut(i64, &[Change]),
    ) -> Result<Option<(i64, Vec<Change>)>>;
    /// Replays the change log for revisions in `[from, to]` until `f`
    /// returns false.
    fn changes(&self, from: i64, to: i64, f: &mut dyn FnMut(i64, Change) -> bool) -> Result<()>;
    /// Marks everything before `rev` as compacted for etcd clients. What
    /// is physically kept is the retention policy's business, not this.
    fn compact(&self, rev: i64) -> Result<()>;
    /// Bytes on disk, and of those the bytes in use.
    fn size(&self) -> Result<(i64, i64)>;
}

const LIVE: u8 = b'k';
const LOG: u8 = b'e';
const META: u8 = b'm';
const COMPACTED: &[u8] = b"compacted";

/// QuantyDB keys are capped at an eighth of a page, 512 bytes at the
/// default size, and every key here carries a one byte prefix.
pub const MAX_KEY: usize = 511;

pub struct Quanty<S: Storage + 'static> {
    db: RwLock<Db<S>>,
    writer: Mutex<()>,
    head: std::sync::atomic::AtomicI64,
    compacted: std::sync::atomic::AtomicI64,
}

impl Quanty<FileStorage> {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        if path.exists() {
            Self::start(Db::open_file(path)?)
        } else {
            Self::start(Db::create_file(path)?)
        }
    }

    /// For reading beside a running server. Without the writer lock a
    /// collection in the server can reuse pages under a long read; the
    /// page checksums turn that into an error, never into wrong data.
    pub fn open_read_only(path: &std::path::Path) -> Result<Self> {
        let db = Db::open_file_unlocked(path)?;
        let head = db.head_commit() as i64;
        Ok(Quanty {
            db: RwLock::new(db),
            writer: Mutex::new(()),
            head: head.into(),
            compacted: 0.into(),
        })
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

impl<S: Storage + 'static> Quanty<S> {
    /// Reads at any retained revision, compacted or not: the history that
    /// etcd clients may no longer ask for is still here (ADR-010).
    pub fn read_retained(
        &self,
        rev: i64,
        f: &mut dyn FnMut(&dyn View) -> Result<()>,
    ) -> Result<()> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let snap = db.snapshot_at(rev as u64)?;
        f(&Reader { get: &snap })
    }

    /// The newest revision committed at or before `unix_ms`.
    pub fn revision_at(&self, unix_ms: i64) -> Result<i64> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        Ok(db.snapshot_at_time(unix_ms.max(0) as u64)?.commit_id() as i64)
    }

    /// Every retained change to keys in `[start, end)`, oldest first;
    /// `end` None means the single key `start`.
    pub fn history(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<HistoryEntry>> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let times: std::collections::HashMap<u64, u64> = db
            .log()?
            .into_iter()
            .map(|c| (c.commit_id, c.unix_ts_ms))
            .collect();
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
                let rev = u64::from_be_bytes(k[1..9].try_into().unwrap());
                out.push(HistoryEntry {
                    rev: rev as i64,
                    unix_ms: times.get(&rev).copied().unwrap_or(0) as i64,
                    change,
                });
            }
        }
        Ok(out)
    }

    /// Drops history older than `keep_ms`, but never a revision etcd
    /// clients may still read, that is anything from the compaction
    /// revision on. Returns how many commits were collected.
    pub fn enforce_retention(&self, keep_ms: i64, now_ms: i64) -> Result<usize> {
        let _w = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let (keep, floor, total) = {
            let db = self.db.read().unwrap_or_else(|e| e.into_inner());
            let log = db.log()?;
            let cutoff = now_ms - keep_ms;
            // keep the newest commit older than the cutoff too: it is the
            // state at the cutoff itself
            let by_time = log
                .iter()
                .position(|c| (c.unix_ts_ms as i64) < cutoff)
                .map_or(log.len(), |i| i + 1);
            let by_etcd = (self.head() - self.compacted() + 1).max(1) as usize;
            let keep = by_time.max(by_etcd).min(log.len());
            (keep, log[keep - 1].commit_id as i64, log.len())
        };
        if keep >= total {
            return Ok(0);
        }
        let pruned = {
            let db = self.db.read().unwrap_or_else(|e| e.into_inner());
            let mut tx = db.begin();
            let old: Vec<Vec<u8>> = tx
                .scan(Some(&[LOG]), Some(&log_key(floor, 0)))?
                .map(|r| r.map(|(k, _)| k))
                .collect::<std::result::Result<_, _>>()?;
            for k in &old {
                tx.delete(k)?;
            }
            if old.is_empty() {
                false
            } else {
                let committed = tx.commit()? as i64;
                self.head
                    .store(committed, std::sync::atomic::Ordering::Release);
                true
            }
        };
        let mut db = self.db.write().unwrap_or_else(|e| e.into_inner());
        let report = db.gc(keep + usize::from(pruned))?;
        Ok(report.pruned_commits as usize)
    }

    fn start(db: Db<S>) -> Result<Self> {
        if db.head_commit() == 0 {
            let mut tx = db.begin();
            tx.put(&[META, b'v'], b"1")?;
            tx.commit()?;
        }
        let compacted = match db.snapshot().get(&prefixed(META, COMPACTED))? {
            Some(v) => decode_i64(&v)?,
            None => 0,
        };
        let head = db.head_commit() as i64;
        Ok(Quanty {
            db: RwLock::new(db),
            writer: Mutex::new(()),
            head: head.into(),
            compacted: compacted.into(),
        })
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
        self.head.load(std::sync::atomic::Ordering::Acquire)
    }

    fn compacted(&self) -> i64 {
        self.compacted.load(std::sync::atomic::Ordering::Acquire)
    }

    fn read(&self, rev: i64, f: &mut dyn FnMut(&dyn View) -> Result<()>) -> Result<()> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        if rev > db.head_commit() as i64 {
            return Err(Error::FutureRevision);
        }
        if rev > 0 && rev < self.compacted() {
            return Err(Error::Compacted(self.compacted()));
        }
        let snap = if rev == 0 {
            db.snapshot()
        } else {
            db.snapshot_at(rev as u64)?
        };
        f(&Reader { get: &snap })
    }

    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
        publish: &mut dyn FnMut(i64, &[Change]),
    ) -> Result<Option<(i64, Vec<Change>)>> {
        let _w = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let mut tx = db.begin();
        let rev = tx.pending_commit_id() as i64;
        let mut w = Writing {
            tx: &mut tx,
            rev,
            changes: Vec::new(),
            meta_dirty: false,
        };
        f(&mut w)?;
        let (changes, meta_dirty) = (w.changes, w.meta_dirty);
        if changes.is_empty() && !meta_dirty {
            return Ok(None);
        }
        for (i, c) in changes.iter().enumerate() {
            tx.put(&log_key(rev, i as u32), &encode_change(c))?;
        }
        let committed = tx.commit()? as i64;
        debug_assert_eq!(committed, rev);
        self.head.store(rev, std::sync::atomic::Ordering::Release);
        publish(rev, &changes);
        Ok(Some((rev, changes)))
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

    fn size(&self) -> Result<(i64, i64)> {
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        let st = db.stats()?;
        let page = quantydb_core::PagerOptions::default().page_size as i64;
        Ok((
            st.page_count as i64 * page,
            (st.page_count - st.free_pages) as i64 * page,
        ))
    }

    fn compact(&self, rev: i64) -> Result<()> {
        if rev <= self.compacted() {
            return Ok(());
        }
        if rev > self.head() {
            return Err(Error::FutureRevision);
        }
        let _w = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.db.read().unwrap_or_else(|e| e.into_inner());
        // bookkeeping is a commit like any other, so it takes a revision;
        // etcd does not, but no client depends on revisions being dense
        let mut tx = db.begin();
        let mut v = Vec::new();
        put_varint(&mut v, rev as u64);
        tx.put(&prefixed(META, COMPACTED), &v)?;
        let committed = tx.commit()? as i64;
        self.head
            .store(committed, std::sync::atomic::Ordering::Release);
        self.compacted
            .store(rev, std::sync::atomic::Ordering::Release);
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

    fn put(s: &Quanty<MemStorage>, k: &[u8], v: &[u8]) -> i64 {
        s.write(&mut |tx| tx.put(k, v, 0).map(|_| ()), &mut |_, _| {})
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
    fn a_write_that_changes_nothing_takes_no_revision() {
        let s = Quanty::in_memory().unwrap();
        let r = s
            .write(&mut |tx| tx.delete(b"/missing").map(|_| ()), &mut |_, _| {})
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
            .write(&mut |tx| tx.delete(b"/a").map(|_| ()), &mut |_, _| {})
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
    fn retention_keeps_what_etcd_clients_may_read_and_drops_the_rest() {
        let s = Quanty::in_memory().unwrap();
        for v in [b"1", b"2", b"3", b"4"] {
            put(&s, b"/k", v);
        }
        assert_eq!(s.history(b"/k", None).unwrap().len(), 4);
        s.compact(4).unwrap();
        let later = super::tests_now() + 1000;
        assert!(s.enforce_retention(0, later).unwrap() > 0);
        let kept: Vec<i64> = s
            .history(b"/k", None)
            .unwrap()
            .iter()
            .map(|h| h.rev)
            .collect();
        assert_eq!(kept, [4, 5]);
        assert!(s.read_retained(2, &mut |_| Ok(())).is_err());
        assert_eq!(get(&s, 4, b"/k").unwrap().value, b"3");
        assert_eq!(get(&s, 0, b"/k").unwrap().value, b"4");
    }

    #[test]
    fn ranges_respect_bounds_and_compaction_is_remembered() {
        let dir = std::env::temp_dir().join(format!("qk-store-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        {
            let s = Quanty::open(&dir).unwrap();
            for k in [&b"/a"[..], b"/b", b"/b/x", b"/c"] {
                s.write(&mut |tx| tx.put(k, b"v", 0).map(|_| ()), &mut |_, _| {})
                    .unwrap();
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
        assert_eq!(s.head(), 6);
        std::fs::remove_file(&dir).unwrap();
    }
}
