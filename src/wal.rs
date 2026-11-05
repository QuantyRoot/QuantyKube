//! The store's storage layer: a commit is durable once its pages are in
//! a log, and reaches the database file at a checkpoint (ADR-041).
//!
//! QuantyDB writes a commit as pages, syncs, writes the meta page that
//! makes them current and syncs again. Here the pages stay in memory;
//! the sync after a meta page appends everything the commit wrote to the
//! log as one frame and syncs the log, once. Reads look at memory before
//! the file. A checkpoint writes the newest copy of every page to the
//! file, syncs it, and only then empties the log.
//!
//! The log: a header, `QKWAL001` and the generation, then frames:
//!
//! ```text
//! "QKWF" | generation u64 | writes u32 | body length u64 | body | CRC-32C
//! body:  (offset u64 | length u32 | bytes) per write
//! ```
//!
//! all little endian, the checksum a CRC-32C over everything before it. Opening
//! replays every frame of the header's generation whose checksum holds,
//! up to the first that does not, and checkpoints at once.

use quantydb_core::{Error, FileStorage, Result, Storage};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

const HEADER_MAGIC: &[u8; 8] = b"QKWAL001";
const HEADER_LEN: usize = 16;
const FRAME_MAGIC: &[u8; 4] = b"QKWF";
const FRAME_HEAD: usize = 4 + 8 + 4 + 8;
const SUM: usize = 4;
/// The largest page QuantyDB writes; overlay entries are pages.
const MAX_PAGE: u64 = 65536;
/// The log is emptied into the file once it has grown past this.
pub const CHECKPOINT_BYTES: u64 = 64 << 20;
/// Written in advance beyond that, for the frame that crosses the line.
const PREPARED_SLACK: u64 = 4 << 20;

/// Where the log lives. A trait so that tests can cut it the way a
/// power cut would.
pub trait LogFile: Send + Sync {
    fn read_all(&self) -> io::Result<Vec<u8>>;
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    fn sync(&self) -> io::Result<()>;
    /// Makes sure the first `len` bytes exist on disk, zeros if new.
    fn prepare(&self, len: u64) -> io::Result<()>;
}

/// The log as a file next to the database.
pub struct DiskLog {
    file: Mutex<File>,
}

impl DiskLog {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(DiskLog {
            file: Mutex::new(file),
        })
    }

    fn file(&self) -> std::sync::MutexGuard<'_, File> {
        self.file.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl LogFile for DiskLog {
    fn read_all(&self) -> io::Result<Vec<u8>> {
        let mut f = self.file();
        let mut out = Vec::new();
        f.seek(SeekFrom::Start(0))?;
        f.read_to_end(&mut out)?;
        Ok(out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file().write_all_at(data, offset)
    }

    fn sync(&self) -> io::Result<()> {
        self.file().sync_data()
    }

    fn prepare(&self, len: u64) -> io::Result<()> {
        // written zeros, not a hole: a sync after writing into blocks
        // that already exist has no size or allocation to record, and
        // costs half of one after an append (measured)
        let mut f = self.file();
        let have = f.metadata()?.len();
        if have >= len {
            return Ok(());
        }
        f.seek(SeekFrom::Start(have))?;
        let zeros = vec![0u8; 1 << 20];
        let mut left = len - have;
        while left > 0 {
            let n = left.min(zeros.len() as u64) as usize;
            f.write_all(&zeros[..n])?;
            left -= n as u64;
        }
        f.sync_all()
    }
}

/// The log's path for the database at `db`.
pub fn log_path(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(".wal");
    PathBuf::from(p)
}

struct Pending {
    /// offsets written since the last commit point
    offsets: BTreeSet<u64>,
    /// whether a meta page is among them
    meta: bool,
}

struct LogState {
    generation: u64,
    len: u64,
}

pub struct WalStorage<M: Storage, L: LogFile> {
    main: M,
    log: L,
    enabled: bool,
    limit: u64,
    /// pages written since the last checkpoint, newest copy, by offset
    overlay: RwLock<BTreeMap<u64, Vec<u8>>>,
    /// the end of the highest page in the overlay
    end: RwLock<u64>,
    pending: Mutex<Pending>,
    state: Mutex<LogState>,
}

/// The store's file with its log next to it.
pub type FileStore = WalStorage<FileStorage, DiskLog>;

impl FileStore {
    /// Opens or creates the database at `path` and its log. Without
    /// `enabled` commits go to the file as before, but a log found from
    /// an earlier run is replayed and emptied first.
    pub fn open_file(path: &Path, enabled: bool) -> Result<(Self, bool)> {
        let created = !path.exists();
        let main = if created {
            // a log without its database belongs to one that is gone
            match std::fs::remove_file(log_path(path)) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(io(e)),
                _ => {}
            }
            FileStorage::create(path)?
        } else {
            FileStorage::open(path)?
        };
        let log = DiskLog::open(&log_path(path))?;
        Ok((
            WalStorage::open(main, log, enabled, CHECKPOINT_BYTES)?,
            created,
        ))
    }
}

fn is_meta(offset: u64, len: usize) -> bool {
    // the two meta slots are the first two pages, each one page long
    offset == 0 || offset == len as u64
}

fn header(generation: u64) -> Vec<u8> {
    let mut h = HEADER_MAGIC.to_vec();
    h.extend_from_slice(&generation.to_le_bytes());
    h
}

fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// The writes of every whole frame of the header's generation, in
/// order, and the generation; a log that does not start with a header
/// holds nothing.
fn replay(log: &[u8]) -> (u64, Vec<(u64, Vec<u8>)>) {
    if log.len() < HEADER_LEN || &log[..8] != HEADER_MAGIC {
        return (0, Vec::new());
    }
    let generation = u64_at(log, 8).unwrap_or(0);
    let mut writes = Vec::new();
    let mut at = HEADER_LEN;
    while let Some(frame) = read_frame(log, at, generation) {
        at = frame.0;
        writes.extend(frame.1);
    }
    (generation, writes)
}

type Frame = (usize, Vec<(u64, Vec<u8>)>);

fn read_frame(log: &[u8], at: usize, generation: u64) -> Option<Frame> {
    if log.get(at..at + 4)? != FRAME_MAGIC || u64_at(log, at + 4)? != generation {
        return None;
    }
    let count = u32_at(log, at + 12)? as usize;
    let body_len = usize::try_from(u64_at(log, at + 16)?).ok()?;
    let body_end = (at + FRAME_HEAD).checked_add(body_len)?;
    let sum = log.get(body_end..body_end.checked_add(SUM)?)?;
    if crc32c(&log[at..body_end]).to_le_bytes()[..] != sum[..] {
        return None;
    }
    let mut writes = Vec::with_capacity(count);
    let mut p = at + FRAME_HEAD;
    for _ in 0..count {
        let offset = u64_at(log, p)?;
        let len = u32_at(log, p + 8)? as usize;
        let bytes = log.get(p + 12..p + 12 + len)?.to_vec();
        writes.push((offset, bytes));
        p += 12 + len;
    }
    (p == body_end).then_some((body_end + SUM, writes))
}

/// CRC-32C, the checksum etcd's log uses, eight bytes at a time.
fn crc32c(data: &[u8]) -> u32 {
    static TABLES: std::sync::OnceLock<[[u32; 256]; 8]> = std::sync::OnceLock::new();
    let t = TABLES.get_or_init(|| {
        let mut t = [[0u32; 256]; 8];
        for (i, slot) in t[0].iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 == 1 {
                    (c >> 1) ^ 0x82f6_3b78
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        for k in 1..8 {
            for i in 0..256 {
                let prev = t[k - 1][i];
                t[k][i] = (prev >> 8) ^ t[0][(prev & 0xff) as usize];
            }
        }
        t
    });
    let mut c = !0u32;
    let (chunks, rest) = data.as_chunks::<8>();
    for b in chunks {
        let lo = c ^ u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        c = t[7][(lo & 0xff) as usize]
            ^ t[6][((lo >> 8) & 0xff) as usize]
            ^ t[5][((lo >> 16) & 0xff) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][b[4] as usize]
            ^ t[2][b[5] as usize]
            ^ t[1][b[6] as usize]
            ^ t[0][b[7] as usize];
    }
    for &b in rest {
        c = (c >> 8) ^ t[0][((c ^ b as u32) & 0xff) as usize];
    }
    !c
}

fn io(e: io::Error) -> Error {
    Error::Io(e)
}

impl<M: Storage, L: LogFile> WalStorage<M, L> {
    pub fn open(main: M, log: L, enabled: bool, limit: u64) -> Result<Self> {
        let (generation, writes) = replay(&log.read_all().map_err(io)?);
        for (offset, bytes) in &writes {
            main.write_at(*offset, bytes)?;
        }
        main.sync()?;
        // whatever was in the log is in the file now
        let generation = generation + 1;
        if enabled {
            log.prepare(limit + PREPARED_SLACK).map_err(io)?;
        }
        log.write_at(0, &header(generation)).map_err(io)?;
        log.sync().map_err(io)?;
        Ok(WalStorage {
            main,
            log,
            enabled,
            limit,
            overlay: RwLock::new(BTreeMap::new()),
            end: RwLock::new(0),
            pending: Mutex::new(Pending {
                offsets: BTreeSet::new(),
                meta: false,
            }),
            state: Mutex::new(LogState {
                generation,
                len: HEADER_LEN as u64,
            }),
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Writes every page in memory to the file, syncs it, and empties
    /// the log; only commits call it, and one at a time.
    fn checkpoint(&self, state: &mut LogState) -> Result<()> {
        let mut overlay = self.overlay.write().unwrap_or_else(|e| e.into_inner());
        for (offset, bytes) in overlay.iter() {
            self.main.write_at(*offset, bytes)?;
        }
        self.main.sync()?;
        // the frames after the header are of an older generation now,
        // and new ones overwrite them from the start
        state.generation += 1;
        self.log
            .write_at(0, &header(state.generation))
            .map_err(io)?;
        self.log.sync().map_err(io)?;
        state.len = HEADER_LEN as u64;
        overlay.clear();
        Ok(())
    }

    /// Appends what was written since the last commit point as one frame
    /// and syncs the log: the commit is durable when this returns.
    fn commit(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let offsets = {
            let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            if !p.meta {
                // the barrier between pages and meta; the frame orders them
                return Ok(());
            }
            p.meta = false;
            std::mem::take(&mut p.offsets)
        };
        let mut frame = FRAME_MAGIC.to_vec();
        frame.extend_from_slice(&state.generation.to_le_bytes());
        frame.extend_from_slice(&(offsets.len() as u32).to_le_bytes());
        frame.extend_from_slice(&[0; 8]);
        {
            let overlay = self.overlay.read().unwrap_or_else(|e| e.into_inner());
            for offset in &offsets {
                let bytes = &overlay[offset];
                frame.extend_from_slice(&offset.to_le_bytes());
                frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                frame.extend_from_slice(bytes);
            }
        }
        let body_len = (frame.len() - FRAME_HEAD) as u64;
        frame[16..24].copy_from_slice(&body_len.to_le_bytes());
        let sum = crc32c(&frame);
        frame.extend_from_slice(&sum.to_le_bytes());
        self.log.write_at(state.len, &frame).map_err(io)?;
        self.log.sync().map_err(io)?;
        state.len += frame.len() as u64;
        if state.len > self.limit {
            self.checkpoint(&mut state)?;
        }
        Ok(())
    }
}

impl<M: Storage, L: LogFile> Storage for WalStorage<M, L> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        if !self.enabled {
            return self.main.read_at(offset, buf);
        }
        let overlay = self.overlay.read().unwrap_or_else(|e| e.into_inner());
        if let Some(page) = overlay.get(&offset) {
            if page.len() == buf.len() {
                buf.copy_from_slice(page);
                return Ok(());
            }
        }
        let want = offset + buf.len() as u64;
        // what the file has, then every page in memory laid over it
        let file_len = self.main.len()?;
        let mut covered = vec![false; buf.len()];
        if offset < file_len {
            let n = (file_len.min(want) - offset) as usize;
            self.main.read_at(offset, &mut buf[..n])?;
            covered[..n].fill(true);
        }
        for (&at, page) in overlay.range(offset.saturating_sub(MAX_PAGE)..want) {
            let (from, to) = (at.max(offset), (at + page.len() as u64).min(want));
            if from >= to {
                continue;
            }
            let (b, p) = ((from - offset) as usize, (from - at) as usize);
            let n = (to - from) as usize;
            buf[b..b + n].copy_from_slice(&page[p..p + n]);
            covered[b..b + n].fill(true);
        }
        if covered.contains(&false) {
            return Err(Error::Io(io::Error::from(io::ErrorKind::UnexpectedEof)));
        }
        Ok(())
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        if !self.enabled {
            return self.main.write_at(offset, buf);
        }
        self.overlay
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(offset, buf.to_vec());
        {
            let mut end = self.end.write().unwrap_or_else(|e| e.into_inner());
            *end = (*end).max(offset + buf.len() as u64);
        }
        let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.offsets.insert(offset);
        p.meta |= is_meta(offset, buf.len());
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        if !self.enabled {
            return self.main.sync();
        }
        self.commit()
    }

    fn len(&self) -> Result<u64> {
        let end = *self.end.read().unwrap_or_else(|e| e.into_inner());
        Ok(self.main.len()?.max(end))
    }
}

impl<M: Storage, L: LogFile> Drop for WalStorage<M, L> {
    fn drop(&mut self) {
        if self.enabled {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let _ = self.checkpoint(&mut state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Backend, Quanty};
    use quantydb_core::MemStorage;
    use std::sync::Arc;

    /// A disk that keeps what was synced and, cut at any moment, any
    /// prefix of what was written after, the last write maybe torn.
    #[derive(Default, Clone)]
    struct Disk {
        durable: Vec<u8>,
        /// writes since the last sync: offset and bytes
        unsynced: Vec<(u64, Vec<u8>)>,
    }

    impl Disk {
        fn apply(image: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
            let end = offset as usize + bytes.len();
            if image.len() < end {
                image.resize(end, 0);
            }
            image[offset as usize..end].copy_from_slice(bytes);
        }

        fn now(&self) -> Vec<u8> {
            self.cut(usize::MAX, usize::MAX)
        }

        fn cut(&self, keep: usize, torn: usize) -> Vec<u8> {
            let mut image = self.durable.clone();
            let n = self.unsynced.len().min(keep);
            for (i, (o, b)) in self.unsynced[..n].iter().enumerate() {
                let b = if i + 1 == n {
                    &b[..torn.min(b.len())]
                } else {
                    &b[..]
                };
                Self::apply(&mut image, *o, b);
            }
            image
        }

        fn synced(&mut self) {
            for (o, b) in std::mem::take(&mut self.unsynced) {
                Self::apply(&mut self.durable, o, &b);
            }
        }
    }

    /// The database file and the log, cut together after every single
    /// operation on either while `cutting`.
    #[derive(Default)]
    struct World {
        file: Disk,
        log: Disk,
        cutting: bool,
        acked: u32,
        seed: u64,
        cuts: Vec<(u32, Vec<u8>, Vec<u8>)>,
    }

    impl World {
        fn rng(&mut self) -> usize {
            self.seed ^= self.seed << 13;
            self.seed ^= self.seed >> 7;
            self.seed ^= self.seed << 17;
            self.seed as usize
        }

        fn after_op(&mut self) {
            if !self.cutting {
                return;
            }
            let keep_f = self.rng() % (self.file.unsynced.len() + 1);
            let keep_l = self.rng() % (self.log.unsynced.len() + 1);
            let torn = self.rng() % 20000;
            let cut = (
                self.acked,
                self.file.cut(keep_f, torn),
                self.log.cut(keep_l, torn),
            );
            self.cuts.push(cut);
        }
    }

    #[derive(Clone, Default)]
    struct W(Arc<Mutex<World>>);

    impl W {
        fn w(&self) -> std::sync::MutexGuard<'_, World> {
            self.0.lock().unwrap()
        }
    }

    struct SimFile(W);
    struct SimLog(W);

    impl Storage for SimFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
            let image = self.0.w().file.now();
            let part = image
                .get(offset as usize..offset as usize + buf.len())
                .ok_or(Error::Io(io::Error::from(io::ErrorKind::UnexpectedEof)))?;
            buf.copy_from_slice(part);
            Ok(())
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
            let mut w = self.0.w();
            w.file.unsynced.push((offset, buf.to_vec()));
            w.after_op();
            Ok(())
        }

        fn sync(&self) -> Result<()> {
            let mut w = self.0.w();
            w.file.synced();
            w.after_op();
            Ok(())
        }

        fn len(&self) -> Result<u64> {
            Ok(self.0.w().file.now().len() as u64)
        }
    }

    impl LogFile for SimLog {
        fn read_all(&self) -> io::Result<Vec<u8>> {
            Ok(self.0.w().log.now())
        }

        fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            let mut w = self.0.w();
            w.log.unsynced.push((offset, data.to_vec()));
            w.after_op();
            Ok(())
        }

        fn sync(&self) -> io::Result<()> {
            let mut w = self.0.w();
            w.log.synced();
            w.after_op();
            Ok(())
        }

        fn prepare(&self, len: u64) -> io::Result<()> {
            let mut w = self.0.w();
            if (w.log.now().len() as u64) < len {
                let at = w.log.now().len();
                w.log.unsynced.push((at as u64, vec![0; len as usize - at]));
                w.log.synced();
            }
            Ok(())
        }
    }

    type Sim = WalStorage<SimFile, SimLog>;

    fn open(w: &W, limit: u64) -> Quanty<Sim> {
        let s = WalStorage::open(SimFile(w.clone()), SimLog(w.clone()), true, limit).unwrap();
        Quanty::with_storage(s).unwrap()
    }

    fn get(q: &Quanty<Sim>, key: &str) -> Option<Vec<u8>> {
        let mut out = None;
        q.read(0, &mut |v| {
            out = v.get(key.as_bytes())?.map(|r| r.value);
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn every_acknowledged_write_survives_a_cut_anywhere() {
        // small enough that checkpoints happen, and are cut too
        let limit = 32 << 10;
        let w = W::default();
        w.w().seed = 0x9e37_79b9_7f4a_7c15;
        let q = open(&w, limit);
        w.w().cutting = true;
        for i in 0..60u32 {
            q.write(&mut |tx| {
                tx.put(format!("k{i:03}").as_bytes(), &[b'v'; 300], 0)?;
                Ok(())
            })
            .unwrap();
            w.w().acked = i + 1;
        }
        let (cuts, generation) = {
            let mut g = w.w();
            g.cutting = false;
            let generation = u64_at(&g.log.now(), 8).unwrap();
            (std::mem::take(&mut g.cuts), generation)
        };
        assert!(generation > 3, "no checkpoint was cut");
        assert!(cuts.len() > 300, "{} cuts", cuts.len());
        // every cut is a different moment; opening a few hundred of them
        // covers each kind and keeps the test short
        let step = (cuts.len() / 300).max(1);
        for (acked, file, log) in cuts.into_iter().step_by(step) {
            let w = W::default();
            (w.w().file.durable, w.w().log.durable) = (file, log);
            let q = open(&w, limit);
            for i in 0..acked {
                assert_eq!(
                    get(&q, &format!("k{i:03}")),
                    Some(vec![b'v'; 300]),
                    "write {i}, acknowledged before a cut after {acked}"
                );
            }
        }
    }

    #[test]
    fn one_commit_syncs_once() {
        struct Counting(SimLog, Arc<Mutex<u32>>);
        impl LogFile for Counting {
            fn read_all(&self) -> io::Result<Vec<u8>> {
                self.0.read_all()
            }
            fn write_at(&self, o: u64, d: &[u8]) -> io::Result<()> {
                self.0.write_at(o, d)
            }
            fn sync(&self) -> io::Result<()> {
                *self.1.lock().unwrap() += 1;
                self.0.sync()
            }
            fn prepare(&self, len: u64) -> io::Result<()> {
                self.0.prepare(len)
            }
        }
        let syncs = Arc::new(Mutex::new(0));
        let log = Counting(SimLog(W::default()), syncs.clone());
        let s = WalStorage::open(MemStorage::new(), log, true, CHECKPOINT_BYTES).unwrap();
        let q = Quanty::with_storage(s).unwrap();
        let before = *syncs.lock().unwrap();
        q.write(&mut |tx| {
            tx.put(b"a", b"1", 0)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(*syncs.lock().unwrap() - before, 1);
    }

    #[test]
    fn a_new_database_does_not_take_an_old_log() {
        let dir = std::env::temp_dir().join(format!("qk-wal-old-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("db");
        {
            let q = Quanty::open_with(&db, true).unwrap();
            q.write(&mut |tx| {
                tx.put(b"old", b"1", 0)?;
                Ok(())
            })
            .unwrap();
            // gone without a checkpoint, as when the process is killed
            std::mem::forget(q);
        }
        std::fs::rename(&db, dir.join("kept")).unwrap();
        let q = Quanty::open_with(&db, true).unwrap();
        let mut old = None;
        q.read(0, &mut |v| {
            old = v.get(b"old")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(old, None);
        drop(q);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crc32c_of_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn a_torn_or_foreign_frame_ends_the_replay() {
        let frame = |generation: u64, offset: u64| {
            let mut f = FRAME_MAGIC.to_vec();
            f.extend_from_slice(&generation.to_le_bytes());
            f.extend_from_slice(&1u32.to_le_bytes());
            f.extend_from_slice(&15u64.to_le_bytes());
            f.extend_from_slice(&offset.to_le_bytes());
            f.extend_from_slice(&3u32.to_le_bytes());
            f.extend_from_slice(b"abc");
            let sum = crc32c(&f);
            f.extend_from_slice(&sum.to_le_bytes());
            f
        };
        let mut log = header(7);
        log.extend(frame(7, 4096));
        log.extend(frame(6, 8192));
        log.extend(frame(7, 12288));
        assert_eq!(replay(&log).1, vec![(4096, b"abc".to_vec())]);
        let mut torn = header(7);
        torn.extend(frame(7, 4096));
        let second = frame(7, 8192);
        torn.extend_from_slice(&second[..second.len() - 1]);
        assert_eq!(replay(&torn).1, vec![(4096, b"abc".to_vec())]);
        assert!(replay(b"garbage at the start of the file").1.is_empty());
    }
}
