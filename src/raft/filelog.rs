//! The Raft log in a QuantyDB file of its own, next to the store's
//! (ADR-034): term, vote and commit under one key, the snapshot's index
//! and term under another, each entry under its index.

use super::host::LogStore;
use super::{Entry, HardState, SnapshotMeta};
use quantydb_core::{Db, FileStorage};
use std::io;
use std::path::Path;

const HARD: &[u8] = b"h";
const SNAP: &[u8] = b"s";
const ENTRY: u8 = b'e';
/// Commits between collections of the file's old snapshots; QuantyDB
/// keeps every commit until told otherwise.
const GC_EVERY: u32 = 1000;

pub struct FileLog {
    db: Db<FileStorage>,
    commits: u32,
}

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn entry_key(index: u64) -> [u8; 9] {
    let mut k = [ENTRY; 9];
    k[1..].copy_from_slice(&index.to_be_bytes());
    k
}

fn u64s(v: &[u64]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_be_bytes()).collect()
}

fn read_u64s(b: &[u8], n: usize) -> io::Result<Vec<u64>> {
    if b.len() != n * 8 {
        return Err(err("raft log: a record of the wrong size"));
    }
    Ok(b.chunks(8)
        .map(|c| u64::from_be_bytes(c.try_into().unwrap()))
        .collect())
}

impl FileLog {
    pub fn open(path: &Path) -> io::Result<FileLog> {
        let db = if path.exists() {
            Db::open_file(path)
        } else {
            Db::create_file(path)
        }
        .map_err(err)?;
        Ok(FileLog { db, commits: 0 })
    }

    fn committed(&mut self) -> io::Result<()> {
        self.commits += 1;
        if self.commits >= GC_EVERY {
            self.commits = 0;
            self.db.gc(1).map_err(err)?;
        }
        Ok(())
    }
}

impl LogStore for FileLog {
    fn load(&mut self) -> io::Result<(HardState, SnapshotMeta, Vec<Entry>)> {
        let snap = self.db.snapshot();
        let mut hard = HardState::default();
        if let Some(v) = snap.get(HARD).map_err(err)? {
            let x = read_u64s(&v, 3)?;
            hard = HardState {
                term: x[0],
                vote: (x[1] != 0).then_some(x[1]),
                commit: x[2],
            };
        }
        let mut meta = SnapshotMeta::default();
        if let Some(v) = snap.get(SNAP).map_err(err)? {
            let x = read_u64s(&v, 2)?;
            meta = SnapshotMeta {
                index: x[0],
                term: x[1],
            };
        }
        let mut entries = Vec::new();
        for item in snap.scan(Some(&[ENTRY]), Some(&[ENTRY + 1])).map_err(err)? {
            let (k, v) = item.map_err(err)?;
            let index = u64::from_be_bytes(k[1..9].try_into().map_err(err)?);
            if v.len() < 8 {
                return Err(err("raft log: a short entry"));
            }
            entries.push(Entry {
                index,
                term: u64::from_be_bytes(v[..8].try_into().unwrap()),
                data: v[8..].to_vec(),
            });
        }
        if let Some((i, e)) = entries
            .iter()
            .enumerate()
            .find(|(i, e)| e.index != meta.index + 1 + *i as u64)
        {
            return Err(err(format!(
                "raft log: entry {} where {} belongs",
                e.index, i
            )));
        }
        Ok((hard, meta, entries))
    }

    fn save(&mut self, hard: Option<HardState>, entries: &[Entry]) -> io::Result<()> {
        let mut tx = self.db.begin();
        if let Some(h) = hard {
            tx.put(HARD, &u64s(&[h.term, h.vote.unwrap_or(0), h.commit]))
                .map_err(err)?;
        }
        if let Some(first) = entries.first() {
            // what followed the first new entry is replaced
            let stale: Vec<Vec<u8>> = tx
                .scan(Some(&entry_key(first.index)), Some(&[ENTRY + 1]))
                .map_err(err)?
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_, _>>()
                .map_err(err)?;
            for k in stale {
                tx.delete(&k).map_err(err)?;
            }
            for e in entries {
                let mut v = e.term.to_be_bytes().to_vec();
                v.extend_from_slice(&e.data);
                tx.put(&entry_key(e.index), &v).map_err(err)?;
            }
        }
        tx.commit().map_err(err)?;
        self.committed()
    }

    fn reset(&mut self, snap: SnapshotMeta) -> io::Result<()> {
        let mut tx = self.db.begin();
        let all: Vec<Vec<u8>> = tx
            .scan(Some(&[ENTRY]), Some(&[ENTRY + 1]))
            .map_err(err)?
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<_, _>>()
            .map_err(err)?;
        for k in all {
            tx.delete(&k).map_err(err)?;
        }
        tx.put(SNAP, &u64s(&[snap.index, snap.term])).map_err(err)?;
        tx.commit().map_err(err)?;
        self.committed()
    }

    fn compact(&mut self, snap: SnapshotMeta) -> io::Result<()> {
        let mut tx = self.db.begin();
        let old: Vec<Vec<u8>> = tx
            .scan(Some(&[ENTRY]), Some(&entry_key(snap.index + 1)))
            .map_err(err)?
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<_, _>>()
            .map_err(err)?;
        for k in old {
            tx.delete(&k).map_err(err)?;
        }
        tx.put(SNAP, &u64s(&[snap.index, snap.term])).map_err(err)?;
        tx.commit().map_err(err)?;
        self.committed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_survives_a_reopen() {
        let dir = std::env::temp_dir().join(format!("qk-raftlog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("raft.db");
        let e = |index, term| Entry {
            index,
            term,
            data: vec![index as u8; 5],
        };
        {
            let mut log = FileLog::open(&path).unwrap();
            let hard = HardState {
                term: 3,
                vote: Some(2),
                commit: 2,
            };
            log.save(Some(hard), &[e(1, 1), e(2, 1), e(3, 2), e(4, 2)])
                .unwrap();
            // a new leader replaces the tail
            log.save(None, &[e(3, 3)]).unwrap();
            log.compact(SnapshotMeta { index: 1, term: 1 }).unwrap();
        }
        let mut log = FileLog::open(&path).unwrap();
        let (hard, snap, entries) = log.load().unwrap();
        assert_eq!(hard.vote, Some(2));
        assert_eq!(snap, SnapshotMeta { index: 1, term: 1 });
        assert_eq!(entries, vec![e(2, 1), e(3, 3)]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
