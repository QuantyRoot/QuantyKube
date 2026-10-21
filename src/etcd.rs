//! The etcd v3 gRPC API on top of a [`Backend`]. `docs/RPCS.md` lists what
//! the Kubernetes API server calls; everything else answers UNIMPLEMENTED.

use crate::etcdpb::*;
use crate::store::{self, Backend, Change, Kind, Record, Tx, View};
use quantycore::grpc::{code, Responder, Service, Status};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// What `Status` reports. The API server only uses watch progress requests
/// when the version says the server has them, which 3.6 does.
pub const ETCD_VERSION: &str = "3.6.5";

const CLUSTER_ID: u64 = 0x5155_414e_5459_4b55;
const MEMBER_ID: u64 = 0x4b55_4245_0000_0001;
const PROGRESS_EVERY: Duration = Duration::from_secs(600);
const EVENTS_PER_RESPONSE: usize = 1000;
/// Messages a watch stream may have waiting. A stream whose client reads
/// slower than the store writes falls behind instead of growing without
/// bound, and catches up from the change log (as etcd's victims do).
const INBOX: usize = 1024;

pub struct Etcd<B: Backend> {
    store: Arc<B>,
    hub: Arc<Hub>,
    leases: Mutex<HashMap<i64, Lease>>,
    endpoint: String,
    /// How often a watch asking for progress notifications gets one;
    /// shortened by tests.
    progress_ms: AtomicU64,
    /// EVENTS_PER_RESPONSE and INBOX, shortened by tests.
    events_per_response: AtomicUsize,
    inbox: AtomicUsize,
    /// Set when this store is one node of several (ADR-034).
    cluster: OnceLock<Cluster>,
    /// The Raft index of the command being applied, kept with every write
    /// it makes; 0 outside a cluster.
    applying: AtomicU64,
}

const RAFT_APPLIED: &[u8] = b"raft-applied";

struct Cluster {
    id: u64,
    host: Arc<crate::raft::host::Host>,
    lease_seq: AtomicU64,
    leading: std::sync::atomic::AtomicBool,
}

/// How long a write or a linearizable read waits for the cluster.
const CLUSTER_WAIT: Duration = Duration::from_secs(5);

/// The methods that change the store go through the Raft log; every
/// node applies them in the same order and gets the same answer.
fn is_write(method: &str) -> bool {
    matches!(
        method,
        "/etcdserverpb.KV/Put"
            | "/etcdserverpb.KV/DeleteRange"
            | "/etcdserverpb.KV/Txn"
            | "/etcdserverpb.KV/Compact"
            | "/etcdserverpb.Lease/LeaseGrant"
            | "/etcdserverpb.Lease/LeaseRevoke"
            | "/quantykube.History/Restore"
    )
}

fn encode_command(origin: u64, req: u64, method: &str, body: &[u8], unix_ms: i64) -> Vec<u8> {
    use quantycore::proto::{put_bytes, put_int, put_uint};
    let mut out = Vec::new();
    put_uint(&mut out, 1, origin);
    put_uint(&mut out, 2, req);
    put_bytes(&mut out, 3, method.as_bytes());
    put_bytes(&mut out, 4, body);
    put_int(&mut out, 5, unix_ms);
    out
}

fn encode_answer(r: &Result<Vec<u8>, Status>) -> Vec<u8> {
    use quantycore::proto::{put_bytes, put_message, put_uint};
    let mut out = Vec::new();
    match r {
        Ok(body) => put_message(&mut out, 1, body),
        Err(s) => {
            put_uint(&mut out, 2, u64::from(s.code));
            put_bytes(&mut out, 3, s.message.as_bytes());
        }
    }
    out
}

fn decode_answer(buf: &[u8]) -> Result<Vec<u8>, Status> {
    let (mut body, mut code, mut message) = (None, 0, String::new());
    let mut r = quantycore::proto::Reader::new(buf);
    while let Some((n, v)) = r.next_field().map_err(bad_request)? {
        match n {
            1 => body = Some(v.bytes(n).map_err(bad_request)?.to_vec()),
            2 => code = v.uint(n).map_err(bad_request)? as u32,
            3 => message = String::from_utf8_lossy(v.bytes(n).map_err(bad_request)?).into(),
            _ => {}
        }
    }
    match body {
        Some(b) if code == 0 => Ok(b),
        _ => Err(Status::new(code, message)),
    }
}

/// The leases kept in the store's metadata. Reading it needs a write
/// transaction, which commits nothing when nothing changes.
fn load_leases<B: Backend>(store: &B) -> Result<HashMap<i64, Lease>, store::Error> {
    let mut leases = HashMap::new();
    store.write(&mut |tx| {
        tx.meta_scan(b"lease/", &mut |k, v| {
            // as etcd after a restart or a leader change: every lease
            // gets its full TTL again, since keepalives are not stored
            if let (Ok(id), Some((ttl, _))) = (<[u8; 8]>::try_from(&k[6..]), decode_pair(v)) {
                leases.insert(
                    u64::from_be_bytes(id) as i64,
                    Lease {
                        ttl,
                        expires_ms: now_ms() + ttl * 1000,
                    },
                );
            }
            true
        })
    })?;
    Ok(leases)
}

/// Set as a watch's `lagged` when the store it watched was replaced.
const REPLACED: i64 = -1;

/// Applies committed commands to the store it belongs to.
struct Applier<B: Backend>(Weak<Etcd<B>>);

impl<B: Backend> crate::raft::host::Machine for Applier<B> {
    fn applied(&self) -> u64 {
        let Some(etcd) = self.0.upgrade() else {
            return 0;
        };
        let mut index = 0;
        let _ = etcd.store.write(&mut |tx| {
            if let Some(v) = tx.meta_get(RAFT_APPLIED)? {
                index = v.try_into().map(u64::from_be_bytes).unwrap_or(0);
            }
            Ok(())
        });
        index
    }

    fn snapshot(&self) -> std::io::Result<Vec<u8>> {
        let etcd = self
            .0
            .upgrade()
            .ok_or_else(|| std::io::Error::other("store gone"))?;
        etcd.store
            .export()
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn restore(&self, index: u64, data: &[u8]) -> std::io::Result<()> {
        let etcd = self
            .0
            .upgrade()
            .ok_or_else(|| std::io::Error::other("store gone"))?;
        let io = |e: store::Error| std::io::Error::other(e.to_string());
        etcd.store.import(data).map_err(io)?;
        etcd.applying.store(index, Ordering::Relaxed);
        let kept = etcd.write(&mut |_| Ok(()));
        etcd.applying.store(0, Ordering::Relaxed);
        kept.map_err(io)?;
        *etcd.leases.lock().unwrap() = load_leases(etcd.store.as_ref()).map_err(io)?;
        etcd.hub.reset(etcd.store.head());
        Ok(())
    }

    fn apply(&self, index: u64, data: &[u8]) -> Option<(u64, Vec<u8>)> {
        let etcd = self.0.upgrade()?;
        let (mut origin, mut req, mut method, mut body, mut unix_ms) =
            (0, 0, String::new(), Vec::new(), 0);
        let mut r = quantycore::proto::Reader::new(data);
        while let Ok(Some((n, v))) = r.next_field() {
            match n {
                1 => origin = v.uint(n).unwrap_or(0),
                2 => req = v.uint(n).unwrap_or(0),
                3 => method = String::from_utf8_lossy(v.bytes(n).unwrap_or_default()).into(),
                4 => body = v.bytes(n).unwrap_or_default().to_vec(),
                5 => unix_ms = v.int(n).unwrap_or(0),
                _ => {}
            }
        }
        etcd.applying.store(index, Ordering::Relaxed);
        let answer = etcd.serve(&method, &body, unix_ms);
        if method == "/etcdserverpb.KV/Compact" {
            // compaction writes through the store, not through a command
            let _ = etcd.write(&mut |_| Ok(()));
        }
        etcd.applying.store(0, Ordering::Relaxed);
        let mine = etcd.cluster.get().is_some_and(|c| c.id == origin);
        mine.then(|| (req, encode_answer(&answer)))
    }
}

#[derive(Clone, Copy)]
struct Lease {
    ttl: i64,
    expires_ms: i64,
}

fn status(code: u32, msg: &str) -> Status {
    Status::new(code, format!("etcdserver: {msg}"))
}

fn store_error(e: store::Error) -> Status {
    let c = match e {
        store::Error::Compacted(_)
        | store::Error::FutureRevision
        | store::Error::NotRetained(_) => code::OUT_OF_RANGE,
        store::Error::KeyTooLong => code::INVALID_ARGUMENT,
        store::Error::Engine(_)
        | store::Error::Corrupt(_)
        | store::Error::Aborted
        | store::Error::Commit(_) => code::INTERNAL,
    };
    status(c, &e.to_string())
}

fn bad_request(_: quantycore::proto::DecodeError) -> Status {
    Status::new(code::INVALID_ARGUMENT, "malformed request")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn kv(r: Record) -> KeyValue {
    KeyValue {
        key: r.key,
        create_revision: r.create_rev,
        mod_revision: r.mod_rev,
        version: r.version,
        value: r.value,
        lease: r.lease,
    }
}

fn lease_key(id: i64) -> Vec<u8> {
    let mut k = b"lease/".to_vec();
    k.extend_from_slice(&(id as u64).to_be_bytes());
    k
}

fn lease_index(id: i64, key: &[u8]) -> Vec<u8> {
    let mut k = b"lkey/".to_vec();
    k.extend_from_slice(&(id as u64).to_be_bytes());
    k.extend_from_slice(key);
    k
}

/// etcd's range convention: no end means one key, an end of a single zero
/// byte means every key from `key` on.
fn range_end(end: &[u8]) -> Option<Option<&[u8]>> {
    match end {
        [] => None,
        [0] => Some(None),
        e => Some(Some(e)),
    }
}

/// etcd's limit on compares and on operations in each branch of a txn.
const MAX_TXN_OPS: usize = 128;

fn empty_key() -> Status {
    status(code::INVALID_ARGUMENT, "etcdserver: key is not provided")
}

/// Refuses what etcd refuses before running a txn: too many operations,
/// an empty key, a key put twice, or a key both put and deleted in one
/// branch. Puts in either branch of a nested txn count for the outer one.
fn check_txn(req: &TxnRequest) -> Result<(), Status> {
    check_branch(req, &req.success)?;
    check_branch(req, &req.failure)?;
    Ok(())
}

type Keys = (Vec<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>);

fn check_branch(req: &TxnRequest, ops: &[RequestOp]) -> Result<Keys, Status> {
    let too_many = |n: usize| n > MAX_TXN_OPS;
    if too_many(req.compare.len()) || too_many(req.success.len()) || too_many(req.failure.len()) {
        return Err(status(
            code::INVALID_ARGUMENT,
            "etcdserver: too many operations in txn request",
        ));
    }
    let dup = || {
        status(
            code::INVALID_ARGUMENT,
            "etcdserver: duplicate key given in txn request",
        )
    };
    let (mut puts, mut dels): Keys = (Vec::new(), Vec::new());
    for op in ops {
        match &op.request {
            Some(OpRequest::Range(r)) if r.key.is_empty() => return Err(empty_key()),
            Some(OpRequest::Range(_)) | None => {}
            Some(OpRequest::Put(p)) => {
                if p.key.is_empty() {
                    return Err(empty_key());
                }
                if puts.contains(&p.key) {
                    return Err(dup());
                }
                puts.push(p.key.clone());
            }
            Some(OpRequest::DeleteRange(d)) => {
                if d.key.is_empty() {
                    return Err(empty_key());
                }
                dels.push((d.key.clone(), d.range_end.clone()));
            }
            Some(OpRequest::Txn(t)) => {
                let (sp, sd) = check_branch(t, &t.success)?;
                let (fp, fd) = check_branch(t, &t.failure)?;
                let mut inner = sp;
                for k in fp {
                    if !inner.contains(&k) {
                        inner.push(k);
                    }
                }
                if inner.iter().any(|k| puts.contains(k)) {
                    return Err(dup());
                }
                puts.extend(inner);
                dels.extend(sd);
                dels.extend(fd);
            }
        }
    }
    if dels
        .iter()
        .any(|(start, end)| puts.iter().any(|k| in_range(k, start, end)))
    {
        return Err(dup());
    }
    Ok((puts, dels))
}

fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    match range_end(end) {
        None => key == start,
        Some(None) => key >= start,
        Some(Some(e)) => key >= start && key < e,
    }
}

impl<B: Backend> Etcd<B> {
    pub fn new(store: Arc<B>, endpoint: String) -> Result<Arc<Self>, store::Error> {
        store.read(0, &mut |_| Ok(()))?;
        let leases = load_leases(store.as_ref())?;
        let hub = Arc::new(Hub {
            state: Mutex::new(HubState {
                last_rev: store.head(),
                next_id: 0,
                subs: HashMap::new(),
            }),
        });
        let publish = hub.clone();
        store.set_publisher(Box::new(move |rev, changes| publish.publish(rev, changes)));
        let etcd = Arc::new(Etcd {
            store,
            hub,
            leases: Mutex::new(leases),
            endpoint,
            progress_ms: AtomicU64::new(PROGRESS_EVERY.as_millis() as u64),
            events_per_response: AtomicUsize::new(EVENTS_PER_RESPONSE),
            inbox: AtomicUsize::new(INBOX),
            cluster: OnceLock::new(),
            applying: AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&etcd);
        std::thread::spawn(move || {
            while let Some(e) = weak.upgrade() {
                e.expire_leases();
                drop(e);
                std::thread::sleep(Duration::from_millis(500));
            }
        });
        Ok(etcd)
    }

    fn member_id(&self) -> u64 {
        self.cluster.get().map_or(MEMBER_ID, |c| MEMBER_ID + c.id)
    }

    fn header(&self, rev: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: CLUSTER_ID,
            member_id: self.member_id(),
            revision: rev,
            raft_term: 1,
        })
    }

    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> store::Result<()>,
    ) -> store::Result<Option<i64>> {
        let index = self.applying.load(Ordering::Relaxed);
        let mut with_index = |tx: &mut dyn Tx| {
            f(tx)?;
            // in the same commit as what the command wrote, so that a
            // restart knows exactly where to go on from
            if index > 0 {
                tx.meta_put(RAFT_APPLIED, &index.to_be_bytes())?;
            }
            Ok(())
        };
        Ok(self.store.write(&mut with_index)?.map(|(rev, _)| rev))
    }

    fn range(&self, req: &RangeRequest, view: &dyn View) -> store::Result<RangeResponse> {
        let mut kvs = Vec::new();
        let mut count = 0i64;
        let keep = |r: &Record| {
            (req.min_mod_revision == 0 || r.mod_rev >= req.min_mod_revision)
                && (req.max_mod_revision == 0 || r.mod_rev <= req.max_mod_revision)
                && (req.min_create_revision == 0 || r.create_rev >= req.min_create_revision)
                && (req.max_create_revision == 0 || r.create_rev <= req.max_create_revision)
        };
        let sorted = !matches!(
            (req.sort_order, req.sort_target),
            (SortOrder::None, _) | (SortOrder::Ascend, SortTarget::Key)
        );
        let limit = if req.limit > 0 && !sorted {
            req.limit as usize
        } else {
            usize::MAX
        };
        let mut take = |r: Record| {
            if !keep(&r) {
                return true;
            }
            count += 1;
            if !req.count_only && kvs.len() < limit {
                let mut k = kv(r);
                if req.keys_only {
                    k.value.clear();
                }
                kvs.push(k);
            }
            true
        };
        match range_end(&req.range_end) {
            None => {
                if let Some(r) = view.get(&req.key)? {
                    take(r);
                }
            }
            Some(end) => view.scan(&req.key, end, &mut take)?,
        }
        if sorted {
            let key = |k: &KeyValue| -> (i64, Vec<u8>) {
                match req.sort_target {
                    SortTarget::Version => (k.version, Vec::new()),
                    SortTarget::Create => (k.create_revision, Vec::new()),
                    SortTarget::Mod => (k.mod_revision, Vec::new()),
                    SortTarget::Value => (0, k.value.clone()),
                    _ => (0, k.key.clone()),
                }
            };
            kvs.sort_by_key(key);
            if req.sort_order == SortOrder::Descend {
                kvs.reverse();
            }
            if req.limit > 0 {
                kvs.truncate(req.limit as usize);
            }
        }
        Ok(RangeResponse {
            header: None,
            more: (kvs.len() as i64) < count && !req.count_only,
            count,
            kvs,
        })
    }

    fn compare(c: &Compare, view: &dyn View) -> store::Result<bool> {
        let mut records = Vec::new();
        match range_end(&c.range_end) {
            None => {
                if let Some(r) = view.get(&c.key)? {
                    records.push(r);
                }
            }
            Some(end) => view.scan(&c.key, end, &mut |r| {
                records.push(r);
                true
            })?,
        }
        if records.is_empty() {
            // a missing key compares as all zeros, except by value
            if c.target == CompareTarget::Value {
                return Ok(false);
            }
            records.push(Record::default());
        }
        let ord = |a: i64, b: i64| a.cmp(&b);
        Ok(records.iter().all(|r| {
            let o = match &c.target_union {
                Some(TargetUnion::Version(v)) => ord(r.version, *v),
                Some(TargetUnion::CreateRevision(v)) => ord(r.create_rev, *v),
                Some(TargetUnion::ModRevision(v)) => ord(r.mod_rev, *v),
                Some(TargetUnion::Lease(v)) => ord(r.lease, *v),
                Some(TargetUnion::Value(v)) => r.value.as_slice().cmp(v.as_slice()),
                None => std::cmp::Ordering::Equal,
            };
            match c.result {
                CompareResult::Equal => o.is_eq(),
                CompareResult::Greater => o.is_gt(),
                CompareResult::Less => o.is_lt(),
                CompareResult::NotEqual => o.is_ne(),
                CompareResult::Other(_) => false,
            }
        }))
    }

    fn put(&self, tx: &mut dyn Tx, req: &PutRequest) -> Result<PutResponse, Status> {
        if req.key.is_empty() {
            return Err(empty_key());
        }
        let prev = tx.get(&req.key).map_err(store_error)?;
        if (req.ignore_value || req.ignore_lease) && prev.is_none() {
            return Err(status(code::NOT_FOUND, "key not found"));
        }
        let value = match (&prev, req.ignore_value) {
            (Some(p), true) => p.value.clone(),
            _ => req.value.clone(),
        };
        let lease = match (&prev, req.ignore_lease) {
            (Some(p), true) => p.lease,
            _ => req.lease,
        };
        if lease != 0 {
            if !self.leases.lock().unwrap().contains_key(&lease) {
                return Err(status(code::NOT_FOUND, "requested lease not found"));
            }
            tx.meta_put(&lease_index(lease, &req.key), b"")
                .map_err(store_error)?;
        }
        tx.put(&req.key, &value, lease).map_err(store_error)?;
        Ok(PutResponse {
            header: self.header(tx.revision()),
            prev_kv: if req.prev_kv { prev.map(kv) } else { None },
        })
    }

    fn delete(
        &self,
        tx: &mut dyn Tx,
        req: &DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Status> {
        let mut keys = Vec::new();
        match range_end(&req.range_end) {
            None => keys.push(req.key.clone()),
            Some(end) => tx
                .scan(&req.key, end, &mut |r| {
                    keys.push(r.key);
                    true
                })
                .map_err(store_error)?,
        }
        let mut resp = DeleteRangeResponse::default();
        for k in keys {
            if let Some(prev) = tx.delete(&k).map_err(store_error)? {
                resp.deleted += 1;
                if req.prev_kv {
                    resp.prev_kvs.push(kv(prev));
                }
            }
        }
        Ok(resp)
    }

    fn txn(&self, tx: &mut dyn Tx, req: &TxnRequest) -> Result<TxnResponse, Status> {
        let mut ok = true;
        for c in &req.compare {
            if !Self::compare(c, tx).map_err(store_error)? {
                ok = false;
                break;
            }
        }
        let ops = if ok { &req.success } else { &req.failure };
        let mut responses = Vec::with_capacity(ops.len());
        for op in ops {
            let r = match &op.request {
                Some(OpRequest::Range(r)) => {
                    OpResponse::Range(self.range(r, tx).map_err(store_error)?)
                }
                Some(OpRequest::Put(p)) => OpResponse::Put(self.put(tx, p)?),
                Some(OpRequest::DeleteRange(d)) => OpResponse::DeleteRange(self.delete(tx, d)?),
                Some(OpRequest::Txn(t)) => OpResponse::Txn(self.txn(tx, t)?),
                None => return Err(status(code::INVALID_ARGUMENT, "empty operation")),
            };
            responses.push(ResponseOp { response: Some(r) });
        }
        Ok(TxnResponse {
            header: None,
            succeeded: ok,
            responses,
        })
    }

    /// Runs a write. The headers inside the response name the revision the
    /// write committed as, or the current one if it changed nothing.
    fn run_write<T>(
        &self,
        mut f: impl FnMut(&mut dyn Tx) -> Result<T, Status>,
        fix: impl Fn(&mut T, i64),
    ) -> Result<T, Status> {
        let mut out = None;
        let mut failed = None;
        let committed = self
            .write(&mut |tx| match f(tx) {
                Ok(v) => {
                    out = Some(v);
                    Ok(())
                }
                Err(s) => {
                    failed = Some(s);
                    Err(store::Error::Aborted)
                }
            })
            .map_err(|e| failed.take().unwrap_or_else(|| store_error(e)))?;
        let mut v = out.expect("set when the closure succeeded");
        fix(&mut v, committed.unwrap_or_else(|| self.store.head()));
        Ok(v)
    }

    fn lease_grant(&self, req: &LeaseGrantRequest, now: i64) -> Result<LeaseGrantResponse, Status> {
        let ttl = req.ttl.max(1);
        let lease = Lease {
            ttl,
            expires_ms: now + ttl * 1000,
        };
        // the id is taken before the write, so two grants never share it
        let id = {
            let mut leases = self.leases.lock().unwrap();
            let id = if req.id != 0 {
                if leases.contains_key(&req.id) {
                    return Err(status(code::FAILED_PRECONDITION, "lease already exists"));
                }
                req.id
            } else {
                let mut id = (now << 16) & i64::MAX;
                while id == 0 || leases.contains_key(&id) {
                    id += 1;
                }
                id
            };
            leases.insert(id, lease);
            id
        };
        let rev = self
            .run_write(
                |tx| {
                    tx.meta_put(&lease_key(id), &encode_pair(lease.ttl, lease.expires_ms))
                        .map_err(store_error)
                },
                |_, _| {},
            )
            .map(|_| self.store.head())
            .inspect_err(|_| {
                self.leases.lock().unwrap().remove(&id);
            })?;
        Ok(LeaseGrantResponse {
            header: self.header(rev),
            id,
            ttl,
            error: String::new(),
        })
    }

    fn revoke(&self, id: i64) -> Result<i64, Status> {
        if self.leases.lock().unwrap().remove(&id).is_none() {
            return Err(status(code::NOT_FOUND, "requested lease not found"));
        }
        self.run_write(
            |tx| {
                let mut prefix = b"lkey/".to_vec();
                prefix.extend_from_slice(&(id as u64).to_be_bytes());
                let mut keys = Vec::new();
                tx.meta_scan(&prefix, &mut |k, _| {
                    keys.push(k[prefix.len()..].to_vec());
                    true
                })
                .map_err(store_error)?;
                for k in keys {
                    tx.meta_delete(&lease_index(id, &k)).map_err(store_error)?;
                    // the key may have moved to another lease since
                    if tx
                        .get(&k)
                        .map_err(store_error)?
                        .is_some_and(|r| r.lease == id)
                    {
                        tx.delete(&k).map_err(store_error)?;
                    }
                }
                tx.meta_delete(&lease_key(id)).map_err(store_error)
            },
            |_, _| {},
        )?;
        Ok(self.store.head())
    }

    fn expire_leases(&self) {
        let now = now_ms();
        if let Some(c) = self.cluster.get() {
            // only the leader decides that a lease ran out, and says so
            // through the log; a new leader first gives every lease its
            // full TTL again, as etcd does
            let leading = c
                .host
                .status()
                .is_ok_and(|s| s.role == crate::raft::Role::Leader);
            if !leading {
                c.leading.store(false, Ordering::Relaxed);
                return;
            }
            if !c.leading.swap(true, Ordering::Relaxed) {
                for l in self.leases.lock().unwrap().values_mut() {
                    l.expires_ms = now + l.ttl * 1000;
                }
                return;
            }
        }
        let due: Vec<i64> = self
            .leases
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, l)| l.expires_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            match self.cluster.get() {
                Some(c) => {
                    let body = LeaseRevokeRequest { id }.encode();
                    let method = "/etcdserverpb.Lease/LeaseRevoke";
                    let _ = c.host.propose(
                        |r| encode_command(c.id, r, method, &body, now),
                        CLUSTER_WAIT,
                    );
                }
                None => {
                    let _ = self.revoke(id);
                }
            }
        }
    }

    fn keep_alive(&self, id: i64) -> LeaseKeepAliveResponse {
        let mut leases = self.leases.lock().unwrap();
        let ttl = match leases.get_mut(&id) {
            Some(l) => {
                l.expires_ms = now_ms() + l.ttl * 1000;
                l.ttl
            }
            None => 0,
        };
        // renewal stays in memory: after a restart every lease runs its
        // full TTL again (Etcd::new), as with etcd after a leader change
        LeaseKeepAliveResponse {
            header: self.header(self.store.head()),
            id,
            ttl,
        }
    }

    fn watch(&self, requests: Receiver<Vec<u8>>, out: &mut dyn WatchOut) {
        let (tx, rx) = mpsc::sync_channel::<Inbox>(self.inbox.load(Ordering::Relaxed));
        let forward = tx.clone();
        std::thread::spawn(move || {
            for r in requests {
                if forward.send(Inbox::Request(r)).is_err() {
                    return;
                }
            }
            let _ = forward.send(Inbox::Done);
        });
        let mut w = WatchStream {
            etcd: self,
            tx,
            local: HashMap::new(),
            by_hub: HashMap::new(),
            next_local: 0,
            deferred: VecDeque::new(),
            sent: 0,
        };
        let every = Duration::from_millis(self.progress_ms.load(Ordering::Relaxed));
        // on a clock of its own, not on the stream falling silent: a busy
        // stream must not starve the watches that wait for progress
        let mut next_progress = Instant::now() + every;
        loop {
            if w.lagging() && !w.recover(&rx, out) {
                break;
            }
            if Instant::now() >= next_progress {
                next_progress = Instant::now() + every;
                if !w.periodic_progress(&rx, out) {
                    break;
                }
            }
            let item = match w.deferred.pop_front() {
                Some(i) => i,
                None => {
                    match rx.recv_timeout(next_progress.saturating_duration_since(Instant::now())) {
                        Ok(i) => i,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
            };
            let alive = match item {
                Inbox::Request(r) => match WatchRequest::decode(&r) {
                    Ok(req) => w.request(req, &rx, out),
                    Err(_) => false,
                },
                Inbox::Events(hub_id, rev, events) => w.deliver(hub_id, rev, events, out),
                Inbox::Done => false,
            };
            if !alive {
                break;
            }
        }
        for (_, sub) in w.local.drain() {
            self.hub.remove(sub.hub_id);
        }
    }
}

fn encode_pair(a: i64, b: i64) -> Vec<u8> {
    let mut v = Vec::new();
    quantycore::proto::encode_varint(a as u64, &mut v);
    quantycore::proto::encode_varint(b as u64, &mut v);
    v
}

fn decode_pair(v: &[u8]) -> Option<(i64, i64)> {
    let (a, n) = quantycore::proto::decode_varint(v).ok()?;
    let (b, _) = quantycore::proto::decode_varint(&v[n..]).ok()?;
    Some((a as i64, b as i64))
}

/// Where a watch stream's responses go: the gRPC call, or a test.
trait WatchOut {
    /// False once the client is gone.
    fn send(&mut self, msg: &[u8]) -> bool;
}

impl WatchOut for Responder {
    fn send(&mut self, msg: &[u8]) -> bool {
        Responder::send(self, msg)
    }
}

enum Inbox {
    Request(Vec<u8>),
    /// Events for the hub subscription with this id, at a revision.
    Events(u64, i64, Vec<Event>),
    Done,
}

struct Sub {
    hub_id: u64,
    progress_notify: bool,
    filter: Filter,
    /// Set by the hub to the first revision it could not queue.
    lagged: Arc<AtomicI64>,
}

struct WatchStream<'a, B: Backend> {
    etcd: &'a Etcd<B>,
    tx: SyncSender<Inbox>,
    local: HashMap<i64, Sub>,
    /// Which watch a hub subscription belongs to. Events are routed by
    /// subscription, so events queued for a canceled watch never reach
    /// a later one, whatever its id.
    by_hub: HashMap<u64, i64>,
    next_local: i64,
    deferred: VecDeque<Inbox>,
    /// The newest revision this stream has told its client about.
    /// Nothing it sends later may report an older one.
    sent: i64,
}

impl<B: Backend> WatchStream<'_, B> {
    fn send(&self, out: &mut dyn WatchOut, resp: WatchResponse) -> bool {
        out.send(&resp.encode())
    }

    fn request(&mut self, req: WatchRequest, rx: &Receiver<Inbox>, out: &mut dyn WatchOut) -> bool {
        match req.request_union {
            Some(WatchRequestUnion::Create(c)) => self.create(c, out),
            Some(WatchRequestUnion::Cancel(c)) => {
                if let Some(sub) = self.local.remove(&c.watch_id) {
                    self.etcd.hub.remove(sub.hub_id);
                    self.by_hub.remove(&sub.hub_id);
                }
                self.send(
                    out,
                    WatchResponse {
                        header: self.etcd.header(self.etcd.store.head()),
                        watch_id: c.watch_id,
                        canceled: true,
                        ..Default::default()
                    },
                )
            }
            Some(WatchRequestUnion::Progress(_)) => match self.catch_up(rx, out) {
                Some(rev) => self.send(
                    out,
                    WatchResponse {
                        header: self.etcd.header(rev),
                        watch_id: -1,
                        ..Default::default()
                    },
                ),
                None => false,
            },
            None => true,
        }
    }

    fn create(&mut self, c: WatchCreateRequest, out: &mut dyn WatchOut) -> bool {
        let id = if c.watch_id != 0 {
            c.watch_id
        } else {
            // as etcd: counted up per stream and never handed out twice
            while self.local.contains_key(&self.next_local) {
                self.next_local += 1;
            }
            self.next_local += 1;
            self.next_local - 1
        };
        if self.local.contains_key(&id) {
            return self.send(
                out,
                WatchResponse {
                    header: self.etcd.header(self.etcd.store.head()),
                    watch_id: id,
                    created: true,
                    canceled: true,
                    cancel_reason: "etcdserver: duplicate watch ID".into(),
                    ..Default::default()
                },
            );
        }
        let compacted = self.etcd.store.compacted();
        if c.start_revision > 0 && c.start_revision < compacted {
            let header = self.etcd.header(self.etcd.store.head());
            return self.send(
                out,
                WatchResponse {
                    header: header.clone(),
                    watch_id: id,
                    created: true,
                    ..Default::default()
                },
            ) && self.send(
                out,
                WatchResponse {
                    header,
                    watch_id: id,
                    canceled: true,
                    compact_revision: compacted,
                    cancel_reason: "etcdserver: mvcc: required revision has been compacted".into(),
                    ..Default::default()
                },
            );
        }
        let filter = Filter {
            key: c.key.clone(),
            end: c.range_end.clone(),
            no_put: c.filters.contains(&FilterType::NoPut),
            no_delete: c.filters.contains(&FilterType::NoDelete),
            prev_kv: c.prev_kv,
        };
        // a start in the future is waited for: nothing before it is sent
        let lagged = Arc::new(AtomicI64::new(0));
        let (hub_id, head) = self.etcd.hub.add(
            filter.clone(),
            c.start_revision,
            self.tx.clone(),
            lagged.clone(),
        );
        self.by_hub.insert(hub_id, id);
        self.sent = self.sent.max(head);
        self.local.insert(
            id,
            Sub {
                hub_id,
                progress_notify: c.progress_notify,
                filter: filter.clone(),
                lagged,
            },
        );
        if !self.send(
            out,
            WatchResponse {
                header: self.etcd.header(head),
                watch_id: id,
                created: true,
                ..Default::default()
            },
        ) {
            return false;
        }
        if c.start_revision == 0 || c.start_revision > head {
            return true;
        }
        self.replay(id, &filter, c.start_revision, head, out)
    }

    /// Sends the events for revisions `[from, to]` from the change log,
    /// a bounded batch at a time, never holding the store while sending.
    fn replay(
        &mut self,
        id: i64,
        filter: &Filter,
        from: i64,
        to: i64,
        out: &mut dyn WatchOut,
    ) -> bool {
        let limit = self.etcd.events_per_response.load(Ordering::Relaxed);
        let mut from = from;
        while from <= to {
            let mut batch: Vec<Event> = Vec::new();
            let mut batch_rev = 0;
            let mut next = None;
            let res = self.etcd.store.changes(from, to, &mut |rev, ch| {
                if let Some(e) = filter.event(&ch) {
                    // a revision's events always go out together
                    if batch.len() >= limit && rev != batch_rev {
                        next = Some(rev);
                        return false;
                    }
                    batch.push(e);
                    batch_rev = rev;
                }
                true
            });
            if res.is_err() {
                return false;
            }
            if !batch.is_empty()
                && !self.send(
                    out,
                    WatchResponse {
                        header: self.etcd.header(to),
                        watch_id: id,
                        events: batch,
                        ..Default::default()
                    },
                )
            {
                return false;
            }
            match next {
                Some(rev) => from = rev,
                None => break,
            }
        }
        self.sent = self.sent.max(to);
        true
    }

    fn lagging(&self) -> bool {
        self.local
            .values()
            .any(|s| s.lagged.load(Ordering::Acquire) != 0)
    }

    /// Brings watches the hub dropped for falling behind up to date: what
    /// was queued first, then the missed revisions from the change log
    /// under a new subscription.
    fn recover(&mut self, rx: &Receiver<Inbox>, out: &mut dyn WatchOut) -> bool {
        if self.catch_up(rx, out).is_none() {
            return false;
        }
        let behind: Vec<(i64, i64)> = self
            .local
            .iter()
            .filter_map(|(id, s)| match s.lagged.load(Ordering::Acquire) {
                0 => None,
                rev => Some((*id, rev)),
            })
            .collect();
        for (id, from) in behind {
            if from == REPLACED {
                // the store under this watch was replaced by a snapshot:
                // the client has to list again, as after a compaction
                if let Some(sub) = self.local.remove(&id) {
                    self.by_hub.remove(&sub.hub_id);
                }
                let compacted = self.etcd.store.compacted();
                let resp = WatchResponse {
                    header: self.etcd.header(self.etcd.store.head()),
                    watch_id: id,
                    canceled: true,
                    compact_revision: compacted,
                    cancel_reason: "etcdserver: mvcc: required revision has been compacted".into(),
                    ..Default::default()
                };
                if !self.send(out, resp) {
                    return false;
                }
                continue;
            }
            let sub = self.local.get_mut(&id).expect("listed above");
            sub.lagged.store(0, Ordering::Release);
            self.by_hub.remove(&sub.hub_id);
            let (hub_id, head) =
                self.etcd
                    .hub
                    .add(sub.filter.clone(), 0, self.tx.clone(), sub.lagged.clone());
            sub.hub_id = hub_id;
            let filter = sub.filter.clone();
            self.by_hub.insert(hub_id, id);
            if !self.replay(id, &filter, from, head, out) {
                return false;
            }
        }
        true
    }

    /// Delivers every event already queued, and returns the revision up
    /// to which the client now has everything, or None if it is gone.
    fn catch_up(&mut self, rx: &Receiver<Inbox>, out: &mut dyn WatchOut) -> Option<i64> {
        // every event up to `rev` is queued by now, since the hub queues
        // before it advances; events after it may come along, and then the
        // revision reported must not fall behind them
        let rev = self.etcd.hub.last_rev();
        while let Ok(item) = rx.try_recv() {
            match item {
                Inbox::Events(hub_id, r, ev) => {
                    if !self.deliver(hub_id, r, ev, out) {
                        return None;
                    }
                }
                other => self.deferred.push_back(other),
            }
        }
        Some(rev.max(self.sent))
    }

    fn deliver(
        &mut self,
        hub_id: u64,
        rev: i64,
        events: Vec<Event>,
        out: &mut dyn WatchOut,
    ) -> bool {
        let Some(&id) = self.by_hub.get(&hub_id) else {
            return true;
        };
        self.sent = self.sent.max(rev);
        self.send(
            out,
            WatchResponse {
                header: self.etcd.header(rev),
                watch_id: id,
                events,
                ..Default::default()
            },
        )
    }

    fn periodic_progress(&mut self, rx: &Receiver<Inbox>, out: &mut dyn WatchOut) -> bool {
        if !self.local.values().any(|s| s.progress_notify) {
            return true;
        }
        let Some(rev) = self.catch_up(rx, out) else {
            return false;
        };
        let ids: Vec<i64> = self
            .local
            .iter()
            .filter(|(_, s)| s.progress_notify)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter().all(|id| {
            self.send(
                out,
                WatchResponse {
                    header: self.etcd.header(rev),
                    watch_id: id,
                    ..Default::default()
                },
            )
        })
    }
}

#[derive(Clone)]
struct Filter {
    key: Vec<u8>,
    end: Vec<u8>,
    no_put: bool,
    no_delete: bool,
    prev_kv: bool,
}

impl Filter {
    fn event(&self, c: &Change) -> Option<Event> {
        if !in_range(&c.kv.key, &self.key, &self.end) {
            return None;
        }
        let kind = match c.kind {
            Kind::Put if self.no_put => return None,
            Kind::Delete if self.no_delete => return None,
            Kind::Put => EventType::Put,
            Kind::Delete => EventType::Delete,
        };
        Some(Event {
            kind,
            kv: Some(kv(c.kv.clone())),
            prev_kv: if self.prev_kv {
                c.prev.clone().map(kv)
            } else {
                None
            },
        })
    }
}

/// Fans committed changes out to every watch. Publishing happens while the
/// store's writer lock is still held, so watches see revisions in order.
struct Hub {
    state: Mutex<HubState>,
}

struct HubSub {
    filter: Filter,
    /// Revisions below this are not sent: they were replayed already, or
    /// come before the revision the watch asked to start at.
    from: i64,
    tx: SyncSender<Inbox>,
    lagged: Arc<AtomicI64>,
}

struct HubState {
    last_rev: i64,
    next_id: u64,
    subs: HashMap<u64, HubSub>,
}

impl Hub {
    /// Subscribes from `from` on, and returns the subscription's id and
    /// the last revision published, after which events will arrive.
    fn add(
        &self,
        filter: Filter,
        from: i64,
        tx: SyncSender<Inbox>,
        lagged: Arc<AtomicI64>,
    ) -> (u64, i64) {
        let mut s = self.state.lock().unwrap();
        s.next_id += 1;
        let id = s.next_id;
        s.subs.insert(
            id,
            HubSub {
                filter,
                from,
                tx,
                lagged,
            },
        );
        (id, s.last_rev)
    }

    fn remove(&self, id: u64) {
        self.state.lock().unwrap().subs.remove(&id);
    }

    /// Ends every watch: the store under them was replaced.
    fn reset(&self, head: i64) {
        let mut s = self.state.lock().unwrap();
        for sub in s.subs.values() {
            sub.lagged.store(REPLACED, Ordering::Release);
        }
        s.subs.clear();
        s.last_rev = head;
    }

    fn last_rev(&self) -> i64 {
        self.state.lock().unwrap().last_rev
    }

    fn publish(&self, rev: i64, changes: &[Change]) {
        let mut s = self.state.lock().unwrap();
        s.subs.retain(|id, sub| {
            if rev < sub.from {
                return true;
            }
            let events: Vec<Event> = changes.iter().filter_map(|c| sub.filter.event(c)).collect();
            if events.is_empty() {
                return true;
            }
            // never blocks a commit: a full stream is dropped here and
            // catches up from the log on its own thread
            match sub.tx.try_send(Inbox::Events(*id, rev, events)) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    sub.lagged.store(rev, Ordering::Release);
                    false
                }
                Err(TrySendError::Disconnected(_)) => false,
            }
        });
        s.last_rev = rev;
    }
}

fn method_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or("")
}

impl<B: Backend> Service for Etcd<B> {
    fn is_streaming(&self, method: &str) -> bool {
        matches!(
            method,
            "/etcdserverpb.Watch/Watch" | "/etcdserverpb.Lease/LeaseKeepAlive"
        )
    }

    fn unary(&self, method: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
        let Some(c) = self.cluster.get() else {
            return self.serve(method, body, now_ms());
        };
        let timed_out = |_| status(code::UNAVAILABLE, "request timed out");
        if is_write(method) {
            let mut body = body.to_vec();
            if method == "/etcdserverpb.Lease/LeaseGrant" {
                // the node that takes the request picks the id, so that
                // every node grants the same lease
                let mut req = LeaseGrantRequest::decode(&body).map_err(bad_request)?;
                if req.id == 0 {
                    let seq = c.lease_seq.fetch_add(1, Ordering::Relaxed) & 0xff;
                    req.id =
                        ((now_ms() << 16) | ((c.id as i64 & 0xff) << 8) | seq as i64) & i64::MAX;
                    body = req.encode();
                }
            }
            let now = now_ms();
            let answer = c
                .host
                .propose(
                    |r| encode_command(c.id, r, method, &body, now),
                    CLUSTER_WAIT,
                )
                .map_err(timed_out)?;
            return decode_answer(&answer);
        }
        let serializable = method == "/etcdserverpb.KV/Range"
            && RangeRequest::decode(body).is_ok_and(|r| r.serializable);
        let local_only = matches!(
            method,
            "/etcdserverpb.Maintenance/Status"
                | "/etcdserverpb.Maintenance/Alarm"
                | "/etcdserverpb.Cluster/MemberList"
                | "/grpc.health.v1.Health/Check"
        );
        if !serializable && !local_only {
            c.host.read_barrier(CLUSTER_WAIT).map_err(timed_out)?;
        }
        self.serve(method, body, now_ms())
    }

    fn stream(&self, method: &str, requests: Receiver<Vec<u8>>, mut out: Responder) {
        match method {
            "/etcdserverpb.Watch/Watch" => self.watch(requests, &mut out),
            "/etcdserverpb.Lease/LeaseKeepAlive" => {
                for r in requests {
                    let Ok(req) = LeaseKeepAliveRequest::decode(&r) else {
                        return out
                            .finish(Status::new(code::INVALID_ARGUMENT, "malformed request"));
                    };
                    if !out.send(&self.keep_alive(req.id).encode()) {
                        return;
                    }
                }
            }
            _ => out.finish(Status::new(code::UNIMPLEMENTED, "not a streaming method")),
        }
    }
}

impl<B: Backend> Etcd<B> {
    /// Makes this store node `id` of a cluster whose Raft host is built
    /// by `start` around the machine that applies its commands here.
    pub fn join(
        self: &Arc<Self>,
        id: u64,
        start: impl FnOnce(
            Arc<dyn crate::raft::host::Machine>,
        ) -> std::io::Result<Arc<crate::raft::host::Host>>,
    ) -> std::io::Result<()> {
        let host = start(Arc::new(Applier(Arc::downgrade(self))))?;
        let _ = self.cluster.set(Cluster {
            id,
            host,
            lease_seq: AtomicU64::new(0),
            leading: std::sync::atomic::AtomicBool::new(false),
        });
        Ok(())
    }

    /// Answers a request from this node's store alone; in a cluster,
    /// writes come here only as committed commands.
    fn serve(&self, method: &str, body: &[u8], unix_ms: i64) -> Result<Vec<u8>, Status> {
        let service = method
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("");
        match (service, method_name(method)) {
            ("etcdserverpb.KV", "Range") => {
                let req = RangeRequest::decode(body).map_err(bad_request)?;
                if req.key.is_empty() {
                    return Err(empty_key());
                }
                let mut resp = None;
                let at = self.etcd_read(req.revision, &mut |v| {
                    resp = Some(self.range(&req, v)?);
                    Ok(())
                })?;
                let mut resp = resp.unwrap();
                // as etcd: the revision read for the newest state, the
                // current head for a read at an older revision
                resp.header = self.header(if req.revision == 0 {
                    at
                } else {
                    self.store.head()
                });
                Ok(resp.encode())
            }
            ("etcdserverpb.KV", "Put") => {
                let req = PutRequest::decode(body).map_err(bad_request)?;
                self.run_write(
                    |tx| self.put(tx, &req),
                    |r, rev| r.header = self.header(rev),
                )
                .map(|r| r.encode())
            }
            ("etcdserverpb.KV", "DeleteRange") => {
                let req = DeleteRangeRequest::decode(body).map_err(bad_request)?;
                if req.key.is_empty() {
                    return Err(empty_key());
                }
                self.run_write(
                    |tx| self.delete(tx, &req),
                    |r, rev| r.header = self.header(rev),
                )
                .map(|r| r.encode())
            }
            ("etcdserverpb.KV", "Txn") => {
                let req = TxnRequest::decode(body).map_err(bad_request)?;
                check_txn(&req)?;
                self.run_write(
                    |tx| self.txn(tx, &req),
                    |r, rev| set_txn_headers(r, &self.header(rev)),
                )
                .map(|r| r.encode())
            }
            ("etcdserverpb.KV", "Compact") => {
                let req = CompactionRequest::decode(body).map_err(bad_request)?;
                if req.revision <= self.store.compacted() {
                    return Err(store_error(store::Error::Compacted(self.store.compacted())));
                }
                self.store.compact(req.revision).map_err(store_error)?;
                Ok(CompactionResponse {
                    header: self.header(self.store.head()),
                }
                .encode())
            }
            ("etcdserverpb.Lease", "LeaseGrant") => {
                let req = LeaseGrantRequest::decode(body).map_err(bad_request)?;
                self.lease_grant(&req, unix_ms).map(|r| r.encode())
            }
            ("etcdserverpb.Lease", "LeaseRevoke") => {
                let req = LeaseRevokeRequest::decode(body).map_err(bad_request)?;
                let rev = self.revoke(req.id)?;
                Ok(LeaseRevokeResponse {
                    header: self.header(rev),
                }
                .encode())
            }
            ("etcdserverpb.Lease", "LeaseTimeToLive") => {
                let req = LeaseTimeToLiveRequest::decode(body).map_err(bad_request)?;
                let l = self.leases.lock().unwrap().get(&req.id).copied();
                let Some(l) = l else {
                    return Ok(LeaseTimeToLiveResponse {
                        header: self.header(self.store.head()),
                        id: req.id,
                        ttl: -1,
                        ..Default::default()
                    }
                    .encode());
                };
                Ok(LeaseTimeToLiveResponse {
                    header: self.header(self.store.head()),
                    id: req.id,
                    ttl: ((l.expires_ms - now_ms()) / 1000).max(0),
                    granted_ttl: l.ttl,
                    keys: Vec::new(),
                }
                .encode())
            }
            ("etcdserverpb.Cluster", "MemberList") => Ok(MemberListResponse {
                header: self.header(self.store.head()),
                members: vec![Member {
                    id: self.member_id(),
                    name: "quantykube".into(),
                    peer_urls: vec![self.endpoint.clone()],
                    client_urls: vec![self.endpoint.clone()],
                    is_learner: false,
                }],
            }
            .encode()),
            ("etcdserverpb.Maintenance", "Status") => {
                let head = self.store.head();
                let (size, in_use) = self.store.size().map_err(store_error)?;
                let (leader, term, index, applied) = match self.cluster.get() {
                    None => (MEMBER_ID, 1, head as u64, head as u64),
                    Some(c) => {
                        let s = c
                            .host
                            .status()
                            .map_err(|_| status(code::UNAVAILABLE, "raft"))?;
                        let leader = s.leader.map_or(0, |l| MEMBER_ID + l);
                        (leader, s.term, s.commit, s.applied)
                    }
                };
                Ok(StatusResponse {
                    header: self.header(head),
                    version: ETCD_VERSION.into(),
                    db_size: size,
                    db_size_in_use: in_use,
                    leader,
                    raft_index: index,
                    raft_term: term,
                    raft_applied_index: applied,
                    ..Default::default()
                }
                .encode())
            }
            // health checks ask for the alarm list; there are never alarms
            ("etcdserverpb.Maintenance", "Alarm") => {
                let mut out = Vec::new();
                let header = self.header(self.store.head()).unwrap_or_default();
                quantycore::proto::put_message(&mut out, 1, &header.encode());
                Ok(out)
            }
            ("quantykube.History", m) => self.history_call(m, body),
            ("grpc.health.v1.Health", "Check") => Ok(HealthCheckResponse {
                status: ServingStatus::Serving,
            }
            .encode()),
            _ => Err(Status::new(
                code::UNIMPLEMENTED,
                format!("{method} is not supported by QuantyKube"),
            )),
        }
    }
}

/// QuantyKube's own service, for history the etcd API cannot express.
/// Its messages are described in `docs/HISTORY.md`.
impl<B: Backend> Etcd<B> {
    fn history_call(&self, method: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
        use quantycore::proto::{put_int, put_message, Reader};
        let (mut key, mut end, mut rev, mut unix_ms) = (Vec::new(), Vec::new(), 0, 0);
        let mut r = Reader::new(body);
        while let Some((n, v)) = r.next_field().map_err(bad_request)? {
            match n {
                1 => key = v.bytes(n).map_err(bad_request)?.to_vec(),
                2 => end = v.bytes(n).map_err(bad_request)?.to_vec(),
                3 => rev = v.int(n).map_err(bad_request)?,
                4 => unix_ms = v.int(n).map_err(bad_request)?,
                _ => {}
            }
        }
        let mut out = Vec::new();
        match method {
            "Log" => {
                let bound = range_end(&end);
                for h in self
                    .store
                    .history(&key, bound.and_then(|b| b.or(Some(&[0xff][..]))))
                    .map_err(store_error)?
                {
                    let mut e = Vec::new();
                    put_int(&mut e, 1, h.rev);
                    put_int(&mut e, 2, h.unix_ms);
                    put_int(&mut e, 3, (h.change.kind == Kind::Delete) as i64);
                    put_message(&mut e, 4, &kv(h.change.kv).encode());
                    if let Some(p) = h.change.prev {
                        put_message(&mut e, 5, &kv(p).encode());
                    }
                    put_message(&mut out, 1, &e);
                }
            }
            "Get" | "Restore" => {
                let rev = if rev > 0 {
                    rev
                } else {
                    self.store.revision_at(unix_ms).map_err(store_error)?
                };
                if rev > self.store.head() {
                    return Err(store_error(store::Error::FutureRevision));
                }
                let mut old = None;
                self.store
                    .read_retained(rev, &mut |v| {
                        old = v.get(&key)?;
                        Ok(())
                    })
                    .map_err(|e| match e {
                        store::Error::Engine(_) => status(
                            code::OUT_OF_RANGE,
                            &format!("revision {rev} is no longer retained"),
                        ),
                        e => store_error(e),
                    })?;
                put_int(&mut out, 1, rev);
                if method == "Restore" {
                    let committed = self.run_write(
                        |tx| {
                            match &old {
                                Some(r) => tx.put(&key, &r.value, 0),
                                None => tx.delete(&key),
                            }
                            .map(|_| ())
                            .map_err(store_error)
                        },
                        |_, _| {},
                    );
                    committed?;
                    put_int(&mut out, 3, self.store.head());
                }
                if let Some(r) = old {
                    put_message(&mut out, 2, &kv(r).encode());
                }
            }
            _ => return Err(Status::new(code::UNIMPLEMENTED, "no such method")),
        }
        Ok(out)
    }

    fn etcd_read(
        &self,
        rev: i64,
        f: &mut dyn FnMut(&dyn View) -> store::Result<()>,
    ) -> Result<i64, Status> {
        self.store.read(rev, f).map_err(store_error)
    }
}

fn set_txn_headers(r: &mut TxnResponse, h: &Option<ResponseHeader>) {
    r.header = h.clone();
    for op in &mut r.responses {
        match &mut op.response {
            Some(OpResponse::Range(x)) => x.header = h.clone(),
            Some(OpResponse::Put(x)) => x.header = h.clone(),
            Some(OpResponse::DeleteRange(x)) => x.header = h.clone(),
            Some(OpResponse::Txn(x)) => set_txn_headers(x, h),
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Quanty;

    fn etcd() -> Arc<Etcd<Quanty<quantydb_core::MemStorage>>> {
        Etcd::new(
            Arc::new(Quanty::in_memory().unwrap()),
            "unix:///test".into(),
        )
        .unwrap()
    }

    fn call<Q: Message, R: Message>(e: &Etcd<impl Backend>, method: &str, req: Q) -> R {
        R::decode(&e.unary(method, &req.encode()).unwrap()).unwrap()
    }

    fn put(e: &Etcd<impl Backend>, k: &str, v: &str, lease: i64) {
        let _: PutResponse = call(
            e,
            "/etcdserverpb.KV/Put",
            PutRequest {
                key: k.into(),
                value: v.into(),
                lease,
                ..Default::default()
            },
        );
    }

    fn keys(r: &RangeResponse) -> Vec<String> {
        r.kvs
            .iter()
            .map(|k| String::from_utf8(k.key.clone()).unwrap())
            .collect()
    }

    // what the API server relies on for paginated lists
    #[test]
    fn limit_reports_more_and_the_full_count() {
        let e = etcd();
        for k in ["/a/1", "/a/2", "/a/3", "/b"] {
            put(&e, k, "v", 0);
        }
        let r: RangeResponse = call(
            &e,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: "/a/".into(),
                range_end: "/a0".into(),
                limit: 2,
                ..Default::default()
            },
        );
        assert_eq!(keys(&r), ["/a/1", "/a/2"]);
        assert!(r.more);
        assert_eq!(r.count, 3);
        let r: RangeResponse = call(
            &e,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: "/".into(),
                range_end: vec![0],
                sort_order: SortOrder::Descend,
                limit: 2,
                ..Default::default()
            },
        );
        assert_eq!(keys(&r), ["/b", "/a/3"]);
    }

    #[test]
    fn a_failed_compare_runs_the_failure_branch_and_writes_nothing() {
        let e = etcd();
        put(&e, "/k", "v1", 0);
        let head = e.store.head();
        let r: TxnResponse = call(
            &e,
            "/etcdserverpb.KV/Txn",
            TxnRequest {
                compare: vec![Compare {
                    key: "/k".into(),
                    target: CompareTarget::Mod,
                    target_union: Some(TargetUnion::ModRevision(0)),
                    ..Default::default()
                }],
                success: vec![RequestOp {
                    request: Some(OpRequest::Put(PutRequest {
                        key: "/k".into(),
                        value: "v2".into(),
                        ..Default::default()
                    })),
                }],
                failure: vec![RequestOp {
                    request: Some(OpRequest::Range(RangeRequest {
                        key: "/k".into(),
                        ..Default::default()
                    })),
                }],
            },
        );
        assert!(!r.succeeded);
        assert_eq!(e.store.head(), head);
        assert_eq!(r.header.unwrap().revision, head);
        let Some(OpResponse::Range(got)) = &r.responses[0].response else {
            panic!("expected a range response");
        };
        assert_eq!(got.kvs[0].value, b"v1");
    }

    #[test]
    fn txns_etcd_refuses_change_nothing() {
        let e = etcd();
        let put_op = |k: &str| RequestOp {
            request: Some(OpRequest::Put(PutRequest {
                key: k.into(),
                ..Default::default()
            })),
        };
        let del_op = |k: &str, end: &str| RequestOp {
            request: Some(OpRequest::DeleteRange(DeleteRangeRequest {
                key: k.into(),
                range_end: end.into(),
                ..Default::default()
            })),
        };
        let txn = |success: Vec<RequestOp>, failure: Vec<RequestOp>| TxnRequest {
            success,
            failure,
            ..Default::default()
        };
        let nested = |t: TxnRequest| RequestOp {
            request: Some(OpRequest::Txn(t)),
        };
        let refused = |t: TxnRequest, why: &str| {
            let head = e.store.head();
            let err = e.unary("/etcdserverpb.KV/Txn", &t.encode()).unwrap_err();
            assert!(err.message.contains(why), "{}", err.message);
            assert_eq!(e.store.head(), head);
        };
        let dup = "duplicate key";
        refused(txn(vec![put_op("/a"), put_op("/a")], vec![]), dup);
        refused(txn(vec![put_op("/a/1"), del_op("/a/", "/a0")], vec![]), dup);
        refused(
            txn(
                vec![put_op("/a"), nested(txn(vec![], vec![put_op("/a")]))],
                vec![],
            ),
            dup,
        );
        refused(txn(vec![put_op("")], vec![]), "key is not provided");
        refused(
            txn(
                (0..=MAX_TXN_OPS)
                    .map(|i| put_op(&format!("/n/{i}")))
                    .collect(),
                vec![],
            ),
            "too many operations",
        );
        // the same key may appear once in each branch, and twice deleted
        let ok: TxnResponse = call(
            &e,
            "/etcdserverpb.KV/Txn",
            txn(
                vec![put_op("/a"), del_op("/b", ""), del_op("/b", "")],
                vec![put_op("/a")],
            ),
        );
        assert!(ok.succeeded);
        let many: TxnResponse = call(
            &e,
            "/etcdserverpb.KV/Txn",
            txn(
                (0..MAX_TXN_OPS)
                    .map(|i| put_op(&format!("/n/{i}")))
                    .collect(),
                vec![],
            ),
        );
        assert_eq!(many.responses.len(), MAX_TXN_OPS);
    }

    #[test]
    fn a_missing_key_compares_as_zero_except_by_value() {
        let e = etcd();
        let missing = |target, union| {
            let c = Compare {
                key: "/none".into(),
                target,
                target_union: Some(union),
                ..Default::default()
            };
            e.store
                .read(0, &mut |v| {
                    assert_eq!(
                        Etcd::<Quanty<quantydb_core::MemStorage>>::compare(&c, v).unwrap(),
                        target != CompareTarget::Value
                    );
                    Ok(())
                })
                .unwrap();
        };
        missing(CompareTarget::Version, TargetUnion::Version(0));
        missing(CompareTarget::Mod, TargetUnion::ModRevision(0));
        missing(CompareTarget::Value, TargetUnion::Value(Vec::new()));
    }

    #[test]
    fn keys_go_with_their_lease() {
        let e = etcd();
        let g: LeaseGrantResponse = call(
            &e,
            "/etcdserverpb.Lease/LeaseGrant",
            LeaseGrantRequest { ttl: 1, id: 0 },
        );
        put(&e, "/event", "x", g.id);
        put(&e, "/kept", "y", 0);
        std::thread::sleep(Duration::from_millis(2200));
        let r: RangeResponse = call(
            &e,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: "/".into(),
                range_end: vec![0],
                ..Default::default()
            },
        );
        assert_eq!(keys(&r), ["/kept"]);
        let err = e
            .unary(
                "/etcdserverpb.KV/Put",
                &PutRequest {
                    key: "/late".into(),
                    lease: g.id,
                    ..Default::default()
                }
                .encode(),
            )
            .unwrap_err();
        assert_eq!(err.code, code::NOT_FOUND);
    }

    /// A store in which another client's write commits in the middle of
    /// every read, after the view is taken and before it is used.
    struct Interleaved(Quanty<quantydb_core::MemStorage>);

    impl Backend for Interleaved {
        fn head(&self) -> i64 {
            self.0.head()
        }
        fn compacted(&self) -> i64 {
            self.0.compacted()
        }
        fn read(
            &self,
            rev: i64,
            f: &mut dyn FnMut(&dyn View) -> store::Result<()>,
        ) -> store::Result<i64> {
            self.0.read(rev, &mut |v| {
                self.0
                    .write(&mut |tx| tx.put(b"/other", b"v", 0).map(|_| ()))?;
                f(v)
            })
        }
        fn write(
            &self,
            f: &mut dyn FnMut(&mut dyn Tx) -> store::Result<()>,
        ) -> store::Result<Option<(i64, Vec<Change>)>> {
            self.0.write(f)
        }
        fn set_publisher(&self, p: store::Publisher) {
            self.0.set_publisher(p)
        }
        fn changes(
            &self,
            from: i64,
            to: i64,
            f: &mut dyn FnMut(i64, Change) -> bool,
        ) -> store::Result<()> {
            self.0.changes(from, to, f)
        }
        fn compact(&self, rev: i64) -> store::Result<()> {
            self.0.compact(rev)
        }
        fn size(&self) -> store::Result<(i64, i64)> {
            self.0.size()
        }
        fn read_retained(
            &self,
            rev: i64,
            f: &mut dyn FnMut(&dyn View) -> store::Result<()>,
        ) -> store::Result<()> {
            self.0.read_retained(rev, f)
        }
        fn revision_at(&self, unix_ms: i64) -> store::Result<i64> {
            self.0.revision_at(unix_ms)
        }
        fn history(
            &self,
            start: &[u8],
            end: Option<&[u8]>,
        ) -> store::Result<Vec<store::HistoryEntry>> {
            self.0.history(start, end)
        }
        fn export(&self) -> store::Result<Vec<u8>> {
            self.0.export()
        }
        fn import(&self, data: &[u8]) -> store::Result<()> {
            self.0.import(data)
        }
    }

    // the API server lists, then watches from the list's revision + 1:
    // a header ahead of the data loses the events in between for good
    #[test]
    fn a_range_reports_the_revision_it_read() {
        let e = Etcd::new(
            Arc::new(Interleaved(Quanty::in_memory().unwrap())),
            "unix:///test".into(),
        )
        .unwrap();
        put(&e, "/k", "v", 0);
        let r: RangeResponse = call(
            &e,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: "/k".into(),
                ..Default::default()
            },
        );
        let read = r.kvs[0].mod_revision;
        assert_eq!(r.header.unwrap().revision, read);
        assert!(e.store.head() > read, "no write landed during the read");
    }

    /// A watch stream run as the gRPC server would run it, with its
    /// responses collected.
    struct WatchClient {
        requests: Option<mpsc::Sender<Vec<u8>>>,
        got: Arc<Mutex<Vec<WatchResponse>>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    struct Collect(Arc<Mutex<Vec<WatchResponse>>>);

    impl WatchOut for Collect {
        fn send(&mut self, msg: &[u8]) -> bool {
            self.0
                .lock()
                .unwrap()
                .push(WatchResponse::decode(msg).unwrap());
            true
        }
    }

    impl WatchClient {
        fn open<B: Backend>(e: &Arc<Etcd<B>>) -> Self {
            let (tx, rx) = mpsc::channel();
            let got = Arc::new(Mutex::new(Vec::new()));
            let (e, mut out) = (e.clone(), Collect(got.clone()));
            let thread = std::thread::spawn(move || e.watch(rx, &mut out));
            WatchClient {
                requests: Some(tx),
                got,
                thread: Some(thread),
            }
        }

        fn send(&self, r: WatchRequestUnion) {
            let req = WatchRequest {
                request_union: Some(r),
            };
            self.requests.as_ref().unwrap().send(req.encode()).unwrap();
        }

        fn create(&self, key: &str, end: &str, start: i64) {
            self.send(WatchRequestUnion::Create(WatchCreateRequest {
                key: key.into(),
                range_end: end.into(),
                start_revision: start,
                ..Default::default()
            }));
        }

        /// Waits until `done` holds for what arrived, or fails the test.
        fn until(&self, what: &str, done: impl Fn(&[WatchResponse]) -> bool) -> Vec<WatchResponse> {
            for _ in 0..500 {
                let got = self.got.lock().unwrap().clone();
                if done(&got) {
                    return got;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!(
                "timed out waiting for {what}: {:?}",
                self.got.lock().unwrap()
            );
        }
    }

    impl Drop for WatchClient {
        fn drop(&mut self) {
            self.requests.take();
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    fn revisions(got: &[WatchResponse]) -> Vec<i64> {
        got.iter()
            .flat_map(|r| &r.events)
            .map(|e| e.kv.as_ref().unwrap().mod_revision)
            .collect()
    }

    // etcd waits for a start revision in the future and sends nothing
    // before it; the API server's watch cache relies on never seeing an
    // event older than the revision it asked for
    #[test]
    fn a_watch_sends_nothing_before_its_start_revision() {
        let e = etcd();
        put(&e, "/w/a", "v", 0);
        let head = e.store.head();
        let w = WatchClient::open(&e);
        w.create("/w/", "/w0", head + 3);
        w.until("created", |g| g.iter().any(|r| r.created));
        for i in 0..4 {
            put(&e, "/w/a", &i.to_string(), 0);
        }
        let got = w.until("two events", |g| revisions(g).len() >= 2);
        std::thread::sleep(Duration::from_millis(100));
        let mut all = revisions(&w.got.lock().unwrap());
        all.sort();
        assert_eq!(all, [head + 3, head + 4], "{got:?}");
    }

    // clientv3 multiplexes many watches on one stream and cancels them
    // all the time: an id handed out twice can carry events for the
    // canceled watch's keys to the new one
    #[test]
    fn watch_ids_are_never_reused_on_a_stream() {
        let e = etcd();
        let w = WatchClient::open(&e);
        w.create("/a", "", 0);
        w.until("first", |g| g.iter().filter(|r| r.created).count() == 1);
        w.send(WatchRequestUnion::Cancel(WatchCancelRequest {
            watch_id: 0,
        }));
        w.until("canceled", |g| g.iter().any(|r| r.canceled));
        w.create("/b", "", 0);
        let got = w.until("second", |g| g.iter().filter(|r| r.created).count() == 2);
        let ids: Vec<i64> = got
            .iter()
            .filter(|r| r.created)
            .map(|r| r.watch_id)
            .collect();
        assert_eq!(ids, [0, 1]);
        put(&e, "/a", "v", 0);
        put(&e, "/b", "v", 0);
        let got = w.until("event", |g| !revisions(g).is_empty());
        let keys: Vec<&[u8]> = got
            .iter()
            .flat_map(|r| &r.events)
            .map(|e| e.kv.as_ref().unwrap().key.as_slice())
            .collect();
        assert_eq!(keys, [b"/b".as_slice()]);
    }

    // the API server turns progress into bookmarks and moves its cache
    // to their revision unconditionally: they must come while a stream
    // is busy, and never report less than what the stream already sent
    #[test]
    fn progress_comes_on_a_busy_stream_and_never_goes_back() {
        let e = etcd();
        e.progress_ms.store(50, Ordering::Relaxed);
        let w = WatchClient::open(&e);
        w.send(WatchRequestUnion::Create(WatchCreateRequest {
            key: "/quiet".into(),
            progress_notify: true,
            ..Default::default()
        }));
        w.create("/busy", "", 0);
        w.until("both created", |g| {
            g.iter().filter(|r| r.created).count() == 2
        });
        let until = Instant::now() + Duration::from_millis(400);
        while Instant::now() < until {
            put(&e, "/busy", "v", 0);
            w.send(WatchRequestUnion::Progress(WatchProgressRequest {}));
            std::thread::sleep(Duration::from_millis(5));
        }
        // counted before the stream falls silent
        let got = w.got.lock().unwrap().clone();
        let quiet = got
            .iter()
            .filter(|r| r.watch_id == 0 && !r.created && r.events.is_empty())
            .count();
        assert!(quiet >= 2, "{quiet} progress notifications while busy");
        let revs: Vec<i64> = got
            .iter()
            .map(|r| r.header.as_ref().unwrap().revision)
            .collect();
        assert!(revs.windows(2).all(|p| p[0] <= p[1]), "{revs:?}");
    }

    // a write published between reading the hub's revision and draining
    // the queue: its events go out, so progress must report it
    #[test]
    fn progress_covers_events_delivered_while_catching_up() {
        let e = etcd();
        let (tx, rx) = mpsc::sync_channel(INBOX);
        let mut w = WatchStream {
            etcd: &e,
            tx: tx.clone(),
            local: HashMap::new(),
            by_hub: HashMap::new(),
            next_local: 0,
            deferred: VecDeque::new(),
            sent: 0,
        };
        let head = e.hub.last_rev();
        w.local.insert(
            0,
            Sub {
                hub_id: 7,
                progress_notify: false,
                filter: Filter {
                    key: Vec::new(),
                    end: Vec::new(),
                    no_put: false,
                    no_delete: false,
                    prev_kv: false,
                },
                lagged: Arc::new(AtomicI64::new(0)),
            },
        );
        w.by_hub.insert(7, 0);
        // queued, as the hub does, before it advances its revision
        tx.send(Inbox::Events(7, head + 1, Vec::new())).unwrap();
        let mut out = Collect(Arc::new(Mutex::new(Vec::new())));
        assert_eq!(w.catch_up(&rx, &mut out), Some(head + 1));
    }

    /// A client that reads its first response, then nothing until it is
    /// told to.
    struct Stalled {
        got: Arc<Mutex<Vec<WatchResponse>>>,
        go: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl WatchOut for Stalled {
        fn send(&mut self, msg: &[u8]) -> bool {
            let (lock, cv) = &*self.go;
            let mut go = lock.lock().unwrap();
            while !*go && !self.got.lock().unwrap().is_empty() {
                go = cv.wait(go).unwrap();
            }
            self.got
                .lock()
                .unwrap()
                .push(WatchResponse::decode(msg).unwrap());
            true
        }
    }

    type Go = Arc<(Mutex<bool>, std::sync::Condvar)>;

    struct StalledWatch {
        got: Arc<Mutex<Vec<WatchResponse>>>,
        go: Go,
        requests: mpsc::Sender<Vec<u8>>,
        thread: std::thread::JoinHandle<()>,
    }

    fn stalled_watch(
        e: &Arc<Etcd<Quanty<quantydb_core::MemStorage>>>,
        create: WatchCreateRequest,
    ) -> StalledWatch {
        let got = Arc::new(Mutex::new(Vec::new()));
        let go: Go = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (requests, rx) = mpsc::channel();
        let mut out = Stalled {
            got: got.clone(),
            go: go.clone(),
        };
        let e2 = e.clone();
        let thread = std::thread::spawn(move || e2.watch(rx, &mut out));
        let req = WatchRequest {
            request_union: Some(WatchRequestUnion::Create(create)),
        };
        requests.send(req.encode()).unwrap();
        StalledWatch {
            got,
            go,
            requests,
            thread,
        }
    }

    fn release(go: &(Mutex<bool>, std::sync::Condvar)) {
        *go.0.lock().unwrap() = true;
        go.1.notify_all();
    }

    // a watch client that stops reading, for example an API server that
    // hangs, must not stop everybody else from writing
    #[test]
    fn a_stalled_watch_client_does_not_stop_writes() {
        let e = etcd();
        e.events_per_response.store(10, Ordering::Relaxed);
        // more than one response's worth, so the replay sends while it
        // reads the log
        for i in 0..30 {
            put(&e, &format!("/r/{i:04}"), "v", 0);
        }
        let StalledWatch {
            got,
            go,
            requests: tx,
            thread: t,
        } = stalled_watch(
            &e,
            WatchCreateRequest {
                key: "/r/".into(),
                range_end: "/r0".into(),
                start_revision: 1,
                ..Default::default()
            },
        );
        // the stream is now stuck sending its first events
        std::thread::sleep(Duration::from_millis(100));
        let (done_tx, done_rx) = mpsc::channel();
        let e2 = e.clone();
        std::thread::spawn(move || {
            e2.store
                .enforce_retention(3_600_000, 3_600_000, now_ms())
                .unwrap();
            put(&e2, "/other", "v", 0);
            let _ = done_tx.send(());
        });
        let wrote = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        release(&go);
        drop(tx);
        t.join().unwrap();
        assert!(wrote, "a write waited for a stalled watch client");
        assert!(!got.lock().unwrap().is_empty());
    }

    // a stream that fell behind and was dropped by the hub catches up
    // from the change log: every revision once, in order
    #[test]
    fn a_watch_that_fell_behind_misses_nothing() {
        let e = etcd();
        e.inbox.store(8, Ordering::Relaxed);
        let StalledWatch {
            got,
            go,
            requests: tx,
            thread: t,
        } = stalled_watch(
            &e,
            WatchCreateRequest {
                key: "/k".into(),
                ..Default::default()
            },
        );
        std::thread::sleep(Duration::from_millis(50));
        let first = e.store.head() + 1;
        let n = 30;
        for i in 0..n {
            put(&e, "/k", &i.to_string(), 0);
        }
        release(&go);
        let want: Vec<i64> = (first..first + n).collect();
        let mut seen = Vec::new();
        for _ in 0..500 {
            seen = revisions(&got.lock().unwrap());
            if seen.len() >= want.len() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(tx);
        t.join().unwrap();
        assert_eq!(seen, want);
    }

    #[test]
    fn two_grants_never_get_the_same_lease() {
        let e = etcd();
        let grants: Vec<_> = (0..8)
            .map(|_| {
                let e = e.clone();
                std::thread::spawn(move || {
                    let req = LeaseGrantRequest { id: 42, ttl: 60 };
                    e.unary("/etcdserverpb.Lease/LeaseGrant", &req.encode())
                        .is_ok()
                })
            })
            .collect();
        let ok = grants
            .into_iter()
            .map(|g| g.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(ok, 1);
    }

    // keepalives are not stored, so after a restart a lease that was kept
    // alive past its first expiry must not be revoked on the spot
    #[test]
    fn a_restart_gives_every_lease_its_full_ttl() {
        let store = Arc::new(Quanty::in_memory().unwrap());
        let e = Etcd::new(store.clone(), "unix:///test".into()).unwrap();
        let grant: LeaseGrantResponse = call(
            &e,
            "/etcdserverpb.Lease/LeaseGrant",
            LeaseGrantRequest { id: 0, ttl: 1 },
        );
        put(&e, "/held", "v", grant.id);
        std::thread::sleep(Duration::from_millis(600));
        e.keep_alive(grant.id);
        std::thread::sleep(Duration::from_millis(600));
        let restarted = Etcd::new(store, "unix:///test".into()).unwrap();
        restarted.expire_leases();
        let r: RangeResponse = call(
            &restarted,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: "/held".into(),
                ..Default::default()
            },
        );
        assert_eq!(r.count, 1);
    }

    type Hosts = Arc<Mutex<HashMap<u64, Weak<crate::raft::host::Host>>>>;
    type Down = Arc<Mutex<Vec<u64>>>;

    struct Loopback(Hosts, Down);

    impl crate::raft::host::Wire for Loopback {
        fn send(&self, m: crate::raft::Message) {
            let down = self.1.lock().unwrap().clone();
            if down.contains(&m.from) || down.contains(&m.to) {
                return;
            }
            let to = self.0.lock().unwrap().get(&m.to).and_then(Weak::upgrade);
            if let Some(h) = to {
                h.step(m);
            }
        }
    }

    /// Three store nodes in one process, joined by Raft.
    fn three() -> (Vec<Arc<Etcd<Quanty<quantydb_core::MemStorage>>>>, Down) {
        three_with(crate::raft::host::Compaction::default())
    }

    fn three_with(
        compaction: crate::raft::host::Compaction,
    ) -> (Vec<Arc<Etcd<Quanty<quantydb_core::MemStorage>>>>, Down) {
        let hosts: Hosts = Arc::default();
        let down: Down = Arc::default();
        let ids = [1, 2, 3];
        let nodes = ids
            .iter()
            .map(|id| {
                let e = etcd();
                let (hosts2, down2) = (hosts.clone(), down.clone());
                e.join(*id, |machine| {
                    let wire = Box::new(Loopback(hosts2, down2));
                    let log = Box::<crate::raft::host::MemLog>::default();
                    crate::raft::host::Host::start_with(
                        *id, &ids, log, wire, machine, None, compaction,
                    )
                })
                .unwrap();
                let host = e.cluster.get().unwrap().host.clone();
                hosts.lock().unwrap().insert(*id, Arc::downgrade(&host));
                e
            })
            .collect();
        (nodes, down)
    }

    fn get(e: &Etcd<impl Backend>, k: &str) -> Option<KeyValue> {
        let r: RangeResponse = call(
            e,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: k.into(),
                ..Default::default()
            },
        );
        r.kvs.into_iter().next()
    }

    // writes through any node, linearizable reads on every node, the
    // same revisions everywhere, and a watch that sees a write made
    // through another node
    #[test]
    fn three_store_nodes_act_as_one() {
        let (nodes, down) = three();
        let w = WatchClient::open(&nodes[1]);
        w.create("/c/", "/c0", 0);
        w.until("created", |g| g.iter().any(|r| r.created));
        for (i, e) in nodes.iter().enumerate() {
            put(e, &format!("/c/{i}"), "v", 0);
        }
        for e in &nodes {
            for i in 0..3 {
                let kv = get(e, &format!("/c/{i}")).expect("written through another node");
                assert_eq!(
                    kv.mod_revision,
                    get(&nodes[0], &format!("/c/{i}")).unwrap().mod_revision
                );
            }
        }
        w.until("three events", |g| revisions(g).len() == 3);

        // a lease granted through a follower ends on every node when the
        // leader lets it run out
        let grant: LeaseGrantResponse = call(
            &nodes[2],
            "/etcdserverpb.Lease/LeaseGrant",
            LeaseGrantRequest { id: 0, ttl: 1 },
        );
        put(&nodes[0], "/c/leased", "v", grant.id);
        assert!(get(&nodes[1], "/c/leased").is_some());
        let mut gone = false;
        for _ in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            if nodes.iter().all(|e| get(e, "/c/leased").is_none()) {
                gone = true;
                break;
            }
        }
        assert!(gone, "the lease did not run out");

        // the cluster goes on without its leader
        let status: StatusResponse = call(
            &nodes[0],
            "/etcdserverpb.Maintenance/Status",
            StatusRequest {},
        );
        let leader = status.leader - MEMBER_ID;
        down.lock().unwrap().push(leader);
        let rest: Vec<&Arc<Etcd<_>>> = nodes
            .iter()
            .filter(|e| e.cluster.get().unwrap().id != leader)
            .collect();
        let mut done = false;
        for _ in 0..10 {
            let r = rest[0].unary(
                "/etcdserverpb.KV/Put",
                &PutRequest {
                    key: "/c/after".into(),
                    value: "v".into(),
                    ..Default::default()
                }
                .encode(),
            );
            if r.is_ok() {
                done = true;
                break;
            }
        }
        assert!(done, "no write without the leader");
        assert!(get(rest[1], "/c/after").is_some());
    }

    // a store node away for longer than the leader's log reaches back
    // takes the leader's state, and from then on has the same revisions
    #[test]
    fn a_store_node_far_behind_takes_a_snapshot() {
        let (nodes, down) = three_with(crate::raft::host::Compaction { after: 20, keep: 5 });
        put(&nodes[0], "/s/first", "v", 0);
        let status: StatusResponse = call(
            &nodes[0],
            "/etcdserverpb.Maintenance/Status",
            StatusRequest {},
        );
        let leader = status.leader - MEMBER_ID;
        let away = nodes
            .iter()
            .position(|e| e.cluster.get().unwrap().id != leader)
            .unwrap();
        down.lock().unwrap().push(away as u64 + 1);
        let up: Vec<usize> = (0..3).filter(|i| *i != away).collect();
        let grant: LeaseGrantResponse = call(
            &nodes[up[0]],
            "/etcdserverpb.Lease/LeaseGrant",
            LeaseGrantRequest { id: 0, ttl: 600 },
        );
        for i in 0..60 {
            put(
                &nodes[up[i % 2]],
                &format!("/s/{i:02}"),
                "v",
                if i == 7 { grant.id } else { 0 },
            );
        }
        down.lock().unwrap().clear();
        let serial = |e: &Etcd<_>, k: &str| -> Option<KeyValue> {
            let r: RangeResponse = call(
                e,
                "/etcdserverpb.KV/Range",
                RangeRequest {
                    key: k.into(),
                    serializable: true,
                    ..Default::default()
                },
            );
            r.kvs.into_iter().next()
        };
        for _ in 0..100 {
            if serial(&nodes[away], "/s/59").is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        for k in ["/s/first", "/s/07", "/s/59"] {
            assert_eq!(serial(&nodes[away], k), serial(&nodes[up[0]], k), "{k}");
        }
        // the leases come with the state, just after it
        for _ in 0..50 {
            if nodes[away].leases.lock().unwrap().contains_key(&grant.id) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(nodes[away].leases.lock().unwrap().contains_key(&grant.id));
        put(&nodes[away], "/s/after", "v", 0);
        let a = get(&nodes[away], "/s/after").unwrap();
        let b = get(&nodes[up[1]], "/s/after").unwrap();
        assert_eq!(a.mod_revision, b.mod_revision);
    }
}
