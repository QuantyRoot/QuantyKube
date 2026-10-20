//! The etcd v3 gRPC API on top of a [`Backend`]. `docs/RPCS.md` lists what
//! the Kubernetes API server calls; everything else answers UNIMPLEMENTED.

use crate::etcdpb::*;
use crate::store::{self, Backend, Change, Kind, Record, Tx, View};
use quantycore::grpc::{code, Responder, Service, Status};
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What `Status` reports. The API server only uses watch progress requests
/// when the version says the server has them, which 3.6 does.
pub const ETCD_VERSION: &str = "3.6.5";

const CLUSTER_ID: u64 = 0x5155_414e_5459_4b55;
const MEMBER_ID: u64 = 0x4b55_4245_0000_0001;
const PROGRESS_EVERY: Duration = Duration::from_secs(600);
const EVENTS_PER_RESPONSE: usize = 1000;

pub struct Etcd<B: Backend> {
    store: Arc<B>,
    hub: Arc<Hub>,
    leases: Mutex<HashMap<i64, Lease>>,
    endpoint: String,
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
        store::Error::Compacted(_) | store::Error::FutureRevision => code::OUT_OF_RANGE,
        store::Error::KeyTooLong => code::INVALID_ARGUMENT,
        store::Error::Engine(_) | store::Error::Corrupt(_) | store::Error::Aborted => {
            code::INTERNAL
        }
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

fn in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    match range_end(end) {
        None => key == start,
        Some(None) => key >= start,
        Some(Some(e)) => key >= start && key < e,
    }
}

impl<B: Backend> Etcd<B> {
    pub fn new(store: Arc<B>, endpoint: String) -> Result<Arc<Self>, store::Error> {
        let mut leases = HashMap::new();
        store.read(0, &mut |_| Ok(()))?;
        // leases live in the store's metadata; reading it needs a write
        // transaction, which commits nothing when nothing changes
        store.write(
            &mut |tx| {
                tx.meta_scan(b"lease/", &mut |k, v| {
                    if let (Ok(id), Some((ttl, exp))) =
                        (<[u8; 8]>::try_from(&k[6..]), decode_pair(v))
                    {
                        leases.insert(
                            u64::from_be_bytes(id) as i64,
                            Lease {
                                ttl,
                                expires_ms: exp,
                            },
                        );
                    }
                    true
                })
            },
            &mut |_, _| {},
        )?;
        let hub = Arc::new(Hub {
            state: Mutex::new(HubState {
                last_rev: store.head(),
                next_id: 0,
                subs: HashMap::new(),
            }),
        });
        let etcd = Arc::new(Etcd {
            store,
            hub,
            leases: Mutex::new(leases),
            endpoint,
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

    fn header(&self, rev: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: CLUSTER_ID,
            member_id: MEMBER_ID,
            revision: rev,
            raft_term: 1,
        })
    }

    fn write(
        &self,
        f: &mut dyn FnMut(&mut dyn Tx) -> store::Result<()>,
    ) -> store::Result<Option<i64>> {
        let hub = self.hub.clone();
        let r = self
            .store
            .write(f, &mut |rev, changes| hub.publish(rev, changes))?;
        Ok(r.map(|(rev, _)| rev))
    }

    fn range(&self, req: &RangeRequest, view: &dyn View, rev: i64) -> store::Result<RangeResponse> {
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
            header: self.header(rev),
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
            return Err(status(code::INVALID_ARGUMENT, "key is not provided"));
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
                    OpResponse::Range(self.range(r, tx, tx.revision()).map_err(store_error)?)
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

    fn lease_grant(&self, req: &LeaseGrantRequest) -> Result<LeaseGrantResponse, Status> {
        let ttl = req.ttl.max(1);
        let id = {
            let leases = self.leases.lock().unwrap();
            if req.id != 0 {
                if leases.contains_key(&req.id) {
                    return Err(status(code::FAILED_PRECONDITION, "lease already exists"));
                }
                req.id
            } else {
                let mut id = (now_ms() << 16) & i64::MAX;
                while id == 0 || leases.contains_key(&id) {
                    id += 1;
                }
                id
            }
        };
        let lease = Lease {
            ttl,
            expires_ms: now_ms() + ttl * 1000,
        };
        let rev = self
            .run_write(
                |tx| {
                    tx.meta_put(&lease_key(id), &encode_pair(lease.ttl, lease.expires_ms))
                        .map_err(store_error)
                },
                |_, _| {},
            )
            .map(|_| self.store.head())?;
        self.leases.lock().unwrap().insert(id, lease);
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
        let due: Vec<i64> = self
            .leases
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, l)| l.expires_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            let _ = self.revoke(id);
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
        // renewal stays in memory: after a restart a lease runs its full
        // TTL again, which is what etcd does after a leader change too
        LeaseKeepAliveResponse {
            header: self.header(self.store.head()),
            id,
            ttl,
        }
    }

    fn watch(&self, requests: Receiver<Vec<u8>>, mut out: Responder) {
        let (tx, rx) = mpsc::channel::<Inbox>();
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
            next_local: 0,
            deferred: Vec::new(),
        };
        loop {
            let item = match w.deferred.pop() {
                Some(i) => i,
                None => match rx.recv_timeout(PROGRESS_EVERY) {
                    Ok(i) => i,
                    Err(RecvTimeoutError::Timeout) => {
                        if !w.periodic_progress(&mut out) {
                            break;
                        }
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                },
            };
            let alive = match item {
                Inbox::Request(r) => match WatchRequest::decode(&r) {
                    Ok(req) => w.request(req, &rx, &mut out),
                    Err(_) => false,
                },
                Inbox::Events(local, rev, events) => w.deliver(local, rev, events, &mut out),
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

enum Inbox {
    Request(Vec<u8>),
    Events(i64, i64, Vec<Event>),
    Done,
}

struct Sub {
    hub_id: u64,
    progress_notify: bool,
}

struct WatchStream<'a, B: Backend> {
    etcd: &'a Etcd<B>,
    tx: Sender<Inbox>,
    local: HashMap<i64, Sub>,
    next_local: i64,
    deferred: Vec<Inbox>,
}

impl<B: Backend> WatchStream<'_, B> {
    fn send(&self, out: &mut Responder, resp: WatchResponse) -> bool {
        out.send(&resp.encode())
    }

    fn request(&mut self, req: WatchRequest, rx: &Receiver<Inbox>, out: &mut Responder) -> bool {
        match req.request_union {
            Some(WatchRequestUnion::Create(c)) => self.create(c, out),
            Some(WatchRequestUnion::Cancel(c)) => {
                if let Some(sub) = self.local.remove(&c.watch_id) {
                    self.etcd.hub.remove(sub.hub_id);
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
            Some(WatchRequestUnion::Progress(_)) => {
                // every event up to `rev` is already queued, because the hub
                // queues before it advances; deliver them, then report
                let rev = self.etcd.hub.last_rev();
                let mut later = Vec::new();
                while let Ok(item) = rx.try_recv() {
                    match item {
                        Inbox::Events(id, r, ev) => {
                            if !self.deliver(id, r, ev, out) {
                                return false;
                            }
                        }
                        other => later.push(other),
                    }
                }
                later.reverse();
                self.deferred.extend(later);
                self.send(
                    out,
                    WatchResponse {
                        header: self.etcd.header(rev),
                        watch_id: -1,
                        ..Default::default()
                    },
                )
            }
            None => true,
        }
    }

    fn create(&mut self, c: WatchCreateRequest, out: &mut Responder) -> bool {
        let id = if c.watch_id != 0 {
            c.watch_id
        } else {
            while self.local.contains_key(&self.next_local) {
                self.next_local += 1;
            }
            self.next_local
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
        let (hub_id, head) = self.etcd.hub.add(filter.clone(), id, self.tx.clone());
        self.local.insert(
            id,
            Sub {
                hub_id,
                progress_notify: c.progress_notify,
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
        let mut batch: Vec<Event> = Vec::new();
        let mut batch_rev = 0;
        let mut alive = true;
        let res = self
            .etcd
            .store
            .changes(c.start_revision, head, &mut |rev, ch| {
                if let Some(e) = filter.event(&ch) {
                    if batch.len() >= EVENTS_PER_RESPONSE && rev != batch_rev {
                        let events = std::mem::take(&mut batch);
                        alive = out.send(
                            &WatchResponse {
                                header: self.etcd.header(head),
                                watch_id: id,
                                events,
                                ..Default::default()
                            }
                            .encode(),
                        );
                    }
                    batch.push(e);
                    batch_rev = rev;
                }
                alive
            });
        if res.is_err() || !alive {
            return false;
        }
        batch.is_empty()
            || self.send(
                out,
                WatchResponse {
                    header: self.etcd.header(head),
                    watch_id: id,
                    events: batch,
                    ..Default::default()
                },
            )
    }

    fn deliver(&self, id: i64, rev: i64, events: Vec<Event>, out: &mut Responder) -> bool {
        if !self.local.contains_key(&id) {
            return true;
        }
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

    fn periodic_progress(&self, out: &mut Responder) -> bool {
        let rev = self.etcd.hub.last_rev();
        for (id, sub) in &self.local {
            if sub.progress_notify
                && !self.send(
                    out,
                    WatchResponse {
                        header: self.etcd.header(rev),
                        watch_id: *id,
                        ..Default::default()
                    },
                )
            {
                return false;
            }
        }
        true
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

struct HubState {
    last_rev: i64,
    next_id: u64,
    subs: HashMap<u64, (Filter, i64, Sender<Inbox>)>,
}

impl Hub {
    fn add(&self, f: Filter, watch_id: i64, tx: Sender<Inbox>) -> (u64, i64) {
        let mut s = self.state.lock().unwrap();
        s.next_id += 1;
        let id = s.next_id;
        s.subs.insert(id, (f, watch_id, tx));
        (id, s.last_rev)
    }

    fn remove(&self, id: u64) {
        self.state.lock().unwrap().subs.remove(&id);
    }

    fn last_rev(&self) -> i64 {
        self.state.lock().unwrap().last_rev
    }

    fn publish(&self, rev: i64, changes: &[Change]) {
        let mut s = self.state.lock().unwrap();
        s.subs.retain(|_, (f, watch_id, tx)| {
            let events: Vec<Event> = changes.iter().filter_map(|c| f.event(c)).collect();
            events.is_empty() || tx.send(Inbox::Events(*watch_id, rev, events)).is_ok()
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
        let service = method
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("");
        match (service, method_name(method)) {
            ("etcdserverpb.KV", "Range") => {
                let req = RangeRequest::decode(body).map_err(bad_request)?;
                let mut resp = None;
                self.etcd_read(req.revision, &mut |v| {
                    resp = Some(self.range(&req, v, self.store.head())?);
                    Ok(())
                })?;
                Ok(resp.unwrap().encode())
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
                self.run_write(
                    |tx| self.delete(tx, &req),
                    |r, rev| r.header = self.header(rev),
                )
                .map(|r| r.encode())
            }
            ("etcdserverpb.KV", "Txn") => {
                let req = TxnRequest::decode(body).map_err(bad_request)?;
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
                self.lease_grant(&req).map(|r| r.encode())
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
                    id: MEMBER_ID,
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
                Ok(StatusResponse {
                    header: self.header(head),
                    version: ETCD_VERSION.into(),
                    db_size: size,
                    db_size_in_use: in_use,
                    leader: MEMBER_ID,
                    raft_index: head as u64,
                    raft_term: 1,
                    raft_applied_index: head as u64,
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

    fn stream(&self, method: &str, requests: Receiver<Vec<u8>>, mut out: Responder) {
        match method {
            "/etcdserverpb.Watch/Watch" => self.watch(requests, out),
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
    ) -> Result<(), Status> {
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
}
