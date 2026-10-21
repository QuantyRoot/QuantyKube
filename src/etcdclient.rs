//! A client of an etcd server over TLS, for moving a cluster's keys from
//! etcd into the store with their revisions (ADR-035). One connection per
//! call: it makes a few hundred calls and is done.

use crate::etcdpb::{
    DeleteRangeRequest, DeleteRangeResponse, LeaseGrantRequest, LeaseGrantResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, OpRequest, PutRequest, PutResponse, RangeRequest,
    RangeResponse, RequestOp, TxnRequest, TxnResponse,
};
use crate::etcdpb::{KeyValue, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, Message};
use crate::store::Record;
use quantycore::grpc::Status;
use quantycore::tls::x509::{pem, PrivateKey, RootStore};
use quantycore::tls::{ClientConfig, ClientConnection, Stream};
use std::collections::BTreeMap;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Keys per page while copying, so no single answer is large.
const PAGE: i64 = 500;
/// Written to move etcd's revision on; deleted with everything else.
const SCRATCH: &[u8] = b"/quantykube/revision";
/// etcd's limit on the operations of one txn.
const TXN_OPS: usize = 128;

pub struct Client {
    addr: String,
    config: Arc<ClientConfig>,
}

/// Everything live in an etcd at one revision.
pub struct Copy {
    pub revision: i64,
    pub records: Vec<Record>,
    /// Each lease a key is attached to, with the TTL it was granted.
    pub leases: BTreeMap<i64, i64>,
}

/// A TLS client configuration that trusts `ca` and, with `cert` and `key`,
/// shows that certificate.
pub fn client_config(
    ca: &Path,
    cert: Option<(&Path, &Path)>,
    alpn: &[&[u8]],
) -> Result<ClientConfig, String> {
    let read = |p: &Path| std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()));
    let mut roots = RootStore::new();
    let (ok, _) = roots
        .add_pem(&read(ca)?)
        .map_err(|e| format!("{}: {e:?}", ca.display()))?;
    if ok == 0 {
        return Err(format!("{}: no CA certificate", ca.display()));
    }
    let mut config = ClientConfig::new(roots);
    if let Some((cert, key)) = cert {
        let chain = pem::decode_all(&read(cert)?, "CERTIFICATE")
            .map_err(|e| format!("{}: {e}", cert.display()))?;
        let key =
            PrivateKey::from_pem(&read(key)?).map_err(|e| format!("{}: {e}", key.display()))?;
        config.set_client_certificate(chain, key);
    }
    config.set_alpn(alpn);
    Ok(config)
}

/// A TLS connection to `addr`, `host:port`, whose certificate must name
/// the host.
pub fn connect(
    addr: &str,
    config: &Arc<ClientConfig>,
) -> io::Result<Stream<ClientConnection, TcpStream>> {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no address");
    let mut tcp = None;
    for sock in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&sock, Duration::from_secs(5)) {
            Ok(t) => {
                tcp = Some(t);
                break;
            }
            Err(e) => last = e,
        }
    }
    let tcp = tcp.ok_or(last)?;
    tcp.set_read_timeout(Some(Duration::from_secs(60)))?;
    let conn = ClientConnection::new(config.clone(), host)
        .map_err(|e| io::Error::other(format!("{e:?}")))?;
    let mut stream = Stream::new(conn, tcp);
    stream.handshake()?;
    Ok(stream)
}

impl Client {
    /// `addr` is `host:port`; the server's certificate must name `host`
    /// and come from `ca`, and `cert` and `key` are what etcd's
    /// `--trusted-ca-file` accepts, as the API server's are.
    pub fn new(addr: &str, ca: &Path, cert: &Path, key: &Path) -> Result<Client, String> {
        Ok(Client {
            addr: addr.to_string(),
            config: Arc::new(client_config(ca, Some((cert, key)), &[b"h2"])?),
        })
    }

    fn revision(&self) -> Result<i64, Status> {
        let r: RangeResponse = self.call(
            "/etcdserverpb.KV/Range",
            &RangeRequest {
                key: SCRATCH.to_vec(),
                count_only: true,
                ..Default::default()
            },
        )?;
        Ok(r.header.map_or(0, |h| h.revision))
    }

    fn call<Q: Message, R: Message>(&self, method: &str, req: &Q) -> Result<R, Status> {
        let stream = connect(&self.addr, &self.config)
            .map_err(|e| Status::new(14, format!("{}: {e}", self.addr)))?;
        let body = quantycore::grpc::call(stream, method, &req.encode())?;
        R::decode(&body).map_err(|e| Status::new(13, format!("{method}: {e:?}")))
    }

    /// Every key at the current revision, read page by page at that one
    /// revision, and the leases they hold.
    pub fn copy(&self) -> Result<Copy, Status> {
        let mut records = Vec::new();
        let mut revision = 0;
        let mut from = vec![0u8];
        loop {
            let page: RangeResponse = self.call(
                "/etcdserverpb.KV/Range",
                &RangeRequest {
                    key: from.clone(),
                    range_end: vec![0],
                    limit: PAGE,
                    revision,
                    ..Default::default()
                },
            )?;
            if revision == 0 {
                revision = page.header.as_ref().map_or(0, |h| h.revision);
            }
            let more = page.more;
            if let Some(last) = page.kvs.last() {
                from = last.key.clone();
                from.push(0);
            }
            records.extend(page.kvs.into_iter().map(record));
            if !more {
                break;
            }
        }
        let mut leases = BTreeMap::new();
        for id in records.iter().map(|r| r.lease).filter(|&l| l != 0) {
            if leases.contains_key(&id) {
                continue;
            }
            let ttl: LeaseTimeToLiveResponse = self.call(
                "/etcdserverpb.Lease/LeaseTimeToLive",
                &LeaseTimeToLiveRequest { id, keys: false },
            )?;
            // a lease that ended after the revision read ends here soon,
            // and takes its keys with it as it did in etcd
            leases.insert(id, if ttl.ttl < 0 { 1 } else { ttl.granted_ttl });
        }
        Ok(Copy {
            revision,
            records,
            leases,
        })
    }
}

impl Client {
    /// Makes the etcd hold exactly `copy`'s keys and the leases they
    /// hold. etcd numbers every write itself, so it is first written to
    /// until its revision passes the copy's: every resourceVersion a
    /// client holds is then older than what it finds. Not atomic; nothing
    /// else may write meanwhile.
    pub fn replace_all(&self, copy: &Copy) -> Result<(), Status> {
        let mut at = self.revision()?;
        while at < copy.revision {
            let r: PutResponse = self.call(
                "/etcdserverpb.KV/Put",
                &PutRequest {
                    key: SCRATCH.to_vec(),
                    ..Default::default()
                },
            )?;
            at = r.header.map_or(at + 1, |h| h.revision);
        }
        for (&id, &ttl) in &copy.leases {
            // a lease etcd still has would refuse the grant
            let _: Result<LeaseRevokeResponse, _> = self.call(
                "/etcdserverpb.Lease/LeaseRevoke",
                &LeaseRevokeRequest { id },
            );
            let _: LeaseGrantResponse = self.call(
                "/etcdserverpb.Lease/LeaseGrant",
                &LeaseGrantRequest { id, ttl },
            )?;
        }
        let _: DeleteRangeResponse = self.call(
            "/etcdserverpb.KV/DeleteRange",
            &DeleteRangeRequest {
                key: vec![0],
                range_end: vec![0],
                prev_kv: false,
            },
        )?;
        for batch in copy.records.chunks(TXN_OPS) {
            let success = batch
                .iter()
                .map(|r| RequestOp {
                    request: Some(OpRequest::Put(PutRequest {
                        key: r.key.clone(),
                        value: r.value.clone(),
                        lease: r.lease,
                        ..Default::default()
                    })),
                })
                .collect();
            let _: TxnResponse = self.call(
                "/etcdserverpb.KV/Txn",
                &TxnRequest {
                    success,
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }
}

fn record(kv: KeyValue) -> Record {
    Record {
        key: kv.key,
        create_rev: kv.create_revision,
        mod_rev: kv.mod_revision,
        version: kv.version,
        lease: kv.lease,
        value: kv.value,
    }
}
