//! A client of an etcd server over TLS, for moving a cluster's keys from
//! etcd into the store with their revisions (ADR-035). One connection per
//! call: it makes a few hundred calls and is done.

use crate::etcdpb::{KeyValue, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, Message};
use crate::etcdpb::{RangeRequest, RangeResponse};
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
