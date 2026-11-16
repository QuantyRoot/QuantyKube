//! A client of an etcd server over TLS, for moving a cluster's keys from
//! etcd into the store with their revisions (ADR-035). One connection per
//! call: it makes a few hundred calls and is done.

use crate::etcdpb::{
    DeleteRangeRequest, DeleteRangeResponse, LeaseGrantRequest, LeaseGrantResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, OpRequest, PutRequest, RangeRequest, RangeResponse,
    RequestOp, TxnRequest, TxnResponse,
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
/// Scratch writes in flight on one connection while etcd's revision is
/// moved on, and how many go over one connection before the next.
const IN_FLIGHT: usize = 256;
const STREAMS_PER_CONNECTION: i64 = 200_000;
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
    tcp.set_nodelay(true)?;
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

    /// Writes the scratch key `count` times on one connection, up to
    /// IN_FLIGHT writes at once: etcd numbers each write, and puts those
    /// that arrive together under one fsync. One call per connection
    /// took 4 ms a revision, hours after a store ran for days.
    fn put_scratch(&self, count: i64) -> Result<(), Status> {
        use quantycore::grpc::{code, frame_message, Deframer};
        use quantycore::h2::{Connection, Event, Settings};
        use std::collections::HashMap;
        use std::io::{Read, Write};
        let unavailable = |e: String| Status::new(code::UNAVAILABLE, e);
        let mut sock = connect(&self.addr, &self.config)
            .map_err(|e| unavailable(format!("{}: {e}", self.addr)))?;
        let body = frame_message(
            &PutRequest {
                key: SCRATCH.to_vec(),
                ..Default::default()
            }
            .encode(),
        );
        let headers: [(&[u8], &[u8], bool); 6] = [
            (b":method", b"POST", false),
            (b":scheme", b"https", false),
            (b":path", b"/etcdserverpb.KV/Put", false),
            (b":authority", b"localhost", false),
            (b"content-type", b"application/grpc", false),
            (b"te", b"trailers", false),
        ];
        let mut conn = Connection::client(Settings::default());
        // per open stream: its grpc-status once seen, and its body
        let mut open: HashMap<u32, (Option<u32>, String, Deframer)> = HashMap::new();
        let (mut sent, mut done) = (0i64, 0i64);
        let mut buf = vec![0u8; 64 * 1024];
        while done < count {
            while sent < count && open.len() < IN_FLIGHT {
                let id = conn.start_request(&headers, false);
                conn.send_data(id, &body, true);
                open.insert(id, (None, String::new(), Deframer::default()));
                sent += 1;
            }
            sock.write_all(&conn.take_output())
                .map_err(|e| unavailable(e.to_string()))?;
            sock.flush().map_err(|e| unavailable(e.to_string()))?;
            let n = sock
                .read(&mut buf)
                .map_err(|e| unavailable(e.to_string()))?;
            if n == 0 {
                return Err(unavailable("etcd closed the connection".into()));
            }
            let events = conn
                .recv(&buf[..n])
                .map_err(|e| Status::new(code::INTERNAL, e.to_string()))?;
            for ev in events {
                let (stream, end) = match ev {
                    Event::Headers {
                        stream,
                        headers,
                        end_stream,
                    } => {
                        if let Some(o) = open.get_mut(&stream) {
                            for h in headers {
                                match h.name.as_slice() {
                                    b"grpc-status" => {
                                        o.0 = std::str::from_utf8(&h.value)
                                            .ok()
                                            .and_then(|v| v.parse().ok())
                                    }
                                    b"grpc-message" => {
                                        o.1 = String::from_utf8_lossy(&h.value).into()
                                    }
                                    _ => {}
                                }
                            }
                        }
                        (stream, end_stream)
                    }
                    Event::Data {
                        stream,
                        data,
                        end_stream,
                    } => {
                        if let Some(o) = open.get_mut(&stream) {
                            o.2.push(&data)?;
                        }
                        (stream, end_stream)
                    }
                    Event::Reset { stream, code: c } => {
                        return Err(unavailable(format!("stream {stream} reset ({c})")))
                    }
                    Event::GoAway { code: c, .. } => {
                        return Err(unavailable(format!("etcd went away ({c})")))
                    }
                };
                if !end {
                    continue;
                }
                if let Some((status, message, _)) = open.remove(&stream) {
                    match status {
                        Some(code::OK) => done += 1,
                        Some(c) => return Err(Status::new(c, message)),
                        None => {
                            return Err(Status::new(code::INTERNAL, "a put without grpc-status"))
                        }
                    }
                }
            }
        }
        Ok(())
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
        let from = at;
        while at < copy.revision {
            self.put_scratch((copy.revision - at).min(STREAMS_PER_CONNECTION))?;
            at = self.revision()?;
            if copy.revision - from > STREAMS_PER_CONNECTION {
                eprintln!("quantykube: etcd at revision {at} of {}", copy.revision);
            }
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
