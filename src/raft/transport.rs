//! Raft messages between store nodes: one TLS connection to each peer,
//! both ends showing a certificate from the peer CA, carrying messages as
//! length-prefixed frames. Raft tolerates loss, so a broken connection
//! drops what was queued and is opened again; nothing here retries a
//! message.

use super::{codec, Message, NodeId};
use crate::tlsnet::TlsSocket;
use quantycore::tls::{ClientConfig, ClientConnection, ServerConfig, Stream};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;

/// Larger than any batch of entries and than the snapshot of a store of
/// the size kube-apiserver keeps by default (etcd's quota is 2 GiB, its
/// usual size tens of megabytes); a frame is read whole.
const MAX_FRAME: usize = 256 << 20;
/// Messages waiting for one peer; beyond it they are dropped, as a slow
/// network would drop them.
const QUEUE: usize = 4096;

pub struct Transport {
    peers: BTreeMap<NodeId, SyncSender<Message>>,
}

impl Transport {
    /// Starts a sender for each of `peers` (id to `host:port`) and a
    /// listener on `listen`; what arrives comes out of the receiver.
    pub fn start(
        peers: &BTreeMap<NodeId, String>,
        listen: TcpListener,
        client: Arc<ClientConfig>,
        server: Arc<ServerConfig>,
    ) -> (Transport, Receiver<Message>) {
        let (inbound, rx) = mpsc::channel();
        let mut senders = BTreeMap::new();
        for (id, addr) in peers {
            let (tx, queue) = mpsc::sync_channel(QUEUE);
            let (addr, client) = (addr.clone(), client.clone());
            std::thread::spawn(move || send_loop(&addr, client, queue));
            senders.insert(*id, tx);
        }
        std::thread::spawn(move || accept_loop(listen, server, inbound));
        (Transport { peers: senders }, rx)
    }

    pub fn send(&self, m: Message) {
        if let Some(tx) = self.peers.get(&m.to) {
            match tx.try_send(m) {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => {}
            }
        }
    }
}

fn write_frame(w: &mut impl Write, m: &Message) -> io::Result<()> {
    let body = codec::encode(m);
    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    w.write_all(&frame)
}

fn read_frame(r: &mut impl Read) -> io::Result<Message> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    codec::decode(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn connect(
    addr: &str,
    client: &Arc<ClientConfig>,
) -> io::Result<Stream<ClientConnection, TcpStream>> {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    // a name may resolve to several addresses, and the peer may listen
    // on only one of them, as localhost does on ::1 and 127.0.0.1
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no address");
    let mut tcp = None;
    for sock in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&sock, Duration::from_secs(3)) {
            Ok(t) => {
                tcp = Some(t);
                break;
            }
            Err(e) => last = e,
        }
    }
    let tcp = tcp.ok_or(last)?;
    tcp.set_nodelay(true)?;
    tcp.set_write_timeout(Some(Duration::from_secs(10)))?;
    let conn = ClientConnection::new(client.clone(), host)
        .map_err(|e| io::Error::other(format!("{e:?}")))?;
    let mut stream = Stream::new(conn, tcp);
    stream.handshake()?;
    Ok(stream)
}

fn send_loop(addr: &str, client: Arc<ClientConfig>, queue: Receiver<Message>) {
    let mut stream = None;
    while let Ok(m) = queue.recv() {
        if stream.is_none() {
            stream = connect(addr, &client).ok();
            if stream.is_none() {
                // what waited is stale by the time a peer comes back
                while queue.try_recv().is_ok() {}
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        }
        let s = stream.as_mut().expect("connected above");
        if write_frame(s, &m).and_then(|()| s.flush()).is_err() {
            stream = None;
        }
    }
}

fn accept_loop(listen: TcpListener, server: Arc<ServerConfig>, inbound: Sender<Message>) {
    for tcp in listen.incoming() {
        let Ok(tcp) = tcp else { continue };
        let (server, inbound) = (server.clone(), inbound.clone());
        std::thread::spawn(move || {
            let _ = tcp.set_nodelay(true);
            let Ok(mut socket) = TlsSocket::accept(tcp, server) else {
                return;
            };
            while let Ok(m) = read_frame(&mut socket) {
                if inbound.send(m).is_err() {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Body;
    use quantycore::tls::crypto::ec::Curve;
    use quantycore::tls::x509::build::{issue, Issuer, Params, SanEntry};
    use quantycore::tls::x509::{PrivateKey, RootStore};

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// A CA, and from it a certificate for localhost that serves and
    /// connects, as each store node's does.
    fn pki() -> (Vec<u8>, Vec<u8>, PrivateKey) {
        let ca_key = PrivateKey::generate_ec(Curve::P256).unwrap();
        let ca = issue(
            &Params::ca("peer CA", now() - 60, now() + 3600),
            &ca_key.public_spki(),
            None,
            &ca_key,
        )
        .unwrap();
        let key = PrivateKey::generate_ec(Curve::P256).unwrap();
        let cert = issue(
            &Params::end_entity(
                "localhost",
                vec![SanEntry::Dns("localhost".into())],
                now() - 60,
                now() + 3600,
            ),
            &key.public_spki(),
            Some(&Issuer::from_certificate(&ca, &ca_key).unwrap()),
            &key,
        )
        .unwrap();
        (ca, cert, key)
    }

    #[test]
    fn messages_cross_between_nodes_over_tls() {
        let (ca, cert, key) = pki();
        let mut roots = RootStore::new();
        roots.add_der(&ca).unwrap();
        let mut client = ClientConfig::new(roots.clone());
        // keys do not clone; the same one twice, through its PEM
        let pem = key.to_pem();
        client.set_client_certificate(vec![cert.clone()], PrivateKey::from_pem(&pem).unwrap());
        let mut server = ServerConfig::new(vec![cert], key).unwrap();
        server.require_client_certificates(roots);
        let (client, server) = (Arc::new(client), Arc::new(server));

        let la = TcpListener::bind("127.0.0.1:0").unwrap();
        let lb = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = |l: &TcpListener| format!("localhost:{}", l.local_addr().unwrap().port());
        let (pa, pb) = (addr(&la), addr(&lb));
        let pb_addr = pb.clone();
        let (a, _ra) = Transport::start(
            &BTreeMap::from([(2, pb)]),
            la,
            client.clone(),
            server.clone(),
        );
        let (b, rb) = Transport::start(&BTreeMap::from([(1, pa)]), lb, client, server);
        let m = |i| Message {
            from: 1,
            to: 2,
            term: 3,
            body: Body::Propose {
                data: vec![i; 1000],
            },
        };
        // the first messages may go before the peer listens; Raft sends
        // again, so does this test
        let warm = Message {
            body: Body::ReadCheck { seq: 0 },
            ..m(0)
        };
        let mut up = false;
        for _ in 0..50 {
            a.send(warm.clone());
            if rb.recv_timeout(Duration::from_millis(100)).is_ok() {
                up = true;
                break;
            }
        }
        assert!(up, "no connection");
        for i in 0..100 {
            a.send(m(i));
        }
        let mut got = Vec::new();
        while got.len() < 100 {
            let x = rb.recv_timeout(Duration::from_secs(5)).unwrap();
            if x != warm {
                got.push(x);
            }
        }
        assert!(got.iter().enumerate().all(|(i, x)| *x == m(i as u8)));

        // a node without a peer certificate is not heard
        let mut anon_roots = RootStore::new();
        anon_roots.add_der(&ca).unwrap();
        let anon = Arc::new(ClientConfig::new(anon_roots));
        if let Ok(mut s) = connect(&pb_addr, &anon) {
            let _ = write_frame(&mut s, &m(7)).and_then(|()| s.flush());
        }
        assert!(rb.recv_timeout(Duration::from_millis(500)).is_err());
        drop(b);
    }
}
