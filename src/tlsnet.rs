//! The store over TCP with TLS, for an API server on another machine and
//! for the other store nodes (ADR-031). Every client presents a
//! certificate from the configured CA: over a network that is the only
//! access control, as the socket's mode is on the node.

use quantycore::grpc::{Options, Server, Service, Socket};
use quantycore::tls::x509::{PrivateKey, RootStore};
use quantycore::tls::{ServerConfig, ServerConnection};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long a client has to finish the TLS handshake.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// A TLS connection the gRPC server can read on one thread and write on
/// another, as it does with a plain socket: both halves share the TLS
/// state, and each holds it only while it moves bytes through it.
pub struct TlsSocket {
    tcp: TcpStream,
    conn: Arc<Mutex<ServerConnection>>,
}

fn tls_error(e: quantycore::tls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}"))
}

impl TlsSocket {
    /// Runs the handshake on `tcp`, which must end with a client
    /// certificate the configuration accepts.
    pub fn accept(tcp: TcpStream, config: Arc<ServerConfig>) -> io::Result<TlsSocket> {
        tcp.set_read_timeout(Some(HANDSHAKE))?;
        tcp.set_write_timeout(Some(HANDSHAKE))?;
        let mut s = TlsSocket {
            tcp,
            conn: Arc::new(Mutex::new(ServerConnection::new(config))),
        };
        while s.lock().is_handshaking() {
            s.pump()?;
        }
        if s.lock().peer_certificates().is_none_or(|c| c.is_empty()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "no client certificate",
            ));
        }
        s.tcp.set_read_timeout(None)?;
        s.tcp.set_write_timeout(None)?;
        Ok(s)
    }

    /// The application protocol the handshake settled on.
    pub fn alpn(&self) -> Option<Vec<u8>> {
        self.lock().alpn_protocol().map(<[u8]>::to_vec)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ServerConnection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reads once from the network, feeds the TLS state, and sends what
    /// it answers. Returns false at the end of the stream.
    fn pump(&mut self) -> io::Result<bool> {
        let mut buf = [0u8; 16 * 1024 + 512];
        let n = self.tcp.read(&mut buf)?;
        let mut conn = self.lock();
        if n == 0 {
            if conn.is_peer_closed() {
                return Ok(false);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection ended without close_notify",
            ));
        }
        let fed = conn.read_tls(&buf[..n]);
        // an alert queued by a failure still goes out
        let out = conn.take_tls();
        if !out.is_empty() {
            (&self.tcp).write_all(&out)?;
        }
        fed.map_err(tls_error)?;
        Ok(true)
    }
}

impl Read for TlsSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            {
                let mut conn = self.lock();
                if conn.has_plaintext() {
                    return Ok(conn.read(buf));
                }
                if conn.is_peer_closed() {
                    return Ok(0);
                }
            }
            if !self.pump()? {
                return Ok(0);
            }
        }
    }
}

impl Write for TlsSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut conn = self.lock();
        conn.write(buf).map_err(tls_error)?;
        // written under the lock, so records leave in the order they were
        // sealed even with the reader answering a key update
        let out = conn.take_tls();
        (&self.tcp).write_all(&out)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tcp.flush()
    }
}

impl Socket for TlsSocket {
    fn try_clone(&self) -> io::Result<Self> {
        Ok(TlsSocket {
            tcp: self.tcp.try_clone()?,
            conn: self.conn.clone(),
        })
    }

    fn set_read_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.tcp.set_read_timeout(t)
    }

    fn set_write_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.tcp.set_write_timeout(t)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        if how != Shutdown::Read {
            let mut conn = self.lock();
            conn.close();
            let _ = (&self.tcp).write_all(&conn.take_tls());
        }
        self.tcp.shutdown(how)
    }
}

/// A server certificate chain and key, and the CA that client
/// certificates must come from, all PEM files.
pub fn server_config(cert: &Path, key: &Path, client_ca: &Path) -> Result<ServerConfig, String> {
    let read = |p: &Path| std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()));
    let chain = quantycore::tls::x509::pem::decode_all(&read(cert)?, "CERTIFICATE")
        .map_err(|e| format!("{}: {e}", cert.display()))?;
    if chain.is_empty() {
        return Err(format!("{}: no certificate", cert.display()));
    }
    let key = PrivateKey::from_pem(&read(key)?).map_err(|e| format!("{}: {e}", key.display()))?;
    let mut roots = RootStore::new();
    let (ok, _) = roots
        .add_pem(&read(client_ca)?)
        .map_err(|e| format!("{}: {e:?}", client_ca.display()))?;
    if ok == 0 {
        return Err(format!("{}: no CA certificate", client_ca.display()));
    }
    let mut config = ServerConfig::new(chain, key).map_err(|e| e.to_string())?;
    config.require_client_certificates(roots);
    // etcd's clients, the API server among them, speak gRPC over h2
    config.set_alpn(&[b"h2"]);
    Ok(config)
}

/// Accepts TLS connections forever on `listener`, one thread each. A
/// client that asks for HTTP/1.1 gets what etcd serves on the same port
/// besides gRPC: `/version`, which kubeadm checks before it uses an
/// external etcd, and `/health`.
pub fn serve(
    listener: TcpListener,
    config: Arc<ServerConfig>,
    server: Arc<Server>,
    service: Arc<dyn Service>,
) -> io::Result<()> {
    for tcp in listener.incoming() {
        let Ok(tcp) = tcp else { continue };
        let (config, server, service) = (config.clone(), server.clone(), service.clone());
        std::thread::spawn(move || {
            let Ok(socket) = TlsSocket::accept(tcp, config) else {
                return;
            };
            if socket.alpn().as_deref() == Some(b"http/1.1") {
                serve_http(socket, service.as_ref());
            } else {
                server.serve_connection(socket);
            }
        });
    }
    Ok(())
}

fn serve_http(socket: TlsSocket, service: &dyn Service) {
    use crate::aggapi::{read_request, reply};
    let Ok(mut w) = socket.try_clone() else {
        return;
    };
    let mut r = io::BufReader::new(socket);
    while let Ok(Some(req)) = read_request(&mut r) {
        let (code, body) = match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/version") => (
                200,
                format!(
                    "{{\"etcdserver\":\"{v}\",\"etcdcluster\":\"{v}\"}}",
                    v = crate::etcd::ETCD_VERSION
                ),
            ),
            // as etcd: healthy when a linearizable read is answered
            ("GET", "/health") => {
                let read = crate::etcdpb::RangeRequest {
                    key: b"health".to_vec(),
                    ..Default::default()
                };
                match service.unary(
                    "/etcdserverpb.KV/Range",
                    &crate::etcdpb::Message::encode(&read),
                ) {
                    Ok(_) => (200, r#"{"health":"true","reason":""}"#.to_string()),
                    Err(_) => (503, r#"{"health":"false","reason":"no answer"}"#.into()),
                }
            }
            _ => (404, "404 page not found\n".into()),
        };
        let ctype = if code == 404 {
            "text/plain"
        } else {
            "application/json"
        };
        if reply(&mut w, code, ctype, body.as_bytes()).is_err() || req.close {
            return;
        }
    }
}

/// The gRPC server both listeners share, so its limits hold across them.
pub fn grpc_server(service: Arc<dyn Service>) -> Arc<Server> {
    Server::new(service, Options::default())
}
