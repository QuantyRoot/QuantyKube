//! A small client of the Kubernetes API for the installer's operator
//! (ADR-040): JSON over HTTP/1.1 and TLS, one connection per request,
//! with the service account the operator's pod is given.

use quantycore::json::{self, Value};
use quantycore::tls::ClientConfig;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

const SERVICE_ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
/// Larger than any answer the operator asks for.
const MAX_BODY: usize = 64 << 20;

pub struct Kube {
    addr: String,
    token: String,
    config: Arc<ClientConfig>,
}

pub struct Response {
    pub status: u16,
    pub body: Value,
}

impl Response {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The API server's message for a refused request.
    pub fn message(&self) -> String {
        self.body
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| format!("status {}", self.status), str::to_string)
    }
}

impl Kube {
    /// The API server as a pod sees it, with the pod's service account.
    pub fn in_cluster() -> Result<Kube, String> {
        let env = |k: &str| std::env::var(k).map_err(|_| format!("{k} is not set"));
        let host = env("KUBERNETES_SERVICE_HOST")?;
        let port = env("KUBERNETES_SERVICE_PORT")?;
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        let dir = Path::new(SERVICE_ACCOUNT);
        let token = std::fs::read_to_string(dir.join("token"))
            .map_err(|e| format!("the service account's token: {e}"))?;
        let config = crate::etcdclient::client_config(&dir.join("ca.crt"), None, &[b"http/1.1"])?;
        Ok(Kube {
            addr: format!("{host}:{port}"),
            token: token.trim().to_string(),
            config: Arc::new(config),
        })
    }

    pub fn get(&self, path: &str) -> Result<Response, String> {
        self.request("GET", path, None)
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Response, String> {
        self.request("POST", path, Some(("application/json", body.to_string())))
    }

    pub fn delete(&self, path: &str) -> Result<Response, String> {
        self.request("DELETE", path, None)
    }

    /// A JSON merge patch (RFC 7386).
    pub fn merge_patch(&self, path: &str, body: &Value) -> Result<Response, String> {
        self.request(
            "PATCH",
            path,
            Some(("application/merge-patch+json", body.to_string())),
        )
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<(&str, String)>,
    ) -> Result<Response, String> {
        let fail = |e: std::io::Error| format!("{method} {path}: {e}");
        let mut stream = crate::etcdclient::connect(&self.addr, &self.config).map_err(fail)?;
        let mut head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
             Accept: application/json\r\nUser-Agent: quantykube\r\nConnection: close\r\n",
            self.addr, self.token
        );
        let payload = match &body {
            Some((ctype, text)) => {
                head.push_str(&format!(
                    "Content-Type: {ctype}\r\nContent-Length: {}\r\n",
                    text.len()
                ));
                text.as_bytes()
            }
            None => &[][..],
        };
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).map_err(fail)?;
        stream.write_all(payload).map_err(fail)?;
        stream.flush().map_err(fail)?;
        let mut raw = Vec::new();
        stream
            .take(MAX_BODY as u64 + 65536)
            .read_to_end(&mut raw)
            .map_err(fail)?;
        let (status, body) = parse_response(&raw).map_err(|e| format!("{method} {path}: {e}"))?;
        // a pod's log is text, everything else JSON
        let body = if body.is_empty() {
            Value::Null
        } else {
            json::parse_bytes(&body)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()))
        };
        Ok(Response { status, body })
    }
}

/// The status and the body of a whole HTTP/1.1 response, read to the end
/// of the connection.
fn parse_response(raw: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no end of the response's head")?;
    let head = std::str::from_utf8(&raw[..end]).map_err(|_| "a head that is not UTF-8")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("no status")?;
    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = &raw[end + 4..];
    if !chunked {
        return Ok((status, body.to_vec()));
    }
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let eol = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("a chunk without its size line")?;
        let size = std::str::from_utf8(&rest[..eol]).map_err(|_| "chunk size")?;
        let size = size.split(';').next().unwrap_or("").trim();
        let n = usize::from_str_radix(size, 16).map_err(|_| "chunk size")?;
        rest = &rest[eol + 2..];
        if n == 0 {
            return Ok((status, out));
        }
        if rest.len() < n + 2 || out.len() + n > MAX_BODY {
            return Err("a chunk cut short".into());
        }
        out.extend_from_slice(&rest[..n]);
        rest = &rest[n + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_response_is_read_plain_or_chunked() {
        let plain = b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(parse_response(plain).unwrap(), (201, b"{}".to_vec()));
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                        3\r\n{\"a\r\n5;x=y\r\n\":1}\n\r\n0\r\n\r\n";
        assert_eq!(
            parse_response(chunked).unwrap(),
            (200, b"{\"a\":1}\n".to_vec())
        );
        assert!(parse_response(b"HTTP/1.1 200 OK\r\n").is_err());
        let short = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nff\r\nab";
        assert!(parse_response(short).is_err());
    }
}
