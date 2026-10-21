//! History through the Kubernetes API (ADR-028): an aggregated API that
//! kube-apiserver forwards to after it has authenticated the caller and
//! checked RBAC, so that `kubectl quanty` needs nothing but a kubeconfig.
//!
//! The requests and answers are the History service's own protobuf
//! messages (docs/HISTORY.md): a read carries its request base64url
//! encoded in `?q=`, a restore its request as the body.
//!
//!   GET  /apis/quanty.quantyroot.io/v1alpha1/logs?q=...      Log
//!   GET  /apis/quanty.quantyroot.io/v1alpha1/objects?q=...   Get
//!   POST /apis/quanty.quantyroot.io/v1alpha1/restores        Restore
//!
//! RBAC therefore speaks of `get` on `logs` and `objects` and `create` on
//! `restores` in the group `quanty.quantyroot.io`.

use crate::tlsnet::TlsSocket;
use quantycore::grpc::{Service, Status};
use quantycore::tls::ServerConfig;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::Arc;

pub const GROUP: &str = "quanty.quantyroot.io";
pub const VERSION: &str = "v1alpha1";
const MAX_HEAD: usize = 64 << 10;
const MAX_BODY: usize = 1 << 20;

pub(crate) struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: Vec<u8>,
    pub user: Option<String>,
    pub close: bool,
}

pub(crate) fn read_request(r: &mut impl BufRead) -> io::Result<Option<Request>> {
    let mut head = Vec::new();
    loop {
        let mut line = Vec::new();
        if r.read_until(b'\n', &mut line)? == 0 {
            return if head.is_empty() {
                Ok(None)
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            };
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        head.extend_from_slice(&line);
        if head.len() > MAX_HEAD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "head too large"));
        }
    }
    let head = String::from_utf8(head).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    let mut lines = head.lines();
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let (method, target) = (first.next().unwrap_or(""), first.next().unwrap_or(""));
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut len = 0usize;
    let mut chunked = false;
    let mut user = None;
    let mut close = false;
    for l in lines {
        let Some((name, value)) = l.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                len = value
                    .parse()
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?
            }
            // kubectl sends a body read from stdin in chunks, and
            // kube-apiserver passes them on as they came
            "transfer-encoding" => {
                if !value.eq_ignore_ascii_case("chunked") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "transfer coding",
                    ));
                }
                chunked = true;
            }
            "x-remote-user" => user = Some(value.to_string()),
            "connection" => close = value.eq_ignore_ascii_case("close"),
            _ => {}
        }
    }
    if len > MAX_BODY {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
    }
    let body = if chunked {
        read_chunked(r)?
    } else {
        let mut body = vec![0u8; len];
        r.read_exact(&mut body)?;
        body
    };
    Ok(Some(Request {
        method: method.to_string(),
        path: path.to_string(),
        query: query.to_string(),
        body,
        user,
        close,
    }))
}

fn read_chunked(r: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        r.read_line(&mut line)?;
        let size = line.trim().split(';').next().unwrap_or("");
        let n = usize::from_str_radix(size, 16).map_err(|_| bad("chunk size"))?;
        if body.len() + n > MAX_BODY {
            return Err(bad("body too large"));
        }
        if n == 0 {
            // trailers, then the empty line
            loop {
                let mut t = String::new();
                if r.read_line(&mut t)? == 0 || t.trim().is_empty() {
                    return Ok(body);
                }
            }
        }
        let start = body.len();
        body.resize(start + n, 0);
        r.read_exact(&mut body[start..])?;
        let mut crlf = [0u8; 2];
        r.read_exact(&mut crlf)?;
        if &crlf != b"\r\n" {
            return Err(bad("chunk end"));
        }
    }
}

pub(crate) fn reply(w: &mut impl Write, code: u16, ctype: &str, body: &[u8]) -> io::Result<()> {
    let reason = match code {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    w.write_all(head.as_bytes())?;
    w.write_all(body)?;
    w.flush()
}

/// A Kubernetes Status object, which kubectl shows as the error.
fn status_json(code: u16, message: &str) -> Vec<u8> {
    let mut m = String::new();
    for c in message.chars() {
        match c {
            '"' => m.push_str("\\\""),
            '\\' => m.push_str("\\\\"),
            c if (c as u32) < 0x20 => m.push(' '),
            c => m.push(c),
        }
    }
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","metadata":{{}},"status":"Failure","message":"{m}","code":{code}}}"#
    )
    .into_bytes()
}

fn grpc_to_http(s: &Status) -> u16 {
    use quantycore::grpc::code;
    match s.code {
        code::INVALID_ARGUMENT | code::OUT_OF_RANGE => 400,
        code::NOT_FOUND => 404,
        code::UNAVAILABLE => 503,
        _ => 500,
    }
}

/// Decodes base64url, padded or not.
pub fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn discovery(path: &str) -> Option<String> {
    let resource = |name: &str, kind: &str, verbs: &str| {
        format!(
            r#"{{"name":"{name}","singularName":"","namespaced":false,"kind":"{kind}","verbs":[{verbs}]}}"#
        )
    };
    let gv = format!("{GROUP}/{VERSION}");
    let group = format!(
        r#"{{"name":"{GROUP}","versions":[{{"groupVersion":"{gv}","version":"{VERSION}"}}],"preferredVersion":{{"groupVersion":"{gv}","version":"{VERSION}"}}}}"#
    );
    if path == format!("/apis/{GROUP}/{VERSION}") {
        return Some(format!(
            r#"{{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"{gv}","resources":[{},{},{}]}}"#,
            resource("logs", "Log", r#""get""#),
            resource("objects", "Object", r#""get""#),
            resource("restores", "Restore", r#""create""#),
        ));
    }
    if path == format!("/apis/{GROUP}") {
        return Some(format!(
            r#"{{"kind":"APIGroup","apiVersion":"v1",{}"#,
            &group[1..]
        ));
    }
    if path == "/apis" {
        return Some(format!(
            r#"{{"kind":"APIGroupList","apiVersion":"v1","groups":[{group}]}}"#
        ));
    }
    None
}

fn handle(service: &dyn Service, req: &Request) -> (u16, &'static str, Vec<u8>) {
    let json = "application/json";
    if let Some(d) = discovery(&req.path) {
        return (200, json, d.into_bytes());
    }
    // kube-apiserver names the caller it authenticated; without one the
    // request did not come through it
    if req.user.is_none() {
        return (401, json, status_json(401, "no authenticated user"));
    }
    let prefix = format!("/apis/{GROUP}/{VERSION}/");
    let Some(resource) = req.path.strip_prefix(&prefix) else {
        return (404, json, status_json(404, "not found"));
    };
    let (method, body) = match (req.method.as_str(), resource) {
        ("GET", "logs" | "objects") => {
            let q = req
                .query
                .split('&')
                .find_map(|kv| kv.strip_prefix("q="))
                .unwrap_or("");
            let Some(body) = base64url_decode(q) else {
                return (400, json, status_json(400, "q is not base64url"));
            };
            (if resource == "logs" { "Log" } else { "Get" }, body)
        }
        ("POST", "restores") => ("Restore", req.body.clone()),
        (_, "logs" | "objects" | "restores") => {
            return (405, json, status_json(405, "method not allowed"));
        }
        _ => return (404, json, status_json(404, "not found")),
    };
    match service.unary(&format!("/quantykube.History/{method}"), &body) {
        Ok(answer) => {
            let code = if method == "Restore" { 201 } else { 200 };
            (code, "application/x-protobuf", answer)
        }
        Err(s) => {
            let code = grpc_to_http(&s);
            (code, json, status_json(code, &s.message))
        }
    }
}

/// Serves the aggregated API on `listener` to clients with a certificate
/// from the front proxy CA in `config`, which only kube-apiserver holds.
pub fn serve(listener: TcpListener, config: Arc<ServerConfig>, service: Arc<dyn Service>) {
    for tcp in listener.incoming() {
        let Ok(tcp) = tcp else { continue };
        let (config, service) = (config.clone(), service.clone());
        std::thread::spawn(move || {
            let Ok(socket) = TlsSocket::accept(tcp, config) else {
                return;
            };
            let Ok(mut w) = quantycore::grpc::Socket::try_clone(&socket) else {
                return;
            };
            let mut r = BufReader::new(socket);
            while let Ok(Some(req)) = read_request(&mut r) {
                let (code, ctype, body) = handle(service.as_ref(), &req);
                if reply(&mut w, code, ctype, &body).is_err() || req.close {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunked_body_is_read_whole() {
        let wire = b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2;x=y\r\nde\r\n0\r\n\r\n";
        let mut r = &wire[..];
        let req = read_request(&mut r).unwrap().unwrap();
        assert_eq!(req.body, b"abcde");
        assert!(r.is_empty());
    }

    #[test]
    fn base64url_comes_back() {
        assert_eq!(base64url_decode("aGVsbG8").unwrap(), b"hello");
        assert_eq!(base64url_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64url_decode("_-8").unwrap(), [0xff, 0xef]);
        assert!(base64url_decode("a b").is_none());
    }
}
