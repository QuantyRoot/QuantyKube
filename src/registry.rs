//! Pulling images over the OCI distribution API: token authentication,
//! indexes resolved to linux/amd64, blobs fetched by digest. What it
//! fetches is written as an OCI image layout and imported from there, so
//! every digest is checked in one place (`Runtime::import`).

use quantycore::http1::{self, Request, Url};
use quantycore::json::{self, Value};
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

/// Registries reached without TLS, by host or host:port. Everything else
/// needs HTTPS, which waits for TLS in QuantyCore.
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub plain_http: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub registry: String,
    pub repository: String,
    /// A tag or a `sha256:` digest.
    pub reference: String,
}

const ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
                      application/vnd.oci.image.manifest.v1+json, \
                      application/vnd.docker.distribution.manifest.list.v2+json, \
                      application/vnd.docker.distribution.manifest.v2+json";

/// The largest manifest or index read into memory; real ones are a few
/// KiB.
const MAX_MANIFEST: u64 = 4 << 20;

fn invalid(why: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why.into())
}

/// Parses a reference the way Docker does: `busybox` is
/// `docker.io/library/busybox:latest`.
pub fn parse(image: &str) -> io::Result<Reference> {
    let (name, reference) = match image.split_once('@') {
        Some((n, d)) => (n, d.to_string()),
        None => match image.rsplit_once(':') {
            Some((n, t)) if !t.contains('/') => (n, t.to_string()),
            _ => (image, "latest".to_string()),
        },
    };
    let (registry, repository) = match name.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            (first.to_string(), rest.to_string())
        }
        _ => ("docker.io".to_string(), name.to_string()),
    };
    let repository = if registry == "docker.io" && !repository.contains('/') {
        format!("library/{repository}")
    } else {
        repository
    };
    let ok = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-/".contains(&b))
    };
    let ref_ok = reference
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-:".contains(&b));
    if !ok(&repository) || reference.is_empty() || !ref_ok {
        return Err(invalid(format!("not an image reference: {image}")));
    }
    Ok(Reference {
        registry,
        repository,
        reference,
    })
}

trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

struct Client<'a> {
    opts: &'a Options,
    token: Option<String>,
}

impl Client<'_> {
    fn connect(&self, url: &Url) -> io::Result<Box<dyn Stream>> {
        let plain = url.scheme == "http"
            || self
                .opts
                .plain_http
                .iter()
                .any(|h| *h == url.host || *h == format!("{}:{}", url.host, url.port));
        if !plain {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("{} needs HTTPS, which is not supported yet", url.host),
            ));
        }
        let s = TcpStream::connect((url.host.as_str(), url.port))?;
        s.set_read_timeout(Some(Duration::from_secs(60)))?;
        s.set_write_timeout(Some(Duration::from_secs(60)))?;
        Ok(Box::new(s))
    }

    /// GETs `url`, answering one Bearer challenge and following up to five
    /// redirects, and hands the successful response to `f`.
    fn get<T>(
        &mut self,
        url: Url,
        accept: Option<&str>,
        f: &mut dyn FnMut(&mut dyn Read) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut url = url;
        let mut challenged = false;
        for _ in 0..6 {
            let host = url.authority();
            let auth = self.token.as_ref().map(|t| format!("Bearer {t}"));
            let mut headers = vec![("User-Agent", "quantykube")];
            if let Some(a) = accept {
                headers.push(("Accept", a));
            }
            if let Some(a) = &auth {
                headers.push(("Authorization", a));
            }
            let mut resp = http1::send(
                self.connect(&url)?,
                &Request {
                    method: "GET",
                    target: &url.target,
                    host: &host,
                    headers: &headers,
                },
            )?;
            match resp.status {
                200 => return f(&mut resp),
                301 | 302 | 303 | 307 | 308 => {
                    let loc = resp
                        .header("location")
                        .ok_or_else(|| invalid("redirect without a location"))?;
                    let next = http1::resolve(&url, loc).map_err(|e| invalid(format!("{e:?}")))?;
                    // a blob store on another host gets no registry token,
                    // and neither does anything reached without TLS
                    if next.host != url.host || next.scheme != url.scheme {
                        self.token = None;
                    }
                    url = next;
                }
                401 if !challenged => {
                    challenged = true;
                    let header = resp
                        .header("www-authenticate")
                        .ok_or_else(|| invalid("401 without a challenge"))?
                        .to_string();
                    self.token = Some(self.authenticate(&header)?);
                }
                status => {
                    return Err(io::Error::other(format!(
                        "{}{}: HTTP {status}",
                        url.host, url.target
                    )))
                }
            }
        }
        Err(io::Error::other("too many redirects"))
    }

    fn authenticate(&mut self, challenge: &str) -> io::Result<String> {
        let (scheme, params) = http1::parse_www_authenticate(challenge)
            .ok_or_else(|| invalid("unreadable challenge"))?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(io::Error::other(format!(
                "registry asks for {scheme} authentication, only anonymous Bearer is supported"
            )));
        }
        let param = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        let realm = param("realm").ok_or_else(|| invalid("challenge without realm"))?;
        let mut target = String::from(realm);
        let mut sep = if realm.contains('?') { '&' } else { '?' };
        for k in ["service", "scope"] {
            if let Some(v) = param(k) {
                target.push(sep);
                target.push_str(&format!("{k}={}", encode(v)));
                sep = '&';
            }
        }
        let url = http1::parse_url(&target).map_err(|e| invalid(format!("{e:?}")))?;
        let mut anonymous = Client {
            opts: self.opts,
            token: None,
        };
        let body = anonymous.get(url, None, &mut |r| read_limited(r))?;
        let v = json::parse_bytes(&body).map_err(|e| invalid(format!("token: {e}")))?;
        v.get("token")
            .or_else(|| v.get("access_token"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| invalid("token response without a token"))
    }
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn read_limited(r: &mut dyn Read) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    r.take(MAX_MANIFEST + 1).read_to_end(&mut buf)?;
    if buf.len() as u64 > MAX_MANIFEST {
        return Err(invalid("manifest too large"));
    }
    Ok(buf)
}

fn hex(d: &[u8]) -> String {
    quantydb_core::to_hex(&quantydb_core::sha256(d))
}

fn base_url(r: &Reference) -> io::Result<Url> {
    let host = if r.registry == "docker.io" {
        "registry-1.docker.io"
    } else {
        &r.registry
    };
    http1::parse_url(&format!("https://{host}/")).map_err(|e| invalid(format!("{e:?}")))
}

fn at(base: &Url, target: String) -> Url {
    Url {
        target,
        ..base.clone()
    }
}

/// Fetches the image into `layout`, an empty directory, as an OCI image
/// layout with the chosen manifest as its only entry.
pub fn pull(image: &str, layout: &Path, opts: &Options) -> io::Result<()> {
    let r = parse(image)?;
    let mut base = base_url(&r)?;
    if opts.plain_http.contains(&r.registry) {
        base = http1::parse_url(&format!("http://{}/", r.registry))
            .map_err(|e| invalid(format!("{e:?}")))?;
    }
    let mut client = Client { opts, token: None };
    let blobs = layout.join("blobs").join("sha256");
    fs::create_dir_all(&blobs)?;
    let manifest_url =
        |reference: &str| at(&base, format!("/v2/{}/manifests/{reference}", r.repository));
    let mut reference = r.reference.clone();
    for _ in 0..2 {
        let body = client.get(manifest_url(&reference), Some(ACCEPT), &mut |rd| {
            read_limited(rd)
        })?;
        let digest = format!("sha256:{}", hex(&body));
        if reference.starts_with("sha256:") && reference != digest {
            return Err(invalid(format!("{reference} does not match its digest")));
        }
        fs::write(blobs.join(&digest[7..]), &body)?;
        let v = json::parse_bytes(&body).map_err(|e| invalid(format!("manifest: {e}")))?;
        if let Some(entries) = v.get("manifests").and_then(Value::as_array) {
            let p = |e: &Value, k: &str| {
                e.pointer(&format!("/platform/{k}"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            };
            let entry = entries
                .iter()
                .find(|e| {
                    p(e, "os").as_deref() == Some("linux")
                        && p(e, "architecture").as_deref() == Some("amd64")
                })
                .ok_or_else(|| invalid(format!("{image} has no linux/amd64 image")))?;
            reference = entry
                .get("digest")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("index entry without digest"))?
                .to_string();
            continue;
        }
        let mut digests: Vec<String> = Vec::new();
        if let Some(d) = v.pointer("/config/digest").and_then(Value::as_str) {
            digests.push(d.to_string());
        }
        for l in v
            .get("layers")
            .and_then(Value::as_array)
            .unwrap_or_default()
        {
            if let Some(d) = l.get("digest").and_then(Value::as_str) {
                digests.push(d.to_string());
            }
        }
        for d in digests {
            let hexd = d
                .strip_prefix("sha256:")
                .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or_else(|| invalid(format!("unsupported digest {d}")))?;
            let path = blobs.join(hexd);
            let url = at(&base, format!("/v2/{}/blobs/{d}", r.repository));
            client.get(url, None, &mut |rd| {
                let mut f = fs::File::create(&path)?;
                io::copy(rd, &mut f)?;
                f.sync_all()
            })?;
        }
        fs::write(
            layout.join("oci-layout"),
            r#"{"imageLayoutVersion":"1.0.0"}"#,
        )?;
        let index = format!(
            r#"{{"schemaVersion":2,"manifests":[{{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"{digest}","size":{}}}]}}"#,
            body.len()
        );
        fs::write(layout.join("index.json"), index)?;
        return Ok(());
    }
    Err(invalid(format!(
        "{image}: an index points to another index"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_are_read_as_docker_reads_them() {
        let r = |s: &str| {
            let r = parse(s).unwrap();
            format!("{} {} {}", r.registry, r.repository, r.reference)
        };
        assert_eq!(r("busybox"), "docker.io library/busybox latest");
        assert_eq!(r("nginx:1.27"), "docker.io library/nginx 1.27");
        assert_eq!(r("bitnami/redis"), "docker.io bitnami/redis latest");
        assert_eq!(r("localhost:5000/a/b:v1"), "localhost:5000 a/b v1");
        let d = format!("sha256:{}", "a".repeat(64));
        assert_eq!(r(&format!("ghcr.io/x/y@{d}")), format!("ghcr.io x/y {d}"));
        assert!(parse("UPPER/case").is_err());
        assert!(parse("a/b:../x").is_err());
    }
}
