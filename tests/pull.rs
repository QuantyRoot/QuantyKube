//! A pull from a registry served here: a login for a Bearer token, an index
//! resolved to linux/amd64, blobs behind a redirect.

use quantykube::registry::{self, Options};
use quantykube::runtime::Runtime;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::{Arc, Mutex};

fn digest(data: &[u8]) -> String {
    format!(
        "sha256:{}",
        quantydb_core::to_hex(&quantydb_core::sha256(data))
    )
}

type Routes = Arc<Mutex<HashMap<String, (String, Vec<u8>)>>>;

/// Answers one request per connection. Without the token, every /v2/ path
/// gets a challenge.
fn serve(listener: TcpListener, routes: Routes, log: Arc<Mutex<Vec<String>>>) {
    for conn in listener.incoming() {
        let Ok(mut conn) = conn else { return };
        let mut reader = BufReader::new(conn.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
        let mut authorized = false;
        let mut logged_in = false;
        loop {
            let mut h = String::new();
            reader.read_line(&mut h).unwrap();
            if h.trim().is_empty() {
                break;
            }
            // user:pass
            if h.trim_end() == "Authorization: Basic dXNlcjpwYXNz" {
                logged_in = true;
            }
            if h.to_ascii_lowercase()
                .starts_with("authorization: bearer secret")
            {
                authorized = true;
            }
        }
        log.lock().unwrap().push(path.clone());
        let port = conn.local_addr().unwrap().port();
        let realm = routes
            .lock()
            .unwrap()
            .get("realm")
            .map_or("127.0.0.1".to_string(), |(host, _)| host.clone());
        let reply = |c: &mut TcpStream, status: &str, extra: &str, ctype: &str, body: &[u8]| {
            let head = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: {ctype}\r\n{extra}Connection: close\r\n\r\n",
                body.len()
            );
            c.write_all(head.as_bytes()).unwrap();
            c.write_all(body).unwrap();
        };
        if path.starts_with("/token?") && !logged_in {
            reply(&mut conn, "401 Unauthorized", "", "text/plain", b"");
        } else if path.starts_with("/token?") {
            assert!(
                path.contains("scope=repository%3Atest%2Fbox%3Apull"),
                "{path}"
            );
            reply(
                &mut conn,
                "200 OK",
                "",
                "application/json",
                br#"{"token":"secret"}"#,
            );
        } else if let Some(d) = path.strip_prefix("/store/") {
            let routes = routes.lock().unwrap();
            let (_, body) = &routes[&format!("/v2/test/box/blobs/{d}")];
            reply(&mut conn, "200 OK", "", "application/octet-stream", body);
        } else if !authorized {
            let challenge = format!(
                "WWW-Authenticate: Bearer realm=\"http://{realm}:{port}/token\",service=\"test\",scope=\"repository:test/box:pull\"\r\n"
            );
            reply(&mut conn, "401 Unauthorized", &challenge, "text/plain", b"");
        } else if let Some(d) = path.strip_prefix("/v2/test/box/blobs/") {
            let loc = format!("Location: /store/{d}\r\n");
            reply(&mut conn, "307 Temporary Redirect", &loc, "text/plain", b"");
        } else {
            match routes.lock().unwrap().get(&path) {
                Some((ctype, body)) => reply(&mut conn, "200 OK", "", ctype, body),
                None => reply(&mut conn, "404 Not Found", "", "text/plain", b""),
            }
        }
    }
}

#[test]
fn an_image_is_pulled_and_imported() {
    let base = std::env::temp_dir().join(format!("qk-pull-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(base.join("root/etc")).unwrap();
    fs::write(base.join("root/etc/greeting"), "pulled\n").unwrap();
    let layer = Command::new("tar")
        .args(["-czf", "-", "-C"])
        .arg(base.join("root"))
        .arg(".")
        .output()
        .unwrap()
        .stdout;
    let config = br#"{"architecture":"amd64","os":"linux","config":{"Cmd":["/bin/sh"]},"rootfs":{"type":"layers","diff_ids":[]}}"#;
    let manifest = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{}","size":{}}}]}}"#,
        digest(config),
        config.len(),
        digest(&layer),
        layer.len()
    );
    let index = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{{"digest":"sha256:{}","platform":{{"os":"linux","architecture":"arm64"}}}},{{"digest":"{}","platform":{{"os":"linux","architecture":"amd64"}}}}]}}"#,
        "0".repeat(64),
        digest(manifest.as_bytes())
    );
    let routes: Routes = Arc::default();
    {
        let mut r = routes.lock().unwrap();
        let oci = |t: &str| format!("application/vnd.oci.image.{t}.v1+json");
        r.insert(
            "/v2/test/box/manifests/1".into(),
            (oci("index"), index.clone().into_bytes()),
        );
        r.insert(
            format!("/v2/test/box/manifests/{}", digest(manifest.as_bytes())),
            (oci("manifest"), manifest.clone().into_bytes()),
        );
        r.insert(
            format!("/v2/test/box/blobs/{}", digest(config)),
            ("application/octet-stream".into(), config.to_vec()),
        );
        r.insert(
            format!("/v2/test/box/blobs/{}", digest(&layer)),
            ("application/octet-stream".into(), layer.clone()),
        );
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let log = Arc::new(Mutex::new(Vec::new()));
    {
        let (routes, log) = (routes.clone(), log.clone());
        std::thread::spawn(move || serve(listener, routes, log));
    }
    let opts = Options {
        plain_http: vec![addr.clone()],
        ..Default::default()
    };

    let image = format!("{addr}/test/box:1");
    assert!(registry::pull(&image, &base.join("anonymous"), &opts, None).is_err());
    let creds = registry::Credentials {
        username: "user".into(),
        password: "pass".into(),
        ..Default::default()
    };
    registry::pull(&image, &base.join("layout"), &opts, Some(&creds)).unwrap();
    let rt = Runtime::new(&base.join("state")).unwrap();
    let img = rt.import(&base.join("layout"), &image).unwrap();
    assert_eq!(img.id, digest(config));
    let unpacked = base
        .join("state/images")
        .join(&img.id[7..])
        .join("rootfs/etc/greeting");
    assert_eq!(fs::read_to_string(unpacked).unwrap(), "pulled\n");
    assert!(log.lock().unwrap().iter().any(|p| p.starts_with("/store/")));

    // a registry that serves other bytes than the digest names is caught
    let mut bad = layer.clone();
    let n = bad.len() / 2;
    bad[n] ^= 1;
    routes.lock().unwrap().insert(
        format!("/v2/test/box/blobs/{}", digest(&layer)),
        ("application/octet-stream".into(), bad),
    );
    registry::pull(&image, &base.join("layout2"), &opts, Some(&creds)).unwrap();
    let rt = Runtime::new(&base.join("state2")).unwrap();
    let err = rt.import(&base.join("layout2"), &image).unwrap_err();
    assert!(err.to_string().contains("does not match"), "{err}");

    // a blob longer than the manifest says is cut off, not stored
    let mut long = layer.clone();
    long.extend_from_slice(&[0; 4096]);
    routes.lock().unwrap().insert(
        format!("/v2/test/box/blobs/{}", digest(&layer)),
        ("application/octet-stream".into(), long),
    );
    let err = registry::pull(&image, &base.join("long"), &opts, Some(&creds)).unwrap_err();
    assert!(err.to_string().contains("were named"), "{err}");

    // and so is a manifest other than the one the index names
    routes.lock().unwrap().insert(
        format!("/v2/test/box/manifests/{}", digest(manifest.as_bytes())),
        (
            "application/vnd.oci.image.manifest.v1+json".into(),
            manifest.replace("\"size\"", "\"size\" ").into_bytes(),
        ),
    );
    let err = registry::pull(&image, &base.join("layout3"), &opts, Some(&creds)).unwrap_err();
    assert!(err.to_string().contains("does not match"), "{err}");

    // a realm on another host gets no login over plain HTTP
    routes
        .lock()
        .unwrap()
        .insert("realm".into(), ("localhost".into(), Vec::new()));
    let err = registry::pull(&image, &base.join("layout4"), &opts, Some(&creds)).unwrap_err();
    assert!(err.to_string().contains("401"), "{err}");
    fs::remove_dir_all(&base).unwrap();
}

/// Reads a request head off any stream and returns its target.
fn read_target(s: &mut impl std::io::Read) -> String {
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if s.read(&mut b).unwrap_or(0) == 0 {
            break;
        }
        head.push(b[0]);
    }
    let line = String::from_utf8_lossy(&head)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    line.split_whitespace().nth(1).unwrap_or("").to_string()
}

#[test]
fn an_image_is_pulled_over_tls_through_a_proxy() {
    use quantycore::tls::crypto::ec::Curve;
    use quantycore::tls::x509::build::{issue, Issuer, Params, SanEntry};
    use quantycore::tls::x509::{PrivateKey, RootStore};
    use quantycore::tls::{ClientConfig, ServerConfig, ServerConnection, Stream};

    let base = std::env::temp_dir().join(format!("qk-pull-tls-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(base.join("root/etc")).unwrap();
    fs::write(base.join("root/etc/greeting"), "over tls\n").unwrap();
    let layer = Command::new("tar")
        .args(["-czf", "-", "-C"])
        .arg(base.join("root"))
        .arg(".")
        .output()
        .unwrap()
        .stdout;
    let config = br#"{"architecture":"amd64","os":"linux","config":{},"rootfs":{"type":"layers","diff_ids":[]}}"#;
    let manifest = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"{}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"{}","size":{}}}]}}"#,
        digest(config),
        config.len(),
        digest(&layer),
        layer.len()
    );
    let mut files: HashMap<String, Vec<u8>> = HashMap::new();
    files.insert("/v2/test/box/manifests/1".into(), manifest.into_bytes());
    files.insert(
        format!("/v2/test/box/blobs/{}", digest(config)),
        config.to_vec(),
    );
    files.insert(format!("/v2/test/box/blobs/{}", digest(&layer)), layer);

    // a CA of the test's own, and a certificate from it for localhost
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let ca_key = PrivateKey::generate_ec(Curve::P256).unwrap();
    let ca = issue(
        &Params::ca("test CA", now - 3600, now + 3600),
        &ca_key.public_spki(),
        None,
        &ca_key,
    )
    .unwrap();
    let key = PrivateKey::generate_ec(Curve::P256).unwrap();
    let leaf = issue(
        &Params::end_entity(
            "localhost",
            vec![SanEntry::Dns("localhost".into())],
            now - 3600,
            now + 3600,
        ),
        &key.public_spki(),
        Some(&Issuer::from_certificate(&ca, &ca_key).unwrap()),
        &key,
    )
    .unwrap();
    let server = Arc::new(ServerConfig::new(vec![leaf], key).unwrap());

    let registry = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = registry.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for conn in registry.incoming() {
            let Ok(tcp) = conn else { return };
            let mut s = Stream::new(ServerConnection::new(server.clone()), tcp);
            if s.handshake().is_err() {
                continue;
            }
            let body = files.get(&read_target(&mut s)).cloned();
            let head = match &body {
                Some(b) => format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.oci.image.manifest.v1+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    b.len()
                ),
                None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            };
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(&body.unwrap_or_default());
            let _ = s.close();
        }
    });

    // a proxy that only tunnels, and counts the tunnels it made
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_port = proxy.local_addr().unwrap().port();
    let tunnels = Arc::new(Mutex::new(0));
    {
        let tunnels = tunnels.clone();
        std::thread::spawn(move || {
            for conn in proxy.incoming() {
                let Ok(mut client) = conn else { return };
                let target = read_target(&mut client);
                assert_eq!(target, format!("localhost:{port}"));
                let upstream = TcpStream::connect(("127.0.0.1", port)).unwrap();
                client
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .unwrap();
                *tunnels.lock().unwrap() += 1;
                let (mut a, b) = (client.try_clone().unwrap(), upstream.try_clone().unwrap());
                std::thread::spawn(move || std::io::copy(&mut a, &mut b.try_clone().unwrap()));
                let (mut c, mut d) = (upstream, client);
                std::thread::spawn(move || std::io::copy(&mut c, &mut d));
            }
        });
    }

    let opts = |roots: RootStore| Options {
        tls: Some(Arc::new(ClientConfig::new(roots))),
        proxy: Some(("127.0.0.1".into(), proxy_port)),
        ..Default::default()
    };
    let mut trusted = RootStore::new();
    trusted.add_der(&ca).unwrap();
    let image = format!("localhost:{port}/test/box:1");

    // a server whose certificate does not chain to a trusted root is
    // refused before a byte of the image is read
    let err = registry::pull(
        &image,
        &base.join("untrusted"),
        &opts(RootStore::new()),
        None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("trusted root"), "{err}");

    registry::pull(&image, &base.join("layout"), &opts(trusted), None).unwrap();
    assert!(*tunnels.lock().unwrap() >= 4);
    let rt = Runtime::new(&base.join("state")).unwrap();
    let img = rt.import(&base.join("layout"), &image).unwrap();
    let unpacked = base
        .join("state/images")
        .join(&img.id[7..])
        .join("rootfs/etc/greeting");
    assert_eq!(fs::read_to_string(unpacked).unwrap(), "over tls\n");
    fs::remove_dir_all(&base).unwrap();
}
