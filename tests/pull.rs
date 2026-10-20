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
                "WWW-Authenticate: Bearer realm=\"http://127.0.0.1:{port}/token\",service=\"test\",scope=\"repository:test/box:pull\"\r\n"
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
    fs::remove_dir_all(&base).unwrap();
}
