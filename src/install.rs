//! Moving a kubeadm control plane node's API server from its etcd onto
//! QuantyKube, and back (ADR-009, ADR-035). Everything happens on the
//! node itself, from files: the API server is a static pod whose manifest
//! the kubelet watches, so moving the manifest out stops it and moving it
//! back starts it. Each step is written to a journal first, so the way
//! back needs neither the API server nor the step that failed.

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Where things are on a kubeadm node, and where QuantyKube puts its own.
pub struct Layout {
    pub kubernetes: PathBuf,
    pub state: PathBuf,
    pub port: u16,
    pub binary: PathBuf,
}

impl Layout {
    fn manifest(&self) -> PathBuf {
        self.kubernetes.join("manifests/kube-apiserver.yaml")
    }
    fn journal(&self) -> PathBuf {
        self.state.join("install")
    }
    fn original(&self) -> PathBuf {
        self.journal().join("kube-apiserver.yaml")
    }
    fn step_file(&self) -> PathBuf {
        self.journal().join("step")
    }
    fn data(&self) -> PathBuf {
        self.state.join("store.db")
    }
    fn pki(&self, name: &str) -> PathBuf {
        self.kubernetes.join("pki").join(name)
    }
    fn history(&self, name: &str) -> PathBuf {
        self.state.join("history").join(name)
    }
}

const UNIT: &str = "/etc/systemd/system/quantykube-store.service";

/// Where the store serves the history API to kube-apiserver (ADR-028).
const HISTORY_PORT: u16 = 2480;
const APISERVICE: &str = "v1alpha1.quanty.quantyroot.io";
/// What `install store` registers with the API server for the history
/// API, as path and name; the undo deletes them before etcd gets the
/// store's keys back, and never copies them back if that failed.
const HISTORY_OBJECTS: [(&str, &str); 3] = [
    ("/apis/apiregistration.k8s.io/v1/apiservices", APISERVICE),
    (
        "/apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices",
        "quantykube",
    ),
    ("/api/v1/namespaces/kube-system/services", "quantykube"),
];
const HISTORY_KEYS: [&str; 3] = [
    "/registry/apiregistration.k8s.io/apiservices/v1alpha1.quanty.quantyroot.io",
    "/registry/endpointslices/kube-system/quantykube",
    "/registry/services/specs/kube-system/quantykube",
];

/// How the store serves the history API: on the node's own address, to
/// the API server's front proxy certificate only.
struct History {
    listen: String,
    client_ca: PathBuf,
}

impl History {
    fn of(manifest: &str) -> Option<History> {
        let host = flag(manifest, "advertise-address")?;
        let client_ca = flag(manifest, "requestheader-client-ca-file")?;
        Some(History {
            listen: format!("{host}:{HISTORY_PORT}"),
            client_ca: PathBuf::from(client_ca),
        })
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// The history API's own CA and serving certificate, made on the first
/// move and kept for the next, so that the CA the API server trusts for
/// it stays the same. Returns the CA as PEM.
fn history_pki(l: &Layout) -> Result<String, String> {
    use quantycore::tls::crypto::ec::Curve;
    use quantycore::tls::x509::build::{issue, Issuer, Params, SanEntry};
    use quantycore::tls::x509::{pem, PrivateKey};
    let (ca_file, cert_file, key_file) = (
        l.history("ca.crt"),
        l.history("server.crt"),
        l.history("server.key"),
    );
    if let (Ok(ca), true, true) = (
        fs::read_to_string(&ca_file),
        cert_file.exists(),
        key_file.exists(),
    ) {
        return Ok(ca);
    }
    let dir = l.state.join("history");
    fs::create_dir_all(&dir).map_err(err(dir.display()))?;
    let gen = |what: &str| {
        PrivateKey::generate_ec(Curve::P256).map_err(|e| format!("a key for the {what}: {e:?}"))
    };
    let (from, until) = (now_secs() - 3600, now_secs() + 10 * 365 * 86400);
    let ca_key = gen("history CA")?;
    let ca = issue(
        &Params::ca("quantykube history CA", from, until),
        &ca_key.public_spki(),
        None,
        &ca_key,
    )
    .map_err(|e| format!("the history CA: {e:?}"))?;
    let key = gen("history API")?;
    let issuer = Issuer::from_certificate(&ca, &ca_key).map_err(|e| format!("{e:?}"))?;
    let cert = issue(
        &Params::end_entity(
            "quantykube.kube-system.svc",
            vec![SanEntry::Dns("quantykube.kube-system.svc".into())],
            from,
            until,
        ),
        &key.public_spki(),
        Some(&issuer),
        &key,
    )
    .map_err(|e| format!("the history API's certificate: {e:?}"))?;
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&key_file)
            .map_err(err(key_file.display()))?;
        io::Write::write_all(&mut f, key.to_pem().as_bytes()).map_err(err(key_file.display()))?;
    }
    fs::write(&cert_file, pem::encode("CERTIFICATE", &cert)).map_err(err(cert_file.display()))?;
    let ca_pem = pem::encode("CERTIFICATE", &ca);
    // last: its presence says the other two are complete
    fs::write(&ca_file, &ca_pem).map_err(err(ca_file.display()))?;
    Ok(ca_pem)
}

fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for c in bytes.chunks(3) {
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Standard base64, padded or not.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    crate::aggapi::base64url_decode(&s.trim().replace('+', "-").replace('/', "_"))
}

/// The value of `name: ` in a kubeconfig, the first one.
fn kubeconfig_field<'a>(config: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}: ");
    config
        .lines()
        .find_map(|l| l.trim().strip_prefix(&prefix))
        .map(str::trim)
}

/// The API server of this node, asked as kubeadm's cluster admin, with
/// the client certificate in admin.conf: since Kubernetes 1.29 the API
/// server's own kubelet client certificate is no longer one.
fn kube(l: &Layout, manifest: &str) -> Result<crate::kube::Kube, String> {
    use std::os::unix::fs::OpenOptionsExt;
    let host = flag(manifest, "advertise-address").ok_or("no --advertise-address")?;
    let port = flag(manifest, "secure-port").unwrap_or_else(|| "6443".into());
    let conf = l.kubernetes.join("admin.conf");
    let text = fs::read_to_string(&conf).map_err(err(conf.display()))?;
    let field = |name: &str| {
        kubeconfig_field(&text, name)
            .and_then(base64_decode)
            .ok_or(format!("no {name} in {}", conf.display()))
    };
    let (cert, key) = (field("client-certificate-data")?, field("client-key-data")?);
    // only for as long as the client is made: it reads them once
    let dir = l.journal();
    fs::create_dir_all(&dir).map_err(err(dir.display()))?;
    let (cert_file, key_file) = (dir.join("admin.crt"), dir.join("admin.key"));
    let write = |path: &Path, data: &[u8]| -> Result<(), String> {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .map_err(err(path.display()))?;
        io::Write::write_all(&mut f, data).map_err(err(path.display()))
    };
    let made = write(&cert_file, &cert)
        .and_then(|()| write(&key_file, &key))
        .and_then(|()| {
            crate::kube::Kube::with_certificate(
                &format!("{host}:{port}"),
                &l.pki("ca.crt"),
                &cert_file,
                &key_file,
            )
        });
    let _ = fs::remove_file(&cert_file);
    let _ = fs::remove_file(&key_file);
    made
}

/// Registers the history API with the API server: a Service without
/// pods, its EndpointSlice with the store's address, and the APIService.
fn register_history(l: &Layout, manifest: &str, h: &History, ca: &str) -> Result<(), String> {
    let k = kube(l, manifest)?;
    let (ip, _) = h.listen.rsplit_once(':').ok_or("no port")?;
    let q = |s: &str| quantycore::json::Value::String(s.to_string()).to_string();
    let objects = [
        format!(
            r#"{{"apiVersion":"v1","kind":"Service","metadata":{{"name":"quantykube","namespace":"kube-system"}},"spec":{{"ports":[{{"port":443,"targetPort":{HISTORY_PORT}}}]}}}}"#
        ),
        format!(
            r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{{"name":"quantykube","namespace":"kube-system","labels":{{"kubernetes.io/service-name":"quantykube"}}}},"addressType":"IPv4","ports":[{{"port":{HISTORY_PORT}}}],"endpoints":[{{"addresses":[{ip}]}}]}}"#,
            ip = q(ip)
        ),
        format!(
            r#"{{"apiVersion":"apiregistration.k8s.io/v1","kind":"APIService","metadata":{{"name":"{APISERVICE}"}},"spec":{{"group":"quanty.quantyroot.io","version":"v1alpha1","service":{{"namespace":"kube-system","name":"quantykube","port":443}},"caBundle":{ca},"groupPriorityMinimum":1000,"versionPriority":15}}}}"#,
            ca = q(&base64(ca.as_bytes()))
        ),
    ];
    let paths = [
        "/api/v1/namespaces/kube-system/services/quantykube".to_string(),
        "/apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices/quantykube".to_string(),
        format!("/apis/apiregistration.k8s.io/v1/apiservices/{APISERVICE}"),
    ];
    for (path, body) in paths.iter().zip(&objects) {
        let v = quantycore::json::parse(body).map_err(|e| format!("{path}: {e:?}"))?;
        let r = k.apply(path, &v)?;
        if !r.ok() {
            return Err(format!("{path}: {}", r.message()));
        }
    }
    // the API server takes its time to find the service
    wait("the history API answering", 60, || {
        let r = k.get("/apis/quanty.quantyroot.io/v1alpha1")?;
        if r.ok() {
            Ok(())
        } else {
            Err(r.message())
        }
    })
}

/// Deletes what `register_history` made; what was never made is fine.
fn unregister_history(l: &Layout, manifest: &str) -> Result<(), String> {
    let k = kube(l, manifest)?;
    for (dir, name) in HISTORY_OBJECTS {
        let r = k.delete(&format!("{dir}/{name}"))?;
        if !r.ok() && r.status != 404 {
            return Err(format!("{dir}/{name}: {}", r.message()));
        }
    }
    Ok(())
}

fn err(context: impl std::fmt::Display) -> impl FnOnce(io::Error) -> String {
    move |e| format!("{context}: {e}")
}

/// The value of `--name=` in a static pod manifest's command.
fn flag(manifest: &str, name: &str) -> Option<String> {
    let prefix = format!("- --{name}=");
    manifest
        .lines()
        .find_map(|l| l.trim().strip_prefix(&prefix).map(str::to_string))
}

/// The manifest with `--name=` set to `value`, the line kept in place.
fn with_flag(manifest: &str, name: &str, value: &str) -> Option<String> {
    let prefix = format!("- --{name}=");
    let mut found = false;
    let mut out = String::with_capacity(manifest.len());
    for line in manifest.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&prefix) {
            found = true;
            let indent = &line[..line.len() - trimmed.len()];
            out.push_str(&format!("{indent}{prefix}{value}\n"));
        } else {
            out.push_str(line);
        }
    }
    found.then_some(out)
}

fn step(l: &Layout, name: &str) -> Result<(), String> {
    let tmp = l.journal().join("step.new");
    fs::write(&tmp, format!("{name}\n")).map_err(err(tmp.display()))?;
    fs::rename(&tmp, l.step_file()).map_err(err(l.step_file().display()))?;
    eprintln!("quantykube: {name}");
    Ok(())
}

fn current_step(l: &Layout) -> Option<String> {
    fs::read_to_string(l.step_file())
        .ok()
        .map(|s| s.trim().to_string())
}

/// True while any process on the node is a kube-apiserver.
fn api_running() -> bool {
    fs::read_dir("/proc").is_ok_and(|rd| {
        rd.filter_map(Result::ok).any(|e| {
            fs::read_to_string(e.path().join("comm")).is_ok_and(|c| c.trim() == "kube-apiserver")
        })
    })
}

/// The API server answers /readyz with 200, asked with the certificate
/// kubeadm gives the API server for its kubelets, which is a cluster
/// admin's.
fn api_ready(l: &Layout, manifest: &str) -> Result<(), String> {
    let host = flag(manifest, "advertise-address").ok_or("no --advertise-address")?;
    let port = flag(manifest, "secure-port").unwrap_or_else(|| "6443".into());
    let config = crate::etcdclient::client_config(
        &l.pki("ca.crt"),
        Some((
            &l.pki("apiserver-kubelet-client.crt"),
            &l.pki("apiserver-kubelet-client.key"),
        )),
        &[b"http/1.1"],
    )?;
    let addr = format!("{host}:{port}");
    let stream =
        crate::etcdclient::connect(&addr, &Arc::new(config)).map_err(|e| format!("{addr}: {e}"))?;
    let req = quantycore::http1::Request {
        method: "GET",
        target: "/readyz",
        host: &addr,
        headers: &[("Connection", "close")],
    };
    let mut resp = quantycore::http1::send(stream, &req).map_err(|e| format!("{addr}: {e}"))?;
    let mut body = String::new();
    let _ = resp.read_to_string(&mut body);
    if resp.status == 200 {
        Ok(())
    } else {
        Err(format!("{addr}/readyz: {} {}", resp.status, body.trim()))
    }
}

/// The store answers /health.
fn store_ready(l: &Layout) -> Result<(), String> {
    let config = crate::etcdclient::client_config(
        &l.pki("etcd/ca.crt"),
        Some((
            &l.pki("apiserver-etcd-client.crt"),
            &l.pki("apiserver-etcd-client.key"),
        )),
        &[b"http/1.1"],
    )?;
    let addr = format!("127.0.0.1:{}", l.port);
    let stream =
        crate::etcdclient::connect(&addr, &Arc::new(config)).map_err(|e| format!("{addr}: {e}"))?;
    let req = quantycore::http1::Request {
        method: "GET",
        target: "/health",
        host: &addr,
        headers: &[("Connection", "close")],
    };
    let resp = quantycore::http1::send(stream, &req).map_err(|e| format!("{addr}: {e}"))?;
    if resp.status == 200 {
        Ok(())
    } else {
        Err(format!("{addr}/health: {}", resp.status))
    }
}

/// Polls `check` until it succeeds; the error names the last reason.
fn wait(
    what: &str,
    secs: u64,
    mut check: impl FnMut() -> Result<(), String>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        match check() {
            Ok(()) => return Ok(()),
            Err(e) if Instant::now() > deadline => {
                return Err(format!("{what} did not happen within {secs}s: {e}"))
            }
            Err(_) => std::thread::sleep(Duration::from_millis(500)),
        }
    }
}

fn stopped() -> Result<(), String> {
    if api_running() {
        Err("a kube-apiserver process is still running".into())
    } else {
        Ok(())
    }
}

fn systemctl(args: &[&str]) -> Result<(), String> {
    let s = Command::new("systemctl")
        .args(args)
        .status()
        .map_err(err("systemctl"))?;
    if s.success() {
        Ok(())
    } else {
        Err(format!("systemctl {}: {s}", args.join(" ")))
    }
}

fn store_unit(l: &Layout, history: Option<&History>) -> String {
    let api = history.map_or(String::new(), |h| {
        format!(
            " --api-listen {} --api-cert {} --api-key {} --api-client-ca {}",
            h.listen,
            l.history("server.crt").display(),
            l.history("server.key").display(),
            h.client_ca.display()
        )
    });
    format!(
        "[Unit]\n\
         Description=QuantyKube store\n\
         After=network-online.target\n\
         [Service]\n\
         ExecStart={bin} serve --data {data} --socket /run/quantykube/store.sock --wal \
         --listen 127.0.0.1:{port} --cert {cert} --key {key} --client-ca {ca}{api}\n\
         RuntimeDirectory=quantykube\n\
         Restart=always\n\
         RestartSec=1\n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        bin = l.binary.display(),
        data = l.data().display(),
        port = l.port,
        cert = l.pki("etcd/server.crt").display(),
        key = l.pki("etcd/server.key").display(),
        ca = l.pki("etcd/ca.crt").display(),
    )
}

/// Moves the API server of this node onto QuantyKube. On any failure
/// after the API server was stopped it is put back on etcd.
pub fn install(l: &Layout) -> Result<(), String> {
    if let Some(s) = current_step(l) {
        return Err(format!(
            "an install is recorded in {} at step {s}; undo it first",
            l.journal().display()
        ));
    }
    let manifest = fs::read_to_string(l.manifest()).map_err(err(l.manifest().display()))?;
    if flag(&manifest, "etcd-servers-overrides").is_some() {
        return Err("the API server keeps some resources in another etcd".into());
    }
    let servers = flag(&manifest, "etcd-servers").ok_or("no --etcd-servers in the manifest")?;
    if servers.contains(',') {
        return Err("the API server uses several etcd members; one is supported".into());
    }
    let endpoint = servers
        .strip_prefix("https://")
        .ok_or("--etcd-servers is not https")?
        .to_string();
    let path = |f: &str| -> Result<PathBuf, String> {
        flag(&manifest, f)
            .map(PathBuf::from)
            .ok_or(format!("no --{f} in the manifest"))
    };
    let (ca, cert, key) = (
        path("etcd-cafile")?,
        path("etcd-certfile")?,
        path("etcd-keyfile")?,
    );
    let target = format!("https://127.0.0.1:{}", l.port);
    let switched = with_flag(&manifest, "etcd-servers", &target).expect("the flag was found");
    if l.data().exists() {
        return Err(format!(
            "{} exists from an earlier move",
            l.data().display()
        ));
    }

    let history = History::of(&manifest);
    let history_ca = match &history {
        Some(_) => Some(history_pki(l)?),
        None => {
            eprintln!(
                "quantykube: no --requestheader-client-ca-file in the manifest; \
                 the store serves no history API"
            );
            None
        }
    };

    fs::create_dir_all(l.journal()).map_err(err(l.journal().display()))?;
    fs::write(l.original(), &manifest).map_err(err(l.original().display()))?;
    step(l, "stopping the API server")?;
    let parked = l.journal().join("kube-apiserver.parked");
    fs::rename(l.manifest(), &parked).map_err(err(l.manifest().display()))?;
    let moved = (|| {
        wait("the API server stopping", 120, stopped)?;
        step(l, "copying the keys from etcd")?;
        let client = crate::etcdclient::Client::new(&endpoint, &ca, &cert, &key)?;
        let copy = client.copy().map_err(|s| s.message)?;
        let blob = crate::etcd::import_of(&copy).map_err(|e| e.to_string())?;
        fs::create_dir_all(&l.state).map_err(err(l.state.display()))?;
        {
            let store = crate::store::Quanty::open(&l.data()).map_err(|e| e.to_string())?;
            crate::store::Backend::import(&store, &blob).map_err(|e| e.to_string())?;
        }
        fs::write(l.journal().join("revision"), format!("{}\n", copy.revision))
            .map_err(err("journal"))?;
        step(l, "starting the store")?;
        fs::write(UNIT, store_unit(l, history.as_ref())).map_err(err(UNIT))?;
        systemctl(&["daemon-reload"])?;
        systemctl(&["enable", "--now", "quantykube-store.service"])?;
        wait("the store starting", 60, || store_ready(l))?;
        step(l, "starting the API server on the store")?;
        fs::write(&parked, &switched).map_err(err(parked.display()))?;
        fs::rename(&parked, l.manifest()).map_err(err(l.manifest().display()))?;
        wait("the API server becoming ready", 180, || {
            api_ready(l, &switched)
        })?;
        Ok::<_, String>(copy)
    })();
    match moved {
        Ok(copy) => {
            if let (Some(h), Some(ca)) = (&history, &history_ca) {
                // the cluster runs without it; the move stays
                match register_history(l, &switched, h, ca) {
                    Ok(()) => {
                        eprintln!("quantykube: the history API is registered as {APISERVICE}")
                    }
                    Err(e) => {
                        eprintln!("quantykube: WARNING: the history API is not registered: {e}")
                    }
                }
            }
            step(l, "done")?;
            println!(
                "the API server is on QuantyKube: {} keys as of etcd revision {}; \
                 etcd is untouched",
                copy.records.len(),
                copy.revision
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("quantykube: {e}; putting the API server back on etcd");
            undo(l).map_err(|u| format!("{e}; and the undo failed too: {u}"))?;
            Err(e)
        }
    }
}

/// The keys written on the store since the move, copied into the etcd
/// the original manifest names; nothing when the API server never ran
/// on the store or nothing changed there.
fn copy_back(l: &Layout, original: &str, at: &str) -> Result<(), String> {
    let ran = matches!(at, "starting the API server on the store" | "done");
    let recorded: Option<i64> = fs::read_to_string(l.journal().join("revision"))
        .ok()
        .and_then(|r| r.trim().parse().ok());
    let (true, Some(recorded)) = (ran, recorded) else {
        return Ok(());
    };
    // read from the file, with the store stopped: nothing writes to it
    // now, and a large answer over gRPC is what C1 of the review is about
    systemctl(&["stop", "quantykube-store.service"])?;
    let mut copy = {
        let store = crate::store::Quanty::open(&l.data()).map_err(|e| e.to_string())?;
        crate::etcd::copy_of(&store).map_err(|e| format!("reading the store: {e}"))?
    };
    // etcd's API server has no store to send history requests to
    copy.records
        .retain(|r| !HISTORY_KEYS.iter().any(|k| r.key == k.as_bytes()));
    if copy.revision == recorded {
        return Ok(());
    }
    let path = |f: &str| -> Result<PathBuf, String> {
        flag(original, f)
            .map(PathBuf::from)
            .ok_or(format!("no --{f} in the original manifest"))
    };
    let servers = flag(original, "etcd-servers").ok_or("no --etcd-servers")?;
    let endpoint = servers
        .strip_prefix("https://")
        .ok_or("--etcd-servers is not https")?;
    let etcd = crate::etcdclient::Client::new(
        endpoint,
        &path("etcd-cafile")?,
        &path("etcd-certfile")?,
        &path("etcd-keyfile")?,
    )?;
    etcd.replace_all(&copy)
        .map_err(|s| format!("writing into etcd: {}", s.message))?;
    eprintln!(
        "quantykube: copied {} keys back into etcd, {} revisions written on the store",
        copy.records.len(),
        copy.revision - recorded
    );
    Ok(())
}

/// Puts the API server back on etcd as the journal recorded it, with
/// what was written on the store since copied into etcd first. The
/// store's file stays, renamed, so nothing is deleted.
pub fn undo(l: &Layout) -> Result<(), String> {
    let Some(at) = current_step(l) else {
        return Err(format!(
            "no install is recorded in {}",
            l.journal().display()
        ));
    };
    let original = fs::read_to_string(l.original()).map_err(err(l.original().display()))?;
    eprintln!("quantykube: undoing the move recorded at step {at}");
    let parked = l.journal().join("kube-apiserver.parked");
    let current = fs::read_to_string(l.manifest()).ok();
    if let Some(c) = current.as_deref().filter(|_| at == "done") {
        if let Err(e) = unregister_history(l, c) {
            eprintln!("quantykube: the history API stays registered on the store: {e}");
        }
    }
    if current.is_some() {
        fs::rename(l.manifest(), &parked).map_err(err(l.manifest().display()))?;
    }
    wait("the API server stopping", 120, stopped)?;
    if let Err(e) = copy_back(l, &original, &at) {
        // nothing is lost: the API server goes back to the store
        let _ = systemctl(&["start", "quantykube-store.service"]);
        if let Some(c) = current {
            fs::write(&parked, c).map_err(err(parked.display()))?;
            fs::rename(&parked, l.manifest()).map_err(err(l.manifest().display()))?;
        }
        return Err(format!("{e}; the API server stays on the store"));
    }
    fs::write(&parked, &original).map_err(err(parked.display()))?;
    fs::rename(&parked, l.manifest()).map_err(err(l.manifest().display()))?;
    wait("the API server becoming ready on etcd", 180, || {
        api_ready(l, &original)
    })?;
    if Path::new(UNIT).exists() {
        let _ = systemctl(&["disable", "--now", "quantykube-store.service"]);
        fs::remove_file(UNIT).map_err(err(UNIT))?;
        let _ = systemctl(&["daemon-reload"]);
    }
    if l.data().exists() {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let kept = l.state.join(format!("store.db.undone-{secs}"));
        fs::rename(l.data(), &kept).map_err(err(l.data().display()))?;
        for side in [".wal", ".raft"] {
            let mut from = l.data().into_os_string();
            from.push(side);
            let mut to = kept.clone().into_os_string();
            to.push(side);
            let _ = fs::rename(from, to);
        }
        println!("the store is kept as {}", kept.display());
    }
    fs::remove_dir_all(l.journal()).map_err(err(l.journal().display()))?;
    println!("the API server is on etcd again");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "spec:\n  containers:\n  - command:\n    - kube-apiserver\n    \
                            - --advertise-address=10.0.0.5\n    \
                            - --etcd-servers=https://127.0.0.1:2379\n    \
                            - --secure-port=6443\n    image: x\n";

    #[test]
    fn a_flag_is_read_and_replaced_in_place() {
        assert_eq!(
            flag(MANIFEST, "etcd-servers").as_deref(),
            Some("https://127.0.0.1:2379")
        );
        assert_eq!(flag(MANIFEST, "etcd-cafile"), None);
        let out = with_flag(MANIFEST, "etcd-servers", "https://127.0.0.1:2479").unwrap();
        assert_eq!(
            out,
            MANIFEST.replace("127.0.0.1:2379", "127.0.0.1:2479"),
            "only the value changes"
        );
        assert_eq!(with_flag(MANIFEST, "etcd-cafile", "x"), None);
    }

    #[test]
    fn the_admin_s_certificate_is_read_from_admin_conf() {
        let conf = "users:\n- name: kubernetes-admin\n  user:\n    \
                    client-certificate-data: Zm9v+/8=\n    client-key-data: YmFy\n";
        let v = kubeconfig_field(conf, "client-certificate-data").unwrap();
        assert_eq!(base64_decode(v).unwrap(), b"foo\xfb\xff");
        assert_eq!(
            base64_decode(kubeconfig_field(conf, "client-key-data").unwrap()).unwrap(),
            b"bar"
        );
    }

    #[test]
    fn base64_is_standard_and_padded() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(&[0xfb, 0xff]), "+/8=");
    }

    #[test]
    fn the_history_api_is_served_on_the_node_s_address_to_the_front_proxy() {
        let m = MANIFEST.replace(
            "    image",
            "    - --requestheader-client-ca-file=/k/pki/front-proxy-ca.crt\n    image",
        );
        assert!(
            History::of(MANIFEST).is_none(),
            "no front proxy, no history API"
        );
        let h = History::of(&m).unwrap();
        assert_eq!(h.listen, "10.0.0.5:2480");
        let dir = std::env::temp_dir().join(format!("qk-history-{}", std::process::id()));
        let l = Layout {
            kubernetes: "/k".into(),
            state: dir.clone(),
            port: 2479,
            binary: "/q".into(),
        };
        let unit = store_unit(&l, Some(&h));
        assert!(unit.contains(&format!(
            " --api-listen 10.0.0.5:2480 --api-cert {d}/history/server.crt \
             --api-key {d}/history/server.key --api-client-ca /k/pki/front-proxy-ca.crt\n",
            d = dir.display()
        )));
        assert!(!store_unit(&l, None).contains("--api-listen"));
        let ca = history_pki(&l).unwrap();
        assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
        assert_eq!(history_pki(&l).unwrap(), ca, "made once, then kept");
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(l.history("server.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "the key is the owner's alone");
        crate::tlsnet::server_config(
            &l.history("server.crt"),
            &l.history("server.key"),
            &l.history("ca.crt"),
        )
        .expect("the store can serve with them");
        let _ = fs::remove_dir_all(dir);
    }
}

/// Where a kubeadm node keeps its kubelet's settings.
pub struct NodeLayout {
    pub kubelet: PathBuf,
    pub state: PathBuf,
    pub binary: PathBuf,
}

const RUNTIME_UNIT: &str = "/etc/systemd/system/quantykube-runtime.service";
const RUNTIME_SOCKET: &str = "/run/quantykube-cri.sock";

impl NodeLayout {
    fn flags(&self) -> PathBuf {
        self.kubelet.join("kubeadm-flags.env")
    }
    fn config(&self) -> PathBuf {
        self.kubelet.join("config.yaml")
    }
    fn journal(&self) -> PathBuf {
        self.state.join("install-runtime")
    }
}

/// Answers nothing, as the CRI's stop and remove calls do.
#[derive(Default)]
struct Empty;

impl crate::cripb::Message for Empty {
    fn write(&self, _out: &mut Vec<u8>) {}
    fn merge(&mut self, _buf: &[u8]) -> Result<(), quantycore::proto::DecodeError> {
        Ok(())
    }
}

fn cri_call<Q: crate::cripb::Message, R: crate::cripb::Message>(
    endpoint: &str,
    method: &str,
    req: &Q,
) -> Result<R, String> {
    let path = endpoint.strip_prefix("unix://").unwrap_or(endpoint);
    let sock = std::os::unix::net::UnixStream::connect(path).map_err(err(path))?;
    sock.set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(err(path))?;
    let body = quantycore::grpc::call(sock, method, &req.encode())
        .map_err(|s| format!("{endpoint} {method}: {}", s.message))?;
    R::decode(&body).map_err(|e| format!("{endpoint} {method}: {e:?}"))
}

/// Stops and removes every pod of the runtime at `endpoint`, so that the
/// kubelet, pointed at the other runtime, starts each once and not twice.
fn clear_pods(endpoint: &str) -> Result<usize, String> {
    use crate::cripb::{ListPodSandboxRequest, ListPodSandboxResponse};
    use crate::cripb::{RemovePodSandboxRequest, StopPodSandboxRequest};
    const RUNTIME: &str = "/runtime.v1.RuntimeService/";
    let list: ListPodSandboxResponse = cri_call(
        endpoint,
        &format!("{RUNTIME}ListPodSandbox"),
        &ListPodSandboxRequest::default(),
    )?;
    for p in &list.items {
        let _: Empty = cri_call(
            endpoint,
            &format!("{RUNTIME}StopPodSandbox"),
            &StopPodSandboxRequest {
                pod_sandbox_id: p.id.clone(),
            },
        )?;
        let _: Empty = cri_call(
            endpoint,
            &format!("{RUNTIME}RemovePodSandbox"),
            &RemovePodSandboxRequest {
                pod_sandbox_id: p.id.clone(),
            },
        )?;
    }
    Ok(list.items.len())
}

fn runtime_ready() -> Result<(), String> {
    let _: crate::cripb::VersionResponse = cri_call(
        RUNTIME_SOCKET,
        "/runtime.v1.RuntimeService/Version",
        &crate::cripb::VersionRequest::default(),
    )?;
    Ok(())
}

/// The kubelet's health endpoint answers ok.
fn kubelet_healthy() -> Result<(), String> {
    let tcp = std::net::TcpStream::connect("127.0.0.1:10248").map_err(err("kubelet"))?;
    tcp.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(err("kubelet"))?;
    let req = quantycore::http1::Request {
        method: "GET",
        target: "/healthz",
        host: "127.0.0.1:10248",
        headers: &[("Connection", "close")],
    };
    let resp = quantycore::http1::send(tcp, &req).map_err(|e| format!("kubelet: {e}"))?;
    if resp.status == 200 {
        Ok(())
    } else {
        Err(format!("kubelet /healthz: {}", resp.status))
    }
}

/// The kubelet's flags with its runtime endpoint replaced.
fn with_endpoint(flags: &str, endpoint: &str) -> Option<String> {
    let key = "--container-runtime-endpoint=";
    let start = flags.find(key)? + key.len();
    let end = flags[start..]
        .find([' ', '"'])
        .map_or(flags.len(), |n| start + n);
    Some(format!("{}{endpoint}{}", &flags[..start], &flags[end..]))
}

fn endpoint_of(flags: &str) -> Option<String> {
    let key = "--container-runtime-endpoint=";
    let start = flags.find(key)? + key.len();
    let rest = &flags[start..];
    Some(rest[..rest.find([' ', '"']).unwrap_or(rest.len())].to_string())
}

/// A top level field of the kubelet's configuration.
fn config_field(config: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}:");
    config
        .lines()
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim().to_string()))
}

/// The kubelet's configuration with a top level field set, in place or
/// added at the end.
fn with_config_field(config: &str, key: &str, value: &str) -> String {
    let prefix = format!("{key}:");
    let mut found = false;
    let mut out: String = config
        .split_inclusive('\n')
        .map(|l| {
            if l.starts_with(&prefix) {
                found = true;
                format!("{key}: {value}\n")
            } else {
                l.to_string()
            }
        })
        .collect();
    if !found {
        if !out.ends_with('\n') && !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("{key}: {value}\n"));
    }
    out
}

fn write_file(path: &Path, text: &str) -> Result<(), String> {
    let tmp = path.with_extension("quantykube-new");
    fs::write(&tmp, text).map_err(err(tmp.display()))?;
    fs::rename(&tmp, path).map_err(err(path.display()))
}

fn runtime_step(l: &NodeLayout, name: &str) -> Result<(), String> {
    fs::write(l.journal().join("step"), format!("{name}\n")).map_err(err("journal"))?;
    eprintln!("quantykube: {name}");
    Ok(())
}

/// Moves this node's kubelet from its runtime onto `quantykube runtime`.
/// The node is drained first, by whoever runs this: what still runs on
/// it, DaemonSets and static pods, is stopped and started again on the
/// new runtime. On any failure the kubelet goes back.
pub fn install_runtime(l: &NodeLayout) -> Result<(), String> {
    if l.journal().exists() {
        return Err(format!(
            "an install is recorded in {}; undo it first",
            l.journal().display()
        ));
    }
    let flags = fs::read_to_string(l.flags()).map_err(err(l.flags().display()))?;
    let config = fs::read_to_string(l.config()).map_err(err(l.config().display()))?;
    // kubeadm writes the endpoint into the configuration since 1.27 and
    // into the flags before
    const FIELD: &str = "containerRuntimeEndpoint";
    let old = config_field(&config, FIELD)
        .or_else(|| endpoint_of(&flags))
        .ok_or("the kubelet names no container runtime endpoint")?;
    let target = format!("unix://{RUNTIME_SOCKET}");
    if old == target {
        return Err("the kubelet already uses quantykube runtime".into());
    }
    let new_flags = with_endpoint(&flags, &target).unwrap_or_else(|| flags.clone());
    let mut new_config = config.clone();
    if config_field(&config, FIELD).is_some() || endpoint_of(&flags).is_none() {
        new_config = with_config_field(&new_config, FIELD, &target);
    }
    // the runtime places containers by cgroupfs paths (RUNTIME.md)
    let new_config = with_config_field(&new_config, "cgroupDriver", "cgroupfs");
    fs::create_dir_all(l.journal()).map_err(err(l.journal().display()))?;
    fs::write(l.journal().join("kubeadm-flags.env"), &flags).map_err(err("journal"))?;
    fs::write(l.journal().join("config.yaml"), &config).map_err(err("journal"))?;
    let moved = (|| {
        runtime_step(l, "stopping the kubelet")?;
        systemctl(&["stop", "kubelet"])?;
        runtime_step(l, "stopping the pods of the old runtime")?;
        let n = clear_pods(&old)?;
        eprintln!("quantykube: {n} pods stopped on {old}");
        runtime_step(l, "starting quantykube runtime")?;
        fs::write(RUNTIME_UNIT, runtime_unit(l)).map_err(err(RUNTIME_UNIT))?;
        systemctl(&["daemon-reload"])?;
        systemctl(&["enable", "--now", "quantykube-runtime.service"])?;
        wait("the runtime starting", 60, runtime_ready)?;
        runtime_step(l, "starting the kubelet on it")?;
        write_file(&l.flags(), &new_flags)?;
        write_file(&l.config(), &new_config)?;
        systemctl(&["start", "kubelet"])?;
        wait("the kubelet becoming healthy", 120, kubelet_healthy)
    })();
    match moved {
        Ok(()) => {
            runtime_step(l, "done")?;
            println!("the kubelet runs on quantykube runtime; {old} is untouched");
            Ok(())
        }
        Err(e) => {
            eprintln!("quantykube: {e}; putting the kubelet back");
            undo_runtime(l).map_err(|u| format!("{e}; and the undo failed too: {u}"))?;
            Err(e)
        }
    }
}

/// Puts the kubelet back on the runtime the journal recorded.
pub fn undo_runtime(l: &NodeLayout) -> Result<(), String> {
    if !l.journal().exists() {
        return Err(format!(
            "no install is recorded in {}",
            l.journal().display()
        ));
    }
    let flags =
        fs::read_to_string(l.journal().join("kubeadm-flags.env")).map_err(err("journal"))?;
    let config = fs::read_to_string(l.journal().join("config.yaml")).map_err(err("journal"))?;
    eprintln!("quantykube: putting the kubelet back on its runtime");
    systemctl(&["stop", "kubelet"])?;
    if runtime_ready().is_ok() {
        let n = clear_pods(&format!("unix://{RUNTIME_SOCKET}"))?;
        eprintln!("quantykube: {n} pods stopped on quantykube runtime");
    }
    write_file(&l.flags(), &flags)?;
    write_file(&l.config(), &config)?;
    if Path::new(RUNTIME_UNIT).exists() {
        let _ = systemctl(&["disable", "--now", "quantykube-runtime.service"]);
        fs::remove_file(RUNTIME_UNIT).map_err(err(RUNTIME_UNIT))?;
        let _ = systemctl(&["daemon-reload"]);
    }
    systemctl(&["start", "kubelet"])?;
    wait("the kubelet becoming healthy", 120, kubelet_healthy)?;
    fs::remove_dir_all(l.journal()).map_err(err(l.journal().display()))?;
    println!("the kubelet runs on its own runtime again");
    Ok(())
}

fn runtime_unit(l: &NodeLayout) -> String {
    format!(
        "[Unit]\n\
         Description=QuantyKube runtime\n\
         Before=kubelet.service\n\
         [Service]\n\
         ExecStart={bin} runtime --root /var/lib/quantykube-runtime --native-network \
         --socket {RUNTIME_SOCKET}\n\
         Restart=always\n\
         RestartSec=1\n\
         Delegate=yes\n\
         KillMode=process\n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        bin = l.binary.display(),
    )
}

#[cfg(test)]
mod node_tests {
    use super::*;

    #[test]
    fn the_kubelet_s_endpoint_and_cgroup_driver_are_replaced_in_place() {
        let flags = "KUBELET_KUBEADM_ARGS=\"--container-runtime-endpoint=unix:///var/run/containerd/containerd.sock --pod-infra-container-image=x\"\n";
        assert_eq!(
            endpoint_of(flags).as_deref(),
            Some("unix:///var/run/containerd/containerd.sock")
        );
        assert_eq!(
            with_endpoint(flags, "unix:///run/q.sock").unwrap(),
            flags.replace(
                "unix:///var/run/containerd/containerd.sock",
                "unix:///run/q.sock"
            )
        );
        assert_eq!(with_endpoint("X=1\n", "y"), None);
        let config = "kind: KubeletConfiguration\ncgroupDriver: systemd\n\
                      containerRuntimeEndpoint: unix:///run/c.sock\nfoo: 1\n";
        assert_eq!(
            config_field(config, "containerRuntimeEndpoint").as_deref(),
            Some("unix:///run/c.sock")
        );
        assert_eq!(
            with_config_field(config, "cgroupDriver", "cgroupfs"),
            config.replace("systemd", "cgroupfs")
        );
        assert_eq!(
            with_config_field("kind: K", "cgroupDriver", "cgroupfs"),
            "kind: K\ncgroupDriver: cgroupfs\n"
        );
    }
}
