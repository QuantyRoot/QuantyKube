use quantycore::proto::{put_bytes, put_int, Reader, Value};
use quantydb_core::FileStorage;
use quantykube::etcdpb::{KeyValue, Message};
use quantykube::store::{self, Quanty};
use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage:
  quantykube serve   --data <file> --socket <path>
                     [--retain <duration>] [--snapshots <duration>]
                     [--listen <addr:port> --cert <pem> --key <pem>
                      --client-ca <pem>]
                     [--node <id> --peers <id=host:port,...> --peer-cert <pem>
                      --peer-key <pem> --peer-ca <pem> [--peer-listen <addr>]]
                     [--api-listen <addr:port> --api-cert <pem> --api-key <pem>
                      --api-client-ca <pem>]
  quantykube history [--socket <path>] [--prefix] <key>
  quantykube get     [--socket <path>] (--rev <n> | --ago <duration>) <key>
  quantykube restore [--socket <path>] (--rev <n> | --ago <duration>) <key>
  quantykube migrate --from <host:port> --etcd-ca <pem> --etcd-cert <pem>
                     --etcd-key <pem> --data <file>
  quantykube install store [--port <n>] [--kubernetes <dir>] [--state <dir>]
  quantykube undo store    [--kubernetes <dir>] [--state <dir>]
  quantykube install runtime [--kubelet <dir>] [--state <dir>]
  quantykube undo runtime    [--kubelet <dir>] [--state <dir>]
  quantykube unpack  --to <dir> <layer.tar.gz>...
  quantykube sandbox --rootfs <dir> [--hostname <name>] [--readonly]
                     [--cgroup <name> [--memory <bytes>] [--pids <n>]] -- <command>...
  quantykube runtime --root <dir> --socket <path>
                     [--cni-conf <dir>] [--cni-bin <dir>] [--plain-http <host>]
                     [--max-image-size <bytes>]
  quantykube pull    --root <dir> [--plain-http <host>] [--max-image-size <bytes>]
                     <image>
  quantykube import  --root <dir> <image dir> <reference>

serve is the store. --retain keeps history, 1h by default; --snapshots
keeps QuantyDB snapshots, 1m by default, and older revisions are rebuilt
from the history (ADR-014). Durations look like 90s, 15m, 2h or 7d.
With --listen it also serves the etcd API over TLS on that address,
only to clients with a certificate from the --client-ca PEM file, as an
API server's --etcd-certfile gives it; --cert and --key are the
server's own certificate chain and key.
With --node it is that node of the Raft cluster --peers lists, its own
entry included; the nodes talk on those addresses over TLS with
certificates from --peer-ca (ADR-033), and the Raft log is kept next to
--data with .raft appended.
With --api-listen it serves history to kube-apiserver as the aggregated
API quanty.quantyroot.io/v1alpha1, to clients with a certificate from
--api-client-ca, the cluster's front proxy CA (docs/HISTORY.md).

history, get and restore read that history through the server's socket,
$QUANTYKUBE_SOCKET or /run/quantykube.sock unless --socket says otherwise.
get writes a value as it was to stdout; restore writes it back, or deletes
the key if it did not exist then, as a new revision every watch sees.
Linked or copied as kubectl-quanty on the PATH, this is also the kubectl
plugin `kubectl quanty`, and keys may then be given as kind/namespace/name,
such as configmap/default/important.

migrate copies every key of the etcd at --from into a new store file
--data, as of one revision and with etcd's revisions, so that every
resourceVersion a client holds stays valid and new writes go on after
them (ADR-035). Nothing may write to the etcd
meanwhile; the API server is stopped first.

install store, run as root on a kubeadm control plane node, moves its
API server from etcd onto a QuantyKube store on 127.0.0.1, port 2479 by
default, run by systemd: it stops the API server, migrates, starts the
store and the API server on it, and goes back on any failure. undo
store puts the API server back on etcd, which install never changes,
with what was written on the store copied into it first.
--kubernetes is /etc/kubernetes and --state /var/lib/quantykube unless
given; the journal of each step is in <state>/install.
install runtime, as root on a kubeadm node that has been drained, moves
its kubelet from its runtime onto quantykube runtime, run by systemd:
it stops the kubelet and the old runtime's pods, starts the runtime,
points the kubelet at it with the cgroupfs driver, and starts it again.
undo runtime puts the kubelet back; --kubelet is /var/lib/kubelet.

unpack and sandbox are the first pieces of the node runtime
(docs/RUNTIME.md), here to be measured, not used: unpack applies image
layers in order, whiteouts included; sandbox runs one command in its own
namespaces with <dir> as its root and exits with its status.

runtime serves the kubelet's Container Runtime Interface on the socket,
keeping images and container state under --root. Pods get their network
from the first CNI configuration in --cni-conf, /etc/cni/net.d by
default, with plugins from --cni-bin, /opt/cni/bin. Images are pulled
over HTTPS against the system's root certificates, through HTTPS_PROXY
unless NO_PROXY names the host, or over plain HTTP from registries named
with --plain-http (host or host:port, comma separated); pull does the
same from the command line. The TLS is QuantyCore's and unreviewed. An
image whose layers unpack to more than --max-image-size (16G by default;
K, M and G suffixes) is refused.
import puts an image there from an OCI image layout, as `skopeo copy
docker://<image> oci:<dir>` writes one, or from a directory holding a
manifest.json and blobs/. Every blob is checked against its digest.";

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let plugin = std::env::args()
        .next()
        .is_some_and(|a| a.rsplit('/').next() == Some("kubectl-quanty"));
    if plugin {
        args = args.into_iter().map(|a| registry_key(&a)).collect();
    }
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
        Some("get") => get(&args[1..]),
        Some("history") => history(&args[1..]),
        Some("restore") => restore(&args[1..]),
        Some("migrate") => migrate(&args[1..]),
        Some(verb @ ("install" | "undo")) => {
            let undo = verb == "undo";
            match args.get(1).map(String::as_str) {
                Some("store") => install(&args[2..], undo),
                Some("runtime") => install_runtime(&args[2..], undo),
                _ => Err(USAGE.to_string()),
            }
        }
        Some("sandbox") => sandbox(&args[1..]),
        Some("unpack") => unpack(&args[1..]),
        Some("runtime") => runtime(&args[1..]),
        Some("import") => import(&args[1..]),
        Some("pull") => pull(&args[1..]),
        Some("--version") => {
            println!("quantykube {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        _ => Err(USAGE.to_string()),
    };
    if let Err(e) = result {
        eprintln!("quantykube: {e}");
        std::process::exit(2);
    }
}

fn value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn required<'a>(args: &'a [String], name: &str) -> Result<&'a str, String> {
    value(args, name).ok_or_else(|| format!("missing {name}\n{USAGE}"))
}

/// The last argument that is neither a flag nor a flag's value.
fn positional(args: &[String]) -> Result<&str, String> {
    let mut skip = false;
    let mut found = None;
    for a in args {
        if skip {
            skip = false;
        } else if a.starts_with("--") {
            skip = a != "--prefix";
        } else {
            found = Some(a.as_str());
        }
    }
    found.ok_or_else(|| format!("missing key\n{USAGE}"))
}

/// `configmap/default/x` to `/registry/configmaps/default/x`, the way the
/// API server lays out keys for built in kinds. Anything starting with `/`
/// or `-` is left alone.
fn registry_key(a: &str) -> String {
    if a.starts_with('/') || a.starts_with('-') || !a.contains('/') {
        return a.to_string();
    }
    let (kind, rest) = a.split_once('/').unwrap();
    let plural = match kind {
        "cm" | "configmap" | "configmaps" => "configmaps",
        "secret" | "secrets" => "secrets",
        "deploy" | "deployment" | "deployments" => "deployments",
        "svc" | "service" | "services" => "services/specs",
        "ns" | "namespace" | "namespaces" => "namespaces",
        "pod" | "pods" => "pods",
        "sa" | "serviceaccount" | "serviceaccounts" => "serviceaccounts",
        "ds" | "daemonset" | "daemonsets" => "daemonsets",
        "sts" | "statefulset" | "statefulsets" => "statefulsets",
        "ing" | "ingress" | "ingresses" => "ingress",
        "role" | "roles" => "roles",
        "rolebinding" | "rolebindings" => "rolebindings",
        "crd" | "customresourcedefinition" => "apiextensions.k8s.io/customresourcedefinitions",
        _ => return a.to_string(),
    };
    format!("/registry/{plural}/{rest}")
}

fn unpack(args: &[String]) -> Result<(), String> {
    let to = PathBuf::from(required(args, "--to")?);
    std::fs::create_dir_all(&to).map_err(|e| format!("{}: {e}", to.display()))?;
    let mut skip = false;
    let mut total = quantykube::image::Stats::default();
    let started = std::time::Instant::now();
    for a in args {
        if std::mem::take(&mut skip) {
            continue;
        }
        if a.starts_with("--") {
            skip = true;
            continue;
        }
        let f = std::fs::File::open(a).map_err(|e| format!("{a}: {e}"))?;
        let s = quantykube::image::unpack_layer(std::io::BufReader::new(f), &to)
            .map_err(|e| format!("{a}: {e}"))?;
        total.entries += s.entries;
        total.bytes += s.bytes;
        total.whiteouts += s.whiteouts;
        total.skipped += s.skipped;
    }
    eprintln!(
        "quantykube: {} entries, {} bytes, {} whiteouts, {} device nodes skipped, {:.0} ms",
        total.entries,
        total.bytes,
        total.whiteouts,
        total.skipped,
        started.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

fn sandbox(args: &[String]) -> Result<(), String> {
    let split = args
        .iter()
        .position(|a| a == "--")
        .ok_or_else(|| format!("missing -- before the command\n{USAGE}"))?;
    let (opts, cmd) = (&args[..split], &args[split + 1..]);
    let root = required(opts, "--rootfs")?;
    let root = std::fs::canonicalize(root).map_err(|e| format!("{root}: {e}"))?;
    // execve does not search PATH, and the search must look in the image,
    // not on this machine
    let program = match cmd.first() {
        Some(c) if !c.contains('/') => [
            "/usr/local/sbin",
            "/usr/local/bin",
            "/usr/sbin",
            "/usr/bin",
            "/sbin",
            "/bin",
        ]
        .iter()
        .map(|d| format!("{d}/{c}"))
        .find(|p| std::fs::symlink_metadata(root.join(&p[1..])).is_ok())
        .unwrap_or_else(|| c.clone()),
        Some(c) => c.clone(),
        None => return Err(format!("missing command\n{USAGE}")),
    };
    let mut argv: Vec<&[u8]> = cmd.iter().map(|a| a.as_bytes()).collect();
    argv[0] = program.as_bytes();
    // the PATH Docker and containerd use when the image config sets none
    let env: [&[u8]; 1] = [b"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"];
    // images need not ship the directories the runtime mounts over
    for dir in ["proc", "dev", "sys", "tmp"] {
        let _ = std::fs::create_dir(root.join(dir));
    }
    let mut spawn = quantykube_sys::Spawn::new(root.as_os_str().as_encoded_bytes(), &argv, &env)
        .map_err(|e| e.to_string())?
        .readonly(opts.iter().any(|a| a == "--readonly"));
    if let Some(h) = value(opts, "--hostname") {
        spawn = spawn.hostname(h.as_bytes()).map_err(|e| e.to_string())?;
    }
    let child = spawn.start().map_err(|e| format!("clone: {e}"))?;
    let pid = child.pid;
    // the child waits until it is in its cgroup, so nothing it runs is
    // ever outside the limits
    let cgroup = match value(opts, "--cgroup") {
        Some(name) => Some(join_cgroup(name, pid, opts)?),
        None => None,
    };
    child.release().map_err(|e| e.to_string())?;
    let exit = quantykube_sys::wait(pid).map_err(|e| e.to_string())?;
    if let Some(dir) = cgroup {
        let _ = std::fs::remove_dir(dir);
    }
    match exit {
        quantykube_sys::Exit::Code(0) => Ok(()),
        quantykube_sys::Exit::Code(c) if quantykube_sys::Step::from_status(c).is_none() => {
            std::process::exit(c)
        }
        e => Err(quantykube_sys::describe(e)),
    }
}

/// Creates `quantykube/<name>` in the cgroup v2 hierarchy, sets the limits
/// asked for and moves `pid` into it.
fn join_cgroup(name: &str, pid: i32, opts: &[String]) -> Result<PathBuf, String> {
    let base = Path::new("/sys/fs/cgroup");
    if !base.join("cgroup.controllers").exists() {
        return Err("--cgroup needs the cgroup v2 hierarchy at /sys/fs/cgroup".into());
    }
    let parent = base.join("quantykube");
    let dir = parent.join(name);
    let io = |e: std::io::Error, p: &Path| format!("{}: {e}", p.display());
    std::fs::create_dir_all(&dir).map_err(|e| io(e, &dir))?;
    // limits take effect only where the parent delegates the controller
    let _ = std::fs::write(base.join("cgroup.subtree_control"), "+memory +pids +cpu");
    let _ = std::fs::write(parent.join("cgroup.subtree_control"), "+memory +pids +cpu");
    for (flag, file) in [("--memory", "memory.max"), ("--pids", "pids.max")] {
        if let Some(v) = value(opts, flag) {
            let p = dir.join(file);
            std::fs::write(&p, v).map_err(|e| io(e, &p))?;
        }
    }
    let procs = dir.join("cgroup.procs");
    std::fs::write(&procs, pid.to_string()).map_err(|e| io(e, &procs))?;
    Ok(dir)
}

fn max_image_size(rt: &quantykube::runtime::Runtime, args: &[String]) -> Result<(), String> {
    if let Some(v) = value(args, "--max-image-size") {
        rt.set_max_image_size(size(v)?);
    }
    Ok(())
}

fn size(s: &str) -> Result<u64, String> {
    let bad = || format!("bad size {s}");
    let (n, shift) = match s.as_bytes().last() {
        Some(b'K') => (&s[..s.len() - 1], 10),
        Some(b'M') => (&s[..s.len() - 1], 20),
        Some(b'G') => (&s[..s.len() - 1], 30),
        _ => (s, 0),
    };
    let n: u64 = n.parse().map_err(|_| bad())?;
    n.checked_mul(1 << shift).ok_or_else(bad)
}

fn duration(s: &str) -> Result<Duration, String> {
    let (n, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = n.parse().map_err(|_| format!("bad duration {s}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => return Err(format!("bad duration {s}")),
    };
    Ok(Duration::from_secs(secs))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A process killed a moment ago may still hold the file's lock while the
/// kernel tears it down, and a supervisor restarts the next one at once.
fn open(data: &Path) -> Result<Quanty<FileStorage>, store::Error> {
    let mut tries = 0;
    loop {
        match Quanty::open(data) {
            Err(store::Error::Engine(quantydb_core::Error::AlreadyOpen)) if tries < 50 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            r => return r,
        }
    }
}

fn install(args: &[String], undo: bool) -> Result<(), String> {
    let binary = PathBuf::from("/usr/local/bin/quantykube");
    let layout = quantykube::install::Layout {
        kubernetes: PathBuf::from(value(args, "--kubernetes").unwrap_or("/etc/kubernetes")),
        state: PathBuf::from(value(args, "--state").unwrap_or("/var/lib/quantykube")),
        port: value(args, "--port")
            .unwrap_or("2479")
            .parse()
            .map_err(|_| "--port is not a port number")?,
        binary: binary.clone(),
    };
    if undo {
        return quantykube::install::undo(&layout);
    }
    place_binary(&binary)?;
    quantykube::install::install(&layout)
}

/// Copies this program to `binary`: what it installs outlives it.
fn place_binary(binary: &Path) -> Result<(), String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    if std::fs::canonicalize(&me).ok() != std::fs::canonicalize(binary).ok() {
        let tmp = binary.with_extension("new");
        std::fs::copy(&me, &tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, binary).map_err(|e| format!("{}: {e}", binary.display()))?;
    }
    Ok(())
}

fn install_runtime(args: &[String], undo: bool) -> Result<(), String> {
    let layout = quantykube::install::NodeLayout {
        kubelet: PathBuf::from(value(args, "--kubelet").unwrap_or("/var/lib/kubelet")),
        state: PathBuf::from(value(args, "--state").unwrap_or("/var/lib/quantykube")),
        binary: PathBuf::from("/usr/local/bin/quantykube"),
    };
    if undo {
        return quantykube::install::undo_runtime(&layout);
    }
    place_binary(&layout.binary)?;
    quantykube::install::install_runtime(&layout)
}

fn migrate(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(required(args, "--data")?);
    // a store that already holds a cluster is never overwritten
    if data.exists() {
        return Err(format!("{} exists", data.display()));
    }
    let client = quantykube::etcdclient::Client::new(
        required(args, "--from")?,
        Path::new(required(args, "--etcd-ca")?),
        Path::new(required(args, "--etcd-cert")?),
        Path::new(required(args, "--etcd-key")?),
    )?;
    let copy = client.copy().map_err(|s| s.message)?;
    let blob = quantykube::etcd::import_of(&copy).map_err(|e| e.to_string())?;
    let store = open(&data).map_err(|e| format!("{}: {e}", data.display()))?;
    store::Backend::import(&store, &blob).map_err(|e| e.to_string())?;
    println!(
        "copied {} keys and {} leases as of revision {}",
        copy.records.len(),
        copy.leases.len(),
        copy.revision
    );
    Ok(())
}

fn serve(args: &[String]) -> Result<(), String> {
    // a soak run with a k3s control plane grew 7 MB a minute without this
    // and levelled off with it (ADR-015)
    quantykube_sys::limit_malloc_arenas(2);
    let data = PathBuf::from(required(args, "--data")?);
    let socket = PathBuf::from(required(args, "--socket")?);
    let retain = duration(value(args, "--retain").unwrap_or("1h"))?;
    let snapshots = duration(value(args, "--snapshots").unwrap_or("1m"))?;
    let store = Arc::new(open(&data).map_err(|e| format!("{}: {e}", data.display()))?);
    // as etcd: after a failed commit only a restart knows what is on disk
    // (ADR-030), and the API server reconnects to the restarted store
    store.on_stop(Box::new(|msg| {
        eprintln!("quantykube: a commit failed, stopping: {msg}");
        std::process::exit(1);
    }));
    let endpoint = format!("unix://{}", socket.display());
    let etcd =
        quantykube::etcd::Etcd::new(store.clone(), endpoint.clone()).map_err(|e| e.to_string())?;
    if let Some(node) = value(args, "--node") {
        join_cluster(&etcd, node, &data, args)?;
    }
    let keep = retain.as_millis() as i64;
    let snap = snapshots.as_millis() as i64;
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = store.enforce_retention(keep, snap, now_ms()) {
            eprintln!("quantykube: retention: {e}");
        }
    });
    // whoever can connect can read and write every object in the cluster
    let listener = private_socket(&socket)?;
    if let Some(addr) = value(args, "--api-listen") {
        // only kube-apiserver, with its front proxy certificate, gets in;
        // it has checked who asks and what RBAC allows them (ADR-028)
        let mut config = quantykube::tlsnet::server_config(
            Path::new(required(args, "--api-cert")?),
            Path::new(required(args, "--api-key")?),
            Path::new(required(args, "--api-client-ca")?),
        )?;
        // the history API speaks HTTP/1.1, which kube-apiserver also
        // offers; without this it would be asked for HTTP/2
        config.set_alpn(&[b"http/1.1"]);
        let tcp = std::net::TcpListener::bind(addr).map_err(|e| format!("{addr}: {e}"))?;
        let service: Arc<dyn quantycore::grpc::Service> = etcd.clone();
        std::thread::spawn(move || quantykube::aggapi::serve(tcp, Arc::new(config), service));
        eprintln!("quantykube: the history API on https://{addr}");
    }
    let server = quantykube::tlsnet::grpc_server(etcd.clone());
    let Some(addr) = value(args, "--listen") else {
        eprintln!("quantykube: serving {} on {endpoint}", data.display());
        return server.serve_unix(listener).map_err(|e| e.to_string());
    };
    // over a network only clients with a certificate from --client-ca
    // get in (ADR-031)
    let mut config = quantykube::tlsnet::server_config(
        Path::new(required(args, "--cert")?),
        Path::new(required(args, "--key")?),
        Path::new(required(args, "--client-ca")?),
    )?;
    // HTTP/1.1 first: kubeadm offers both and speaks only HTTP/1.1,
    // gRPC clients offer only h2
    config.set_alpn(&[b"http/1.1", b"h2"]);
    let tcp = std::net::TcpListener::bind(addr).map_err(|e| format!("{addr}: {e}"))?;
    let unix = server.clone();
    std::thread::spawn(move || unix.serve_unix(listener));
    eprintln!(
        "quantykube: serving {} on {endpoint} and https://{}",
        data.display(),
        tcp.local_addr().map_err(|e| e.to_string())?
    );
    quantykube::tlsnet::serve(tcp, Arc::new(config), server, etcd).map_err(|e| e.to_string())
}

/// Makes this store one node of the cluster named by --peers (ADR-034),
/// its Raft log in a file next to the store's.
fn join_cluster<B: quantykube::store::Backend>(
    etcd: &Arc<quantykube::etcd::Etcd<B>>,
    node: &str,
    data: &Path,
    args: &[String],
) -> Result<(), String> {
    use quantycore::tls::x509::{pem, PrivateKey, RootStore};
    use quantykube::raft::{filelog::FileLog, host::Host, transport::Transport};
    let id: u64 = node.parse().map_err(|_| format!("bad node id {node}"))?;
    let mut peers = std::collections::BTreeMap::new();
    for p in required(args, "--peers")?.split(',') {
        let (pid, addr) = p
            .split_once('=')
            .ok_or_else(|| format!("bad peer {p}, want id=host:port"))?;
        let pid: u64 = pid.parse().map_err(|_| format!("bad peer id in {p}"))?;
        peers.insert(pid, addr.to_string());
    }
    let ids: Vec<u64> = peers.keys().copied().collect();
    let listen_addr = peers
        .remove(&id)
        .ok_or_else(|| format!("--peers does not name node {id}"))?;
    let listen_addr = value(args, "--peer-listen").map_or(listen_addr, str::to_string);
    let (cert, key, ca) = (
        Path::new(required(args, "--peer-cert")?),
        Path::new(required(args, "--peer-key")?),
        Path::new(required(args, "--peer-ca")?),
    );
    let server = quantykube::tlsnet::server_config(cert, key, ca)?;
    let read = |p: &Path| std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()));
    let mut roots = RootStore::new();
    roots
        .add_pem(&read(ca)?)
        .map_err(|e| format!("{}: {e:?}", ca.display()))?;
    let mut client = quantycore::tls::ClientConfig::new(roots);
    let chain = pem::decode_all(&read(cert)?, "CERTIFICATE").map_err(|e| e.to_string())?;
    let key = PrivateKey::from_pem(&read(key)?).map_err(|e| e.to_string())?;
    client.set_client_certificate(chain, key);
    let listener =
        std::net::TcpListener::bind(&listen_addr).map_err(|e| format!("{listen_addr}: {e}"))?;
    let (wire, inbound) = Transport::start(&peers, listener, Arc::new(client), Arc::new(server));
    let mut log_path = data.as_os_str().to_owned();
    log_path.push(".raft");
    let log = FileLog::open(Path::new(&log_path)).map_err(|e| e.to_string())?;
    etcd.join(id, |machine| {
        Host::start(
            id,
            &ids,
            Box::new(log),
            Box::new(wire),
            machine,
            Some(inbound),
        )
    })
    .map_err(|e| e.to_string())?;
    eprintln!("quantykube: node {id} of {ids:?}, peers on {listen_addr}");
    Ok(())
}

/// A socket only its owner can connect to: for both servers the mode is
/// the only access control (ADR-004).
fn private_socket(path: &Path) -> Result<UnixListener, String> {
    use std::os::unix::fs::PermissionsExt;
    let err = |e: std::io::Error| format!("{}: {e}", path.display());
    if path.exists() {
        std::fs::remove_file(path).map_err(err)?;
    }
    let listener = UnixListener::bind(path).map_err(err)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(err)?;
    Ok(listener)
}

fn runtime(args: &[String]) -> Result<(), String> {
    let root = PathBuf::from(required(args, "--root")?);
    let socket = PathBuf::from(required(args, "--socket")?);
    let conf = PathBuf::from(value(args, "--cni-conf").unwrap_or("/etc/cni/net.d"));
    let bin = PathBuf::from(value(args, "--cni-bin").unwrap_or("/opt/cni/bin"));
    let rt = quantykube::runtime::Runtime::with_network(&root, Some((conf.clone(), bin)))
        .map_err(|e| e.to_string())?;
    max_image_size(&rt, args)?;
    match rt.network() {
        Some(c) => eprintln!("quantykube: pod network {}", c.name()),
        None => eprintln!("quantykube: no CNI configuration in {} yet", conf.display()),
    }
    let registries = registry_options(args)?;
    // whoever can connect can run anything as root on the node
    let listener = private_socket(&socket)?;
    eprintln!("quantykube: runtime on unix://{}", socket.display());
    quantycore::grpc::serve_unix(listener, quantykube::cri::Cri::new(rt, registries))
        .map_err(|e| e.to_string())
}

fn registry_options(args: &[String]) -> Result<quantykube::registry::Options, String> {
    let plain = value(args, "--plain-http")
        .map(|v| v.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    quantykube::registry::Options::from_system(plain).map_err(|e| format!("registries: {e}"))
}

fn pull(args: &[String]) -> Result<(), String> {
    let root = PathBuf::from(required(args, "--root")?);
    let image = args.last().filter(|a| !a.starts_with("--")).ok_or(USAGE)?;
    let rt = quantykube::runtime::Runtime::new(&root).map_err(|e| e.to_string())?;
    max_image_size(&rt, args)?;
    let img = rt
        .pull(image, &registry_options(args)?, None)
        .map_err(|e| format!("{image}: {e}"))?;
    println!("{}", img.id);
    Ok(())
}

fn import(args: &[String]) -> Result<(), String> {
    let root = PathBuf::from(required(args, "--root")?);
    let rest: Vec<&String> = args
        .iter()
        .enumerate()
        .filter(|(i, a)| *a != "--root" && (*i == 0 || args[i - 1] != "--root"))
        .map(|(_, a)| a)
        .collect();
    let [from, reference] = rest[..] else {
        return Err(USAGE.to_string());
    };
    let rt = quantykube::runtime::Runtime::new(&root).map_err(|e| e.to_string())?;
    let img = rt
        .import(Path::new(from), reference)
        .map_err(|e| format!("{from}: {e}"))?;
    println!("{}", img.id);
    Ok(())
}

fn history_call(args: &[String], method: &str, req: &[u8]) -> Result<Vec<u8>, String> {
    let socket = value(args, "--socket")
        .map(str::to_string)
        .or_else(|| std::env::var("QUANTYKUBE_SOCKET").ok())
        .unwrap_or_else(|| "/run/quantykube.sock".to_string());
    let socket = socket.as_str();
    let conn = UnixStream::connect(socket).map_err(|e| format!("{socket}: {e}"))?;
    quantycore::grpc::call(conn, &format!("/quantykube.History/{method}"), req)
        .map_err(|s| s.message)
}

fn request(args: &[String], with_end: bool) -> Result<(Vec<u8>, Vec<u8>), String> {
    let key = positional(args)?.as_bytes().to_vec();
    let mut req = Vec::new();
    put_bytes(&mut req, 1, &key);
    if with_end && args.iter().any(|a| a == "--prefix") {
        let mut end = key.clone();
        end.push(0xff);
        put_bytes(&mut req, 2, &end);
    }
    match (value(args, "--rev"), value(args, "--ago")) {
        (Some(r), _) => put_int(
            &mut req,
            3,
            r.parse().map_err(|_| format!("bad revision {r}"))?,
        ),
        (None, Some(a)) => put_int(&mut req, 4, now_ms() - duration(a)?.as_millis() as i64),
        (None, None) if !with_end => return Err(format!("needs --rev or --ago\n{USAGE}")),
        (None, None) => {}
    }
    Ok((key, req))
}

fn fields(buf: &[u8]) -> Result<Vec<(u32, Value<'_>)>, String> {
    let mut r = Reader::new(buf);
    let mut out = Vec::new();
    while let Some(f) = r.next_field().map_err(|e| e.to_string())? {
        out.push(f);
    }
    Ok(out)
}

fn int_field(fields: &[(u32, Value)], n: u32) -> i64 {
    fields
        .iter()
        .find(|(f, _)| *f == n)
        .and_then(|(_, v)| v.int(n).ok())
        .unwrap_or(0)
}

fn kv_field(fields: &[(u32, Value)], n: u32) -> Option<KeyValue> {
    fields
        .iter()
        .find(|(f, _)| *f == n)
        .and_then(|(_, v)| v.bytes(n).ok())
        .and_then(|b| KeyValue::decode(b).ok())
}

fn get(args: &[String]) -> Result<(), String> {
    let (key, req) = request(args, false)?;
    let resp = history_call(args, "Get", &req)?;
    let f = fields(&resp)?;
    let rev = int_field(&f, 1);
    let kv = kv_field(&f, 2).ok_or_else(|| {
        format!(
            "{} did not exist at revision {rev}",
            String::from_utf8_lossy(&key)
        )
    })?;
    eprintln!("quantykube: as of revision {rev}");
    std::io::stdout()
        .write_all(&kv.value)
        .map_err(|e| e.to_string())
}

fn restore(args: &[String]) -> Result<(), String> {
    let (key, req) = request(args, false)?;
    let resp = history_call(args, "Restore", &req)?;
    let f = fields(&resp)?;
    let key = String::from_utf8_lossy(&key);
    let (rev, now) = (int_field(&f, 1), int_field(&f, 3));
    match kv_field(&f, 2) {
        Some(_) => println!("{key}: restored as of revision {rev}, now revision {now}"),
        None => println!("{key}: did not exist at revision {rev}, deleted, now revision {now}"),
    }
    Ok(())
}

fn history(args: &[String]) -> Result<(), String> {
    let (_, req) = request(args, true)?;
    let resp = history_call(args, "Log", &req)?;
    let mut out = std::io::stdout().lock();
    for (_, entry) in fields(&resp)? {
        let e = fields(entry.bytes(1).map_err(|e| e.to_string())?)?;
        let kv = kv_field(&e, 4).unwrap_or_default();
        let what = match (int_field(&e, 3), kv_field(&e, 5).is_some()) {
            (1, _) => "delete",
            (_, false) => "create",
            (_, true) => "update",
        };
        let _ = writeln!(
            out,
            "{:>8}  {}  {:<6}  {}  {} bytes",
            int_field(&e, 1),
            utc(int_field(&e, 2)),
            what,
            String::from_utf8_lossy(&kv.key),
            kv.value.len()
        );
    }
    Ok(())
}

fn utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // days since 1970-01-01 to a civil date (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn kubectl_names_become_registry_keys() {
        use super::registry_key as k;
        assert_eq!(k("cm/default/x"), "/registry/configmaps/default/x");
        assert_eq!(k("deploy/team/web"), "/registry/deployments/team/web");
        assert_eq!(k("ns/team"), "/registry/namespaces/team");
        assert_eq!(k("/raw/key"), "/raw/key");
        assert_eq!(k("--rev"), "--rev");
        assert_eq!(k("widget.example.com/x"), "widget.example.com/x");
    }

    #[test]
    fn dates() {
        assert_eq!(super::utc(0), "1970-01-01 00:00:00Z");
        assert_eq!(super::utc(951_782_400_000), "2000-02-29 00:00:00Z");
        assert_eq!(super::utc(1_792_500_000_000), "2026-10-20 12:40:00Z");
    }
}
