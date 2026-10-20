//! The node runtime's core: an image store, pod sandboxes and containers,
//! in the shape the Container Runtime Interface asks for, without the
//! gRPC around it (docs/RUNTIME.md).
//!
//! State lives in memory and under one directory. A restart of the
//! runtime forgets its pods; the kubelet then recreates them, which is
//! also what it does after a node reboot. Persisting state is later work.

use crate::image;
use quantycore::json::{self, Value};
use quantykube_sys as sys;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Image {
    /// `sha256:` and the digest of the image config, as containerd names
    /// images.
    pub id: String,
    pub refs: Vec<String>,
    pub size: u64,
    pub config: ImageConfig,
}

#[derive(Debug, Clone, Default)]
pub struct PodSpec {
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub attempt: u32,
    pub hostname: String,
    pub log_directory: String,
    pub labels: Vec<(String, String)>,
    pub annotations: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodState {
    Ready,
    NotReady,
}

#[derive(Debug, Clone)]
pub struct Pod {
    pub id: String,
    pub spec: PodSpec,
    pub state: PodState,
    pub created_ns: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ContainerSpec {
    pub name: String,
    pub attempt: u32,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: String,
    /// Relative to the pod's log directory, as the kubelet sends it.
    pub log_path: String,
    pub labels: Vec<(String, String)>,
    pub annotations: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerState {
    Created,
    Running,
    Exited,
}

#[derive(Debug, Clone)]
pub struct Container {
    pub id: String,
    pub pod_id: String,
    pub spec: ContainerSpec,
    pub image_id: String,
    pub state: ContainerState,
    pub pid: i32,
    pub created_ns: i64,
    pub started_ns: i64,
    pub finished_ns: i64,
    pub exit_code: i32,
    pub reason: String,
}

#[derive(Debug)]
pub enum Error {
    NotFound(String),
    Invalid(String),
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound(what) => write!(f, "{what} not found"),
            Error::Invalid(why) => f.write_str(why),
            Error::Io(e) => write!(f, "{e}"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

fn new_id() -> io::Result<String> {
    let mut b = [0u8; 32];
    io::Read::read_exact(&mut File::open("/dev/urandom")?, &mut b)?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

/// RFC 3339 with nanoseconds, UTC: the timestamp format of CRI log lines.
pub fn rfc3339_nano(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000);
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
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Longest line written as one log entry; longer lines are split into
/// partial entries, as containerd splits them.
const MAX_LOG_LINE: usize = 16 * 1024;

/// Copies a stream into the CRI log format the kubelet reads:
/// `<time> <stream> <F|P> <text>`, F for a full line, P for a piece of a
/// longer one.
fn pump(from: File, stream: &'static str, to: Arc<Mutex<File>>) {
    let mut r = BufReader::new(from);
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = match (&mut r)
            .take(MAX_LOG_LINE as u64)
            .read_until(b'\n', &mut line)
        {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let full = line.ends_with(b"\n");
        let text = if full { &line[..n - 1] } else { &line[..] };
        let mut entry = format!(
            "{} {stream} {} ",
            rfc3339_nano(now_ns()),
            if full { "F" } else { "P" }
        )
        .into_bytes();
        entry.extend_from_slice(text);
        entry.push(b'\n');
        let mut f = to.lock().unwrap_or_else(|e| e.into_inner());
        if f.write_all(&entry).is_err() {
            return;
        }
    }
}

use std::io::Read as _;

pub struct Runtime {
    dir: PathBuf,
    images: Mutex<Vec<Image>>,
    pods: Mutex<HashMap<String, Pod>>,
    containers: Mutex<HashMap<String, Container>>,
    exited: Condvar,
}

fn strings(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .unwrap_or_default()
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

fn digest_hex(d: &str) -> Result<&str> {
    d.strip_prefix("sha256:")
        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| Error::Invalid(format!("unsupported digest {d}")))
}

fn read_blob(blobs: &Path, digest: &str) -> Result<Vec<u8>> {
    let hex = digest_hex(digest)?;
    let data = fs::read(blobs.join(hex))?;
    if quantydb_core::to_hex(&quantydb_core::sha256(&data)) != hex {
        return Err(Error::Invalid(format!(
            "blob {digest} does not match its digest"
        )));
    }
    Ok(data)
}

impl Runtime {
    pub fn new(dir: &Path) -> Result<Arc<Runtime>> {
        for sub in ["images", "pods", "containers"] {
            fs::create_dir_all(dir.join(sub))?;
        }
        let rt = Runtime {
            dir: fs::canonicalize(dir)?,
            images: Mutex::new(Vec::new()),
            pods: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
            exited: Condvar::new(),
        };
        rt.load_images()?;
        Ok(Arc::new(rt))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn load_images(&self) -> Result<()> {
        let mut images = Vec::new();
        for e in fs::read_dir(self.dir.join("images"))? {
            let dir = e?.path();
            let Ok(meta) = fs::read_to_string(dir.join("image.json")) else {
                continue;
            };
            let v = json::parse(&meta).map_err(|e| Error::Invalid(e.to_string()))?;
            let config = fs::read_to_string(dir.join("config.json"))?;
            images.push(Image {
                id: v
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                refs: strings(v.get("refs")),
                size: v.get("size").and_then(Value::as_u64).unwrap_or(0),
                config: parse_config(&config)?,
            });
        }
        *self.images.lock().unwrap() = images;
        Ok(())
    }

    /// Imports an image from a directory holding `manifest.json`, an OCI
    /// or Docker v2 image manifest, and `blobs/<hex digest>`. Every blob
    /// is checked against its digest before anything is unpacked.
    pub fn import(&self, from: &Path, reference: &str) -> Result<Image> {
        let manifest = fs::read_to_string(from.join("manifest.json"))?;
        let m = json::parse(&manifest).map_err(|e| Error::Invalid(format!("manifest: {e}")))?;
        let blobs = from.join("blobs");
        let config_digest = m
            .pointer("/config/digest")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Invalid("manifest without config".into()))?;
        let config = read_blob(&blobs, config_digest)?;
        let config =
            String::from_utf8(config).map_err(|_| Error::Invalid("config is not UTF-8".into()))?;
        let id = format!("sha256:{}", digest_hex(config_digest)?);
        let dir = self.dir.join("images").join(digest_hex(config_digest)?);
        if let Some(img) = self.find_image(&id) {
            return self.tag(&img.id, reference);
        }
        let staging = dir.with_extension("partial");
        let _ = fs::remove_dir_all(&staging);
        fs::create_dir_all(staging.join("rootfs"))?;
        let mut size = 0;
        for layer in m
            .get("layers")
            .and_then(Value::as_array)
            .unwrap_or_default()
        {
            let digest = layer.get("digest").and_then(Value::as_str).unwrap_or("");
            let media = layer.get("mediaType").and_then(Value::as_str).unwrap_or("");
            if !media.ends_with("tar.gzip") && !media.ends_with("tar+gzip") {
                return Err(Error::Invalid(format!("unsupported layer type {media}")));
            }
            let data = read_blob(&blobs, digest)?;
            size += data.len() as u64;
            image::unpack_layer(&data[..], &staging.join("rootfs"))?;
        }
        fs::write(staging.join("config.json"), &config)?;
        let img = Image {
            id: id.clone(),
            refs: vec![reference.to_string()],
            size,
            config: parse_config(&config)?,
        };
        write_meta(&staging, &img)?;
        // a half unpacked image is never visible under its final name
        fs::rename(&staging, &dir)?;
        let mut images = self.images.lock().unwrap();
        for other in images.iter_mut() {
            other.refs.retain(|r| r != reference);
        }
        images.push(img.clone());
        Ok(img)
    }

    fn tag(&self, id: &str, reference: &str) -> Result<Image> {
        let mut images = self.images.lock().unwrap();
        for other in images.iter_mut() {
            other.refs.retain(|r| r != reference);
        }
        let img = images
            .iter_mut()
            .find(|i| i.id == id)
            .ok_or_else(|| Error::NotFound(id.to_string()))?;
        img.refs.push(reference.to_string());
        write_meta(&self.dir.join("images").join(&id[7..]), img)?;
        Ok(img.clone())
    }

    /// By reference, or by id with or without `sha256:`.
    pub fn find_image(&self, name: &str) -> Option<Image> {
        let images = self.images.lock().unwrap();
        images
            .iter()
            .find(|i| i.id == name || i.id[7..] == *name || i.refs.iter().any(|r| r == name))
            .cloned()
    }

    pub fn images(&self) -> Vec<Image> {
        self.images.lock().unwrap().clone()
    }

    pub fn remove_image(&self, name: &str) -> Result<()> {
        let img = self
            .find_image(name)
            .ok_or_else(|| Error::NotFound(name.into()))?;
        if self
            .containers
            .lock()
            .unwrap()
            .values()
            .any(|c| c.image_id == img.id)
        {
            return Err(Error::Invalid(format!("image {} is in use", img.id)));
        }
        fs::remove_dir_all(self.dir.join("images").join(&img.id[7..]))?;
        self.images.lock().unwrap().retain(|i| i.id != img.id);
        Ok(())
    }

    pub fn run_pod(&self, spec: PodSpec) -> Result<String> {
        let id = new_id()?;
        let dir = self.dir.join("pods").join(&id);
        fs::create_dir_all(dir.join("ns"))?;
        let hostname = if spec.hostname.is_empty() {
            &spec.name
        } else {
            &spec.hostname
        };
        sys::hold_namespaces(&dir.join("ns"), hostname.as_bytes())?;
        if !spec.log_directory.is_empty() {
            fs::create_dir_all(&spec.log_directory)?;
        }
        let pod = Pod {
            id: id.clone(),
            spec,
            state: PodState::Ready,
            created_ns: now_ns(),
        };
        self.pods.lock().unwrap().insert(id.clone(), pod);
        Ok(id)
    }

    pub fn pod(&self, id: &str) -> Result<Pod> {
        self.pods
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("pod {id}")))
    }

    pub fn pods(&self) -> Vec<Pod> {
        self.pods.lock().unwrap().values().cloned().collect()
    }

    /// Stops every container of the pod and lets its namespaces go. The
    /// kubelet may call it more than once.
    pub fn stop_pod(&self, id: &str, grace: Duration) -> Result<()> {
        self.pod(id)?;
        let members: Vec<String> = self
            .containers
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.pod_id == id)
            .map(|c| c.id.clone())
            .collect();
        for c in members {
            self.stop_container(&c, grace)?;
        }
        sys::release_namespaces(&self.dir.join("pods").join(id).join("ns"))?;
        if let Some(p) = self.pods.lock().unwrap().get_mut(id) {
            p.state = PodState::NotReady;
        }
        Ok(())
    }

    pub fn remove_pod(&self, id: &str) -> Result<()> {
        if self.pod(id).is_err() {
            return Ok(());
        }
        self.stop_pod(id, Duration::ZERO)?;
        let members: Vec<String> = self
            .containers
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.pod_id == id)
            .map(|c| c.id.clone())
            .collect();
        for c in members {
            self.remove_container(&c)?;
        }
        fs::remove_dir_all(self.dir.join("pods").join(id))?;
        self.pods.lock().unwrap().remove(id);
        Ok(())
    }

    pub fn create_container(&self, pod_id: &str, spec: ContainerSpec) -> Result<String> {
        let pod = self.pod(pod_id)?;
        if pod.state != PodState::Ready {
            return Err(Error::Invalid(format!("pod {pod_id} is not ready")));
        }
        let img = self
            .find_image(&spec.image)
            .ok_or_else(|| Error::NotFound(format!("image {}", spec.image)))?;
        let id = new_id()?;
        let dir = self.dir.join("containers").join(&id);
        for sub in ["upper", "work", "rootfs"] {
            fs::create_dir_all(dir.join(sub))?;
        }
        let c = Container {
            id: id.clone(),
            pod_id: pod_id.to_string(),
            spec,
            image_id: img.id,
            state: ContainerState::Created,
            pid: 0,
            created_ns: now_ns(),
            started_ns: 0,
            finished_ns: 0,
            exit_code: 0,
            reason: String::new(),
        };
        self.containers.lock().unwrap().insert(id.clone(), c);
        Ok(id)
    }

    pub fn container(&self, id: &str) -> Result<Container> {
        self.containers
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("container {id}")))
    }

    pub fn containers(&self) -> Vec<Container> {
        self.containers.lock().unwrap().values().cloned().collect()
    }

    pub fn log_path(&self, c: &Container) -> Option<PathBuf> {
        let pod = self.pod(&c.pod_id).ok()?;
        (!pod.spec.log_directory.is_empty() && !c.spec.log_path.is_empty())
            .then(|| Path::new(&pod.spec.log_directory).join(&c.spec.log_path))
    }

    pub fn start_container(self: &Arc<Self>, id: &str) -> Result<()> {
        let c = self.container(id)?;
        if c.state != ContainerState::Created {
            return Err(Error::Invalid(format!(
                "container {id} was already started"
            )));
        }
        let img = self
            .find_image(&c.image_id)
            .ok_or_else(|| Error::NotFound(format!("image {}", c.image_id)))?;
        // the CRI's command replaces the image's entrypoint and its args
        // the image's cmd, each only when given, as in Docker
        let mut argv: Vec<String> = if c.spec.command.is_empty() {
            img.config.entrypoint.clone()
        } else {
            c.spec.command.clone()
        };
        if !c.spec.args.is_empty() {
            argv.extend(c.spec.args.iter().cloned());
        } else if c.spec.command.is_empty() {
            argv.extend(img.config.cmd.iter().cloned());
        }
        if argv.is_empty() {
            return Err(Error::Invalid("no command and the image has none".into()));
        }
        let lower = self.dir.join("images").join(&img.id[7..]).join("rootfs");
        if !argv[0].contains('/') {
            argv[0] = resolve_in_path(&lower, &argv[0], &img.config.env, &c.spec.env);
        }
        let mut env = img.config.env.clone();
        for (k, v) in &c.spec.env {
            env.retain(|e| e.split('=').next() != Some(k));
            env.push(format!("{k}={v}"));
        }
        let dir = self.dir.join("containers").join(id);
        let bytes = |p: &Path| p.as_os_str().as_encoded_bytes().to_vec();
        let argv_b: Vec<&[u8]> = argv.iter().map(|a| a.as_bytes()).collect();
        let env_b: Vec<&[u8]> = env.iter().map(|a| a.as_bytes()).collect();
        let cwd = if !c.spec.working_dir.is_empty() {
            c.spec.working_dir.clone()
        } else if !img.config.working_dir.is_empty() {
            img.config.working_dir.clone()
        } else {
            "/".into()
        };
        let (out_r, out_w) = pipe()?;
        let (err_r, err_w) = pipe()?;
        let spawn = sys::Spawn::new(&bytes(&dir.join("rootfs")), &argv_b, &env_b)?
            .overlay(
                &bytes(&lower),
                &bytes(&dir.join("upper")),
                &bytes(&dir.join("work")),
            )?
            .join(&self.dir.join("pods").join(&c.pod_id).join("ns"))?
            .stdio(File::open("/dev/null")?, out_w, err_w)
            .cwd(cwd.as_bytes())?;
        for d in ["proc", "dev", "sys"] {
            let _ = fs::create_dir(dir.join("upper").join(d));
        }
        let started = spawn.start()?;
        let pid = started.pid;
        drop(spawn);
        started.release()?;
        if let Some(path) = self.log_path(&c) {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let log = Arc::new(Mutex::new(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?,
            ));
            let l2 = log.clone();
            std::thread::spawn(move || pump(out_r, "stdout", log));
            std::thread::spawn(move || pump(err_r, "stderr", l2));
        }
        {
            let mut cs = self.containers.lock().unwrap();
            let c = cs.get_mut(id).expect("checked above");
            c.state = ContainerState::Running;
            c.pid = pid;
            c.started_ns = now_ns();
        }
        let rt = Arc::clone(self);
        let id = id.to_string();
        std::thread::spawn(move || {
            let exit = sys::wait(pid);
            let mut cs = rt.containers.lock().unwrap();
            if let Some(c) = cs.get_mut(&id) {
                c.state = ContainerState::Exited;
                c.finished_ns = now_ns();
                (c.exit_code, c.reason) = match exit {
                    Ok(sys::Exit::Code(0)) => (0, "Completed".into()),
                    Ok(sys::Exit::Code(n)) => match sys::Step::from_status(n) {
                        Some(step) => (128, format!("StartError: setup failed at {step:?}")),
                        None => (n, "Error".into()),
                    },
                    Ok(sys::Exit::Signal(s)) => (128 + s, "Error".into()),
                    Err(e) => (255, format!("Unknown: {e}")),
                };
            }
            rt.exited.notify_all();
        });
        Ok(())
    }

    /// SIGTERM, then SIGKILL once `grace` has passed.
    pub fn stop_container(&self, id: &str, grace: Duration) -> Result<()> {
        let c = self.container(id)?;
        if c.state != ContainerState::Running {
            if c.state == ContainerState::Created {
                if let Some(c) = self.containers.lock().unwrap().get_mut(id) {
                    c.state = ContainerState::Exited;
                    c.finished_ns = now_ns();
                }
            }
            return Ok(());
        }
        let _ = sys::signal(c.pid, 15);
        let deadline = Instant::now() + grace;
        let mut cs = self.containers.lock().unwrap();
        loop {
            match cs.get(id) {
                Some(c) if c.state == ContainerState::Running => {}
                _ => return Ok(()),
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                let _ = sys::signal(c.pid, 9);
                let (g, _) = self
                    .exited
                    .wait_timeout(cs, Duration::from_secs(5))
                    .unwrap();
                cs = g;
                continue;
            }
            cs = self.exited.wait_timeout(cs, left).unwrap().0;
        }
    }

    pub fn remove_container(&self, id: &str) -> Result<()> {
        let Ok(c) = self.container(id) else {
            return Ok(());
        };
        if c.state == ContainerState::Running {
            self.stop_container(id, Duration::ZERO)?;
        }
        fs::remove_dir_all(self.dir.join("containers").join(id))?;
        self.containers.lock().unwrap().remove(id);
        Ok(())
    }
}

fn pipe() -> io::Result<(File, File)> {
    let (r, w) = std::io::pipe()?;
    Ok((
        File::from(std::os::fd::OwnedFd::from(r)),
        File::from(std::os::fd::OwnedFd::from(w)),
    ))
}

/// The PATH search a shell would do, but inside the image.
fn resolve_in_path(
    root: &Path,
    cmd: &str,
    image_env: &[String],
    env: &[(String, String)],
) -> String {
    let path = env
        .iter()
        .find(|(k, _)| k == "PATH")
        .map(|(_, v)| v.clone())
        .or_else(|| {
            image_env
                .iter()
                .find_map(|e| e.strip_prefix("PATH=").map(str::to_string))
        })
        .unwrap_or_else(|| "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into());
    path.split(':')
        .map(|d| format!("{}/{cmd}", d.trim_end_matches('/')))
        .find(|p| fs::symlink_metadata(root.join(p.trim_start_matches('/'))).is_ok())
        .unwrap_or_else(|| cmd.to_string())
}

fn parse_config(text: &str) -> Result<ImageConfig> {
    let v = json::parse(text).map_err(|e| Error::Invalid(format!("image config: {e}")))?;
    let c = v.get("config");
    let field = |name: &str| c.and_then(|c| c.get(name));
    Ok(ImageConfig {
        entrypoint: strings(field("Entrypoint")),
        cmd: strings(field("Cmd")),
        env: strings(field("Env")),
        working_dir: field("WorkingDir")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    })
}

fn write_meta(dir: &Path, img: &Image) -> Result<()> {
    let mut o = json::Object::default();
    o.insert("id", Value::from(img.id.as_str()));
    o.insert(
        "refs",
        Value::Array(img.refs.iter().map(|r| Value::from(r.as_str())).collect()),
    );
    o.insert("size", Value::from(img.size));
    fs::write(dir.join("image.json"), Value::Object(o).to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_timestamps() {
        assert_eq!(rfc3339_nano(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            rfc3339_nano(1_792_500_000_123_456_789),
            "2026-10-20T12:40:00.123456789Z"
        );
    }
}
