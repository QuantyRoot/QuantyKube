//! Pod networks through CNI plugins: separate executables, run with a
//! JSON configuration on stdin and the pod's network namespace in the
//! environment, as containerd and CRI-O run them (CNI spec 1.0).

use quantycore::json::{self, Object, Value};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub struct Cni {
    name: String,
    version: String,
    plugins: Vec<Object>,
    bin_dir: PathBuf,
}

fn invalid(why: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

impl Cni {
    /// The first configuration in `conf_dir` by name, as the kubelet's
    /// other runtimes choose it. None if the directory has none.
    pub fn load(conf_dir: &Path, bin_dir: &Path) -> io::Result<Option<Cni>> {
        let mut files: Vec<PathBuf> = match fs::read_dir(conf_dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.extension()
                        .is_some_and(|e| e == "conflist" || e == "conf" || e == "json")
                })
                .collect(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        files.sort();
        let Some(path) = files.first() else {
            return Ok(None);
        };
        let text = fs::read_to_string(path)?;
        let v = json::parse(&text).map_err(|e| invalid(format!("{}: {e}", path.display())))?;
        let str_of = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
        let plugins = match v.get("plugins").and_then(Value::as_array) {
            Some(list) => list
                .iter()
                .map(|p| p.as_object().cloned())
                .collect::<Option<Vec<_>>>(),
            // a .conf file is a single plugin
            None => v.as_object().cloned().map(|o| vec![o]),
        }
        .filter(|p| !p.is_empty())
        .ok_or_else(|| invalid(format!("{}: no plugins", path.display())))?;
        Ok(Some(Cni {
            name: str_of(&v, "name").unwrap_or_default(),
            version: str_of(&v, "cniVersion").unwrap_or_else(|| "1.0.0".into()),
            plugins,
            bin_dir: bin_dir.into(),
        }))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Attaches the pod and returns the last plugin's result.
    pub fn add(&self, id: &str, netns: &Path) -> io::Result<Value> {
        let mut prev: Option<Value> = None;
        for p in &self.plugins {
            prev = Some(self.exec("ADD", p, id, netns, prev.as_ref())?);
        }
        prev.ok_or_else(|| invalid("no plugins".into()))
    }

    /// Detaches the pod, plugins in reverse order. Each plugin runs even
    /// if one before it failed; the first error is returned.
    pub fn del(&self, id: &str, netns: &Path, result: Option<&Value>) -> io::Result<()> {
        let mut first = Ok(());
        for p in self.plugins.iter().rev() {
            if let Err(e) = self.exec("DEL", p, id, netns, result) {
                if first.is_ok() {
                    first = Err(e);
                }
            }
        }
        first
    }

    fn exec(
        &self,
        command: &str,
        plugin: &Object,
        id: &str,
        netns: &Path,
        prev: Option<&Value>,
    ) -> io::Result<Value> {
        let kind = plugin
            .get("type")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty() && !t.contains('/'))
            .ok_or_else(|| invalid("a CNI plugin without a valid type".into()))?;
        let mut conf = plugin.clone();
        conf.insert("name", Value::from(self.name.as_str()));
        conf.insert("cniVersion", Value::from(self.version.as_str()));
        if let Some(prev) = prev {
            conf.insert("prevResult", prev.clone());
        }
        let mut child = Command::new(self.bin_dir.join(kind))
            .env_clear()
            .env("CNI_COMMAND", command)
            .env("CNI_CONTAINERID", id)
            .env("CNI_NETNS", netns)
            .env("CNI_IFNAME", "eth0")
            .env("CNI_PATH", &self.bin_dir)
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| io::Error::new(e.kind(), format!("CNI plugin {kind}: {e}")))?;
        child
            .stdin
            .take()
            .expect("piped")
            .write_all(Value::Object(conf).to_string().as_bytes())?;
        let out = child.wait_with_output()?;
        let text = String::from_utf8_lossy(&out.stdout);
        if !out.status.success() {
            // plugins report errors as JSON on stdout
            let msg = json::parse(&text)
                .ok()
                .and_then(|v| {
                    let m = v.get("msg")?.as_str()?.to_string();
                    let d = v.get("details").and_then(Value::as_str).unwrap_or("");
                    Some(if d.is_empty() { m } else { format!("{m}: {d}") })
                })
                .unwrap_or_else(|| String::from_utf8_lossy(&out.stderr).trim().to_string());
            return Err(io::Error::other(format!("CNI {command} {kind}: {msg}")));
        }
        if command == "DEL" || text.trim().is_empty() {
            return Ok(prev.cloned().unwrap_or(Value::Null));
        }
        json::parse(&text).map_err(|e| invalid(format!("CNI plugin {kind}: {e}")))
    }
}

/// The pod's first address, without its prefix length.
pub fn ip(result: &Value) -> Option<String> {
    let addr = result.pointer("/ips/0/address")?.as_str()?;
    Some(addr.split('/').next()?.to_string())
}
