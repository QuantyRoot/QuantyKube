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
  quantykube serve   --data <file> --socket <path> [--retain <duration>]
  quantykube history [--socket <path>] [--prefix] <key>
  quantykube get     [--socket <path>] (--rev <n> | --ago <duration>) <key>
  quantykube restore [--socket <path>] (--rev <n> | --ago <duration>) <key>

The socket defaults to $QUANTYKUBE_SOCKET, then /run/quantykube.sock.
Linked or copied as kubectl-quanty on the PATH, this is also the kubectl
plugin `kubectl quanty`, and keys may be given as kind/name or
kind/namespace/name, such as configmap/default/important.

Durations look like 90s, 15m, 2h or 7d. get writes the stored value as it
was to stdout. restore writes it back, or deletes the key if it did not
exist then, as a new revision that every watch sees.";

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

fn serve(args: &[String]) -> Result<(), String> {
    let data = PathBuf::from(required(args, "--data")?);
    let socket = PathBuf::from(required(args, "--socket")?);
    let retain = duration(value(args, "--retain").unwrap_or("1h"))?;
    let store = Arc::new(open(&data).map_err(|e| format!("{}: {e}", data.display()))?);
    let endpoint = format!("unix://{}", socket.display());
    let etcd =
        quantykube::etcd::Etcd::new(store.clone(), endpoint.clone()).map_err(|e| e.to_string())?;
    let keep = retain.as_millis() as i64;
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(60));
        if let Err(e) = store.enforce_retention(keep, now_ms()) {
            eprintln!("quantykube: retention: {e}");
        }
    });
    if socket.exists() {
        std::fs::remove_file(&socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    }
    let listener = UnixListener::bind(&socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    eprintln!("quantykube: serving {} on {endpoint}", data.display());
    quantycore::grpc::serve_unix(listener, etcd).map_err(|e| e.to_string())
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
