//! `kubectl quanty`: the history of a QuantyKube store through the
//! Kubernetes API (ADR-028). It asks kubectl to make the requests, so it
//! needs nothing but the kubeconfig kubectl uses, and runs wherever
//! kubectl does, Windows included. Only the standard library: the
//! protobuf it speaks is a handful of fields.

use std::io::Write;
use std::process::{Command, Stdio};

const USAGE: &str = "usage:
  kubectl quanty history [--prefix] <object>
  kubectl quanty get     (--rev <n> | --ago <duration>) <object>
  kubectl quanty restore (--rev <n> | --ago <duration>) <object>

An object is kind/namespace/name, such as configmap/default/important,
or a raw store key starting with /. Durations look like 90s, 15m, 2h.
kubectl is found on the PATH, or named by $KUBECTL.";

const API: &str = "/apis/quanty.quantyroot.io/v1alpha1";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("history") => history(&args[1..]),
        Some("get") => get(&args[1..]),
        Some("restore") => restore(&args[1..]),
        _ => Err(USAGE.to_string()),
    };
    if let Err(e) = result {
        eprintln!("kubectl-quanty: {e}");
        std::process::exit(1);
    }
}

fn value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn positional(args: &[String]) -> Result<String, String> {
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
    found
        .map(registry_key)
        .ok_or_else(|| format!("missing object\n{USAGE}"))
}

/// Where the API server keeps an object, from kind/namespace/name.
fn registry_key(a: &str) -> String {
    if a.starts_with('/') || !a.contains('/') {
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

fn duration_ms(s: &str) -> Result<i64, String> {
    let (n, unit) = s.split_at(s.len().saturating_sub(1));
    let n: i64 = n.parse().map_err(|_| format!("bad duration {s}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => return Err(format!("bad duration {s}")),
    };
    Ok(secs * 1000)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

// protobuf, as much as History needs

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, field: u32, v: &[u8]) {
    put_varint(out, u64::from(field) << 3 | 2);
    put_varint(out, v.len() as u64);
    out.extend_from_slice(v);
}

fn put_int(out: &mut Vec<u8>, field: u32, v: i64) {
    put_varint(out, u64::from(field) << 3);
    put_varint(out, v as u64);
}

enum Field<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

fn varint(buf: &[u8], at: &mut usize) -> Result<u64, String> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *buf.get(*at).ok_or("a truncated answer")?;
        *at += 1;
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Ok(v);
        }
    }
    Err("a varint too long".into())
}

fn fields(buf: &[u8]) -> Result<Vec<(u32, Field<'_>)>, String> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < buf.len() {
        let key = varint(buf, &mut at)?;
        let n = (key >> 3) as u32;
        match key & 7 {
            0 => out.push((n, Field::Int(varint(buf, &mut at)? as i64))),
            2 => {
                let len = varint(buf, &mut at)? as usize;
                let b = buf.get(at..at + len).ok_or("a truncated answer")?;
                at += len;
                out.push((n, Field::Bytes(b)));
            }
            1 => at += 8,
            5 => at += 4,
            w => return Err(format!("wire type {w} in the answer")),
        }
    }
    Ok(out)
}

fn int(f: &[(u32, Field)], n: u32) -> i64 {
    f.iter()
        .find_map(|(m, v)| match v {
            Field::Int(i) if *m == n => Some(*i),
            _ => None,
        })
        .unwrap_or(0)
}

fn bytes<'a>(f: &[(u32, Field<'a>)], n: u32) -> Option<&'a [u8]> {
    f.iter().find_map(|(m, v)| match v {
        Field::Bytes(b) if *m == n => Some(*b),
        _ => None,
    })
}

/// key and value of an etcd KeyValue
fn kv(b: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let f = fields(b)?;
    Ok((
        bytes(&f, 1).unwrap_or_default().to_vec(),
        bytes(&f, 5).unwrap_or_default().to_vec(),
    ))
}

fn base64url(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = c.iter().fold(0u32, |a, b| a << 8 | u32::from(*b)) << (8 * (3 - c.len()));
        for i in 0..=c.len() {
            out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

fn kubectl() -> Command {
    Command::new(std::env::var("KUBECTL").unwrap_or_else(|_| "kubectl".into()))
}

fn run(mut cmd: Command, input: Option<&[u8]>) -> Result<Vec<u8>, String> {
    cmd.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("kubectl: {e}"))?;
    if let (Some(data), Some(mut stdin)) = (input, child.stdin.take()) {
        stdin.write_all(data).map_err(|e| e.to_string())?;
    }
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(out.stdout)
}

fn read(resource: &str, req: &[u8]) -> Result<Vec<u8>, String> {
    let mut cmd = kubectl();
    cmd.args(["get", "--raw"])
        .arg(format!("{API}/{resource}?q={}", base64url(req)));
    run(cmd, None)
}

/// The request History takes: the key, the end of a range, a revision or
/// a time.
fn request(args: &[String], with_end: bool) -> Result<(String, Vec<u8>), String> {
    let key = positional(args)?;
    let mut req = Vec::new();
    put_bytes(&mut req, 1, key.as_bytes());
    if with_end && args.iter().any(|a| a == "--prefix") {
        let mut end = key.as_bytes().to_vec();
        end.push(0xff);
        put_bytes(&mut req, 2, &end);
    }
    match (value(args, "--rev"), value(args, "--ago")) {
        (Some(r), _) => put_int(
            &mut req,
            3,
            r.parse().map_err(|_| format!("bad revision {r}"))?,
        ),
        (None, Some(a)) => put_int(&mut req, 4, now_ms() - duration_ms(a)?),
        (None, None) if !with_end => return Err(format!("needs --rev or --ago\n{USAGE}")),
        (None, None) => {}
    }
    Ok((key, req))
}

fn history(args: &[String]) -> Result<(), String> {
    let (_, req) = request(args, true)?;
    let resp = read("logs", &req)?;
    let mut out = std::io::stdout().lock();
    for (_, entry) in fields(&resp)? {
        let Field::Bytes(entry) = entry else { continue };
        let e = fields(entry)?;
        let (key, value) = kv(bytes(&e, 4).unwrap_or_default())?;
        let what = match (int(&e, 3), bytes(&e, 5).is_some()) {
            (1, _) => "delete",
            (_, false) => "create",
            (_, true) => "update",
        };
        let _ = writeln!(
            out,
            "{:>8}  {}  {:<6}  {}  {} bytes",
            int(&e, 1),
            utc(int(&e, 2)),
            what,
            String::from_utf8_lossy(&key),
            value.len()
        );
    }
    Ok(())
}

fn get(args: &[String]) -> Result<(), String> {
    let (key, req) = request(args, false)?;
    let resp = read("objects", &req)?;
    let f = fields(&resp)?;
    let rev = int(&f, 1);
    let Some(b) = bytes(&f, 2) else {
        return Err(format!("{key} did not exist at revision {rev}"));
    };
    eprintln!("kubectl-quanty: as of revision {rev}");
    std::io::stdout()
        .write_all(&kv(b)?.1)
        .map_err(|e| e.to_string())
}

fn restore(args: &[String]) -> Result<(), String> {
    let (key, req) = request(args, false)?;
    let mut cmd = kubectl();
    cmd.args(["create", "--raw", &format!("{API}/restores"), "-f", "-"]);
    let resp = run(cmd, Some(&req))?;
    let f = fields(&resp)?;
    let (rev, now) = (int(&f, 1), int(&f, 3));
    match bytes(&f, 2) {
        Some(_) => println!("{key}: restored as of revision {rev}, now revision {now}"),
        None => println!("{key}: did not exist at revision {rev}, deleted, now revision {now}"),
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
    use super::*;

    #[test]
    fn requests_encode_as_the_server_reads_them() {
        assert_eq!(base64url(b"hello"), "aGVsbG8");
        assert_eq!(base64url(&[0xff, 0xef]), "_-8");
        let args: Vec<String> = ["--rev", "7", "cm/default/x"].map(String::from).to_vec();
        let (key, req) = request(&args, false).unwrap();
        assert_eq!(key, "/registry/configmaps/default/x");
        let f = fields(&req).unwrap();
        assert_eq!(bytes(&f, 1).unwrap(), key.as_bytes());
        assert_eq!(int(&f, 3), 7);
    }
}
