use quantydb_core::FileStorage;
use quantykube::store::{self, Kind, Quanty};
use std::io::Write;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage:
  quantykube serve   --data <file> --socket <path> [--retain <duration>]
  quantykube get     --data <file> (--rev <n> | --ago <duration>) <key>
  quantykube history --data <file> [--prefix] <key>

Durations look like 90s, 15m, 2h or 7d. get writes the raw stored value to
stdout, so piping it into `etcdctl put <key>` restores it.";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
        Some("get") => get(&args[1..]),
        Some("history") => history(&args[1..]),
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
        } else if a == "--prefix" {
        } else if a.starts_with("--") {
            skip = true;
        } else {
            found = Some(a.as_str());
        }
    }
    found.ok_or_else(|| format!("missing key\n{USAGE}"))
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

fn read_only(args: &[String]) -> Result<Quanty<FileStorage>, String> {
    let data = required(args, "--data")?;
    Quanty::open_read_only(Path::new(data)).map_err(|e| format!("{data}: {e}"))
}

fn get(args: &[String]) -> Result<(), String> {
    let store = read_only(args)?;
    let key = positional(args)?;
    let rev = match (value(args, "--rev"), value(args, "--ago")) {
        (Some(r), _) => r.parse().map_err(|_| format!("bad revision {r}"))?,
        (None, Some(a)) => store
            .revision_at(now_ms() - duration(a)?.as_millis() as i64)
            .map_err(|e| e.to_string())?,
        (None, None) => return Err(format!("get needs --rev or --ago\n{USAGE}")),
    };
    let mut found = None;
    store
        .read_retained(rev, &mut |v| {
            found = v.get(key.as_bytes())?;
            Ok(())
        })
        .map_err(|e| format!("revision {rev}: {e}"))?;
    let rec = found.ok_or_else(|| format!("{key} did not exist at revision {rev}"))?;
    eprintln!("quantykube: {key} as of revision {rev}");
    std::io::stdout()
        .write_all(&rec.value)
        .map_err(|e| e.to_string())
}

fn history(args: &[String]) -> Result<(), String> {
    let store = read_only(args)?;
    let key = positional(args)?.as_bytes();
    let end = args.iter().any(|a| a == "--prefix").then(|| {
        let mut e = key.to_vec();
        e.push(0xff);
        e
    });
    let entries = store
        .history(key, end.as_deref())
        .map_err(|e| e.to_string())?;
    let mut out = std::io::stdout().lock();
    for h in entries {
        let what = match h.change.kind {
            Kind::Put if h.change.prev.is_none() => "create",
            Kind::Put => "update",
            Kind::Delete => "delete",
        };
        let _ = writeln!(
            out,
            "{:>8}  {}  {:<6}  {}  {} bytes",
            h.rev,
            utc(h.unix_ms),
            what,
            String::from_utf8_lossy(&h.change.kv.key),
            h.change.kv.value.len()
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
    fn dates() {
        assert_eq!(super::utc(0), "1970-01-01 00:00:00Z");
        assert_eq!(super::utc(951_782_400_000), "2000-02-29 00:00:00Z");
        assert_eq!(super::utc(1_792_500_000_000), "2026-10-20 12:40:00Z");
    }
}
