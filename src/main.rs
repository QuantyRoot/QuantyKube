use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;

const USAGE: &str = "usage: quantykube serve --data <file> --socket <path>";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]),
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

fn flag(args: &[String], name: &str) -> Result<PathBuf, String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {name}\n{USAGE}"))
}

/// A process killed a moment ago may still hold the file's lock while the
/// kernel tears it down, and a supervisor restarts the new one at once.
fn open(
    data: &std::path::Path,
) -> Result<quantykube::store::Quanty<quantydb_core::FileStorage>, quantykube::store::Error> {
    let mut tries = 0;
    loop {
        match quantykube::store::Quanty::open(data) {
            Err(quantykube::store::Error::Engine(quantydb_core::Error::AlreadyOpen))
                if tries < 50 =>
            {
                tries += 1;
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            r => return r,
        }
    }
}

fn serve(args: &[String]) -> Result<(), String> {
    let data = flag(args, "--data")?;
    let socket = flag(args, "--socket")?;
    let store = open(&data).map_err(|e| format!("{}: {e}", data.display()))?;
    let endpoint = format!("unix://{}", socket.display());
    let etcd = quantykube::etcd::Etcd::new(Arc::new(store), endpoint.clone())
        .map_err(|e| e.to_string())?;
    // a socket left behind by a previous run would make bind fail
    if socket.exists() {
        std::fs::remove_file(&socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    }
    let listener = UnixListener::bind(&socket).map_err(|e| format!("{}: {e}", socket.display()))?;
    eprintln!("quantykube: serving {} on {endpoint}", data.display());
    quantycore::grpc::serve_unix(listener, etcd).map_err(|e| e.to_string())
}
