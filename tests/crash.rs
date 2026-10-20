//! Kills the server with SIGKILL while a client writes, restarts it, and
//! checks the one promise that matters: every write that was acknowledged
//! is there, with the revision it was acknowledged at. Writes that were in
//! flight may or may not have landed; either is correct.
//!
//! QUANTY_CRASH_ITERS sets the number of kills (default 20).

use quantycore::grpc::call;
use quantykube::etcdpb::*;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Kills the server when dropped, so a failing assertion cannot leave it
/// running with the test's output pipe open, which hangs `cargo test`.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(dir: &Path) -> Server {
    let sock = dir.join("s.sock");
    let _ = std::fs::remove_file(&sock);
    let child = Command::new(env!("CARGO_BIN_EXE_quantykube"))
        .args(["serve", "--data"])
        .arg(dir.join("db"))
        .arg("--socket")
        .arg(&sock)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let child = Server(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    while UnixStream::connect(&sock).is_err() {
        assert!(Instant::now() < deadline, "server did not come up");
        std::thread::sleep(Duration::from_millis(10));
    }
    child
}

fn rpc<R: Message>(dir: &Path, method: &str, req: impl Message) -> Option<R> {
    let conn = UnixStream::connect(dir.join("s.sock")).ok()?;
    conn.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    let resp = call(conn, method, &req.encode()).ok()?;
    Some(R::decode(&resp).expect("a reply that decodes"))
}

#[test]
fn acknowledged_writes_survive_sigkill() {
    let iters: u64 = std::env::var("QUANTY_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let dir = std::env::temp_dir().join(format!("qk-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir: Arc<PathBuf> = Arc::new(dir);
    let acked: Arc<Mutex<Vec<(u64, i64)>>> = Arc::default();
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = 0u64;
    let mut last_head = 0;

    for round in 0..iters {
        let child = start(&dir);
        if let Some(r) = rpc::<RangeResponse>(
            &dir,
            "/etcdserverpb.KV/Range",
            RangeRequest {
                key: b"/".to_vec(),
                range_end: vec![0],
                count_only: true,
                ..Default::default()
            },
        ) {
            let head = r.header.unwrap().revision;
            assert!(head >= last_head, "round {round}: head went back to {head}");
            last_head = head;
        }
        for &(i, rev) in acked.lock().unwrap().iter() {
            let r: RangeResponse = rpc(
                &dir,
                "/etcdserverpb.KV/Range",
                RangeRequest {
                    key: format!("/k/{i}").into_bytes(),
                    ..Default::default()
                },
            )
            .expect("the restarted server answers");
            let kv = r
                .kvs
                .first()
                .unwrap_or_else(|| panic!("round {round}: /k/{i} lost"));
            assert_eq!(kv.value, i.to_string().as_bytes(), "round {round}: /k/{i}");
            assert_eq!(kv.mod_revision, rev, "round {round}: /k/{i} revision");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (dir, acked, stop) = (dir.clone(), acked.clone(), stop.clone());
            let first = next;
            std::thread::spawn(move || {
                let mut i = first;
                while !stop.load(Ordering::Relaxed) {
                    let put = PutRequest {
                        key: format!("/k/{i}").into_bytes(),
                        value: i.to_string().into_bytes(),
                        ..Default::default()
                    };
                    match rpc::<PutResponse>(&dir, "/etcdserverpb.KV/Put", put) {
                        Some(r) => acked.lock().unwrap().push((i, r.header.unwrap().revision)),
                        None => break,
                    }
                    i += 1;
                }
                i
            })
        };
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        std::thread::sleep(Duration::from_millis(20 + seed % 200));
        drop(child);
        stop.store(true, Ordering::Relaxed);
        // keys written in flight may have landed; never reuse their names
        next = writer.join().unwrap() + 1;
    }
    let n = acked.lock().unwrap().len();
    eprintln!("{iters} kills, {n} acknowledged writes checked after each");
    assert!(n > iters as usize, "too few writes to mean anything");
    std::fs::remove_dir_all(dir.as_path()).unwrap();
}
