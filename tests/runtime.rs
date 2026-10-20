//! The runtime core against a small image built here: busybox-free, from
//! this machine's own /bin/sh and its libraries, packed as a real OCI
//! image with digests. Needs root; skips otherwise.

use quantykube::runtime::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn is_root() -> bool {
    fs::read_to_string("/proc/self/status")
        .map(|s| {
            s.lines()
                .any(|l| l.starts_with("Uid:") && l.split_whitespace().nth(1) == Some("0"))
        })
        .unwrap_or(false)
}

fn scratch(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("qk-rt-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn digest(data: &[u8]) -> String {
    quantydb_core::to_hex(&quantydb_core::sha256(data))
}

/// An OCI image directory, `manifest.json` plus `blobs/`, with one layer.
fn build_image(at: &Path) {
    let root = at.join("layer");
    fs::create_dir_all(root.join("bin")).unwrap();
    fs::create_dir_all(root.join("etc")).unwrap();
    for b in [
        "/bin/sh",
        "/bin/cat",
        "/bin/ls",
        "/bin/sleep",
        "/bin/hostname",
    ] {
        let real = fs::canonicalize(b).unwrap();
        fs::copy(&real, root.join(&b[1..])).unwrap();
        let ldd = Command::new("ldd").arg(&real).output().unwrap();
        for lib in String::from_utf8_lossy(&ldd.stdout)
            .split_whitespace()
            .filter(|w| w.starts_with('/'))
        {
            let dest = root.join(&lib[1..]);
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            if !dest.exists() {
                fs::copy(lib, dest).unwrap();
            }
        }
    }
    // points at the host, should anything resolve it outside the container
    std::os::unix::fs::symlink(ESCAPE, root.join("escape")).unwrap();
    fs::create_dir_all(root.join(&ESCAPE[1..])).unwrap();
    let tar = Command::new("tar")
        .args(["-czf", "-", "-C"])
        .arg(&root)
        .arg(".")
        .output()
        .unwrap()
        .stdout;
    let config = br#"{"architecture":"amd64","os":"linux","config":{"Env":["PATH=/bin","FROM_IMAGE=yes"],"Cmd":["/bin/sh","-c","echo default"],"WorkingDir":"/"},"rootfs":{"type":"layers","diff_ids":[]}}"#;
    fs::create_dir_all(at.join("blobs")).unwrap();
    fs::write(at.join("blobs").join(digest(&tar)), &tar).unwrap();
    fs::write(at.join("blobs").join(digest(config)), config).unwrap();
    let manifest = format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:{}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"sha256:{}","size":{}}}]}}"#,
        digest(config),
        config.len(),
        digest(&tar),
        tar.len()
    );
    fs::write(at.join("manifest.json"), manifest).unwrap();
}

const ESCAPE: &str = "/tmp/qk-rt-escape-target";

fn wait_exited(rt: &Runtime, id: &str) -> Container {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let c = rt.container(id).unwrap();
        if c.state == ContainerState::Exited {
            return c;
        }
        assert!(Instant::now() < deadline, "container {id} did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn spec(name: &str, args: &[&str]) -> ContainerSpec {
    ContainerSpec {
        name: name.into(),
        image: "test/sh:1".into(),
        command: vec!["sh".into(), "-c".into()],
        args: args.iter().map(|a| a.to_string()).collect(),
        log_path: format!("{name}.log"),
        ..Default::default()
    }
}

#[test]
fn a_pod_from_import_to_removal() {
    if !is_root() {
        eprintln!("not root, runtime test skipped");
        return;
    }
    let base = scratch("pod");
    build_image(&base.join("image"));
    let rt = Runtime::new(&base.join("state")).unwrap();
    let img = rt.import(&base.join("image"), "test/sh:1").unwrap();
    assert_eq!(
        rt.import(&base.join("image"), "test/sh:latest").unwrap().id,
        img.id
    );
    assert_eq!(rt.find_image("test/sh:1").unwrap().refs.len(), 2);

    let logs = base.join("logs");
    let pod = rt
        .run_pod(PodSpec {
            name: "web".into(),
            hostname: "web-0".into(),
            log_directory: logs.to_str().unwrap().into(),
            ..Default::default()
        })
        .unwrap();

    let mut first = spec(
        "first",
        &["echo out; echo err >&2; hostname; echo $FOO $FROM_IMAGE; pwd; ls -l /proc/self/ns/net; cat /sys/class/net/lo/flags"],
    );
    first.env = vec![("FOO".into(), "bar".into())];
    first.working_dir = "/etc".into();
    let c1 = rt.create_container(&pod, first).unwrap();
    rt.start_container(&c1).unwrap();
    let done = wait_exited(&rt, &c1);
    assert_eq!((done.exit_code, done.reason.as_str()), (0, "Completed"));

    let c2 = rt
        .create_container(&pod, spec("second", &["ls -l /proc/self/ns/net"]))
        .unwrap();
    rt.start_container(&c2).unwrap();
    wait_exited(&rt, &c2);

    let log = fs::read_to_string(logs.join("first.log")).unwrap();
    let lines: Vec<(&str, &str, &str)> = log
        .lines()
        .map(|l| {
            let mut p = l.splitn(4, ' ');
            let (t, stream, tag, text) = (
                p.next().unwrap(),
                p.next().unwrap(),
                p.next().unwrap(),
                p.next().unwrap_or(""),
            );
            assert!(t.ends_with('Z') && t.len() == 30, "timestamp {t}");
            assert_eq!(tag, "F");
            (stream, text, t)
        })
        .collect();
    let text: Vec<(&str, &str)> = lines.iter().map(|(s, t, _)| (*s, *t)).collect();
    assert!(text.contains(&("stdout", "out")));
    assert!(text.contains(&("stderr", "err")));
    assert!(text.contains(&("stdout", "web-0")));
    assert!(text.contains(&("stdout", "bar yes")));
    assert!(text.contains(&("stdout", "/etc")));
    assert!(text.contains(&("stdout", "0x9")), "lo is up");
    let net = |log: &str| -> String {
        log.lines()
            .find(|l| l.contains("net:["))
            .unwrap()
            .rsplit("-> ")
            .next()
            .unwrap()
            .to_string()
    };
    let shared = net(&log);
    assert_eq!(
        shared,
        net(&fs::read_to_string(logs.join("second.log")).unwrap())
    );
    let host = fs::read_link("/proc/self/ns/net").unwrap();
    assert_ne!(
        shared,
        host.to_string_lossy(),
        "the pod has its own network"
    );

    // a shell as PID 1 ignores SIGTERM, so stopping it takes the SIGKILL
    let c3 = rt
        .create_container(&pod, spec("stubborn", &["while true; do sleep 1; done"]))
        .unwrap();
    rt.start_container(&c3).unwrap();
    let t = Instant::now();
    rt.stop_container(&c3, Duration::from_millis(300)).unwrap();
    let stopped = rt.container(&c3).unwrap();
    assert_eq!(
        (stopped.state, stopped.exit_code),
        (ContainerState::Exited, 137)
    );
    assert!(t.elapsed() < Duration::from_secs(3));

    let c4 = rt
        .create_container(
            &pod,
            ContainerSpec {
                name: "default".into(),
                image: img.id.clone(),
                log_path: "default.log".into(),
                ..Default::default()
            },
        )
        .unwrap();
    rt.start_container(&c4).unwrap();
    wait_exited(&rt, &c4);
    assert!(fs::read_to_string(logs.join("default.log"))
        .unwrap()
        .ends_with(" stdout F default\n"));

    assert!(rt.remove_image(&img.id).is_err(), "in use");
    rt.remove_pod(&pod).unwrap();
    assert!(rt.containers().is_empty() && rt.pods().is_empty());
    let mounts = fs::read_to_string("/proc/self/mounts").unwrap();
    assert!(
        !mounts.contains(base.to_str().unwrap()),
        "a mount was left behind"
    );
    rt.remove_image("test/sh:1").unwrap();
    assert!(rt.images().is_empty());
    fs::remove_dir_all(&base).unwrap();
}

#[test]
fn a_tampered_layer_is_refused() {
    let base = scratch("tamper");
    build_image(&base.join("image"));
    let blobs = base.join("image/blobs");
    let layer = fs::read_dir(&blobs)
        .unwrap()
        .map(|e| e.unwrap().path())
        .max_by_key(|p| fs::metadata(p).unwrap().len())
        .unwrap();
    let mut data = fs::read(&layer).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 1;
    fs::write(&layer, data).unwrap();
    let rt = Runtime::new(&base.join("state")).unwrap();
    let err = rt.import(&base.join("image"), "x").unwrap_err();
    assert!(
        err.to_string().contains("does not match its digest"),
        "{err}"
    );
    assert!(rt.images().is_empty());
    fs::remove_dir_all(&base).unwrap();
}

#[test]
fn volumes_are_mounted_inside_the_container() {
    if !is_root() {
        eprintln!("not root, runtime test skipped");
        return;
    }
    let base = scratch("volumes");
    build_image(&base.join("image"));
    let rt = Runtime::new(&base.join("state")).unwrap();
    rt.import(&base.join("image"), "test/sh:1").unwrap();
    let shared = base.join("shared");
    fs::create_dir_all(&shared).unwrap();
    let secret = base.join("secret");
    fs::write(&secret, "s3cret\n").unwrap();
    let _ = fs::remove_dir_all(ESCAPE);
    fs::create_dir_all(ESCAPE).unwrap();

    let logs = base.join("logs");
    let pod = rt
        .run_pod(PodSpec {
            name: "vol".into(),
            log_directory: logs.to_str().unwrap().into(),
            ..Default::default()
        })
        .unwrap();
    let mut c = spec(
        "vol",
        &[
            "echo hi > /data/out; cat /run/secret; echo x > /run/secret || echo ro; \
           echo x > /bin/new || echo rootro; cat /escape/inside/f; echo done",
        ],
    );
    let mount = |host: &Path, at: &str, readonly| Mount {
        host_path: host.into(),
        container_path: at.into(),
        readonly,
    };
    c.mounts = vec![
        mount(&shared, "/data", false),
        mount(&secret, "/run/secret", true),
        mount(&shared, "/escape/inside", true),
    ];
    c.readonly_rootfs = true;
    fs::write(shared.join("f"), "through the volume\n").unwrap();
    let id = rt.create_container(&pod, c).unwrap();
    rt.start_container(&id).unwrap();
    let done = wait_exited(&rt, &id);
    let log = fs::read_to_string(logs.join("vol.log")).unwrap();
    let out: Vec<&str> = log
        .lines()
        .filter(|l| l.contains(" stdout "))
        .map(|l| l.rsplit(" F ").next().unwrap())
        .collect();
    assert_eq!(done.exit_code, 0, "{} {log}", done.reason);
    assert_eq!(
        out,
        ["s3cret", "ro", "rootro", "through the volume", "done"],
        "{log}"
    );
    assert_eq!(fs::read_to_string(shared.join("out")).unwrap(), "hi\n");
    assert_eq!(fs::read_to_string(&secret).unwrap(), "s3cret\n");
    assert!(
        fs::read_dir(ESCAPE).unwrap().next().is_none(),
        "a mount target was resolved on the host"
    );
    rt.remove_pod(&pod).unwrap();
    rt.remove_image("test/sh:1").unwrap();
    fs::remove_dir_all(&base).unwrap();
    fs::remove_dir_all(ESCAPE).unwrap();
}

#[test]
fn limits_hold_and_an_oom_kill_is_reported() {
    if !is_root() || !quantykube::cgroup::available() {
        eprintln!("needs root and cgroup v2, skipped");
        return;
    }
    let base = scratch("limits");
    build_image(&base.join("image"));
    let rt = Runtime::new(&base.join("state")).unwrap();
    rt.import(&base.join("image"), "test/sh:1").unwrap();
    let logs = base.join("logs");
    let pod = rt
        .run_pod(PodSpec {
            name: "limits".into(),
            log_directory: logs.to_str().unwrap().into(),
            ..Default::default()
        })
        .unwrap();
    let mut c = spec(
        "hog",
        &["cat /proc/self/cgroup /sys/fs/cgroup/cpu.max; \
           a=x; while :; do a=$a$a; done"],
    );
    c.limits = quantykube::cgroup::Limits {
        memory: 32 << 20,
        memory_swap: 32 << 20,
        cpu_quota: 50_000,
        cpu_period: 100_000,
        ..Default::default()
    };
    let id = rt.create_container(&pod, c).unwrap();
    rt.start_container(&id).unwrap();
    let done = wait_exited(&rt, &id);
    let log = fs::read_to_string(logs.join("hog.log")).unwrap();
    assert!(log.contains("stdout F 0::/\n"), "{log}");
    assert!(log.contains("50000 100000"), "{log}");
    assert_eq!((done.exit_code, done.reason.as_str()), (137, "OOMKilled"));
    assert!(!Path::new("/sys/fs/cgroup/quantykube").join(&id).exists());
    rt.remove_pod(&pod).unwrap();
    fs::remove_dir_all(&base).unwrap();
}

#[test]
fn a_user_other_than_root_has_no_capabilities() {
    if !is_root() {
        eprintln!("not root, runtime test skipped");
        return;
    }
    let base = scratch("user");
    build_image(&base.join("image"));
    let rt = Runtime::new(&base.join("state")).unwrap();
    rt.import(&base.join("image"), "test/sh:1").unwrap();
    let logs = base.join("logs");
    let pod = rt
        .run_pod(PodSpec {
            name: "user".into(),
            log_directory: logs.to_str().unwrap().into(),
            ..Default::default()
        })
        .unwrap();
    let status = "while read -r k v; do case $k in Uid:|Gid:|Groups:|CapEff:) \
                  echo $k $v;; esac; done < /proc/self/status";
    let mut c = spec("user", &[&format!("{status}; sleep 60")]);
    c.user = quantykube::user::Request {
        uid: Some(1000),
        gid: Some(1000),
        supplemental_groups: vec![2000],
        ..Default::default()
    };
    let id = rt.create_container(&pod, c).unwrap();
    rt.start_container(&id).unwrap();
    let want = "Uid: 1000 1000 1000 1000\nGid: 1000 1000 1000 1000\n\
                Groups: 1000 2000\nCapEff: 0000000000000000\n";
    let exec = rt
        .exec_sync(&id, &["sh".into(), "-c".into(), status.into()], None)
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&exec.stdout), want, "{exec:?}");
    rt.stop_container(&id, Duration::ZERO).unwrap();
    let log: String = fs::read_to_string(logs.join("user.log"))
        .unwrap()
        .lines()
        .map(|l| format!("{}\n", l.splitn(4, ' ').nth(3).unwrap_or("")))
        .collect();
    assert_eq!(log, want);
    rt.remove_pod(&pod).unwrap();
    fs::remove_dir_all(&base).unwrap();
}
