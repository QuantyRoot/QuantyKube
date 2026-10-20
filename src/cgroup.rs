//! Container cgroups on the cgroup v2 hierarchy, laid out the way the
//! kubelet's cgroupfs driver expects: the kubelet creates the pod's cgroup
//! and passes it as the parent, the runtime creates one per container in it.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const ROOT: &str = "/sys/fs/cgroup";

/// The limits of one container, in the CRI's units.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Limits {
    pub cpu_period: i64,
    pub cpu_quota: i64,
    pub cpu_shares: i64,
    pub memory: i64,
    pub cpuset_cpus: String,
    pub cpuset_mems: String,
    /// Page size as the kubelet names it, such as "2MB", and the limit.
    pub hugepages: Vec<(String, u64)>,
    /// cgroup v2 files and their contents, written as they are.
    pub unified: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub cpu_ns: u64,
    pub memory: u64,
    pub working_set: u64,
    pub rss: u64,
    pub page_faults: u64,
    pub major_page_faults: u64,
    pub processes: u64,
}

pub fn available() -> bool {
    Path::new(ROOT).join("cgroup.controllers").exists()
}

/// `parent` is a cgroupfs path such as `/kubepods/burstable/pod<uid>`.
pub fn container_dir(parent: &str, id: &str) -> io::Result<PathBuf> {
    let invalid = |why: String| io::Error::new(io::ErrorKind::InvalidInput, why);
    let parent = if parent.is_empty() {
        "/quantykube"
    } else {
        parent
    };
    // a systemd slice name would need the systemd driver
    if !parent.starts_with('/') {
        return Err(invalid(format!(
            "cgroup parent {parent} is not a cgroupfs path"
        )));
    }
    if parent.split('/').any(|p| p == "..") {
        return Err(invalid(format!(
            "cgroup parent {parent} leaves the hierarchy"
        )));
    }
    Ok(Path::new(ROOT).join(&parent[1..]).join(id))
}

/// The kubelet's conversion from cgroup v1 shares to a v2 weight.
fn weight(shares: i64) -> i64 {
    if shares <= 0 {
        return 100;
    }
    1 + (shares.clamp(2, 262_144) - 2) * 9999 / 262_142
}

fn write(dir: &Path, file: &str, value: &str) -> io::Result<()> {
    fs::write(dir.join(file), value)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", dir.join(file).display())))
}

/// Creates the cgroup with its limits set, ready for a process.
pub fn create(dir: &Path, limits: &Limits) -> io::Result<()> {
    let root = Path::new(ROOT);
    fs::create_dir_all(dir)?;
    // a limit takes effect only where every ancestor delegates the
    // controller; this is what the kubelet itself does for its cgroups
    let mut at = dir.parent();
    let mut ancestors = Vec::new();
    while let Some(a) = at.filter(|a| a.starts_with(root)) {
        ancestors.push(a);
        at = a.parent();
    }
    for a in ancestors.iter().rev() {
        for c in ["cpu", "memory", "pids", "cpuset", "hugetlb"] {
            let _ = fs::write(a.join("cgroup.subtree_control"), format!("+{c}"));
        }
    }
    if limits.memory > 0 {
        write(dir, "memory.max", &limits.memory.to_string())?;
    }
    if limits.cpu_quota > 0 {
        let period = if limits.cpu_period > 0 {
            limits.cpu_period
        } else {
            100_000
        };
        write(dir, "cpu.max", &format!("{} {period}", limits.cpu_quota))?;
    }
    if limits.cpu_shares > 0 {
        write(dir, "cpu.weight", &weight(limits.cpu_shares).to_string())?;
    }
    if !limits.cpuset_cpus.is_empty() {
        write(dir, "cpuset.cpus", &limits.cpuset_cpus)?;
    }
    if !limits.cpuset_mems.is_empty() {
        write(dir, "cpuset.mems", &limits.cpuset_mems)?;
    }
    for (size, limit) in &limits.hugepages {
        let file = format!("hugetlb.{size}.max");
        // the kubelet sends a zero limit for every page size the node
        // has; without the controller there is nothing to limit
        if *limit == 0 && !dir.join(&file).exists() {
            continue;
        }
        write(dir, &file, &limit.to_string())?;
    }
    for (file, value) in &limits.unified {
        if file.contains('/') || file.starts_with('.') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{file} is not a cgroup file"),
            ));
        }
        write(dir, file, value)?;
    }
    Ok(())
}

pub fn add(dir: &Path, pid: i32) -> io::Result<()> {
    write(dir, "cgroup.procs", &pid.to_string())
}

/// Whether the kernel killed a process of the cgroup for lack of memory.
pub fn oom_killed(dir: &Path) -> bool {
    field(dir, "memory.events", "oom_kill").is_some_and(|n| n > 0)
}

pub fn remove(dir: &Path) {
    let _ = fs::remove_dir(dir);
}

fn field(dir: &Path, file: &str, name: &str) -> Option<u64> {
    fs::read_to_string(dir.join(file))
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
}

fn single(dir: &Path, file: &str) -> Option<u64> {
    fs::read_to_string(dir.join(file)).ok()?.trim().parse().ok()
}

pub fn usage(dir: &Path) -> Option<Usage> {
    let memory = single(dir, "memory.current")?;
    let inactive = field(dir, "memory.stat", "inactive_file").unwrap_or(0);
    Some(Usage {
        cpu_ns: field(dir, "cpu.stat", "usage_usec").unwrap_or(0) * 1000,
        memory,
        // what the kubelet evicts by: usage less what the kernel can drop
        working_set: memory.saturating_sub(inactive),
        rss: field(dir, "memory.stat", "anon").unwrap_or(0),
        page_faults: field(dir, "memory.stat", "pgfault").unwrap_or(0),
        major_page_faults: field(dir, "memory.stat", "pgmajfault").unwrap_or(0),
        processes: single(dir, "pids.current").unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_convert_as_the_kubelet_converts_them() {
        assert_eq!(weight(2), 1);
        assert_eq!(weight(1024), 39);
        assert_eq!(weight(262_144), 10_000);
        assert!(container_dir("kubepods-burstable.slice", "x").is_err());
        assert!(container_dir("/kubepods/../..", "x").is_err());
        assert_eq!(
            container_dir("/kubepods/pod1", "c").unwrap(),
            Path::new("/sys/fs/cgroup/kubepods/pod1/c")
        );
    }
}
