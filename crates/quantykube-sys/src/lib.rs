//! The syscalls the node runtime needs and the standard library does not
//! wrap. Every `unsafe` block of QuantyKube is in this file (ADR-013).
//!
//! The functions are declared by hand against glibc, which every Rust
//! program on Linux already links. Linux only.

use std::ffi::{c_char, c_int, c_long, c_ulong, c_void, CString};
use std::io;

extern "C" {
    fn syscall(number: c_long, ...) -> c_long;
    fn mount(
        source: *const c_char,
        target: *const c_char,
        fstype: *const c_char,
        flags: c_ulong,
        data: *const c_void,
    ) -> c_int;
    fn umount2(target: *const c_char, flags: c_int) -> c_int;
    fn chdir(path: *const c_char) -> c_int;
    fn sethostname(name: *const c_char, len: usize) -> c_int;
    fn execve(path: *const c_char, argv: *const *const c_char, envp: *const *const c_char)
        -> c_int;
    fn waitpid(pid: c_int, status: *mut c_int, options: c_int) -> c_int;
    fn _exit(code: c_int) -> !;
    fn mallopt(param: c_int, value: c_int) -> c_int;
}

const SYS_CLONE: c_long = 56;
const SYS_PIVOT_ROOT: c_long = 155;
const SIGCHLD: c_ulong = 17;

pub const CLONE_NEWNS: c_ulong = 0x0002_0000;
pub const CLONE_NEWUTS: c_ulong = 0x0400_0000;
pub const CLONE_NEWIPC: c_ulong = 0x0800_0000;
pub const CLONE_NEWPID: c_ulong = 0x2000_0000;
pub const CLONE_NEWNET: c_ulong = 0x4000_0000;

const MS_RDONLY: c_ulong = 1;
const MS_NOSUID: c_ulong = 2;
const MS_NODEV: c_ulong = 4;
const MS_NOEXEC: c_ulong = 8;
const MS_REMOUNT: c_ulong = 32;
const MS_BIND: c_ulong = 4096;
const MS_REC: c_ulong = 16384;
const MS_PRIVATE: c_ulong = 1 << 18;
const MNT_DETACH: c_int = 2;

/// Everything the child needs, prepared before the clone. Between clone
/// and exec the child may not allocate: another thread of the parent may
/// have held the allocator's lock at the moment of the clone, and the
/// child would wait for it forever.
pub struct Spawn {
    namespaces: c_ulong,
    root: CString,
    proc_dir: CString,
    hostname: Option<CString>,
    readonly: bool,
    path: CString,
    argv: Vec<CString>,
    argv_ptrs: Vec<*const c_char>,
    env: Vec<CString>,
    env_ptrs: Vec<*const c_char>,
}

/// Where the child failed, reported as its exit status. Above anything a
/// program usually exits with, below the 128 + signal range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    MakePrivate = 101,
    BindRoot = 102,
    MountProc = 103,
    EnterRoot = 104,
    PivotRoot = 105,
    DetachOld = 106,
    Hostname = 107,
    ReadOnly = 108,
    Exec = 109,
}

impl Step {
    pub fn from_status(code: i32) -> Option<Step> {
        use Step::*;
        [
            MakePrivate,
            BindRoot,
            MountProc,
            EnterRoot,
            PivotRoot,
            DetachOld,
            Hostname,
            ReadOnly,
            Exec,
        ]
        .into_iter()
        .find(|s| *s as i32 == code)
    }
}

fn cstr(s: &[u8]) -> io::Result<CString> {
    CString::new(s).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in argument"))
}

impl Spawn {
    /// `root` is the container's root filesystem, `argv[0]` a path inside
    /// it.
    pub fn new(root: &[u8], argv: &[&[u8]], env: &[&[u8]]) -> io::Result<Spawn> {
        let first = *argv
            .first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty argv"))?;
        let mut proc_dir = root.to_vec();
        proc_dir.extend_from_slice(b"/proc");
        let argv: Vec<CString> = argv.iter().map(|a| cstr(a)).collect::<io::Result<_>>()?;
        let env: Vec<CString> = env.iter().map(|a| cstr(a)).collect::<io::Result<_>>()?;
        let mut s = Spawn {
            namespaces: CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWNET,
            root: cstr(root)?,
            proc_dir: cstr(&proc_dir)?,
            hostname: None,
            readonly: false,
            path: cstr(first)?,
            argv,
            argv_ptrs: Vec::new(),
            env,
            env_ptrs: Vec::new(),
        };
        s.argv_ptrs = s.argv.iter().map(|a| a.as_ptr()).collect();
        s.argv_ptrs.push(std::ptr::null());
        s.env_ptrs = s.env.iter().map(|a| a.as_ptr()).collect();
        s.env_ptrs.push(std::ptr::null());
        Ok(s)
    }

    pub fn hostname(mut self, name: &[u8]) -> io::Result<Self> {
        self.hostname = Some(cstr(name)?);
        Ok(self)
    }

    pub fn readonly(mut self, yes: bool) -> Self {
        self.readonly = yes;
        self
    }

    /// Clone into new namespaces and exec. Returns the child's pid as seen
    /// from here.
    pub fn start(&self) -> io::Result<i32> {
        // SAFETY: a raw clone without CLONE_VM is a fork into the given
        // namespaces. The child runs only `child`, which makes syscalls on
        // pointers prepared above and ends in execve or _exit.
        let pid = unsafe {
            syscall(
                SYS_CLONE,
                self.namespaces | SIGCHLD,
                0usize,
                0usize,
                0usize,
                0usize,
            )
        };
        match pid {
            -1 => Err(io::Error::last_os_error()),
            0 => self.child(),
            pid => Ok(pid as i32),
        }
    }

    fn child(&self) -> ! {
        let fail = |step: Step| -> ! {
            // SAFETY: _exit is async-signal-safe and does not return.
            unsafe { _exit(step as c_int) }
        };
        let null = std::ptr::null();
        let dot = c".".as_ptr();
        // SAFETY: every pointer is either null where the call allows it or
        // points into a CString owned by `self`, which the parent keeps
        // alive and the child's copy of memory still holds.
        unsafe {
            if mount(
                null,
                c"/".as_ptr(),
                null,
                MS_REC | MS_PRIVATE,
                null as *const c_void,
            ) != 0
            {
                fail(Step::MakePrivate);
            }
            let root = self.root.as_ptr();
            if mount(root, root, null, MS_BIND | MS_REC, null as *const c_void) != 0 {
                fail(Step::BindRoot);
            }
            if mount(
                c"proc".as_ptr(),
                self.proc_dir.as_ptr(),
                c"proc".as_ptr(),
                MS_NOSUID | MS_NODEV | MS_NOEXEC,
                null as *const c_void,
            ) != 0
            {
                fail(Step::MountProc);
            }
            if chdir(root) != 0 {
                fail(Step::EnterRoot);
            }
            // pivot_root(".", ".") stacks the old root under the new one,
            // and detaching "." then removes it: no directory to create
            if syscall(SYS_PIVOT_ROOT, dot, dot) != 0 {
                fail(Step::PivotRoot);
            }
            if umount2(dot, MNT_DETACH) != 0 {
                fail(Step::DetachOld);
            }
            if chdir(c"/".as_ptr()) != 0 {
                fail(Step::EnterRoot);
            }
            if let Some(h) = &self.hostname {
                if sethostname(h.as_ptr(), h.as_bytes().len()) != 0 {
                    fail(Step::Hostname);
                }
            }
            if self.readonly
                && mount(
                    null,
                    c"/".as_ptr(),
                    null,
                    MS_REMOUNT | MS_BIND | MS_RDONLY,
                    null as *const c_void,
                ) != 0
            {
                fail(Step::ReadOnly);
            }
            execve(
                self.path.as_ptr(),
                self.argv_ptrs.as_ptr(),
                self.env_ptrs.as_ptr(),
            );
        }
        fail(Step::Exec)
    }
}

/// Caps glibc's malloc at `n` arenas. By default it creates up to eight
/// per core as threads contend, and memory freed in one arena is not
/// reused by threads allocating in another: a server whose calls hop
/// between many threads grows for as long as it runs. Call before
/// spawning threads.
pub fn limit_malloc_arenas(n: i32) -> bool {
    const M_ARENA_MAX: c_int = -8;
    // SAFETY: mallopt takes two integers and only changes allocator
    // tuning; glibc documents it as callable at any time.
    unsafe { mallopt(M_ARENA_MAX, n) == 1 }
}

/// How a child ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Code(i32),
    Signal(i32),
}

pub fn wait(pid: i32) -> io::Result<Exit> {
    let mut status: c_int = 0;
    loop {
        // SAFETY: status is a valid out pointer.
        let r = unsafe { waitpid(pid, &mut status, 0) };
        if r == pid {
            break;
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
    Ok(if status & 0x7f == 0 {
        Exit::Code((status >> 8) & 0xff)
    } else {
        Exit::Signal(status & 0x7f)
    })
}

pub fn describe(e: Exit) -> String {
    match e {
        Exit::Code(c) => match Step::from_status(c) {
            Some(step) => format!("setup failed at {step:?}"),
            None => format!("exited with {c}"),
        },
        Exit::Signal(s) => format!("killed by signal {s}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_name_the_failed_step() {
        assert_eq!(Step::from_status(105), Some(Step::PivotRoot));
        assert_eq!(Step::from_status(0), None);
        assert_eq!(describe(Exit::Code(109)), "setup failed at Exec");
    }
}
