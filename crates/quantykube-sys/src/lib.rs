//! The syscalls the node runtime needs and the standard library does not
//! wrap. Every `unsafe` block of QuantyKube is in this file (ADR-013).
//!
//! The functions are declared by hand against glibc, which every Rust
//! program on Linux already links. Linux on x86_64 only.

use std::ffi::{c_char, c_int, c_long, c_uint, c_ulong, c_void, CString};
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
    fn mkdir(path: *const c_char, mode: c_uint) -> c_int;
    fn symlink(target: *const c_char, path: *const c_char) -> c_int;
    fn umask(mask: c_uint) -> c_uint;
    fn sethostname(name: *const c_char, len: usize) -> c_int;
    fn prctl(option: c_int, arg2: c_ulong, arg3: c_ulong, arg4: c_ulong, arg5: c_ulong) -> c_int;
    fn pipe2(fds: *mut c_int, flags: c_int) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, n: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, n: usize) -> isize;
    fn close(fd: c_int) -> c_int;
    fn execve(path: *const c_char, argv: *const *const c_char, envp: *const *const c_char)
        -> c_int;
    fn waitpid(pid: c_int, status: *mut c_int, options: c_int) -> c_int;
    fn __errno_location() -> *mut c_int;
    fn _exit(code: c_int) -> !;
    fn mallopt(param: c_int, value: c_int) -> c_int;
}

const SYS_CLONE: c_long = 56;
const SYS_CAPSET: c_long = 126;
const SYS_PIVOT_ROOT: c_long = 155;
const SYS_MKNODAT: c_long = 259;
const AT_FDCWD: c_long = -100;
const SIGCHLD: c_ulong = 17;
const O_CLOEXEC: c_int = 0o2000000;
const ENOENT: c_int = 2;

const CLONE_NEWNS: c_ulong = 0x0002_0000;
const CLONE_NEWUTS: c_ulong = 0x0400_0000;
const CLONE_NEWIPC: c_ulong = 0x0800_0000;
const CLONE_NEWPID: c_ulong = 0x2000_0000;
const CLONE_NEWNET: c_ulong = 0x4000_0000;

const MS_RDONLY: c_ulong = 1;
const MS_NOSUID: c_ulong = 2;
const MS_NODEV: c_ulong = 4;
const MS_NOEXEC: c_ulong = 8;
const MS_REMOUNT: c_ulong = 32;
const MS_BIND: c_ulong = 4096;
const MS_REC: c_ulong = 16384;
const MS_PRIVATE: c_ulong = 1 << 18;
const MS_STRICTATIME: c_ulong = 1 << 24;
const MNT_DETACH: c_int = 2;

const PR_SET_SECCOMP: c_int = 22;
const SECCOMP_MODE_FILTER: c_ulong = 2;
const PR_CAPBSET_DROP: c_int = 24;
const PR_SET_NO_NEW_PRIVS: c_int = 38;
const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;

/// The capabilities Docker and containerd leave a container by default:
/// CHOWN, DAC_OVERRIDE, FOWNER, FSETID, KILL, SETGID, SETUID, SETPCAP,
/// NET_BIND_SERVICE, NET_RAW, SYS_CHROOT, MKNOD, AUDIT_WRITE, SETFCAP.
pub const DEFAULT_CAPS: u64 = (1 << 0)
    | (1 << 1)
    | (1 << 3)
    | (1 << 4)
    | (1 << 5)
    | (1 << 6)
    | (1 << 7)
    | (1 << 8)
    | (1 << 10)
    | (1 << 13)
    | (1 << 18)
    | (1 << 27)
    | (1 << 29)
    | (1 << 31);

/// Character devices every container gets: path, major, minor.
const DEVICES: [(&std::ffi::CStr, u64, u64); 6] = [
    (c"/dev/null", 1, 3),
    (c"/dev/zero", 1, 5),
    (c"/dev/full", 1, 7),
    (c"/dev/random", 1, 8),
    (c"/dev/urandom", 1, 9),
    (c"/dev/tty", 5, 0),
];

const DEV_LINKS: [(&std::ffi::CStr, &std::ffi::CStr); 5] = [
    (c"/proc/self/fd", c"/dev/fd"),
    (c"/proc/self/fd/0", c"/dev/stdin"),
    (c"/proc/self/fd/1", c"/dev/stdout"),
    (c"/proc/self/fd/2", c"/dev/stderr"),
    (c"pts/ptmx", c"/dev/ptmx"),
];

/// Hidden from the container, as runc hides them by default: kernel
/// memory, key and timer state, and firmware interfaces.
const MASKED: [&std::ffi::CStr; 11] = [
    c"/proc/asound",
    c"/proc/acpi",
    c"/proc/kcore",
    c"/proc/keys",
    c"/proc/latency_stats",
    c"/proc/timer_list",
    c"/proc/timer_stats",
    c"/proc/sched_debug",
    c"/proc/scsi",
    c"/sys/firmware",
    c"/sys/devices/virtual/powercap",
];

const READ_ONLY: [&std::ffi::CStr; 5] = [
    c"/proc/bus",
    c"/proc/fs",
    c"/proc/irq",
    c"/proc/sys",
    c"/proc/sysrq-trigger",
];

/// Syscalls refused with EPERM, the ones Docker's default profile refuses
/// that lead out of a container or into the kernel: modules, kexec,
/// reboot, mounts and namespaces, tracing other processes, BPF, keyrings,
/// the clock, swap, NUMA policy and raw I/O ports. Docker's profile is an
/// allowlist of about 350 calls; this is the part of it that matters for
/// escaping, as a denylist, until the allowlist is worth its size.
pub const DENIED: [(&str, u32); 47] = [
    ("acct", 163),
    ("add_key", 248),
    ("bpf", 321),
    ("clock_adjtime", 305),
    ("clock_settime", 227),
    ("create_module", 174),
    ("delete_module", 176),
    ("finit_module", 313),
    ("get_kernel_syms", 177),
    ("get_mempolicy", 239),
    ("init_module", 175),
    ("ioperm", 173),
    ("iopl", 172),
    ("kcmp", 312),
    ("kexec_file_load", 320),
    ("kexec_load", 246),
    ("keyctl", 250),
    ("lookup_dcookie", 212),
    ("mbind", 237),
    ("mount", 165),
    ("move_mount", 429),
    ("fsopen", 430),
    ("fsconfig", 431),
    ("fsmount", 432),
    ("fspick", 433),
    ("open_tree", 428),
    ("move_pages", 279),
    ("name_to_handle_at", 303),
    ("nfsservctl", 180),
    ("open_by_handle_at", 304),
    ("perf_event_open", 298),
    ("pivot_root", 155),
    ("process_vm_readv", 310),
    ("process_vm_writev", 311),
    ("ptrace", 101),
    ("query_module", 178),
    ("quotactl", 179),
    ("reboot", 169),
    ("request_key", 249),
    ("set_mempolicy", 238),
    ("setns", 308),
    ("settimeofday", 164),
    ("swapon", 167),
    ("swapoff", 168),
    ("syslog", 103),
    ("umount2", 166),
    ("unshare", 272),
];

const SYS_CLONE_NR: u32 = 56;
const SYS_CLONE3_NR: u32 = 435;
const AUDIT_ARCH_X86_64: u32 = 0xc000_003e;
const X32_BIT: u32 = 0x4000_0000;
const NAMESPACE_FLAGS: u32 = 0x7e02_0080;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SockFilter {
    code: u16,
    jt: u8,
    jf: u8,
    k: u32,
}

#[repr(C)]
struct SockFprog {
    len: u16,
    filter: *const SockFilter,
}

const BPF_LD_W_ABS: u16 = 0x20;
const BPF_JEQ_K: u16 = 0x15;
const BPF_JSET_K: u16 = 0x45;
const BPF_RET_K: u16 = 0x06;
const RET_ALLOW: u32 = 0x7fff_0000;
const RET_KILL_PROCESS: u32 = 0x8000_0000;
const RET_ERRNO: u32 = 0x0005_0000;
const EPERM: u32 = 1;
const ENOSYS: u32 = 38;

fn stmt(code: u16, k: u32) -> SockFilter {
    SockFilter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

fn jump(code: u16, k: u32, jt: u8, jf: u8) -> SockFilter {
    SockFilter { code, jt, jf, k }
}

/// The seccomp program: the wrong architecture or the x32 ABI kills,
/// clone with namespace flags and the denied calls get EPERM, clone3
/// gets ENOSYS so libc falls back to clone, where the flags can be seen
/// (clone3 passes them in memory a filter cannot read), and the rest is
/// allowed. Offsets in seccomp_data: nr at 0, arch at 4, args from 16.
fn seccomp_program() -> Vec<SockFilter> {
    let mut p = vec![
        stmt(BPF_LD_W_ABS, 4),
        jump(BPF_JEQ_K, AUDIT_ARCH_X86_64, 1, 0),
        stmt(BPF_RET_K, RET_KILL_PROCESS),
        stmt(BPF_LD_W_ABS, 0),
        jump(BPF_JSET_K, X32_BIT, 0, 1),
        stmt(BPF_RET_K, RET_KILL_PROCESS),
        jump(BPF_JEQ_K, SYS_CLONE3_NR, 0, 1),
        stmt(BPF_RET_K, RET_ERRNO | ENOSYS),
        jump(BPF_JEQ_K, SYS_CLONE_NR, 0, 3),
        stmt(BPF_LD_W_ABS, 16),
        jump(BPF_JSET_K, NAMESPACE_FLAGS, 0, 1),
        stmt(BPF_RET_K, RET_ERRNO | EPERM),
    ];
    // the clone branch reloaded the accumulator, so the denylist starts
    // with the syscall number again
    p.push(stmt(BPF_LD_W_ABS, 0));
    let n = DENIED.len();
    for (i, (_, nr)) in DENIED.iter().enumerate() {
        // jump over the remaining checks and the allow to the EPERM return
        p.push(jump(BPF_JEQ_K, *nr, (n - i) as u8, 0));
    }
    p.push(stmt(BPF_RET_K, RET_ALLOW));
    p.push(stmt(BPF_RET_K, RET_ERRNO | EPERM));
    p
}

/// Everything the child needs, prepared before the clone. Between clone
/// and exec the child may not allocate: another thread of the parent may
/// have held the allocator's lock at the moment of the clone, and the
/// child would wait for it forever.
pub struct Spawn {
    root: CString,
    hostname: Option<CString>,
    readonly: bool,
    caps: u64,
    seccomp: Vec<SockFilter>,
    path: CString,
    _argv: Vec<CString>,
    argv_ptrs: Vec<*const c_char>,
    _env: Vec<CString>,
    env_ptrs: Vec<*const c_char>,
}

/// Where the child failed, reported as its exit status. Above anything a
/// program usually exits with, below the 128 + signal range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Release = 100,
    MakePrivate = 101,
    BindRoot = 102,
    EnterRoot = 103,
    PivotRoot = 104,
    DetachOld = 105,
    MountProc = 106,
    MountSys = 107,
    MountDev = 108,
    Devices = 109,
    MaskPaths = 110,
    Hostname = 111,
    ReadOnly = 112,
    Capabilities = 113,
    Seccomp = 114,
    Exec = 115,
}

impl Step {
    pub fn from_status(code: i32) -> Option<Step> {
        use Step::*;
        [
            Release,
            MakePrivate,
            BindRoot,
            EnterRoot,
            PivotRoot,
            DetachOld,
            MountProc,
            MountSys,
            MountDev,
            Devices,
            MaskPaths,
            Hostname,
            ReadOnly,
            Capabilities,
            Seccomp,
            Exec,
        ]
        .into_iter()
        .find(|s| *s as i32 == code)
    }
}

fn cstr(s: &[u8]) -> io::Result<CString> {
    CString::new(s).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in argument"))
}

/// A child that is cloned and set up to the point of waiting for
/// [`Started::release`], so the caller can put it into a cgroup before
/// any of its own code runs.
pub struct Started {
    pub pid: i32,
    release: c_int,
}

impl Started {
    pub fn release(mut self) -> io::Result<()> {
        self.signal()
    }

    fn signal(&mut self) -> io::Result<()> {
        if self.release < 0 {
            return Ok(());
        }
        let byte = 1u8;
        // SAFETY: writes one byte from a live local to a pipe this struct
        // owns, then closes it exactly once.
        let n = unsafe { write(self.release, (&byte as *const u8).cast(), 1) };
        unsafe { close(self.release) };
        self.release = -1;
        if n == 1 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        let _ = self.signal();
    }
}

impl Spawn {
    /// `root` is the container's root filesystem, `argv[0]` a path inside
    /// it.
    pub fn new(root: &[u8], argv: &[&[u8]], env: &[&[u8]]) -> io::Result<Spawn> {
        let first = *argv
            .first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty argv"))?;
        let argv: Vec<CString> = argv.iter().map(|a| cstr(a)).collect::<io::Result<_>>()?;
        let env: Vec<CString> = env.iter().map(|a| cstr(a)).collect::<io::Result<_>>()?;
        let mut argv_ptrs: Vec<*const c_char> = argv.iter().map(|a| a.as_ptr()).collect();
        argv_ptrs.push(std::ptr::null());
        let mut env_ptrs: Vec<*const c_char> = env.iter().map(|a| a.as_ptr()).collect();
        env_ptrs.push(std::ptr::null());
        Ok(Spawn {
            root: cstr(root)?,
            hostname: None,
            readonly: false,
            caps: DEFAULT_CAPS,
            seccomp: seccomp_program(),
            path: cstr(first)?,
            _argv: argv,
            argv_ptrs,
            _env: env,
            env_ptrs,
        })
    }

    pub fn hostname(mut self, name: &[u8]) -> io::Result<Self> {
        self.hostname = Some(cstr(name)?);
        Ok(self)
    }

    pub fn readonly(mut self, yes: bool) -> Self {
        self.readonly = yes;
        self
    }

    /// The capability set the command keeps; [`DEFAULT_CAPS`] unless set.
    pub fn capabilities(mut self, caps: u64) -> Self {
        self.caps = caps;
        self
    }

    /// Clones into new namespaces. The child waits until the returned
    /// handle is released or dropped, then sets up and execs.
    pub fn start(&self) -> io::Result<Started> {
        let mut fds = [0 as c_int; 2];
        // SAFETY: fds is a valid two element array.
        if unsafe { pipe2(fds.as_mut_ptr(), O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let flags =
            CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWNET | SIGCHLD;
        // SAFETY: a raw clone without CLONE_VM is a fork into the given
        // namespaces. The child runs only `child`, which makes syscalls on
        // memory prepared above and ends in execve or _exit.
        let pid = unsafe { syscall(SYS_CLONE, flags, 0usize, 0usize, 0usize, 0usize) };
        match pid {
            -1 => {
                let e = io::Error::last_os_error();
                unsafe {
                    close(fds[0]);
                    close(fds[1]);
                }
                Err(e)
            }
            0 => self.child(fds[0]),
            pid => {
                unsafe { close(fds[0]) };
                Ok(Started {
                    pid: pid as i32,
                    release: fds[1],
                })
            }
        }
    }

    fn child(&self, wait: c_int) -> ! {
        let fail = |step: Step| -> ! {
            // SAFETY: _exit is async-signal-safe and does not return.
            unsafe { _exit(step as c_int) }
        };
        let null = std::ptr::null();
        let nodata = null as *const c_void;
        // SAFETY: every pointer is null where the call allows it or points
        // into a CString or a static C string that outlives the call; the
        // child's copy of the parent's memory holds all of them.
        unsafe {
            let mut byte = 0u8;
            if read(wait, (&mut byte as *mut u8).cast(), 1) != 1 {
                fail(Step::Release);
            }
            if mount(null, c"/".as_ptr(), null, MS_REC | MS_PRIVATE, nodata) != 0 {
                fail(Step::MakePrivate);
            }
            let root = self.root.as_ptr();
            if mount(root, root, null, MS_BIND | MS_REC, nodata) != 0 {
                fail(Step::BindRoot);
            }
            if chdir(root) != 0 {
                fail(Step::EnterRoot);
            }
            // pivot_root(".", ".") stacks the old root under the new one,
            // and detaching "." then removes it: no directory to create
            let dot = c".".as_ptr();
            if syscall(SYS_PIVOT_ROOT, dot, dot) != 0 {
                fail(Step::PivotRoot);
            }
            if umount2(dot, MNT_DETACH) != 0 {
                fail(Step::DetachOld);
            }
            if chdir(c"/".as_ptr()) != 0 {
                fail(Step::EnterRoot);
            }
            let hardened = MS_NOSUID | MS_NODEV | MS_NOEXEC;
            if mount(
                c"proc".as_ptr(),
                c"/proc".as_ptr(),
                c"proc".as_ptr(),
                hardened,
                nodata,
            ) != 0
            {
                fail(Step::MountProc);
            }
            if mount(
                c"sysfs".as_ptr(),
                c"/sys".as_ptr(),
                c"sysfs".as_ptr(),
                hardened | MS_RDONLY,
                nodata,
            ) != 0
            {
                fail(Step::MountSys);
            }
            self.make_dev();
            for p in MASKED {
                if mount(c"/dev/null".as_ptr(), p.as_ptr(), null, MS_BIND, nodata) != 0
                    && mount(
                        c"tmpfs".as_ptr(),
                        p.as_ptr(),
                        c"tmpfs".as_ptr(),
                        MS_RDONLY,
                        nodata,
                    ) != 0
                    && *__errno_location() != ENOENT
                {
                    fail(Step::MaskPaths);
                }
            }
            for p in READ_ONLY {
                if mount(p.as_ptr(), p.as_ptr(), null, MS_BIND | MS_REC, nodata) != 0 {
                    if *__errno_location() == ENOENT {
                        continue;
                    }
                    fail(Step::MaskPaths);
                }
                let ro = MS_BIND | MS_REC | MS_REMOUNT | MS_RDONLY;
                if mount(p.as_ptr(), p.as_ptr(), null, ro, nodata) != 0 {
                    fail(Step::MaskPaths);
                }
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
                    nodata,
                ) != 0
            {
                fail(Step::ReadOnly);
            }
            self.drop_capabilities();
            // last: the filter refuses mount and pivot_root, used above
            let prog = SockFprog {
                len: self.seccomp.len() as u16,
                filter: self.seccomp.as_ptr(),
            };
            if prctl(
                PR_SET_SECCOMP,
                SECCOMP_MODE_FILTER,
                &prog as *const SockFprog as c_ulong,
                0,
                0,
            ) != 0
            {
                fail(Step::Seccomp);
            }
            execve(
                self.path.as_ptr(),
                self.argv_ptrs.as_ptr(),
                self.env_ptrs.as_ptr(),
            );
        }
        fail(Step::Exec)
    }

    /// A fresh tmpfs on /dev with only the standard devices, so nothing of
    /// the host's /dev is reachable.
    unsafe fn make_dev(&self) {
        let fail = |step: Step| -> ! { _exit(step as c_int) };
        let nodata = std::ptr::null::<c_void>();
        if mount(
            c"tmpfs".as_ptr(),
            c"/dev".as_ptr(),
            c"tmpfs".as_ptr(),
            MS_NOSUID | MS_STRICTATIME,
            c"mode=755,size=65536k".as_ptr().cast(),
        ) != 0
        {
            fail(Step::MountDev);
        }
        umask(0);
        for (path, major, minor) in DEVICES {
            let dev = (major << 8) | minor;
            if syscall(
                SYS_MKNODAT,
                AT_FDCWD,
                path.as_ptr(),
                0o020666 as c_uint,
                dev,
            ) != 0
            {
                fail(Step::Devices);
            }
        }
        for (target, link) in DEV_LINKS {
            if symlink(target.as_ptr(), link.as_ptr()) != 0 {
                fail(Step::Devices);
            }
        }
        if mkdir(c"/dev/pts".as_ptr(), 0o755) != 0 || mkdir(c"/dev/shm".as_ptr(), 0o1777) != 0 {
            fail(Step::Devices);
        }
        if mount(
            c"devpts".as_ptr(),
            c"/dev/pts".as_ptr(),
            c"devpts".as_ptr(),
            MS_NOSUID | MS_NOEXEC,
            c"newinstance,ptmxmode=0666,mode=0620".as_ptr().cast(),
        ) != 0
            || mount(
                c"shm".as_ptr(),
                c"/dev/shm".as_ptr(),
                c"tmpfs".as_ptr(),
                MS_NOSUID | MS_NODEV | MS_NOEXEC,
                c"mode=1777,size=65536k".as_ptr().cast(),
            ) != 0
        {
            fail(Step::MountDev);
        }
        umask(0o022);
        let _ = nodata;
    }

    /// Bounding set first, which needs CAP_SETPCAP still held, then the
    /// sets the command starts with, then no_new_privs so a setuid binary
    /// cannot gain back what was dropped.
    unsafe fn drop_capabilities(&self) {
        let fail = || -> ! { _exit(Step::Capabilities as c_int) };
        for cap in 0..64u64 {
            if self.caps & (1 << cap) == 0
                && prctl(PR_CAPBSET_DROP, cap as c_ulong, 0, 0, 0) != 0
                && *__errno_location() != 22
            {
                fail();
            }
        }
        let header = [LINUX_CAPABILITY_VERSION_3, 0u32];
        let (lo, hi) = (self.caps as u32, (self.caps >> 32) as u32);
        // effective, permitted, inheritable; low 32 bits, then high
        let data = [lo, lo, lo, hi, hi, hi];
        if syscall(SYS_CAPSET, header.as_ptr(), data.as_ptr()) != 0 {
            fail();
        }
        if prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            fail();
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_name_the_failed_step() {
        assert_eq!(Step::from_status(104), Some(Step::PivotRoot));
        assert_eq!(Step::from_status(0), None);
        assert_eq!(describe(Exit::Code(115)), "setup failed at Exec");
    }

    #[test]
    fn denied_syscall_numbers_match_the_kernel_headers() {
        let Ok(h) = std::fs::read_to_string("/usr/include/x86_64-linux-gnu/asm/unistd_64.h") else {
            eprintln!("no kernel headers, not checked");
            return;
        };
        for (name, nr) in DENIED
            .iter()
            .chain(&[("clone", SYS_CLONE_NR), ("clone3", SYS_CLONE3_NR)])
        {
            let line = format!("#define __NR_{name} {nr}");
            assert!(h.lines().any(|l| l.trim() == line), "{name} is not {nr}");
        }
    }

    #[test]
    fn every_seccomp_jump_lands_inside_the_program() {
        let p = seccomp_program();
        assert!(p.len() < 4096, "the kernel's limit");
        for (i, ins) in p.iter().enumerate() {
            if ins.code == BPF_JEQ_K || ins.code == BPF_JSET_K {
                for off in [ins.jt, ins.jf] {
                    assert!(
                        i + 1 + (off as usize) < p.len(),
                        "jump at {i} leaves the program"
                    );
                }
            }
        }
        assert_eq!(p[p.len() - 1], stmt(BPF_RET_K, RET_ERRNO | EPERM));
        assert_eq!(p[p.len() - 2], stmt(BPF_RET_K, RET_ALLOW));
    }

    #[test]
    fn the_default_capabilities_are_dockers_fourteen() {
        assert_eq!(DEFAULT_CAPS.count_ones(), 14);
        assert_eq!(DEFAULT_CAPS, 0xa80425fb);
    }
}
