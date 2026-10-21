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
    fn setns(fd: c_int, nstype: c_int) -> c_int;
    fn dup2(old: c_int, new: c_int) -> c_int;
    fn kill(pid: c_int, sig: c_int) -> c_int;
    fn open(path: *const c_char, flags: c_int, ...) -> c_int;
    fn unshare(flags: c_int) -> c_int;
    fn pause() -> c_int;
    fn fchdir(fd: c_int) -> c_int;
    fn chroot(path: *const c_char) -> c_int;
    fn socket(domain: c_int, kind: c_int, protocol: c_int) -> c_int;
    fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
}

const SYS_CLONE: c_long = 56;
const SYS_CAPSET: c_long = 126;
// raw, because glibc's wrappers would try to signal threads that exist
// only in the parent
const SYS_SETRESUID: c_long = 117;
const SYS_SETRESGID: c_long = 119;
const SYS_SETGROUPS: c_long = 116;
const SYS_PIVOT_ROOT: c_long = 155;
const SYS_MKNODAT: c_long = 259;
const SYS_OPEN_TREE: c_long = 428;
const SYS_CLOSE_RANGE: c_long = 436;
const CLOSE_RANGE_CLOEXEC: c_uint = 4;
const SYS_MOVE_MOUNT: c_long = 429;
const OPEN_TREE_CLONE: c_uint = 1;
const AT_RECURSIVE: c_uint = 0x8000;
const MOVE_MOUNT_F_EMPTY_PATH: c_uint = 4;
const AT_FDCWD: c_long = -100;
const SIGCHLD: c_ulong = 17;
const O_CLOEXEC: c_int = 0o2000000;
const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 0o100;
const ENOENT: c_int = 2;
const EEXIST: c_int = 17;
const EINTR: c_int = 4;

const CLONE_NEWNS: c_ulong = 0x0002_0000;
const CLONE_NEWCGROUP: c_ulong = 0x0200_0000;
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

const PR_SET_PDEATHSIG: c_int = 1;
const SIOCGIFFLAGS: c_ulong = 0x8913;
const SIOCSIFFLAGS: c_ulong = 0x8914;
const IFF_UP: i16 = 1;
const PR_SET_SECCOMP: c_int = 22;
const SECCOMP_MODE_FILTER: c_ulong = 2;
const PR_CAPBSET_DROP: c_int = 24;
const PR_SET_NO_NEW_PRIVS: c_int = 38;
const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;

/// The capabilities Docker and containerd leave a container by default,
/// except MKNOD: CHOWN, DAC_OVERRIDE, FOWNER, FSETID, KILL, SETGID,
/// SETUID, SETPCAP, NET_BIND_SERVICE, NET_RAW, SYS_CHROOT, AUDIT_WRITE,
/// SETFCAP. They keep MKNOD because a device cgroup refuses access to
/// any node made with it; without that filter here, MKNOD would let a
/// container make a node for the host's disk and open it.
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

/// The calls a container may make without condition: Docker's default
/// seccomp profile (moby/profiles, seccomp/default.json as of commit
/// 6fe7deb1b9fb) on x86_64, for the fourteen default capabilities, by
/// number. Anything not here is refused with EPERM, as Docker refuses it.
/// clone, clone3, socket and personality are checked apart, on their
/// arguments, in [`seccomp_program`].
pub const ALLOWED: [(&str, u32); 306] = [
    ("read", 0),
    ("write", 1),
    ("open", 2),
    ("close", 3),
    ("stat", 4),
    ("fstat", 5),
    ("lstat", 6),
    ("poll", 7),
    ("lseek", 8),
    ("mmap", 9),
    ("mprotect", 10),
    ("munmap", 11),
    ("brk", 12),
    ("rt_sigaction", 13),
    ("rt_sigprocmask", 14),
    ("rt_sigreturn", 15),
    ("ioctl", 16),
    ("pread64", 17),
    ("pwrite64", 18),
    ("readv", 19),
    ("writev", 20),
    ("access", 21),
    ("pipe", 22),
    ("select", 23),
    ("sched_yield", 24),
    ("mremap", 25),
    ("msync", 26),
    ("mincore", 27),
    ("madvise", 28),
    ("shmget", 29),
    ("shmat", 30),
    ("shmctl", 31),
    ("dup", 32),
    ("dup2", 33),
    ("pause", 34),
    ("nanosleep", 35),
    ("getitimer", 36),
    ("alarm", 37),
    ("setitimer", 38),
    ("getpid", 39),
    ("sendfile", 40),
    ("connect", 42),
    ("accept", 43),
    ("sendto", 44),
    ("recvfrom", 45),
    ("sendmsg", 46),
    ("recvmsg", 47),
    ("shutdown", 48),
    ("bind", 49),
    ("listen", 50),
    ("getsockname", 51),
    ("getpeername", 52),
    ("socketpair", 53),
    ("setsockopt", 54),
    ("getsockopt", 55),
    ("fork", 57),
    ("vfork", 58),
    ("execve", 59),
    ("exit", 60),
    ("wait4", 61),
    ("kill", 62),
    ("uname", 63),
    ("semget", 64),
    ("semop", 65),
    ("semctl", 66),
    ("shmdt", 67),
    ("msgget", 68),
    ("msgsnd", 69),
    ("msgrcv", 70),
    ("msgctl", 71),
    ("fcntl", 72),
    ("flock", 73),
    ("fsync", 74),
    ("fdatasync", 75),
    ("truncate", 76),
    ("ftruncate", 77),
    ("getdents", 78),
    ("getcwd", 79),
    ("chdir", 80),
    ("fchdir", 81),
    ("rename", 82),
    ("mkdir", 83),
    ("rmdir", 84),
    ("creat", 85),
    ("link", 86),
    ("unlink", 87),
    ("symlink", 88),
    ("readlink", 89),
    ("chmod", 90),
    ("fchmod", 91),
    ("chown", 92),
    ("fchown", 93),
    ("lchown", 94),
    ("umask", 95),
    ("gettimeofday", 96),
    ("getrlimit", 97),
    ("getrusage", 98),
    ("sysinfo", 99),
    ("times", 100),
    ("ptrace", 101),
    ("getuid", 102),
    ("getgid", 104),
    ("setuid", 105),
    ("setgid", 106),
    ("geteuid", 107),
    ("getegid", 108),
    ("setpgid", 109),
    ("getppid", 110),
    ("getpgrp", 111),
    ("setsid", 112),
    ("setreuid", 113),
    ("setregid", 114),
    ("getgroups", 115),
    ("setgroups", 116),
    ("setresuid", 117),
    ("getresuid", 118),
    ("setresgid", 119),
    ("getresgid", 120),
    ("getpgid", 121),
    ("setfsuid", 122),
    ("setfsgid", 123),
    ("getsid", 124),
    ("capget", 125),
    ("capset", 126),
    ("rt_sigpending", 127),
    ("rt_sigtimedwait", 128),
    ("rt_sigqueueinfo", 129),
    ("rt_sigsuspend", 130),
    ("sigaltstack", 131),
    ("utime", 132),
    ("mknod", 133),
    ("statfs", 137),
    ("fstatfs", 138),
    ("getpriority", 140),
    ("setpriority", 141),
    ("sched_setparam", 142),
    ("sched_getparam", 143),
    ("sched_setscheduler", 144),
    ("sched_getscheduler", 145),
    ("sched_get_priority_max", 146),
    ("sched_get_priority_min", 147),
    ("sched_rr_get_interval", 148),
    ("mlock", 149),
    ("munlock", 150),
    ("mlockall", 151),
    ("munlockall", 152),
    ("modify_ldt", 154),
    ("prctl", 157),
    ("arch_prctl", 158),
    ("adjtimex", 159),
    ("setrlimit", 160),
    ("chroot", 161),
    ("sync", 162),
    ("gettid", 186),
    ("readahead", 187),
    ("setxattr", 188),
    ("lsetxattr", 189),
    ("fsetxattr", 190),
    ("getxattr", 191),
    ("lgetxattr", 192),
    ("fgetxattr", 193),
    ("listxattr", 194),
    ("llistxattr", 195),
    ("flistxattr", 196),
    ("removexattr", 197),
    ("lremovexattr", 198),
    ("fremovexattr", 199),
    ("tkill", 200),
    ("time", 201),
    ("futex", 202),
    ("sched_setaffinity", 203),
    ("sched_getaffinity", 204),
    ("set_thread_area", 205),
    ("io_setup", 206),
    ("io_destroy", 207),
    ("io_getevents", 208),
    ("io_submit", 209),
    ("io_cancel", 210),
    ("get_thread_area", 211),
    ("epoll_create", 213),
    ("epoll_ctl_old", 214),
    ("epoll_wait_old", 215),
    ("remap_file_pages", 216),
    ("getdents64", 217),
    ("set_tid_address", 218),
    ("restart_syscall", 219),
    ("semtimedop", 220),
    ("fadvise64", 221),
    ("timer_create", 222),
    ("timer_settime", 223),
    ("timer_gettime", 224),
    ("timer_getoverrun", 225),
    ("timer_delete", 226),
    ("clock_gettime", 228),
    ("clock_getres", 229),
    ("clock_nanosleep", 230),
    ("exit_group", 231),
    ("epoll_wait", 232),
    ("epoll_ctl", 233),
    ("tgkill", 234),
    ("utimes", 235),
    ("mq_open", 240),
    ("mq_unlink", 241),
    ("mq_timedsend", 242),
    ("mq_timedreceive", 243),
    ("mq_notify", 244),
    ("mq_getsetattr", 245),
    ("waitid", 247),
    ("ioprio_set", 251),
    ("ioprio_get", 252),
    ("inotify_init", 253),
    ("inotify_add_watch", 254),
    ("inotify_rm_watch", 255),
    ("openat", 257),
    ("mkdirat", 258),
    ("mknodat", 259),
    ("fchownat", 260),
    ("futimesat", 261),
    ("newfstatat", 262),
    ("unlinkat", 263),
    ("renameat", 264),
    ("linkat", 265),
    ("symlinkat", 266),
    ("readlinkat", 267),
    ("fchmodat", 268),
    ("faccessat", 269),
    ("pselect6", 270),
    ("ppoll", 271),
    ("set_robust_list", 273),
    ("get_robust_list", 274),
    ("splice", 275),
    ("tee", 276),
    ("sync_file_range", 277),
    ("vmsplice", 278),
    ("utimensat", 280),
    ("epoll_pwait", 281),
    ("signalfd", 282),
    ("timerfd_create", 283),
    ("eventfd", 284),
    ("fallocate", 285),
    ("timerfd_settime", 286),
    ("timerfd_gettime", 287),
    ("accept4", 288),
    ("signalfd4", 289),
    ("eventfd2", 290),
    ("epoll_create1", 291),
    ("dup3", 292),
    ("pipe2", 293),
    ("inotify_init1", 294),
    ("preadv", 295),
    ("pwritev", 296),
    ("rt_tgsigqueueinfo", 297),
    ("recvmmsg", 299),
    ("fanotify_mark", 301),
    ("prlimit64", 302),
    ("name_to_handle_at", 303),
    ("clock_adjtime", 305),
    ("syncfs", 306),
    ("sendmmsg", 307),
    ("getcpu", 309),
    ("process_vm_readv", 310),
    ("process_vm_writev", 311),
    ("sched_setattr", 314),
    ("sched_getattr", 315),
    ("renameat2", 316),
    ("seccomp", 317),
    ("getrandom", 318),
    ("memfd_create", 319),
    ("execveat", 322),
    ("membarrier", 324),
    ("mlock2", 325),
    ("copy_file_range", 326),
    ("preadv2", 327),
    ("pwritev2", 328),
    ("pkey_mprotect", 329),
    ("pkey_alloc", 330),
    ("pkey_free", 331),
    ("statx", 332),
    ("io_pgetevents", 333),
    ("rseq", 334),
    ("uretprobe", 335),
    ("pidfd_send_signal", 424),
    ("pidfd_open", 434),
    ("close_range", 436),
    ("openat2", 437),
    ("faccessat2", 439),
    ("epoll_pwait2", 441),
    ("landlock_create_ruleset", 444),
    ("landlock_add_rule", 445),
    ("landlock_restrict_self", 446),
    ("memfd_secret", 447),
    ("process_mrelease", 448),
    ("futex_waitv", 449),
    ("cachestat", 451),
    ("fchmodat2", 452),
    ("map_shadow_stack", 453),
    ("futex_wake", 454),
    ("futex_wait", 455),
    ("futex_requeue", 456),
    ("statmount", 457),
    ("listmount", 458),
    ("mseal", 462),
    ("setxattrat", 463),
    ("getxattrat", 464),
    ("listxattrat", 465),
    ("removexattrat", 466),
];

const SYS_CLONE_NR: u32 = 56;
const SYS_CLONE3_NR: u32 = 435;
const SYS_SOCKET_NR: u32 = 41;
const SYS_PERSONALITY_NR: u32 = 135;
const AUDIT_ARCH_X86_64: u32 = 0xc000_003e;
const X32_BIT: u32 = 0x4000_0000;
const NAMESPACE_FLAGS: u32 = 0x7e02_0080;

/// The socket families Docker allows. Missing are AF_ALG, AF_VSOCK, the
/// old radio and LAN protocols (AX.25, IPX, AppleTalk, DECnet and their
/// kind) and every family added after the profile.
const SOCKET_FAMILIES: [u32; 32] = [
    0, 1, 2, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 21, 22, 24, 26, 27, 28, 29, 30, 31, 32, 33, 35,
    36, 39, 41, 42, 43, 44, 45,
];

/// Linux, the 32 bit personality, UNAME26, both, and the query.
const PERSONALITIES: [u32; 5] = [0, 8, 0x2_0000, 0x2_0008, 0xffff_ffff];

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
const BPF_JGT_K: u16 = 0x25;
const BPF_JGE_K: u16 = 0x35;
const BPF_JSET_K: u16 = 0x45;
const BPF_RET_K: u16 = 0x06;
const RET_ALLOW: u32 = 0x7fff_0000;
const RET_KILL_PROCESS: u32 = 0x8000_0000;
const RET_ERRNO: u32 = 0x0005_0000;
const EPERM: u32 = 1;
const ENOSYS: u32 = 38;
const ARG0_LO: u32 = 16;
const ARG0_HI: u32 = 20;

fn stmt(code: u16, k: u32) -> SockFilter {
    SockFilter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

fn jump(code: u16, k: u32, jt: usize, jf: usize) -> SockFilter {
    let off = |n: usize| u8::try_from(n).expect("a BPF jump is at most 255 forward");
    SockFilter {
        code,
        jt: off(jt),
        jf: off(jf),
        k,
    }
}

/// A call allowed only when its first argument, all 64 bits, is one of
/// `values`. Expects the syscall number in the accumulator and leaves it
/// there for whatever follows when the number does not match.
fn first_arg_one_of(nr: u32, values: &[u32]) -> Vec<SockFilter> {
    let n = values.len();
    let mut b = vec![
        stmt(BPF_LD_W_ABS, ARG0_HI),
        jump(BPF_JEQ_K, 0, 0, n + 1),
        stmt(BPF_LD_W_ABS, ARG0_LO),
    ];
    for (i, v) in values.iter().enumerate() {
        b.push(jump(BPF_JEQ_K, *v, n - i, 0));
    }
    b.push(stmt(BPF_RET_K, RET_ERRNO | EPERM));
    b.push(stmt(BPF_RET_K, RET_ALLOW));
    let mut p = vec![jump(BPF_JEQ_K, nr, 0, b.len())];
    p.append(&mut b);
    p
}

/// The seccomp program: the wrong architecture or the x32 ABI kills;
/// clone3 gets ENOSYS so libc falls back to clone, where the flags can be
/// seen (clone3 passes them in memory a filter cannot read); clone with
/// namespace flags, socket with a family Docker refuses and personality
/// with a value it refuses get EPERM; then [`ALLOWED`], as ranges of
/// consecutive numbers, and EPERM for the rest. Offsets in seccomp_data:
/// nr at 0, arch at 4, args from 16.
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
        jump(BPF_JEQ_K, SYS_CLONE_NR, 0, 4),
        stmt(BPF_LD_W_ABS, ARG0_LO),
        jump(BPF_JSET_K, NAMESPACE_FLAGS, 0, 1),
        stmt(BPF_RET_K, RET_ERRNO | EPERM),
        stmt(BPF_RET_K, RET_ALLOW),
    ];
    p.extend(first_arg_one_of(SYS_SOCKET_NR, &SOCKET_FAMILIES));
    p.extend(first_arg_one_of(SYS_PERSONALITY_NR, &PERSONALITIES));
    let mut ranges: Vec<(u32, u32)> = Vec::new();
    for (_, nr) in ALLOWED {
        match ranges.last_mut() {
            Some((_, hi)) if *hi + 1 == nr => *hi = nr,
            _ => ranges.push((nr, nr)),
        }
    }
    // ascending: below a range's start nothing later can match
    let n = ranges.len();
    for (i, (lo, hi)) in ranges.into_iter().enumerate() {
        let left = 2 * (n - i) - 1;
        p.push(jump(BPF_JGE_K, lo, 0, left));
        p.push(jump(BPF_JGT_K, hi, 0, left));
    }
    p.push(stmt(BPF_RET_K, RET_ERRNO | EPERM));
    p.push(stmt(BPF_RET_K, RET_ALLOW));
    p
}

/// Everything the child needs, prepared before the clone. Between clone
/// and exec the child may not allocate: another thread of the parent may
/// have held the allocator's lock at the moment of the clone, and the
/// child would wait for it forever.
pub struct Spawn {
    root: CString,
    overlay: Option<CString>,
    join: Vec<(std::fs::File, c_int)>,
    join_pid: Option<std::fs::File>,
    binds: Vec<Bind>,
    stdio: Option<(std::fs::File, std::fs::File, std::fs::File)>,
    cwd: Option<CString>,
    hostname: Option<CString>,
    readonly: bool,
    cgroupns: bool,
    enter: Option<Enter>,
    user: Option<(u32, u32, Vec<u32>)>,
    caps: u64,
    seccomp: Vec<SockFilter>,
    path: CString,
    _argv: Vec<CString>,
    argv_ptrs: Vec<*const c_char>,
    _env: Vec<CString>,
    env_ptrs: Vec<*const c_char>,
}

/// A host path mounted into the container. The child clones the source
/// as a detached mount before the pivot and attaches it after, so the
/// target is created and resolved only inside the container, and a
/// symlink in the image cannot point it at the host.
struct Bind {
    source: CString,
    tree: std::sync::atomic::AtomicI32,
    ancestors: Vec<CString>,
    target: CString,
    is_dir: bool,
    readonly: bool,
}

/// The namespaces and root of a running container, for a process that
/// joins it rather than starting a new one.
struct Enter {
    namespaces: Vec<(std::fs::File, c_int)>,
    root: std::fs::File,
}

/// Where the child failed, reported as its exit status. Above anything a
/// program usually exits with, below the 128 + signal range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Release = 98,
    Namespaces = 99,
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
    Volumes = 116,
}

impl Step {
    pub fn from_status(code: i32) -> Option<Step> {
        use Step::*;
        [
            Release,
            Namespaces,
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
            Volumes,
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
    exec: c_int,
}

impl Started {
    /// Lets the child go on, and returns once it has called execve or
    /// died, so that whatever joins the container next finds it set up.
    pub fn release(mut self) -> io::Result<()> {
        self.signal()?;
        let mut byte = 0u8;
        // SAFETY: reads into a live local from a pipe this struct owns.
        // The child holds the only write end, close on exec, so this
        // returns at its execve or its exit.
        while unsafe { read(self.exec, (&mut byte as *mut u8).cast(), 1) } < 0 {
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        Ok(())
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
        // SAFETY: closes a descriptor this struct owns, once.
        unsafe { close(self.exec) };
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
            overlay: None,
            join: Vec::new(),
            join_pid: None,
            binds: Vec::new(),
            stdio: None,
            cwd: None,
            hostname: None,
            readonly: false,
            cgroupns: false,
            enter: None,
            user: None,
            caps: DEFAULT_CAPS,
            seccomp: seccomp_program(),
            path: cstr(first)?,
            _argv: argv,
            argv_ptrs,
            _env: env,
            env_ptrs,
        })
    }

    /// A process that joins the running container whose first process is
    /// `pid`: its namespaces and root, with the same capabilities and
    /// seccomp filter a started container gets.
    pub fn enter(pid: i32, argv: &[&[u8]], env: &[&[u8]]) -> io::Result<Spawn> {
        let mut spawn = Spawn::new(b"/", argv, env)?;
        let proc = std::path::PathBuf::from(format!("/proc/{pid}"));
        let mut namespaces = Vec::new();
        // mnt last: after it the host's /proc is out of reach
        for (name, kind) in [
            ("ipc", CLONE_NEWIPC),
            ("uts", CLONE_NEWUTS),
            ("net", CLONE_NEWNET),
            ("cgroup", CLONE_NEWCGROUP),
            ("pid", CLONE_NEWPID),
            ("mnt", CLONE_NEWNS),
        ] {
            match std::fs::File::open(proc.join("ns").join(name)) {
                Ok(f) => namespaces.push((f, kind as c_int)),
                Err(e) if name == "cgroup" && e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        let root = std::fs::File::open(proc.join("root"))?;
        spawn.enter = Some(Enter { namespaces, root });
        Ok(spawn)
    }

    /// Mounts an overlay on the root first: `lower` read only, writes go
    /// to `upper`. Overlayfs needs `work` on the same filesystem as
    /// `upper`.
    pub fn overlay(mut self, lower: &[u8], upper: &[u8], work: &[u8]) -> io::Result<Self> {
        for p in [lower, upper, work] {
            if p.iter().any(|&b| b == b',' || b == b':') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "overlay paths may not contain ',' or ':'",
                ));
            }
        }
        let mut opts = b"lowerdir=".to_vec();
        opts.extend_from_slice(lower);
        opts.extend_from_slice(b",upperdir=");
        opts.extend_from_slice(upper);
        opts.extend_from_slice(b",workdir=");
        opts.extend_from_slice(work);
        self.overlay = Some(cstr(&opts)?);
        Ok(self)
    }

    /// Joins the namespaces held at these paths (see [`hold_namespaces`])
    /// instead of creating network, IPC and UTS namespaces of its own.
    pub fn join(mut self, held: &std::path::Path) -> io::Result<Self> {
        for (name, kind) in [
            ("net", CLONE_NEWNET),
            ("ipc", CLONE_NEWIPC),
            ("uts", CLONE_NEWUTS),
        ] {
            let f = std::fs::File::open(held.join(name))?;
            self.join.push((f, kind as c_int));
        }
        Ok(self)
    }

    /// Mounts `source` from the host at `target`, an absolute path in the
    /// container, creating the target and its parents if missing.
    pub fn bind(
        mut self,
        source: &std::path::Path,
        target: &[u8],
        readonly: bool,
    ) -> io::Result<Self> {
        let invalid = |why: &str| io::Error::new(io::ErrorKind::InvalidInput, why.to_string());
        if target.first() != Some(&b'/') {
            return Err(invalid("a mount target must be absolute"));
        }
        let parts: Vec<&[u8]> = target
            .split(|&b| b == b'/')
            .filter(|p| !p.is_empty())
            .collect();
        if parts.iter().any(|p| *p == b"." || *p == b"..") {
            return Err(invalid("a mount target may not contain . or .."));
        }
        if parts.is_empty() {
            return Err(invalid("cannot mount over the root"));
        }
        let is_dir = std::fs::metadata(source)?.is_dir();
        let mut ancestors = Vec::new();
        let mut path = Vec::new();
        for p in &parts[..parts.len() - 1] {
            path.push(b'/');
            path.extend_from_slice(p);
            ancestors.push(cstr(&path)?);
        }
        self.binds.push(Bind {
            source: cstr(source.as_os_str().as_encoded_bytes())?,
            tree: (-1).into(),
            ancestors,
            target: cstr(target)?,
            is_dir,
            readonly,
        });
        Ok(self)
    }

    /// Starts the command in the PID namespace whose first process is
    /// `holder` (see [`hold_pid_namespace`]), shared with the other
    /// containers of its pod, instead of in a new one.
    pub fn join_pid_namespace(mut self, holder: i32) -> io::Result<Self> {
        self.join_pid = Some(std::fs::File::open(format!("/proc/{holder}/ns/pid"))?);
        Ok(self)
    }

    /// Standard input, output and error of the command.
    pub fn stdio(
        mut self,
        stdin: std::fs::File,
        stdout: std::fs::File,
        stderr: std::fs::File,
    ) -> Self {
        self.stdio = Some((stdin, stdout, stderr));
        self
    }

    pub fn cwd(mut self, dir: &[u8]) -> io::Result<Self> {
        self.cwd = Some(cstr(dir)?);
        Ok(self)
    }

    pub fn hostname(mut self, name: &[u8]) -> io::Result<Self> {
        self.hostname = Some(cstr(name)?);
        Ok(self)
    }

    pub fn readonly(mut self, yes: bool) -> Self {
        self.readonly = yes;
        self
    }

    /// A cgroup namespace rooted at the cgroup the child is in when it is
    /// released, and that cgroup mounted read only at /sys/fs/cgroup,
    /// where runtimes such as Go's and the JVM read their limits. Needs
    /// cgroup v2.
    pub fn cgroup_namespace(mut self, yes: bool) -> Self {
        self.cgroupns = yes;
        self
    }

    /// Runs the command as `uid` and `gid` with these supplementary
    /// groups. A user other than root starts with no capabilities, as
    /// under Docker and containerd: it would need file capabilities to
    /// gain any, and `no_new_privs` refuses those.
    pub fn user(mut self, uid: u32, gid: u32, groups: &[u32]) -> Self {
        self.user = Some((uid, gid, groups.to_vec()));
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
        let mut exec = [0 as c_int; 2];
        // SAFETY: as above.
        if unsafe { pipe2(exec.as_mut_ptr(), O_CLOEXEC) } != 0 {
            let e = io::Error::last_os_error();
            unsafe {
                close(fds[0]);
                close(fds[1]);
            }
            return Err(e);
        }
        let own = if self.join.is_empty() {
            CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWNET
        } else {
            0
        };
        let flags = if self.enter.is_some() {
            SIGCHLD
        } else if self.join_pid.is_some() {
            CLONE_NEWNS | own | SIGCHLD
        } else {
            CLONE_NEWNS | CLONE_NEWPID | own | SIGCHLD
        };
        // joining a PID namespace changes where this thread's children go,
        // not the thread itself, and is undone right after the clone
        let restore = match &self.join_pid {
            Some(ns) => {
                use std::os::fd::AsRawFd;
                let own_ns = std::fs::File::open("/proc/thread-self/ns/pid")?;
                // SAFETY: setns on a descriptor this function holds.
                if unsafe { setns(ns.as_raw_fd(), CLONE_NEWPID as c_int) } != 0 {
                    let e = io::Error::last_os_error();
                    unsafe {
                        close(fds[0]);
                        close(fds[1]);
                        close(exec[0]);
                        close(exec[1]);
                    }
                    return Err(e);
                }
                Some(own_ns)
            }
            None => None,
        };
        // SAFETY: a raw clone without CLONE_VM is a fork into the given
        // namespaces. The child runs only `child`, which makes syscalls on
        // memory prepared above and ends in execve or _exit.
        let pid = unsafe { syscall(SYS_CLONE, flags, 0usize, 0usize, 0usize, 0usize) };
        if pid != 0 {
            if let Some(own_ns) = &restore {
                use std::os::fd::AsRawFd;
                // SAFETY: as above. Failing here would leave this thread
                // starting every later child in the pod.
                if unsafe { setns(own_ns.as_raw_fd(), CLONE_NEWPID as c_int) } != 0 {
                    panic!("cannot return to the runtime's PID namespace");
                }
            }
        }
        match pid {
            -1 => {
                let e = io::Error::last_os_error();
                unsafe {
                    close(fds[0]);
                    close(fds[1]);
                    close(exec[0]);
                    close(exec[1]);
                }
                Err(e)
            }
            0 if self.enter.is_some() => self.child_enter(fds[0], exec[1]),
            0 => self.child(fds[0]),
            pid => {
                unsafe {
                    close(fds[0]);
                    close(exec[1]);
                }
                Ok(Started {
                    pid: pid as i32,
                    release: fds[1],
                    exec: exec[0],
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
            if self.cgroupns && unshare(CLONE_NEWCGROUP as c_int) != 0 {
                fail(Step::Namespaces);
            }
            use std::os::fd::AsRawFd;
            for (f, kind) in &self.join {
                if setns(f.as_raw_fd(), *kind) != 0 {
                    fail(Step::Namespaces);
                }
            }
            if let Some((i, o, e)) = &self.stdio {
                if dup2(i.as_raw_fd(), 0) < 0
                    || dup2(o.as_raw_fd(), 1) < 0
                    || dup2(e.as_raw_fd(), 2) < 0
                {
                    fail(Step::Namespaces);
                }
            }
            if mount(null, c"/".as_ptr(), null, MS_REC | MS_PRIVATE, nodata) != 0 {
                fail(Step::MakePrivate);
            }
            let root = self.root.as_ptr();
            let bound = match &self.overlay {
                Some(opts) => mount(
                    c"overlay".as_ptr(),
                    root,
                    c"overlay".as_ptr(),
                    0,
                    opts.as_ptr().cast(),
                ),
                None => mount(root, root, null, MS_BIND | MS_REC, nodata),
            };
            if bound != 0 {
                fail(Step::BindRoot);
            }
            self.clone_binds();
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
            if self.cgroupns
                && mount(
                    c"cgroup2".as_ptr(),
                    c"/sys/fs/cgroup".as_ptr(),
                    c"cgroup2".as_ptr(),
                    hardened | MS_RDONLY,
                    nodata,
                ) != 0
            {
                fail(Step::MountSys);
            }
            self.make_dev();
            self.mount_binds();
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
            if let Some(dir) = &self.cwd {
                if chdir(dir.as_ptr()) != 0 {
                    fail(Step::EnterRoot);
                }
            }
            self.drop_capabilities();
            // last: the filter refuses mount and pivot_root, used above
            if !self.seccomp.is_empty() {
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

    /// Joining a PID namespace only places the children of the caller in
    /// it, so this child joins and forks once more, then waits and exits
    /// with the status of the command, 128 and the signal if it was
    /// killed by one.
    fn child_enter(&self, wait: c_int, exec: c_int) -> ! {
        use std::os::fd::AsRawFd;
        let fail = |step: Step| -> ! { unsafe { _exit(step as c_int) } };
        let enter = self.enter.as_ref().expect("checked by the caller");
        // SAFETY: as in `child`; every pointer is prepared before the
        // clone and outlives the calls.
        unsafe {
            let mut byte = 0u8;
            if read(wait, (&mut byte as *mut u8).cast(), 1) != 1 {
                fail(Step::Release);
            }
            if let Some((i, o, e)) = &self.stdio {
                if dup2(i.as_raw_fd(), 0) < 0
                    || dup2(o.as_raw_fd(), 1) < 0
                    || dup2(e.as_raw_fd(), 2) < 0
                {
                    fail(Step::Namespaces);
                }
            }
            for (f, kind) in &enter.namespaces {
                if setns(f.as_raw_fd(), *kind) != 0 {
                    fail(Step::Namespaces);
                }
            }
            if fchdir(enter.root.as_raw_fd()) != 0 || chroot(c".".as_ptr()) != 0 {
                fail(Step::EnterRoot);
            }
            let pid = syscall(SYS_CLONE, SIGCHLD, 0usize, 0usize, 0usize, 0usize);
            if pid < 0 {
                fail(Step::Namespaces);
            }
            if pid > 0 {
                // only the command may hold it, or release would wait for
                // the command to end
                close(exec);
                let mut status: c_int = 0;
                while waitpid(pid as c_int, &mut status, 0) != pid as c_int {
                    if *__errno_location() != EINTR {
                        _exit(255);
                    }
                }
                _exit(if status & 0x7f == 0 {
                    (status >> 8) & 0xff
                } else {
                    128 + (status & 0x7f)
                });
            }
            // the waiting parent is what a timeout kills
            prctl(PR_SET_PDEATHSIG, 9, 0, 0, 0);
            let dir = self.cwd.as_ref().map_or(c"/".as_ptr(), |d| d.as_ptr());
            if chdir(dir) != 0 {
                fail(Step::EnterRoot);
            }
            self.drop_capabilities();
            if !self.seccomp.is_empty() {
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
            }
            execve(
                self.path.as_ptr(),
                self.argv_ptrs.as_ptr(),
                self.env_ptrs.as_ptr(),
            );
        }
        fail(Step::Exec)
    }

    unsafe fn clone_binds(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        for b in &self.binds {
            let flags = OPEN_TREE_CLONE | O_CLOEXEC as c_uint | AT_RECURSIVE;
            let fd = syscall(SYS_OPEN_TREE, AT_FDCWD, b.source.as_ptr(), flags);
            if fd < 0 {
                _exit(Step::Volumes as c_int);
            }
            b.tree.store(fd as i32, Relaxed);
        }
    }

    /// Runs after the pivot, so every path here resolves inside the
    /// container.
    unsafe fn mount_binds(&self) {
        let fail = || -> ! { _exit(Step::Volumes as c_int) };
        let null = std::ptr::null::<c_char>();
        let nodata = std::ptr::null::<c_void>();
        for b in &self.binds {
            for dir in &b.ancestors {
                if mkdir(dir.as_ptr(), 0o755) != 0 && *__errno_location() != EEXIST {
                    fail();
                }
            }
            if b.is_dir {
                if mkdir(b.target.as_ptr(), 0o755) != 0 && *__errno_location() != EEXIST {
                    fail();
                }
            } else {
                let fd = open(
                    b.target.as_ptr(),
                    O_WRONLY | O_CREAT | O_CLOEXEC,
                    0o644 as c_uint,
                );
                if fd < 0 {
                    fail();
                }
                close(fd);
            }
            let tree = b.tree.load(std::sync::atomic::Ordering::Relaxed);
            if syscall(
                SYS_MOVE_MOUNT,
                tree,
                c"".as_ptr(),
                AT_FDCWD,
                b.target.as_ptr(),
                MOVE_MOUNT_F_EMPTY_PATH,
            ) != 0
            {
                fail();
            }
            close(tree);
            let ro = MS_REMOUNT | MS_BIND | MS_RDONLY;
            if b.readonly && mount(null, b.target.as_ptr(), null, ro, nodata) != 0 {
                fail();
            }
        }
    }

    /// Bounding set first, which needs CAP_SETPCAP still held, then the
    /// sets the command starts with, then no_new_privs so a setuid binary
    /// cannot gain back what was dropped, then no open file passed on.
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
        let mut caps = self.caps;
        if let Some((uid, gid, groups)) = &self.user {
            if syscall(SYS_SETGROUPS, groups.len(), groups.as_ptr()) != 0
                || syscall(SYS_SETRESGID, *gid, *gid, *gid) != 0
                || syscall(SYS_SETRESUID, *uid, *uid, *uid) != 0
            {
                fail();
            }
            if *uid != 0 {
                caps = 0;
            }
        }
        let header = [LINUX_CAPABILITY_VERSION_3, 0u32];
        let (lo, hi) = (caps as u32, (caps >> 32) as u32);
        // effective, permitted, inheritable; low 32 bits, then high. None
        // inheritable, as containerd since CVE-2022-24769.
        let data = [lo, lo, 0, hi, hi, 0];
        if syscall(SYS_CAPSET, header.as_ptr(), data.as_ptr()) != 0 {
            fail();
        }
        if prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            fail();
        }
        // whatever the runtime holds open without O_CLOEXEC ends at
        // execve; the pipe that reports the execve is already marked
        if syscall(SYS_CLOSE_RANGE, 3, c_uint::MAX, CLOSE_RANGE_CLOEXEC) != 0 {
            fail();
        }
    }
}

/// Creates network, IPC and UTS namespaces and keeps them alive without a
/// process, as files `net`, `ipc` and `uts` in `dir`: a short lived child
/// makes them, the parent bind mounts them, the child exits. This is a
/// pod sandbox without the pause container (docs/RUNTIME.md).
/// Starts the first process of a new PID namespace, for a pod whose
/// containers share one: it reaps whatever is orphaned into it and does
/// nothing else. The namespace ends when it is killed.
pub fn hold_pid_namespace() -> io::Result<i32> {
    // SAFETY: as in Spawn::start; the child only waits and pauses.
    let pid = unsafe {
        syscall(
            SYS_CLONE,
            CLONE_NEWPID | SIGCHLD,
            0usize,
            0usize,
            0usize,
            0usize,
        )
    };
    if pid == 0 {
        unsafe {
            // it lives as long as the pod: a pipe end of the parent's held
            // open here would keep that pipe from ever reporting its end
            syscall(SYS_CLOSE_RANGE, 0, c_uint::MAX, 0);
            let mut status: c_int = 0;
            loop {
                if waitpid(-1, &mut status, 0) < 0 {
                    pause();
                }
            }
        }
    }
    if pid < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pid as i32)
}

/// Brings up `lo` in the caller's network namespace, which a new one
/// starts without: a pod's containers talk to each other over it.
unsafe fn loopback_up() -> bool {
    // struct ifreq: the name, then the flags as a short
    let mut req = [0u8; 40];
    req[..2].copy_from_slice(b"lo");
    let fd = socket(2, 2 | O_CLOEXEC, 0);
    if fd < 0 {
        return false;
    }
    let mut ok = ioctl(fd, SIOCGIFFLAGS, req.as_mut_ptr()) == 0;
    if ok {
        let flags = i16::from_ne_bytes([req[16], req[17]]) | IFF_UP;
        req[16..18].copy_from_slice(&flags.to_ne_bytes());
        ok = ioctl(fd, SIOCSIFFLAGS, req.as_mut_ptr()) == 0;
    }
    close(fd);
    ok
}

pub fn hold_namespaces(dir: &std::path::Path, hostname: &[u8]) -> io::Result<()> {
    let name = cstr(hostname)?;
    let mut fds = [0 as c_int; 2];
    // SAFETY: fds is a valid two element array.
    if unsafe { pipe2(fds.as_mut_ptr(), O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let flags = CLONE_NEWNET | CLONE_NEWIPC | CLONE_NEWUTS | SIGCHLD;
    // SAFETY: as in Spawn::start; the child only sets its hostname and
    // blocks on the pipe, then exits.
    let pid = unsafe { syscall(SYS_CLONE, flags, 0usize, 0usize, 0usize, 0usize) };
    if pid == 0 {
        unsafe {
            let ok = sethostname(name.as_ptr(), name.as_bytes().len()) == 0 && loopback_up();
            let mut b = 0u8;
            read(fds[0], (&mut b as *mut u8).cast(), 1);
            _exit(if ok { 0 } else { 1 });
        }
    }
    unsafe { close(fds[0]) };
    if pid < 0 {
        unsafe { close(fds[1]) };
        return Err(io::Error::last_os_error());
    }
    let mut result = Ok(());
    for ns in ["uts", "ipc", "net"] {
        let target = dir.join(ns);
        let made = std::fs::File::create(&target).and_then(|_| {
            let src = cstr(format!("/proc/{pid}/ns/{ns}").as_bytes())?;
            let dst = cstr(target.as_os_str().as_encoded_bytes())?;
            // SAFETY: both are valid C strings; a bind mount of an nsfs
            // file keeps the namespace alive after its last process.
            let r = unsafe {
                mount(
                    src.as_ptr(),
                    dst.as_ptr(),
                    std::ptr::null(),
                    MS_BIND,
                    std::ptr::null(),
                )
            };
            if r == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        if let Err(e) = made {
            result = Err(e);
            break;
        }
    }
    let byte = 1u8;
    unsafe {
        write(fds[1], (&byte as *const u8).cast(), 1);
        close(fds[1]);
    }
    match (wait(pid as i32)?, result) {
        (_, Err(e)) => {
            let _ = release_namespaces(dir);
            Err(e)
        }
        (Exit::Code(0), Ok(())) => Ok(()),
        (other, Ok(())) => {
            let _ = release_namespaces(dir);
            Err(io::Error::other(format!(
                "namespace holder: {}",
                describe(other)
            )))
        }
    }
}

/// Undoes [`hold_namespaces`]; the namespaces end once no process is in
/// them.
pub fn release_namespaces(dir: &std::path::Path) -> io::Result<()> {
    for ns in ["net", "ipc", "uts"] {
        let p = dir.join(ns);
        let c = cstr(p.as_os_str().as_encoded_bytes())?;
        // SAFETY: a valid C string; an absent mount is reported, not unsafe.
        unsafe { umount2(c.as_ptr(), MNT_DETACH) };
        let _ = std::fs::remove_file(&p);
    }
    Ok(())
}

pub fn signal(pid: i32, sig: i32) -> io::Result<()> {
    // SAFETY: kill takes two integers.
    if unsafe { kill(pid, sig) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
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
        assert_eq!(Step::from_status(99), Some(Step::Namespaces));
        assert_eq!(Step::from_status(0), None);
        assert_eq!(describe(Exit::Code(115)), "setup failed at Exec");
    }

    #[test]
    fn syscall_numbers_match_the_kernel_headers() {
        let Ok(h) = std::fs::read_to_string("/usr/include/x86_64-linux-gnu/asm/unistd_64.h") else {
            eprintln!("no kernel headers, not checked");
            return;
        };
        let known: Vec<(&str, u32)> = h
            .lines()
            .filter_map(|l| {
                let mut w = l.strip_prefix("#define __NR_")?.split_whitespace();
                Some((w.next()?, w.next()?.parse().ok()?))
            })
            .collect();
        assert!(known.len() > 300, "headers not parsed");
        let special = [
            ("clone", SYS_CLONE_NR),
            ("clone3", SYS_CLONE3_NR),
            ("socket", SYS_SOCKET_NR),
            ("personality", SYS_PERSONALITY_NR),
        ];
        for (name, nr) in ALLOWED.iter().chain(&special) {
            match known.iter().find(|(n, _)| n == name) {
                Some((_, k)) => assert_eq!(k, nr, "{name}"),
                // newer than these headers, so its number must be free here
                None => assert!(known.iter().all(|(_, k)| k != nr), "{name} is not {nr}"),
            }
        }
    }

    #[test]
    fn the_allowlist_is_sorted_and_leaves_the_checked_calls_out() {
        assert!(ALLOWED.windows(2).all(|w| w[0].1 < w[1].1));
        for nr in [
            SYS_CLONE_NR,
            SYS_CLONE3_NR,
            SYS_SOCKET_NR,
            SYS_PERSONALITY_NR,
        ] {
            assert!(ALLOWED.iter().all(|(_, n)| *n != nr));
        }
        let allowed = |name| ALLOWED.iter().any(|(n, _)| *n == name);
        for name in ["mount", "bpf", "keyctl", "userfaultfd", "io_uring_setup"] {
            assert!(!allowed(name), "{name}");
        }
        // as in Docker's profile from kernel 4.8, where ptrace can no
        // longer be used to get around seccomp
        assert!(allowed("ptrace"));
    }

    /// Runs the program the way the kernel would, on one seccomp_data.
    fn run(p: &[SockFilter], arch: u32, nr: u32, arg0: u64) -> u32 {
        let mut data = [0u8; 64];
        data[0..4].copy_from_slice(&nr.to_ne_bytes());
        data[4..8].copy_from_slice(&arch.to_ne_bytes());
        data[16..24].copy_from_slice(&arg0.to_ne_bytes());
        let (mut pc, mut a) = (0, 0u32);
        loop {
            let ins = p[pc];
            let taken = match ins.code {
                BPF_LD_W_ABS => {
                    let at = ins.k as usize;
                    a = u32::from_ne_bytes(data[at..at + 4].try_into().unwrap());
                    pc += 1;
                    continue;
                }
                BPF_RET_K => return ins.k,
                BPF_JEQ_K => a == ins.k,
                BPF_JGT_K => a > ins.k,
                BPF_JGE_K => a >= ins.k,
                BPF_JSET_K => a & ins.k != 0,
                c => panic!("unexpected instruction {c:#x}"),
            };
            pc += 1 + if taken { ins.jt } else { ins.jf } as usize;
        }
    }

    #[test]
    fn the_program_allows_exactly_the_allowlist() {
        let p = seccomp_program();
        assert!(p.len() < 4096, "the kernel's limit");
        let x86 = AUDIT_ARCH_X86_64;
        let eperm = RET_ERRNO | EPERM;
        for nr in 0..1024 {
            let want = match nr {
                SYS_CLONE3_NR => RET_ERRNO | ENOSYS,
                // with a first argument of 0: no flags, AF_UNSPEC, Linux
                SYS_CLONE_NR | SYS_SOCKET_NR | SYS_PERSONALITY_NR => RET_ALLOW,
                _ if ALLOWED.iter().any(|(_, n)| *n == nr) => RET_ALLOW,
                _ => eperm,
            };
            assert_eq!(run(&p, x86, nr, 0), want, "syscall {nr}");
        }
        assert_eq!(run(&p, 0x4000_0003, 39, 0), RET_KILL_PROCESS, "i386");
        assert_eq!(run(&p, x86, X32_BIT | 39, 0), RET_KILL_PROCESS, "x32");
        let clone = |flags| run(&p, x86, SYS_CLONE_NR, flags);
        assert_eq!(clone(0x003d_0f00), RET_ALLOW, "a thread");
        assert_eq!(clone(CLONE_NEWNS | SIGCHLD), eperm);
        assert_eq!(clone(0x80), eperm, "CLONE_NEWTIME");
        let socket = |family| run(&p, x86, SYS_SOCKET_NR, family);
        for f in SOCKET_FAMILIES {
            assert_eq!(socket(f as u64), RET_ALLOW, "family {f}");
        }
        for f in [3, 4, 5, 38, 40, 46, 1 << 32 | 2] {
            assert_eq!(socket(f), eperm, "family {f}");
        }
        let personality = |v| run(&p, x86, SYS_PERSONALITY_NR, v);
        assert_eq!(personality(0xffff_ffff), RET_ALLOW, "the query");
        assert_eq!(personality(0x0040_0000), eperm, "READ_IMPLIES_EXEC");
    }

    #[test]
    fn the_default_capabilities_are_dockers_without_mknod() {
        assert_eq!(DEFAULT_CAPS.count_ones(), 13);
        assert_eq!(DEFAULT_CAPS, 0xa00425fb);
    }
}

/// One instruction of an eBPF program, as `struct bpf_insn`.
#[repr(C)]
#[derive(Clone, Copy)]
struct BpfInsn {
    code: u8,
    regs: u8,
    off: i16,
    imm: i32,
}

const fn insn(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> BpfInsn {
    BpfInsn {
        code,
        regs: dst | (src << 4),
        off,
        imm,
    }
}

/// The character devices a container may open, major and minor, minor -1
/// for any: the six of `/dev`, `/dev/console`, `/dev/ptmx` and the pty
/// slaves, as runc allows them.
const ALLOWED_DEVICES: [(i32, i32); 9] = [
    (1, 3),
    (1, 5),
    (1, 7),
    (1, 8),
    (1, 9),
    (5, 0),
    (5, 1),
    (5, 2),
    (136, -1),
];

/// A device cgroup program: 1 lets the access through, 0 refuses it.
/// The context is `struct bpf_cgroup_dev_ctx`: access and type in one
/// word, then major and minor.
fn device_program() -> Vec<BpfInsn> {
    const LDX_W: u8 = 0x61;
    const AND_K: u8 = 0x57;
    const JNE_K: u8 = 0x55;
    const JEQ_K: u8 = 0x15;
    const MOV_K: u8 = 0xb7;
    const EXIT: u8 = 0x95;
    const CHAR: i32 = 2;
    let mut p = vec![
        insn(LDX_W, 2, 1, 0, 0),
        insn(AND_K, 2, 0, 0, 0xffff),
        insn(LDX_W, 3, 1, 4, 0),
        insn(LDX_W, 4, 1, 8, 0),
    ];
    // filled in below: the jump over the allow list to the refusal
    let not_char = p.len();
    p.push(insn(JNE_K, 2, 0, 0, CHAR));
    let mut allows = Vec::new();
    for (major, minor) in ALLOWED_DEVICES {
        if minor < 0 {
            allows.push(p.len());
            p.push(insn(JEQ_K, 3, 0, 0, major));
        } else {
            p.push(insn(JNE_K, 3, 0, 1, major));
            allows.push(p.len());
            p.push(insn(JEQ_K, 4, 0, 0, minor));
        }
    }
    let deny = p.len();
    p.push(insn(MOV_K, 0, 0, 0, 0));
    p.push(insn(EXIT, 0, 0, 0, 0));
    let allow = p.len();
    p.push(insn(MOV_K, 0, 0, 0, 1));
    p.push(insn(EXIT, 0, 0, 0, 0));
    p[not_char].off = (deny - not_char - 1) as i16;
    for at in allows {
        p[at].off = (allow - at - 1) as i16;
    }
    p
}

/// Loads the device program and attaches it to the cgroup at `dir`, so
/// that processes in it can open only the devices in ALLOWED_DEVICES,
/// whatever nodes they find or make.
pub fn restrict_devices(dir: &std::path::Path) -> io::Result<()> {
    const SYS_BPF: c_long = 321;
    const BPF_PROG_LOAD: c_long = 5;
    const BPF_PROG_ATTACH: c_long = 8;
    const BPF_PROG_TYPE_CGROUP_DEVICE: u32 = 15;
    const BPF_CGROUP_DEVICE: u32 = 6;
    let prog = device_program();
    let license = c"Apache-2.0";
    // union bpf_attr for BPF_PROG_LOAD: type, count, insns, license, then
    // the log fields, zero
    let mut load = [0u8; 128];
    load[0..4].copy_from_slice(&BPF_PROG_TYPE_CGROUP_DEVICE.to_ne_bytes());
    load[4..8].copy_from_slice(&(prog.len() as u32).to_ne_bytes());
    load[8..16].copy_from_slice(&(prog.as_ptr() as u64).to_ne_bytes());
    load[16..24].copy_from_slice(&(license.as_ptr() as u64).to_ne_bytes());
    // SAFETY: the attribute points at the program and license, both live
    // for the call; the kernel copies them.
    let fd = unsafe { syscall(SYS_BPF, BPF_PROG_LOAD, load.as_ptr(), load.len()) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        use std::os::fd::AsRawFd;
        let cgroup = std::fs::File::open(dir)?;
        // union bpf_attr for BPF_PROG_ATTACH: target fd, program fd,
        // attach type, flags
        let mut attach = [0u8; 128];
        attach[0..4].copy_from_slice(&(cgroup.as_raw_fd() as u32).to_ne_bytes());
        attach[4..8].copy_from_slice(&(fd as u32).to_ne_bytes());
        attach[8..12].copy_from_slice(&BPF_CGROUP_DEVICE.to_ne_bytes());
        // SAFETY: as above.
        if unsafe { syscall(SYS_BPF, BPF_PROG_ATTACH, attach.as_ptr(), attach.len()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    })();
    // the cgroup holds the program now; the descriptor is not needed
    unsafe { close(fd as c_int) };
    result
}

#[cfg(test)]
mod device_tests {
    use super::*;

    /// Runs the program the way the kernel would, for the few opcodes it
    /// uses.
    fn run(prog: &[BpfInsn], kind: u32, major: u32, minor: u32) -> u64 {
        let ctx = [kind | (3 << 16), major, minor];
        let mut r = [0u64; 11];
        let mut pc = 0;
        loop {
            let i = prog[pc];
            let (dst, src) = ((i.regs & 15) as usize, (i.regs >> 4) as usize);
            assert_eq!(src, if i.code == 0x61 { 1 } else { 0 });
            let imm = i.imm as i64 as u64;
            let jump = |taken: bool| if taken { i.off as usize } else { 0 };
            pc += 1 + match i.code {
                0x61 => {
                    r[dst] = ctx[i.off as usize / 4] as u64;
                    0
                }
                0x57 => {
                    r[dst] &= imm;
                    0
                }
                0xb7 => {
                    r[dst] = imm;
                    0
                }
                0x55 => jump(r[dst] != imm),
                0x15 => jump(r[dst] == imm),
                0x95 => return r[0],
                op => panic!("opcode {op:#x}"),
            };
        }
    }

    #[test]
    fn only_the_standard_devices_are_let_through() {
        let p = device_program();
        let (block, char) = (1, 2);
        for (major, minor) in [(1, 3), (1, 9), (5, 2), (136, 0), (136, 7)] {
            assert_eq!(run(&p, char, major, minor), 1, "{major}:{minor}");
        }
        for (kind, major, minor) in [
            (block, 8, 0),
            (block, 1, 3),
            (char, 1, 1),
            (char, 4, 0),
            (char, 10, 200),
            (char, 5, 3),
        ] {
            assert_eq!(run(&p, kind, major, minor), 0, "{kind} {major}:{minor}");
        }
    }
}
