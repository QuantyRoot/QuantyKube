# QuantyKube Design Decisions

Short ADRs. Newest at the bottom. If a decision gets reversed, strike it
through and add a new entry, never silently edit history.

## Index

- [ADR-001](#adr-001-kubernetes-first-the-etcd-subset-the-api-server-uses) Kubernetes first: the etcd subset the API server uses
- [ADR-002](#adr-002-honesty-rule-for-public-claims) Honesty rule for public claims
- [ADR-003](#adr-003-no-third-party-crates-the-engine-comes-from-quantydb) No third-party crates, the engine comes from QuantyDB
- [ADR-004](#adr-004-plaintext-over-a-unix-socket-until-quantycore-has-tls) Plaintext over a Unix socket until QuantyCore has TLS
- [ADR-005](#adr-005-sandbox-means-the-node-runtime-behind-cri) Sandbox means the node runtime behind CRI
- [ADR-006](#adr-006-one-storage-interface-quantydb-behind-it-first) One storage interface, QuantyDB behind it first
- [ADR-007](#adr-007-apache-20-and-money-from-what-surrounds-the-code) Apache 2.0, and money from what surrounds the code
- [ADR-008](#adr-008-raft-comes-later-and-starts-with-a-simulator) Raft comes later, and starts with a simulator
- [ADR-009](#adr-009-kubectl-apply-installs-an-operator-that-swaps-and-undoes) kubectl apply installs an operator that swaps and undoes
- [ADR-010](#adr-010-a-revision-is-a-commit-and-the-change-log-keeps-both-sides) A revision is a commit, and the change log keeps both sides
- [ADR-011](#adr-011-history-is-kept-by-time-whatever-the-api-server-compacts) History is kept by time, whatever the API server compacts
- [ADR-012](#adr-012-a-commit-holds-a-range-of-revisions-so-writes-share-fsyncs) A commit holds a range of revisions, so writes share fsyncs
- [ADR-013](#adr-013-the-runtimes-syscalls-sit-behind-one-unsafe-crate) The runtime's syscalls sit behind one unsafe crate
- [ADR-014](#adr-014-history-lives-in-the-change-log-snapshots-only-briefly) History lives in the change log, snapshots only briefly
- [ADR-015](#adr-015-two-malloc-arenas-for-the-server) Two malloc arenas for the server
- [ADR-016](#adr-016-layers-are-unpacked-with-every-path-resolved-inside-the-root) Layers are unpacked with every path resolved inside the root
- [ADR-017](#adr-017-a-pod-is-three-bind-mounted-namespaces-and-runtime-state-is-not-kept) A pod is three bind mounted namespaces, and runtime state is not kept
- [ADR-018](#adr-018-the-runtime-refuses-what-it-cannot-honour) The runtime refuses what it cannot honour
- [ADR-019](#adr-019-volumes-are-attached-after-the-pivot) Volumes are attached after the pivot
- [ADR-020](#adr-020-cgroups-follow-the-cgroupfs-driver) cgroups follow the cgroupfs driver
- [ADR-021](#adr-021-exec-joins-the-container-through-a-second-fork) Exec joins the container through a second fork

## ADR-001: Kubernetes first: the etcd subset the API server uses

QuantyKube implements the part of the etcd v3 API that the Kubernetes API
server actually calls, on a single node, without Raft. A general etcd
replacement for arbitrary clients is not the goal of this phase.

This is the shape kine has proven: k3s ships it in production, it
implements a subset of the etcd API on top of SQL databases, and it does
not cluster. That a store with these limits can carry a real Kubernetes
control plane is therefore known, not hoped.

Raft is left out because writing it is a project of the same size as
everything else here combined, and because a single node store is a form
Kubernetes users already run and understand.

**The cost:** QuantyKube is unsuitable for general etcd use for now, and
the README says so in its first section, as kine's does. Any etcd client
other than the API server may call something that is not there. High
availability of the store is not available at all.

## ADR-002: Honesty rule for public claims

Carried over from QuantyDB's ADR-009. No benchmark numbers, compatibility
claims or feature checkmarks in public text unless they are reproducible
from this repository. Built things are described as built and planned
things as planned.

For QuantyKube in particular: "works with Kubernetes" means a named,
pinned Kubernetes release passing a named check that anyone can rerun. A
claim of efficiency against containerd, etcd or kine needs a measurement
in the repository that produced it.

## ADR-003: No third-party crates, the engine comes from QuantyDB

QuantyKube follows QuantyCore's ADR-001: nothing outside the standard
library except crates from the QuantyRoot organization.

The storage engine is QuantyDB's, used as a crate and not copied. etcd's
data model is a key value store with a single, monotonically increasing
revision; you read at a revision, watch from a revision and compact old
revisions away. QuantyDB is that shape already: copy on write trees,
numbered commits, reads as of a commit, and a collector for old states. A
copy would be faster to start and would drift from the original the first
week nobody compares them.

Which QuantyDB crate is the right entry point is decided when the store is
built, in its own record, because it depends on what the mapping from
etcd revisions to commits needs.

The protocol stack (protobuf, HPACK, HTTP/2, gRPC) comes from QuantyCore,
whose ADR-004 says why it lives there.

**The cost:** two repositories to change for one protocol fix, and a
release of QuantyKube on crates.io has to wait for QuantyCore and QuantyDB
releases it can depend on.

## ADR-004: Plaintext over a Unix socket until QuantyCore has TLS

QuantyKube listens on a Unix socket and speaks gRPC without TLS. The API
server is pointed at it with `--etcd-servers=unix://<path>`.

This is how k3s runs kine by default, so it is the established form for
a store that lives on the same machine as its API server. It also takes
TLS off the critical path: TLS is being written in QuantyCore (ADR-003
there) and is a long piece of work that the first demonstration must not
wait for.

Only a Unix socket, not plaintext TCP on loopback. A socket keeps the
trade visible in the path name and puts access under file permissions,
which a loopback port does not have.

**The cost:** the store cannot be reached from another machine, so the
API server and QuantyKube always share a host. The only access control is
the socket's file mode and owner, and the server creates it 0600, owner
only. A CIS style benchmark scan will flag the
missing etcd TLS settings, and that finding is correct. This record is
superseded when QuantyCore's TLS meets its own bar.

## ADR-005: Sandbox means the node runtime behind CRI

"Sandbox" in the description of this project means the container runtime
on every worker node: the program the kubelet drives through the Container
Runtime Interface, whose central object is literally called the pod
sandbox. Since Kubernetes 1.24 removed dockershim, the kubelet speaks only
CRI, and what clusters run there today is containerd or CRI-O.

The goal is not a general container engine. No image building, no command
line for people, no compose, nothing the kubelet does not call. Only the
CRI runtime and image services, as the kubelet uses them, made as lean as
they can be because that narrow job is all they do.

It is a later phase and is not part of the first demonstration. It will be
a separate binary in this repository, since it runs on every node while
the store runs only beside the API server.

**What it will need, which is why it waits:**

- Linux namespaces, cgroup v2 and mounts. The standard library wraps none
  of these, so they go behind one unsafe syscall boundary, as QuantyDB's
  reactor does (its ADR-023).
- Image pulls: HTTPS to registries, so QuantyCore's TLS client first,
  plus gzip decompression, tar extraction and SHA-256 digest checks.
- gRPC as a server on the node, which QuantyCore provides by then anyway
  (its ADR-004).
- Pod networking through the standard CNI plugins, which are separate
  executables and are invoked, not rewritten.

"Most efficient" is a claim under ADR-002: pod start latency and memory
per pod against containerd and runc, measured on the same machine, or it
is not said. Stronger isolation through micro VMs is a separate product
idea and stays out of this record.

## ADR-006: One storage interface, QuantyDB behind it first

The etcd layer talks to storage through one Rust trait, defined from the
first line of the store. QuantyDB is the default implementation and, until
after the first demonstration, the only one. Other backends are possible
later and are chosen by configuration.

**Why the interface now:** it costs a few dozen lines while the store is
being written and is expensive to retrofit once the etcd layer has grown
around one engine's habits. It also lets anyone write their own backend in
Rust without forking the project.

**Why only QuantyDB for now:** every other backend is a separate piece of
work under ADR-003, since its client has to be written here too. They are
not equally worth it:

- PostgreSQL is the one with a real reason. It is how k3s gets a highly
  available control plane from kine without Raft: the database does the
  replication. Its wire protocol and its SCRAM authentication are
  manageable, and the hashing SCRAM needs is part of QuantyCore's TLS work
  anyway. A remote PostgreSQL usually requires TLS, so it waits for that.
- SQLite is not worth it. Its job, an embedded single file store, is what
  QuantyDB already does here, and writing to its file format safely is far
  more work than reading it.

**The cost, and the part that must stay visible:** reading the cluster at
a past revision cheaply, and branches of cluster state, come from
QuantyDB's design. Behind another backend they are emulated with a log
table, as kine does, or not offered. The README must say which features
depend on the backend, so that nobody picks PostgreSQL for availability
and expects the history to behave the same.

## ADR-007: Apache 2.0, and money from what surrounds the code

QuantyKube is licensed under the Apache License 2.0. Anyone may use,
change and redistribute it, companies included.

**What was rejected.** Source available licenses such as the Business
Source License or the Functional Source License do what was first asked
for: free for everyone, paid for those who compete. They were rejected for
two reasons. They are not open source under the Open Source Definition,
which forbids treating commercial use differently, and this project will
not call itself open source if it is not (ADR-002). And the projects this
one competes with, kine first among them, are Apache 2.0: a stricter
license here would give every potential user a reason to stay where they
are. Recent relicensings in this ecosystem were followed by community
forks, which is the outcome to avoid.

Apache 2.0 rather than MIT, which QuantyDB uses, because it includes an
explicit patent license, which the legal departments of the companies
this is meant for look for, and because it is the license the Kubernetes
ecosystem expects.

**The cost:** the code itself cannot be sold. Revenue has to come from
what surrounds it: support, operation, and features built on top that are
offered separately. Anything that might ever be sold separately must be
designed as a separate piece from the start, not cut out of the open code
later.

## ADR-008: Raft comes later, and starts with a simulator

Multiple store nodes with Raft are the goal of the next phase, not of the
first demonstration. ADR-001 stands for that date.

Raft is short on paper and notoriously hard to get right in practice. The
failures that matter, a lost acknowledged write or two leaders at once,
appear only under particular interleavings of crashes, partitions and
delays, and a demonstration date is the wrong pressure to build that
under. The weeks before it are already filled by HTTP/2, gRPC and Watch,
which everything else depends on. Raft also needs a transport between the
store nodes, and across machines that transport needs TLS, which
QuantyCore does not have yet.

**How it will be built.** The measuring instrument first: a deterministic
simulator in which several nodes run in one process against a simulated
network and simulated disks, with time, message order, loss, partitions
and crashes all driven by a seed. Any failure is reproduced exactly from
its seed. Only when the simulator can find planted bugs is the protocol
written against it.

**Until then,** high availability comes from a PostgreSQL backend
(ADR-006), the way k3s gets it from kine.

**The cost:** the first thing many companies will ask about, a highly
available store, is answered on 17 November with a plan and not with a
running system, and the talk has to say so.

## ADR-009: kubectl apply installs an operator that swaps and undoes

Moving an existing cluster onto QuantyKube is one command,
`kubectl apply` of a published manifest. It installs an operator that
does the rest: replacing the store, replacing the node runtime, draining
nodes on the way, and undoing all of it in an emergency.

**The shape.** The manifest contains two things. A controller that plans
and drives the change, and a node agent that runs on every node as a
privileged DaemonSet with access to the host, because changing a node's
runtime means changing files and services on the node itself. What the
person wants is written as a custom resource in the same manifest, so
`kubectl get` shows the progress of the change like any other object.
This is an established pattern: Kata Containers installs its runtime on
nodes the same way.

**Replacing the runtime, per node, one node at a time:** cordon, drain
with respect for disruption budgets, install the runtime next to the old
one, point the kubelet at it, restart the kubelet, prove the node with a
test pod, uncordon. Any failed step stops the rollout and puts that node
back.

**Replacing the store:** snapshot etcd, copy every key at one revision
into QuantyKube, pause writes briefly, point the API server at QuantyKube,
prove it with `/readyz` and a read back, and keep etcd's data untouched
for the rollback window. Whether QuantyKube keeps etcd's revision numbers,
so that no client has to list again, or forces a relist, is decided when
this is built.

**Undo must work when everything else is broken.** The old runtime and
the old store stay installed until the person removes them. The node
agent keeps a journal on each node's disk and can reverse its own steps
without the API server, because the API server is one of the things that
might be broken.

**Interactive setup** is not something `kubectl apply` can do, since it
only sends a file. The wizard is a kubectl plugin, `kubectl quanty`, that
asks its questions and writes the custom resource. Applying the manifest
by hand works without it.

**What it depends on, which is why it comes after 17 November:**

- The operator talks to the API server, which inside a cluster only
  speaks HTTPS. QuantyCore's TLS client is a hard prerequisite.
- The node runtime has to exist first (ADR-005).
- It only works where the person controls the control plane machines.
  On managed services such as EKS, GKE or AKS the provider runs etcd, so
  only the runtime part can apply there, and only on nodes the person
  manages.

**The cost:** this is the most dangerous code in the project. It runs as
root on every node of a cluster somebody depends on, and a security
review at any serious company will start there, so it must be small,
auditable and completely removable. It also has to be tested against the
ways real clusters differ: kubeadm, k3s, RKE2 and the distributions in
between each place the kubelet's configuration somewhere else.

## ADR-010: A revision is a commit, and the change log keeps both sides

*Partly superseded by ADR-012: a commit now holds a range of revisions.
The change log and the layout below are unchanged.*

Every etcd write is one QuantyDB commit, and its commit id is the etcd
revision. A request that changes nothing, such as a transaction whose
compare fails, commits nothing and takes no revision. Revision 1 is the
empty store, committed when the file is created, so that every revision a
client has seen can be read back.

Live keys, the change log and the store's own bookkeeping share one tree,
told apart by a one byte prefix. Each log entry holds the new record and
the previous one. Watches replay from the log, and storing both sides
there is what makes a replay a single scan: rebuilding the previous value
from snapshots would walk QuantyDB's commit chain once per event, because
finding an old commit walks back from the head.

Writes are serialized, and each commit waits for two fsyncs before it
answers. There is no group commit yet. QuantyDB's server batches
statements into one commit (its ADR-028) and the same can be done here;
it waits for a measurement that says the API server is waiting on it.

**The cost:** revisions are not dense. Compaction, history pruning and
lease bookkeeping are commits too and take revision numbers that no key
carries. etcd does not do this, but no client found so far depends on
consecutive revisions. The log also doubles what an update writes, since
the old value is stored a second time.

## ADR-011: History is kept by time, whatever the API server compacts

*How it is kept changed with ADR-014; what is kept did not.*

The API server compacts etcd every five minutes, and etcd then throws the
older versions away. QuantyKube honors compaction toward etcd clients
exactly: a read or a watch below the compacted revision fails with etcd's
own error, so nothing in Kubernetes sees different behavior. What is
physically kept is decided separately, by `--retain`, one hour by default.
Within that window every commit and every log entry stays. A gRPC
service of QuantyKube's own on the same socket serves it
(`docs/HISTORY.md`), and `quantykube history`, `get` and `restore` use
that service: list what happened to a key, read it as it was at a
revision or a time, and write it back. A restore is an ordinary write,
so the API server's watch cache sees the object return.

Once a minute the server drops what is older than the window: log entries
first, in a commit, then QuantyDB's collector frees the pages. It never
drops a revision at or after the compaction point, whatever the window,
because etcd clients may still read those.

**The cost:** disk. Copy on write means every commit writes new pages for
the path it touched, and keeping an hour of commits keeps all of those
pages. The benchmarks will say how much that is under a real API server
before the default is defended in public. A restore brings back the
value, not the lease, so an object that had a lease comes back without
one.

## ADR-012: A commit holds a range of revisions, so writes share fsyncs

ADR-010 made every write its own QuantyDB commit, with two fsyncs each.
The first benchmark against etcd (16 clients, 256 byte puts, same machine
and disk) showed what that costs: 1,409 writes a second against etcd's
7,129, because etcd lets concurrent writes share one sync and this did
not. Revision numbers are kept distinct and dense, as etcd's are; what
changed is how many of them one commit carries.

**How a group forms.** A write that arrives while a commit is on its way
to disk waits. When the commit lands, the waiting writes apply one after
another into a single new transaction, each with the next revision, and
the last of them commits for all. A write that fails rolls back to a
savepoint and leaves the others alone. Nobody waits for a timer: a lone
write commits at once, and groups are exactly as large as the load
makes them. Watchers are told about each revision in order, after the
commit is durable and before any later one.

**Reading at a revision inside a group.** The commit index maps each
commit to the last revision it holds. Reading at the last one is a plain
snapshot. Reading at an earlier one takes that snapshot and sets back
the keys changed later in the same commit, using the previous state the
change log already stores (ADR-010). A test writes from eight threads at
once and checks a scan and every key at every single revision against a
model.

**Measured afterwards,** same benchmark, same machine: 4,200 to 4,800
writes a second with 16 clients, about 8 writes per commit, and half the
disk per write. The other half of the gain was not in the store: the
gRPC server ran unary calls on 4 threads, so no more than 4 writes were
ever in flight. QuantyCore now uses 64. A single client is unchanged at
about 950 a second, since it has nobody to share a commit with.

**The cost:** revision and commit are no longer the same number, so the
store keeps an index between them, and reading inside a group costs a
scan of that group's log. Bookkeeping commits, such as compaction and
leases, no longer take a revision at all, which also makes revisions
dense again. etcd is still faster at 16 clients on this machine, by
roughly half again; the server is now bound by CPU per write, which is
the next thing to profile.

## ADR-013: The runtime's syscalls sit behind one unsafe crate

The node runtime (ADR-005) needs namespaces, mounts, `pivot_root` and a
clone into new namespaces, none of which the standard library wraps. They
are declared by hand against glibc in `crates/quantykube-sys`, and that
crate holds every `unsafe` block in QuantyKube. This is QuantyDB's answer
to the same question (its ADR-023): unsafe at one small boundary where no
safe way exists, never as a shortcut.

The one rule the crate exists to keep: between the clone and the exec,
the child only makes syscalls on memory prepared before the clone. It
does not allocate, lock or print, because another thread of the parent
may have held a lock at the moment of the clone, and the child's copy of
that lock would never be released. Failures are reported as the child's
exit status, one number per setup step.

`quantykube sandbox` exposes it for measuring against runc, with what
runc's default configuration does (docs/RUNTIME.md), the seccomp filter
built by hand as BPF: wrong architecture and the x32 ABI kill, clone
with namespace flags and 47 dangerous calls get EPERM, and clone3 gets
ENOSYS so libc falls back to clone, whose flags a filter can see. The
child waits on a pipe until the parent has put it into its cgroup, so
none of its own code runs outside the limits. It is not a runtime yet:
no CRI and no image store.

**The cost:** a crate of code the compiler cannot check, written against
one libc and one architecture, x86_64 Linux, until a second one is needed.

## ADR-014: History lives in the change log, snapshots only briefly

ADR-011 kept every QuantyDB commit for the whole history window. A soak
run with a k3s control plane under steady writes showed what that costs
with copy on write: about 30 KB of disk per revision, because every
commit keeps every page it replaced, and nothing came back until the API
server's compaction, five to ten minutes later, let go. Memory grew
alongside.

The change log already stores the state before every change (ADR-010).
So any revision can be rebuilt from any later snapshot by undoing the
changes in between, the same mechanism ADR-012 uses inside a group. That
makes old snapshots a speed-up, not the history itself:

- `--snapshots`, one minute by default, is how long QuantyDB keeps
  commits. Reading a revision inside it is a snapshot read.
- `--retain`, one hour by default, is how long the change log is kept.
  Reading a revision inside it but outside the snapshot window starts
  from the oldest snapshot left and undoes the log back to it.
- Neither drops anything an etcd client may still read: the log keeps
  everything after the compaction revision, whatever the windows say.

The commit index now records each commit's time too, so `--ago` and
`history` work for the whole window after the snapshots are gone. A test
writes from eight threads, then reads a scan and every key at every
revision against a model, once with all snapshots and once after all but
one are collected.

**The cost:** an old read is no longer one snapshot lookup. It costs a
scan of the log from that revision to the oldest snapshot and memory for
every key changed in between. The API server reads at old revisions
rarely, mostly to resume a paginated list a few seconds old, which is
inside the snapshot window. And "the revision is a QuantyDB commit"
holds only for the last minute; beyond it, the history is QuantyKube's
own log on top of QuantyDB.

## ADR-015: Two malloc arenas for the server

A soak run, a k3s control plane on QuantyKube under steady writes for
eight minutes, showed the server's memory growing by about 7 MB a minute
and not stopping: 18 MB to 74 MB. The store alone, driven in a loop with
retention, stayed flat at 7 MB, so the growth was in the server around
it. Rerunning with `MALLOC_ARENA_MAX=2` cut it to a curve that levelled
off near 26 MB.

The cause is glibc's allocator, not a leak in the code: it gives
contending threads arenas of their own, up to eight per core, and memory
freed into one arena is not reused by a thread allocating from another.
A gRPC call is read on one thread, handled on a pool thread and answered
on a third, so memory moves between arenas all day. The server now caps
glibc at two arenas through `mallopt` at startup, from the syscall crate
(ADR-013).

**The cost:** some contention inside malloc under heavy parallel load,
which the benchmarks will show if it matters, and a dependency on glibc's
behavior that a musl build would not share.

## ADR-016: Layers are unpacked with every path resolved inside the root

`src/image.rs` applies OCI image layers to a directory, with QuantyCore's
streaming gzip and tar readers underneath (its ADR-007). A layer is
input from a registry, and the classic way to break out of a container
is a layer that escapes the directory it is unpacked into.

`..` in a path is the easy half, refused outright. The hard half is
symlinks: a lower layer creates `a -> /etc`, a higher one writes
`a/passwd`, and an unpacker that lets the kernel follow the link writes
the host's file. Refusing to write through symlinks is not an option,
because real images depend on it: on any distribution with a merged
`/usr`, `/lib` is a symlink and later layers write through it. So every
parent component is resolved by this code, inside the root: an absolute
target starts over at the root, `..` stops at it, and more than 40 links
is an error, the kernel's own limit. Only the last component is created
by a syscall, never followed. A test builds exactly that attack with GNU
tar and fails if a single file lands outside; it was shown to fail when
resolution was handed back to the kernel.

Whiteouts follow the OCI image spec: `.wh.name` removes `name` from the
layers below, and `.wh..wh..opq` empties a directory of what lower
layers put there but not of what the same layer already wrote. Device
nodes are skipped: the runtime builds `/dev` itself.

Checked against real images, alpine, busybox and nginx with eight
layers, all fetched with verified digests: the unpacked alpine is
identical, file for file, to what GNU tar unpacks, and all three run in
`quantykube sandbox`.

**The cost:** a lookup per path component per entry, and modification
times are set on files but not on directories, which no runtime reads.

## ADR-017: A pod is three bind mounted namespaces, and runtime state is not kept

`src/runtime.rs` is the core of the node runtime: images, pods and
containers in the shape the CRI asks for, without the gRPC around them.

**A pod sandbox holds no process.** containerd keeps a pause container
in every pod so that the pod's network, IPC and UTS namespaces outlive
its containers. Here a short lived child creates them and sets the pod's
hostname, the runtime bind mounts the three namespace files into the
pod's directory, and the child exits; containers join them with
`setns`. Releasing the pod unmounts them. CRI-O can do the same and
notes the same limit: a pod that shares one PID namespace needs a
process, which is not supported yet.

**Each container gets an overlay** over the image's unpacked root, so
the image is never written to, and its output goes through a pipe into
the CRI log format the kubelet reads, split into partial lines past
16 KiB as containerd splits them.

**Images are imported, not pulled,** from a manifest and its blobs on
disk, every blob checked against its SHA-256 before anything is
unpacked and the image visible under its final name only once complete.
Pulling needs HTTPS and waits for QuantyCore's TLS.

**State is kept in memory.** After a restart the runtime has forgotten
its pods and the kubelet recreates them, as it does after a reboot.

**The cost:** a runtime restart kills nothing but orphans running
containers and their mounts until the node reboots; making state
durable is required before this runs anywhere real. And a pod with a
shared PID namespace is refused.

A test builds an image from this machine's own binaries with real
digests and takes a pod through its whole life as root, in CI too: two
containers share the pod's network but not the host's, logs arrive in
the CRI format, a shell that ignores SIGTERM is killed after the grace
period with 137, and nothing stays mounted after removal. A tampered
layer is refused.

## ADR-018: The runtime refuses what it cannot honour

The CRI service (`src/cri.rs`) is in place before most of what the
kubelet can ask of it. A request this runtime would carry out differently
from what it says is refused with INVALID_ARGUMENT and the reason, never
run in part: image volumes and mount propagation, devices, a terminal
or stdin, a privileged container, a user other than root, a pod on the
host's network or with a shared PID namespace, any runtime handler but
the default. Status reports NetworkReady false, because no CNI plugin is
run yet.

A container started without the volume holding its secrets, or as root
when it asked for another user, would look like it works and be wrong;
refusing shows exactly where the runtime stands. PullImage succeeds for
an image already on the node, which is what a pull would give, and
fails with UNIMPLEMENTED otherwise.

**The cost:** a pod that needs any of these does not start here until
the piece is built, and the kubelet reports the reason.

## ADR-019: Volumes are attached after the pivot

A volume is a host path the kubelet asks to see at a path inside the
container: the service account token, `/etc/hosts`, the termination log,
every hostPath and emptyDir. The target lies in the image, and the image
is not trusted. Resolve `/etc/hosts` in a root filesystem whose `/etc` is
a symlink to `/` and the mount lands on the host's own; runc has had
this class of hole more than once (CVE-2021-30465, CVE-2024-21626).

Here the child clones each source as a detached mount with `open_tree`
while it still sees the host, pivots, and only then creates the target
and its parents and attaches the mount with `move_mount`. After the pivot
an absolute symlink can only lead somewhere inside the container. A test
puts exactly such a symlink in an image and mounts through it; done the
naive way, before the pivot, it creates a directory on the host and the
test fails.

**The cost:** Linux 5.2 or newer, for the two calls. Every kernel a
current Kubernetes supports has them.

## ADR-020: cgroups follow the cgroupfs driver

The kubelet creates a cgroup per pod and sends its path as the pod's
cgroup parent; the runtime makes one cgroup per container inside it,
named by the container's id, and writes the limits the kubelet sent:
`memory.max`, `cpu.max`, `cpu.weight` converted from shares the way the
kubelet converts them, cpusets, huge pages, and the `unified` files as
they are. The child is in its cgroup before it runs anything (ADR-013),
gets a cgroup namespace rooted there, and sees it read only at
`/sys/fs/cgroup`, where Go and the JVM look for their limits. A
container the kernel killed for memory is reported as OOMKilled, read
from `memory.events`. ContainerStats and PodSandboxStats come from the
same files.

Only the cgroupfs driver is supported. The systemd driver names slices
instead of paths and needs a D-Bus client to ask systemd for them; a
slice name is refused. On a host without cgroup v2, a container with
limits is refused rather than run without them.

**The cost:** the kubelet must run with `--cgroup-driver=cgroupfs`,
which most distributions no longer default to. A pod's own cgroup and
its pids limit are the kubelet's, and stats for a pod are the sum of its
containers, without network.

## ADR-021: Exec joins the container through a second fork

ExecSync runs a command inside a running container: every exec probe
and every `kubectl exec` without a terminal. The command must see what
the container sees, so it joins the container's namespaces, enters its
root through `/proc/<pid>/root`, is placed in its cgroup, and gets the
same capabilities, `no_new_privs` and seccomp filter as the container's
first process.

Joining a PID namespace moves only the caller's future children into
it, so the joining process forks once more and waits for the command,
passing on its exit status. The command dies with it (`PR_SET_PDEATHSIG`),
which is how a timeout kills both. Output beyond 16 MiB per stream is
read and dropped, so a chatty command can neither block nor fill memory.

**The cost:** two processes per exec, where runc's `nsenter` uses the
same two. Streaming exec, attach and port forwarding need an HTTP server
of their own and are not built.

