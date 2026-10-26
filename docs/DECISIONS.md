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
- [ADR-022](#adr-022-pod-networks-come-from-cni-plugins) Pod networks come from CNI plugins
- [ADR-023](#adr-023-users-resolve-inside-the-image-and-non-root-means-no-capabilities) Users resolve inside the image, and non-root means no capabilities
- [ADR-024](#adr-024-the-seccomp-filter-is-dockers-allowlist) The seccomp filter is Docker's allowlist
- [ADR-025](#adr-025-pulls-go-through-the-import) Pulls go through the import
- [ADR-026](#adr-026-without-a-device-filter-there-is-no-mknod) Without a device filter there is no MKNOD
- [ADR-027](#adr-027-raft-before-17-november-with-a-cut-off) Raft before 17 November, with a cut off
- [ADR-028](#adr-028-history-is-served-through-the-kubernetes-api) History is served through the Kubernetes API
- [ADR-029](#adr-029-the-demonstration-and-the-installer-target-kubeadm) The demonstration and the installer target kubeadm
- [ADR-030](#adr-030-a-failed-commit-stops-the-store) A failed commit stops the store
- [ADR-031](#adr-031-over-a-network-the-store-speaks-tls-and-wants-a-client-certificate) Over a network the store speaks TLS and wants a client certificate
- [ADR-032](#adr-032-raft-is-a-state-machine-without-io-tested-by-a-simulator-that-finds-planted-bugs) Raft is a state machine without I/O, tested by a simulator that finds planted bugs
- [ADR-033](#adr-033-store-nodes-talk-over-tls-in-frames-not-grpc-with-certificates-from-a-peer-ca) Store nodes talk over TLS in frames, not gRPC, with certificates from a peer CA
- [ADR-034](#adr-034-the-store-on-raft-every-write-a-command-in-the-log-the-log-in-a-file-of-its-own) The store on Raft: every write a command in the log, the log in a file of its own
- [ADR-035](#adr-035-a-cluster-moves-from-etcd-with-its-revisions-while-the-api-server-is-stopped) A cluster moves from etcd with its revisions, while the API server is stopped
- [ADR-036](#adr-036-the-runtime-runs-what-a-kubeadm-node-needs-host-namespaces-added-capabilities-privileged) The runtime runs what a kubeadm node needs: host namespaces, added capabilities, privileged
- [ADR-037](#adr-037-ci-publishes-a-public-image-of-every-commit-on-main) CI publishes a public image of every commit on main

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

*Its date is superseded by ADR-027: Raft is attempted before 17 November.
How it is built stands.*

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

*Its date is superseded by ADR-029: the installer is attempted before 17
November, for kubeadm first. The shape stands.*

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

ADR-024 replaced the 47 calls with Docker's allowlist.

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
notes the same limit: a PID namespace ends with its first process, so a
pod whose containers share one (`shareProcessNamespace`) gets a holder
process that only reaps orphans, a fork of the runtime that closes every
descriptor it inherited. Its containers are cloned into that namespace
by the thread that starts them, which joins it for the one clone and
returns.

**Each container gets an overlay** over the image's unpacked root, so
the image is never written to, and its output goes through a pipe into
the CRI log format the kubelet reads, split into partial lines past
16 KiB as containerd splits them.

**Images are imported** from a manifest and its blobs on disk, and a
pull is an import of what was fetched (ADR-025). Manifests and configs
are checked against their SHA-256 before they are read; a layer is
hashed while it is unpacked into a staging directory, read from disk in
pieces so that no layer is ever whole in memory, and a layer that does
not match its digest throws the staging directory away. The image is
visible under its final name only once complete. Pulling nginx this way
peaks at 6 MiB of memory.

**State is kept in memory.** A restart of the runtime is treated as a
reboot of the node: at start it kills the containers a previous run left,
found by pid and start time so that a reused pid is never hit, takes
their pods off the network with the CNI result saved for each, releases
their namespaces and cgroups, and the kubelet starts the pods again.

**The cost:** every pod on a node restarts when the runtime does, where
containerd keeps them running through its shims. Keeping them would need
a process per container that holds its output pipe and its exit status,
which is what a shim is.

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
or stdin, a privileged container, a pod on the host's network, any
runtime handler but the default.

A container started without the volume holding its secrets would look
like it works and be wrong; refusing shows exactly where the runtime
stands.

**The cost:** a pod that needs any of these does not start here until
the piece is built, and the kubelet reports the reason.

*Added on 21 October,* after a review found fields that were decoded and
then ignored: sysctls, SELinux labels, a supplemental groups policy
other than Merge, a user namespace (`hostUsers: false`, which would
otherwise run as root on the host while believing it does not) and
joining another container's PID namespace are refused too. AppArmor is
the one exception and is still ignored: on hosts with AppArmor the
kubelet can ask for the default profile for pods that name none, so
refusing it could refuse every pod there. Profiles are being built; until they
are applied, pods here run without the confinement containerd gives
them on such hosts, and the comparison says so.

*Superseded in part by ADR-036 on 26 October:* a pod on the host's
network or in its IPC or PID namespace, added capabilities and a
privileged container are now run, as containerd runs them.

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
same files, and UpdateContainerResources rewrites them in place, which is
how the kubelet resizes a running pod.

Only the cgroupfs driver is supported. The systemd driver names slices
instead of paths and needs a D-Bus client to ask systemd for them; a
slice name is refused. On a host without cgroup v2, a container with
limits is refused rather than run without them.

RuntimeConfig tells the kubelet so, and a kubelet that asks, as current
ones do, needs no flag.

**The cost:** a kubelet that does not ask must run with
`--cgroup-driver=cgroupfs`, which most distributions no longer default
to. A pod's own cgroup and
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

## ADR-022: Pod networks come from CNI plugins

Every Kubernetes network, Flannel, Calico, Cilium and the rest, is
installed as a CNI configuration in `/etc/cni/net.d` and plugin binaries
in `/opt/cni/bin`, and every runtime runs them the same way: the pod's
network namespace and the command in the environment, the configuration
on stdin, the result on stdout. This runtime does the same with the
first configuration by name, adds a pod after creating its namespaces
and deletes it when the pod stops, in reverse plugin order, with the
result of the add. A pod whose network cannot be set up is not created.

Each pod's loopback is brought up by the runtime itself when the
namespace is made, so containers of a pod reach each other on
`127.0.0.1` with or without a CNI network.

Status reports NetworkReady only once a configuration exists, which is
how the kubelet learns that the network add-on has been installed.

The configuration is looked for until one is found, so a network add-on
installed after the runtime started, as they usually are, is picked up.

**The cost:** once found, the configuration is kept, where containerd
watches the directory; a changed network needs a restart of the
runtime. CNI's CHECK and GC commands are not run, and of the plugin
capabilities only port mappings are passed, to plugins such as
`portmap` that declare them, which is how a pod's `hostPort` works.

## ADR-023: Users resolve inside the image, and non-root means no capabilities

A container runs as the user the kubelet sends (`runAsUser`,
`runAsGroup`, a user name, supplemental groups), and otherwise as the
image's `User`. Names are looked up in the image's own `/etc/passwd` and
`/etc/group`, never the host's, and the groups a user is a member of
there are added, as runc does. An image user that does not exist is an
error, not root. ImageStatus reports the image's user so that the
kubelet can enforce `runAsNonRoot` before a container starts.

A process that is not root starts with no capabilities at all, the
bounding set still limited to the default set and `no_new_privs`
set, which is what Docker and containerd give it. The ids are set with
raw system calls: glibc's wrappers would try to synchronise threads that
exist only in the parent.

StartContainer now returns only once the container's process has called
execve, read from a pipe that closes on exec. Without that, an exec
right after a start could join a mount namespace still being set up.

**The cost:** user namespaces are not used, so root in a container is
root on the host, held in only by capabilities, seccomp and the mounts.

## ADR-024: The seccomp filter is Docker's allowlist

ADR-013's filter refused 47 calls and let the rest through. It now
allows what Docker's default profile allows and refuses everything else
with EPERM, as Docker does: a call the kernel gains next year is refused
until someone decides it is safe, instead of allowed until someone
notices that it is not.

The list is `seccomp/default.json` in moby/profiles as of commit
6fe7deb1b9fb, read for x86_64 and the fourteen default capabilities.
That is 306 calls without condition, `chroot` among them because
CAP_SYS_CHROOT is kept, and none of the groups for capabilities a
container does not have. Three more are allowed on their first argument,
as in the profile: `clone` without namespace flags, `socket` for the 32
families Docker lists, which leaves out AF_ALG, AF_VSOCK and the old
radio and LAN protocols, and `personality` for the five values it lists.
`clone3` still gets ENOSYS. The numbers are written out by hand; a test
checks them against the kernel headers on the machine, and another runs
the program over every call number and the arguments that matter.

Five calls the old list refused are allowed now, because Docker allows
them: `ptrace`, `process_vm_readv` and `process_vm_writev` on kernels
from 4.8, where ptrace can no longer be used to get around seccomp;
`clock_adjtime`, which only reads without CAP_SYS_TIME; and
`name_to_handle_at`, whose handles are of no use while
`open_by_handle_at` stays refused.

Left out: Docker admits i386 and x32 calls on x86_64, and this filter
still kills a process that makes either, as ADR-013's did. No argument
check of the profile is skipped on x86_64, and the clone check is the
stricter one, since it also refuses CLONE_NEWTIME. Nothing is logged:
the profile has no SCMP_ACT_LOG rule, and a refused call shows only as
the EPERM the program gets.

**The cost:** the list follows Docker's by hand, and a program that
needs a call Docker refuses fails here as it does there; starts are no
slower (3.73 against 3.74 ms median over 300 alternating starts), since
the kernel caches its answer for calls allowed whatever their arguments.

## ADR-025: Pulls go through the import

PullImage fetches an image over the OCI distribution API
(`src/registry.rs`): the manifest or index by tag or digest, an index
resolved to linux/amd64, a Bearer token fetched anonymously when the
registry challenges for one, and blobs followed through the redirects
registries use to send them to a storage host, which never gets the
registry's token. What it fetches is written as an OCI image layout and
imported from there (ADR-017), so a digest is checked in exactly one
place whichever way an image arrives, and a registry that serves the
wrong bytes produces an error, not an image. References are read the
way Docker reads them: `busybox` is `docker.io/library/busybox:latest`.

HTTP goes through QuantyCore's HTTP/1.1 client and HTTPS through its
TLS, which is unreviewed (QuantyCore ADR-003), against the system's root
certificates. Behind a proxy, HTTPS is tunnelled with CONNECT through
the proxy `HTTPS_PROXY` names, except for hosts in `NO_PROXY`, the way
containerd and most other clients read them. Registries named with
`--plain-http` are reached without TLS, as a local registry in an air
gapped network often is. A test pulls through a CONNECT proxy from a
registry whose certificate comes from a CA made for the test, and is
refused when that CA is not trusted; by hand, the same binary pulls from
GCR, registry.k8s.io, GHCR and Quay through an intercepting proxy.

Credentials from an image pull secret are sent only to the token
endpoint, as HTTP Basic, or to a registry that asks for Basic itself; a
token issued by the registry is used as it is. They never follow a
redirect to another host.

**The cost:** the registry and the proxy are trusted with whatever TLS
does not catch until the TLS has been reviewed; identity tokens, which
need an OAuth2 exchange, are refused; and plain HTTP is never sent
through the proxy.

## ADR-026: Without a device filter there is no MKNOD

A review of the runtime against what a container can reach found that
Docker's default capabilities include MKNOD, and Docker can afford it
because every container also gets a device cgroup that refuses to open
any node but the standard ones. This runtime has no such filter, so with
MKNOD a container could make a node for the host's disk in its own
`/dev` and open it: root in a container is root on the host here
(ADR-023). MKNOD is therefore not in the default set, cannot be added
back, and the kubelet's capability lists are honoured otherwise: drops
always, adds only within the default set, since most capabilities
beyond it lead out of a container without a user namespace. Device
nodes in image layers were already skipped when unpacking (ADR-016).

The same review closed three smaller holes. A log path from the kubelet
that is absolute or climbs out with `..` is refused, since the runtime
writes the log as root. A registry login goes to the token realm the
registry names only over TLS or on the registry's own host. And image
references whose repository has empty, `.` or `..` parts are refused.

A seccomp profile of the node's own (Localhost) is refused; Unconfined,
which the kubelet sends for every pod that names no profile, gets the
default filter, as with the kubelet's `--seccomp-default`.

The device filter followed the same evening: every container cgroup
gets an eBPF program of type cgroup device, as runc loads one, that lets
a container make or open only the standard character devices and the
pty slaves. A test interprets the program for allowed and refused
devices, and in CI a container that holds MKNOD can neither make a
node for a disk nor open one, while `/dev/null` works. MKNOD stays out
of the default set all the same: two locks where one would do.

**The cost:** a program that makes device nodes fails here where it
works under Docker, and a pod that asks for Unconfined to make a call
Docker's profile refuses fails too.

## ADR-027: Raft before 17 November, with a cut off

ADR-008 put several store nodes after the first demonstration. Two
things changed. Most companies that would run QuantyKube run a highly
available store today and will not consider one that is not, so the
talk without it answers the first question with a plan. And the
reasons for waiting are gone: HTTP/2, gRPC and Watch are built, and
QuantyCore has TLS for the transport between the store nodes.

Raft is built as ADR-008 says: a deterministic simulator first, in which
several members run in one process against a simulated network and
disks, driven by a seed, and which must find bugs planted in it before
the protocol is written against it. Then three processes on one machine
with real partitions in CI, then the three rented servers, one member
on each.

The store fixes from the review of 20 October come first, since Raft
replicates exactly that write path, and so does the store over TCP with
TLS and client certificates, which Raft, the installer and the
aggregated API (ADR-028) all need.

**The cut off** is 7 November. If the simulator and the partition test
in CI do not pass by then, the demonstration shows a single store node
and the talk says why. A Raft that loses a write on stage would cost
more than one that is not shown.

**The cost:** three weeks of work go into the part of the project that
is hardest to get right, alongside everything else for the same date,
and the comparison with etcd has to be made again, three members
against three.

## ADR-028: History is served through the Kubernetes API

`kubectl quanty` reached the History service over the store's Unix
socket, so it had to run on the machine of the store, as root. On stage
the cluster is driven from a Windows laptop that holds a kubeconfig and
nothing else, and at a company nobody gets a shell on the control plane
to bring back a ConfigMap.

The store registers an aggregated API with the API server, an
APIService under its own group, and serves history, reads at a revision
and restores over HTTPS. The API server authenticates the caller,
authorizes with RBAC against that group, and forwards the request with
the caller's identity, which the store checks against the API server's
client certificate. `kubectl-quanty` becomes a thin program that calls
`kubectl get --raw` and `kubectl create --raw`, so it needs no TLS and
no kubeconfig parsing of its own, and it is built for Windows in CI.

The Unix socket keeps History for the node itself and for tests.

Rejected: a TCP endpoint of the store with client certificates for the
plugin, since every user would need a certificate from the store and
none of the cluster's access control would apply; and SSH to the
control plane, for the reason above.

**The cost:** the store becomes an HTTPS server on the cluster network
that the API server must reach, and a restore is now something RBAC
can grant, so the default roles must not grant it.

*Built on 21 October* as described in docs/HISTORY.md: the History
service's own protobuf messages over HTTP/1.1, which the store offers
by ALPN so that kube-apiserver does not ask for HTTP/2; reads as `get`
on `logs` and `objects`, a restore as `create` on `restores`. In CI an
unmodified kube-apiserver forwards to it, `kubectl-quanty` lists a
ConfigMap's history and brings it back, and a user without a role is
refused.

## ADR-029: The demonstration and the installer target kubeadm

The cluster on stage is upstream Kubernetes set up with kubeadm, on
three servers with Ubuntu 26.04. k3s was the first target because it is
one binary and starts in seconds, and it stays in CI for that reason,
but it is a distribution, and the question a k3s demonstration invites
is whether it works with real Kubernetes. Nothing in the store or the
runtime depends on the distribution: they serve the etcd v3 API and
the CRI.

The installer (ADR-009) does depend on it, since every distribution
keeps the API server's flags and the kubelet's configuration somewhere
else. kubeadm is the reference for clusters people run themselves, and
it makes the change clean: the API server is a static pod whose
manifest the kubelet watches, so pointing `--etcd-servers` at
QuantyKube restarts it. kubeadm is the first installer target and the
only one for 17 November; k3s follows.

**The cost:** kubeadm needs more memory and more steps than k3s, and CI
gets a kubeadm cluster as an additional job.

*Added on 21 October:* the CI job (`scripts/kubeadm.sh`) runs
`kubeadm init` with QuantyKube as an external etcd, on the certificates
kubeadm makes for a local one. kubeadm asks an external etcd for
`/version` over HTTP/1.1 before it uses it, as monitoring asks for
`/health`, so the store's TLS port answers both besides gRPC (ADR-031).

## ADR-030: A failed commit stops the store

When a commit fails, from a full disk or an I/O error, the store
refuses every write after it, and `quantykube serve` exits. A restart
recovers what is on disk, and the API server reconnects to the restarted
store as it does after `kill -9`, the path the crash test exercises 200
times in CI.

A review found that carrying on was wrong in two ways. The record of a
failed group commit was a single slot, so when the next commit failed
too before the first one's writers woke, those writers were told they
had succeeded, and their revision was handed out again. And when
QuantyDB wrote its meta page but the sync after it failed, every later
commit failed while the store kept answering reads, and a restart
brought back the writes that had been reported as failed. After a
failed commit, the state in memory and the state on disk can differ in
ways only recovery from the disk sorts out. etcd does the same: a
failed write to its log or backend is fatal.

Rejected: retrying the commit, which is new code on a path that almost
never runs and so is never really tested; and staying up read only,
which answers reads from a state the disk may not hold.

**The cost:** a full disk stops the store instead of leaving it
readable, and the restart fails too until there is space. A quota that
refuses writes before the disk is full, as etcd's NOSPACE alarm does, is
the friendlier half and comes later.

## ADR-031: Over a network the store speaks TLS and wants a client certificate

*Supersedes ADR-004 where the store is reached over a network; the Unix
socket stays as it was.*

`quantykube serve --listen` serves the same etcd API over TCP with
QuantyCore's TLS, as etcd does on port 2379. Every client must present a
certificate from the CA given with `--client-ca`; a connection without
one ends in the handshake. On the node the socket's mode was the only
access control (ADR-004), and over a network the certificate takes its
place: whoever holds one can read and write every object, which is what
an etcd client certificate means too. kube-apiserver connects with its
usual `--etcd-cafile`, `--etcd-certfile` and `--etcd-keyfile`.

Raft between store nodes (ADR-027), the API server on another machine,
and the installer (ADR-009) all need this. In CI an unmodified
kube-apiserver runs against the store over TLS, and `etcdctl` reads,
writes and watches over it, and is refused without a certificate.

The gRPC server reads a connection on one thread and writes on another.
The TLS state of a connection is shared between them behind a lock that
each holds only while it moves bytes through it, never while it waits
on the network.

Rejected: a token or password on top of TLS, which etcd's clients
support but the API server does not use for etcd.

**The cost:** the TLS is QuantyCore's and has not been reviewed by
anyone who did not write it (QuantyCore ADR-003), and it is now the
lock on the whole cluster's data whenever the store is reached over a
network. Until the review, a store reachable over a network should
also be fenced in by the network itself.

*Added on 21 October:* a client that speaks HTTP/1.1 gets `/version`
and `/health` as etcd answers them, and a client that starts with the
HTTP/2 preface gets gRPC. The first bytes decide, not the handshake:
kubeadm offers h2 there and then speaks HTTP/1.1. Nothing else is
served over HTTP/1.1, and the client certificate is required the same
way.

## ADR-032: Raft is a state machine without I/O, tested by a simulator that finds planted bugs

The Raft node (`src/raft/mod.rs`) never touches a disk, a clock or a
network. Its host calls `tick`, `step` with a message and `propose`
with a command, and after each call takes a `Ready`: first the term,
vote and log entries to make durable, then the messages to send, then
the committed entries to apply. This is how etcd's raft library is
built, and for the same reason: the protocol can then run anywhere,
in particular in a simulator.

The simulator (`src/raft/sim.rs`, ADR-008) runs a cluster in one
process from one seed: message delays, reordering, loss, duplication,
partitions into up to three parts or around one node, crashes that
keep only what was made durable, restarts, pauses in which a node
neither ticks nor hears anything, fault rates that differ from seed to
seed, and faults aimed at the moments that matter, as FoundationDB's
buggify aims them: a new leader cut off before it replicates, a leader
dying right after a commit, a node dying right after it voted, a leader
cut off or paused while its clients keep reading from it. It checks at
every step that no term has two leaders, that every index is applied as
the same entry everywhere, and that a read sees every command
acknowledged before it began; and at the end, with the network healed,
that the cluster makes progress and every acknowledged command is
applied on every node.

It counts only because it finds what was planted: a second vote in a
term, a vote for a candidate whose log is behind, committing an entry
of an earlier term by counting replicas (Figure 8 of the Raft paper,
found only after the aimed faults were added), keeping entries that
conflict with the leader's, a host that sends before its disk has what
the message promises, a read answered without asking a majority, a
read answered before the leader committed an entry of its own term, and
a snapshot holding entries that were not yet applied.
Each is found within 300 seeds, the rarest in about one seed of 25; the
correct node passed 20,000. Every CI run checks the planted bugs and
5,000 seeds per cluster size. The simulator found one real fault on the
way: a reply claiming more of the log than the leader holds made it
index past its log, which it now ignores. A second one showed only in
a debug build: an index one before the log's start, which a release
build wrapped around harmlessly by luck. The deep run in CI therefore
keeps overflow checks on.

What the node does: elections with pre-vote, so that a node returning
from a partition cannot unseat a working leader; check quorum, so that
a leader that cannot reach a majority steps down; a node that heard
from a leader within the election timeout refuses to vote for another;
replication in batches with fast backtracking; a new leader's empty
entry to commit its term; linearizable reads through ReadIndex, which
a follower asks of its leader; a log the host compacts once its state
holds what the entries said, and a snapshot for a follower that needs
more than the log still has. The host carries the snapshot's data; in
the simulator, 200,000 snapshots were installed over 20,000 runs.

**What it does not do yet, and what that costs:** membership is fixed
when the cluster starts, so a node is replaced by restarting all of
them with the new list; a leader sends the next batch only once the
last is acknowledged, so a follower far behind catches up one round
trip per batch.

## ADR-033: Store nodes talk over TLS in frames, not gRPC, with certificates from a peer CA

Raft messages between store nodes go over one TLS connection to each
peer, as protobuf messages each prefixed with its length
(`src/raft/transport.rs`), on a port of their own. Both ends show a
certificate, and the peers' certificates come from a CA of their own,
the peer CA, as etcd separates its peer certificates from its client
certificates.

gRPC was the obvious choice and was rejected for now: QuantyCore's
gRPC client makes one unary call per connection, and Raft wants a
stream that stays open with messages flowing both ways. Frames on a
TLS connection are a few dozen lines and give exactly that. If
QuantyCore gets a streaming client, the frames can become a gRPC
stream without the protocol noticing.

A sender drops what is queued for a peer while the peer cannot be
reached, and a full queue drops too: Raft sends again, and a stale
message reaching a peer that came back is worse than none.

A follower that gets a write from a client passes the command to its
leader inside Raft, as etcd does, and answers the client when it has
applied the command itself; a node answers reads after a ReadIndex
from its leader. Every node applies the same commands in the same
order, so every node computes the same answer.

**The cost:** a separate peer CA is one more thing to set up, and a
client certificate from it is a key to the cluster's log, not only to
its data. Anyone with a client certificate from the client CA must not
also hold one from the peer CA. Using one CA for both, which small
setups will want, makes every etcd client able to speak Raft.

## ADR-034: The store on Raft: every write a command in the log, the log in a file of its own

`quantykube serve --node <id> --peers ...` makes the store one node of
a cluster. Every request that changes the store, Put, DeleteRange, Txn,
Compact, LeaseGrant, LeaseRevoke and History's Restore, becomes a
command in the Raft log: the method, the request as the client sent it,
the time it was taken, and which node and caller wait for it. Every
node applies the commands in log order through the same code that
serves them on a single node, so revisions, answers and watch events
are the same everywhere, and the node a client asked answers it when it
applies the command itself. A follower passes commands to its leader
inside Raft. Reads wait for a ReadIndex unless the client asked for a
serializable read; watches are served from each node's own store.

Every write a command makes carries the command's Raft index into the
store, in the same commit, so that a node restarting knows exactly what
it already applied and applies nothing twice. A command that writes
nothing leaves no index behind; applied again after a crash it meets
the same state it met the first time, since nothing changed in between,
and does nothing again. The check in CI: the killed leader comes back
with the same revisions as the others. Without the index it came back
three revisions ahead.

Leases follow etcd: the node that takes a grant picks the id, so every
node grants the same lease; only the leader decides that a lease ran
out, and revokes it through the log; a new leader first gives every
lease its full TTL again. A keepalive refreshes the lease only on the
node it reaches, which is the leader's for kube-apiserver in practice
and not always for other clients.

The Raft log lives in a QuantyDB file next to the store's, `<data>.raft`
(option a of 21 October). It is the simplest separation, and it costs
a second fsync per write: one for the log, one when the command is
applied to the store. Writing the log into the store's own file, so
that applying and remembering the log share one commit (option b),
waits for the measurement on the rented servers.

*Decided on 26 October:* option a stays for 17 November. It is built,
runs in CI under SIGKILL and snapshots, and keeps the store's file
usable on its own. Option b is the optimisation to try once the
servers show where a write's time goes; etcd itself keeps a WAL apart
from its data file. The price: on a cluster every write costs two
fsyncs, which the talk's numbers say.

In CI three nodes run on one machine against the real `etcdctl` and an
unmodified kube-apiserver given all three endpoints: writes through
every node, reads and a watch through another, the same revision on
every node, the leader killed with SIGKILL while the others go on, and
the killed node coming back and catching up.

A node cuts its Raft log once 10,000 entries are applied beyond the
last cut, keeping the last 5,000, as etcd's snapshot count does. A
follower that needs more than the leader's log still holds gets a
snapshot: the leader's live keys and metadata as of its last applied
command, without history, in one message. The follower replaces its
store with it, ends every watch on it as compacted so that its clients
list again, and reads before the snapshot's revision are refused as
compacted there; history on that node begins at the snapshot.

**What it does not do yet, and what that costs:** a snapshot travels as
one frame of at most 256 MiB, so a larger store cannot bring a follower
up yet. A command passed to a leader that dies before appending it
is lost, and the client sees a timeout and sends it again, which etcd
clients do. The store's history timestamps are each node's own clock at
apply time, not the command's, so `--ago` can differ by milliseconds
between nodes.

## ADR-035: A cluster moves from etcd with its revisions, while the API server is stopped

ADR-009 left open whether the store keeps etcd's revision numbers when
a cluster moves onto it. It keeps them. `quantykube migrate` reads
every key of the etcd at one revision, page by page at that revision,
and writes them into a new store file with their own create and mod
revisions and versions, the store's head at that revision and
everything before it compacted. The leases the keys hold come along
with the TTL they were granted, and get it in full, as after any
restart of etcd; one that ended after the read gets a second, so its
keys go as they would have in etcd.

Kept revisions are what make the move invisible to clients: every
resourceVersion a controller or kubelet holds stays valid, and every
write after the move gets a higher one. With numbers starting over,
a client holding a resourceVersion from before would be ahead of the
store and be refused until the store caught up with it.

Nothing writes while it copies: the API server is the only client of a
kubeadm cluster's etcd, and it is stopped first by moving its static
pod manifest out of the kubelet's directory. etcd itself stays as it
was, untouched, so the way back is to point the API server at it
again. `migrate` refuses a store file that exists, so a cluster is
never copied over another.

In CI (`scripts/migrate.sh`) an unmodified kube-apiserver writes into a
real etcd, is stopped, the keys are copied, and the API server started
on QuantyKube finds every object at its resourceVersion, events with
their lease among them.

**The cost:** the API server is down for the copy and its restart,
seconds for a cluster of the usual size. Its clients' watches are told
to list again, as after any restart of the API server, since its watch
cache starts anew. History in QuantyKube begins at the move. The way back copies
every key from the store into etcd, which numbers writes itself: it
is first written to until its revision passes the store's, so no
client is left holding a resourceVersion etcd has not reached. That
costs one write per revision the store went past etcd, and the copy
into etcd is not atomic; the API server stays stopped until it is
done, and stays on the store if it fails.

*How a node is moved:* `quantykube install store`, as root on a
kubeadm control plane node, does it from files alone. It keeps the API
server's manifest in a journal under `/var/lib/quantykube/install`,
moves the manifest out of the kubelet's directory and waits until no
kube-apiserver process is left, migrates from the etcd the manifest
names, with the API server's own etcd client certificate, starts the
store as the systemd unit `quantykube-store` on 127.0.0.1:2479 with the
certificates kubeadm made for etcd, and puts the manifest back with only
`--etcd-servers` changed. It writes each step to the journal before it
takes it, and on any failure after the API server was stopped it puts
it back on etcd. `quantykube undo store` does that from the journal
alone, without the API server, and keeps the store's file under a new
name. The `kubectl apply` of ADR-009 is to run this command on the
node; the command is what CI tests (`scripts/kubeadm.sh move`).

## ADR-036: The runtime runs what a kubeadm node needs: host namespaces, added capabilities, privileged

On a kubeadm node kube-proxy and the CNI's daemon run as DaemonSets on
the host's network, kube-proxy as a privileged container, the CNI's
with NET_ADMIN and NET_RAW added. k3s runs both inside its own process,
which is why nothing asked for them before. Without them w1 has no
Service network, and a pod there cannot resolve a name.

The runtime now does what containerd does. A pod on the host's network
gets no network or UTS namespace of its own and no CNI call; one in
the host's IPC or PID namespace gets none of those. Capabilities a
container adds are added. A privileged container gets every capability
the runtime itself holds, no seccomp filter, no no_new_privs, `/sys`
and its cgroup writable, nothing of `/proc` masked or made read only,
and every character and block device of the host in its `/dev`, with
no device filter in its cgroup. Ambient capabilities stay refused.

**The cost:** a pod that asks for any of this is confined as little as
under containerd, which is to say a privileged pod owns the node. What
keeps ordinary workloads from asking is the cluster's admission, Pod
Security Admission at `baseline` or stricter for every namespace but
kube-system, as on any cluster with containerd. The runtime's own
default for pods that ask for nothing is unchanged.

## ADR-037: CI publishes a public image of every commit on main

`kubectl apply` of the installer (ADR-009) can only bring QuantyKube
to a node as a container image, so CI builds one on every push to main,
once the tests and both kubeadm jobs have passed, and pushes it to
`ghcr.io/quantyroot/quantykube`, tagged with the commit and `main`. It
is public, so a cluster pulls it without credentials. The image is the
release binary on distroless's `cc-debian13`: glibc and nothing else,
no shell. The binary is built on the runner, whose glibc is older than
the image's and the nodes', so `install store` can copy it onto the host
from inside the container.

**The cost:** every commit on main is public as a runnable image, not
only as source. The manifest names an image by commit, never `main`,
so a cluster runs what was tested. The base image is named by tag, not
digest; pinning it is a follow-up.

