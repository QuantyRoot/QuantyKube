# QuantyKube Design Decisions

Short ADRs. Newest at the bottom. If a decision gets reversed, strike it
through and add a new entry, never silently edit history.

## Index

- [ADR-001](#adr-001-kubernetes-first-the-etcd-subset-the-api-server-uses) Kubernetes first: the etcd subset the API server uses
- [ADR-002](#adr-002-honesty-rule-for-public-claims) Honesty rule for public claims
- [ADR-003](#adr-003-no-third-party-crates-the-engine-comes-from-quantydb) No third-party crates, the engine comes from QuantyDB
- [ADR-004](#adr-004-plaintext-over-a-unix-socket-until-quantycore-has-tls) Plaintext over a Unix socket until QuantyCore has TLS
- [ADR-005](#adr-005-sandbox-means-the-node-runtime-behind-cri) Sandbox means the node runtime behind CRI

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
the socket's file mode and owner. A CIS style benchmark scan will flag the
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
