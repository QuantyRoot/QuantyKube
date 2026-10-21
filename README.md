# QuantyKube

Two parts of Kubernetes, rewritten in Rust against the standard library
and other QuantyRoot crates only: a data store with an undo button that
speaks the etcd v3 API the API server uses and keeps history after the
API server compacts it, and a node runtime the kubelet starts containers
through, with no shim and no pause container per pod.

[![CI](https://github.com/QuantyRoot/QuantyKube/actions/workflows/ci.yml/badge.svg)](https://github.com/QuantyRoot/QuantyKube/actions/workflows/ci.yml)
![Status](https://img.shields.io/badge/status-early-red)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Made by Elchi](https://img.shields.io/badge/made%20by-Elchi-8A2BE2)](https://github.com/Elchi-dev)

## Read this first

QuantyKube is not a general etcd replacement. It implements the part of
the etcd v3 API that the Kubernetes API server uses, on one node or on
several joined by Raft with a membership fixed at start. Other etcd
clients may call things that are not there
([ADR-001](docs/DECISIONS.md)); kine makes the same choice.

It is early. Everything marked built below is exercised in CI against the
real `etcdctl`, an unmodified `kube-apiserver`, a control plane set up by
kubeadm and one by k3s (`scripts/e2e.sh`, `scripts/kubeadm.sh`), and
against SIGKILL under load (`tests/crash.rs`).
Nothing has run in production yet.

The second half is a node runtime, the program the kubelet starts
containers through. In CI a k3s node runs on both halves at once, its
API server on the store and its kubelet on the runtime, and an unchanged
kubelet starts a pod with a ConfigMap volume, a service account token,
CPU and memory limits, a pod network, an exec liveness probe, a user
other than root and dropped capabilities (`scripts/cluster.sh`).

## What it does

```
$ kubectl delete configmap important
configmap "important" deleted from default namespace
$ kubectl quanty history configmap/default/important
     218  2026-10-20 15:25:32Z  create  /registry/configmaps/default/important  220 bytes
     243  2026-10-20 15:25:33Z  update  /registry/configmaps/default/important  288 bytes
     251  2026-10-20 15:25:34Z  delete  /registry/configmaps/default/important  0 bytes
$ kubectl quanty restore --rev 243 configmap/default/important
/registry/configmaps/default/important: restored as of revision 243, now revision 253
$ kubectl get configmap important -o jsonpath={.data.password}
correct horse
```

etcd numbers every change to a cluster with a revision and throws old
revisions away when the API server compacts, every five minutes.
QuantyKube stores data in the QuantyDB engine, where every commit is an
immutable snapshot, and keeps them for a window you choose, an hour by
default ([ADR-011](docs/DECISIONS.md)). Inside that window you can list
what happened to any object, read it as it was at a revision or a time,
and put it back. A restore is an ordinary write, so controllers and
watches see the object return.

## Status

| Part | State |
| --- | --- |
| etcd v3 subset used by kube-apiserver, Unix socket ([docs/RPCS.md](docs/RPCS.md)) | built |
| Watch, with replay from a past revision and progress notifications | built |
| Leases, with expiry | built |
| Group commit: concurrent writes share fsyncs ([ADR-012](docs/DECISIONS.md)) | built |
| History beyond compaction: `history`, `get`, `restore` | built |
| `kubectl quanty` plugin | built |
| Benchmarks against etcd ([docs/BENCHMARKS.md](docs/BENCHMARKS.md)) | measured |
| Retention: long history in the log, short lived snapshots ([ADR-014](docs/DECISIONS.md)) | built, soak tested |
| The store over a network with TLS and client certificates ([ADR-031](docs/DECISIONS.md)) | built, unreviewed TLS |
| Node runtime: layer unpacking and a hardened sandbox ([docs/RUNTIME.md](docs/RUNTIME.md)) | built, measured against runc |
| A k3s node on the store and the runtime together ([scripts/cluster.sh](scripts/cluster.sh)) | runs in CI |
| Node runtime: the CRI service, pods and containers with crictl | built with volumes, limits, stats, exec probes, CNI, pulls over HTTPS; pod start measured against containerd |
| Several store nodes (Raft): a simulator that finds planted bugs, three nodes under kube-apiserver with the leader killed, log compaction and snapshots ([ADR-032 to 034](docs/DECISIONS.md)) | built, runs in CI |
| kubeadm with the store as its etcd ([scripts/kubeadm.sh](scripts/kubeadm.sh)) | runs in CI |
| Moving a kubeadm cluster from etcd and back: `migrate`, `install store`, `undo store`, every object at its resourceVersion ([ADR-035](docs/DECISIONS.md)) | runs in CI; writes made on the store are not copied back on undo yet |

## Trying it

Rust 1.89 or newer.

```
cargo build --release
target/release/quantykube serve --data /var/lib/quantykube/db --socket /run/quantykube.sock
kube-apiserver --etcd-servers=unix:///run/quantykube.sock ...
```

With k3s, `--datastore-endpoint=unix:///run/quantykube.sock`. For the
kubectl plugin, put the binary on the PATH as `kubectl-quanty`:

```
ln -s "$PWD/target/release/quantykube" /usr/local/bin/kubectl-quanty
kubectl quanty history --prefix configmap/default/
```

`etcdctl --endpoints=unix:///run/quantykube.sock` works for looking at
the raw keys.

The runtime, on a node with cgroup v2 and CNI plugins, for a kubelet
started with `--container-runtime-endpoint=unix:///run/quantykube-cri.sock`
and `--cgroup-driver=cgroupfs`:

```
target/release/quantykube runtime --root /var/lib/quantykube-runtime \
  --socket /run/quantykube-cri.sock
```

Images are pulled from their registries, or put there with `quantykube
import`. **The TLS under those pulls is QuantyCore's own and has not been
reviewed by anyone who did not write it**; until it has, treat a pull as
no safer than the network it crosses.

## How it is built

- [docs/DECISIONS.md](docs/DECISIONS.md): every decision that might be
  regretted, with what it costs and what was rejected.
- [docs/ACCEPTANCE.md](docs/ACCEPTANCE.md): what has to work for the first
  public demonstration, written before the code.
- [docs/DEMO.md](docs/DEMO.md): that demonstration, as commands.
- [docs/RUNTIME.md](docs/RUNTIME.md): the node runtime.
- The protocol stack, protobuf, HPACK, HTTP/2, gRPC, HTTP/1.1 and TLS, lives in
  [QuantyCore](https://github.com/QuantyRoot/QuantyCore); the storage
  engine is [QuantyDB](https://github.com/QuantyRoot/QuantyDatabase).

## License

Apache License 2.0, see [LICENSE](LICENSE).

QuantyKube is a project by Elchi Studios.
