# Acceptance for the first demonstration

Date: 17 November 2026. This file says what has to work that day. It is
written before the code so that the code has something to be measured
against, and so that anything not listed here can be recognized as not
needed yet.

What is shown is a demonstration, not the finished product. Breadth of
the etcd API is explicitly not a goal (ADR-001).

## Must

Without all of these there is no demonstration.

1. QuantyKube serves the etcd v3 gRPC API on a Unix socket, without TLS
   (ADR-004).
2. An unmodified kube-apiserver of a pinned release, version recorded in
   `docs/DEMO.md`, starts with `--etcd-servers=unix://<socket>` and its
   `/readyz` endpoint reports ok, including the hooks that create the
   default namespaces and RBAC objects.
3. `kubectl get namespaces` lists `default`, `kube-system`,
   `kube-public` and `kube-node-lease`.
4. A ConfigMap can be created, read back, changed and deleted with
   `kubectl`, and `kubectl get configmaps --watch` in a second terminal
   shows each change as it happens.
5. QuantyKube is killed with SIGKILL and restarted. The API server
   reconnects without being restarted and every object is still there.
6. The state at an earlier revision can be shown: the deleted ConfigMap
   from step 4 is read back as of a revision before the delete. This is
   the point of building on QuantyDB and the demonstration is built
   around it.
7. Measurements, because the talk announces honest benchmarks: the same
   API server under the same load, once on etcd and once on QuantyKube,
   on the same machine, comparing memory, request latency and write
   throughput. The harness and the raw results are in this repository
   and the numbers are shown whichever way they come out (ADR-002).
8. Measurements of the sandbox layer as it is today: pod start time and
   memory per pod with containerd and runc, so that the talk can show
   where the time goes before any runtime of our own exists.

## Should

9. A real small cluster: one control plane machine running the API
   server, controller manager, scheduler and QuantyKube, plus at least two
   worker machines with kubelets and an existing CRI runtime, all of them
   small rented servers. `kubectl get nodes` reports every node
   Ready, a deployment spreads its pods across the workers, and the
   cluster is driven from the presenter's laptop. The store is still a
   single node; only the workers are many, and the talk says so.
10. `etcdctl get`, `put` and `watch` work against QuantyKube for the keys
    the API server writes, for showing the raw store on stage.
11. Added on 20 October, once the runtime ran a k3s node in CI: in the
    cluster of item 9, one worker runs its pods on `quantykube runtime`
    and the other on containerd, the same deployment on both, and pod
    start time and memory per node are compared there, on cgroup v2
    (BENCHMARKS.md has the first numbers and why they are not final).

## Not part of this date

TLS, more than one store node, Raft (ADR-008), the parts of the node
runtime still missing (docs/RUNTIME.md; pulls over HTTPS need TLS),
installing into an existing cluster with one command,
authentication and authorization on the etcd API, and any RPC the API
server does not call.

## How it is run on the day

- Must items 1 to 6 also run entirely on the presenter's laptop, so that
  the demonstration survives a venue network that fails. The real cluster
  from item 9 is reached through a tunnel over a phone hotspot, not over
  the venue network.
- `docs/DEMO.md` contains the exact commands, and they are run verbatim
  from a clean machine at least five times before the day. A step that is
  only ever run from memory is not part of the demonstration.
- A recording of one complete, successful run exists by 10 November. If
  anything fails live, the recording is shown instead.
