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

## Should

7. A complete single node cluster: controller manager, scheduler and a
   kubelet with an existing CRI runtime, on one of Elchi's machines.
   `kubectl get nodes` reports the node Ready, and
   `kubectl create deployment` results in a running pod.
8. `etcdctl get`, `put` and `watch` work against QuantyKube for the keys
   the API server writes, for showing the raw store on stage.

## Not part of this date

TLS, more than one node, Raft, the node runtime (ADR-005), authentication
and authorization on the etcd API, and any RPC the API server does not
call.

## How it is run on the day

- Everything runs on one machine that is physically present. Nothing
  depends on the venue network.
- `docs/DEMO.md` contains the exact commands, and they are run verbatim
  from a clean machine at least five times before the day. A step that is
  only ever run from memory is not part of the demonstration.
- A recording of one complete, successful run exists by 10 November. If
  anything fails live, the recording is shown instead.
