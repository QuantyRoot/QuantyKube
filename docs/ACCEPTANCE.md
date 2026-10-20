# Acceptance for the first demonstration

Date: 17 November 2026. This file says what has to work that day. It was
first written before the code so that the code has something to be
measured against, and so that anything not listed here can be recognized
as not needed yet.

Rewritten on 21 October. The first version planned a single store node,
a demonstration that could run entirely on the presenter's Linux laptop,
and no installer. The plan changed: the laptop runs Windows and only
drives the cluster, the cluster is three rented servers, and the store
and the installer are attempted for this date, each with a cut off date
after which it is shown as a plan instead (ADR-027).

## The setup

| Machine | Runs |
| --- | --- |
| cp | `quantykube serve`, the Kubernetes control plane |
| w1 | a kubelet that uses `quantykube runtime` |
| w2 | a kubelet with containerd, unchanged, for comparison |

All three are rented servers with Ubuntu 26.04 and cgroup v2, and the
cluster is upstream Kubernetes set up with kubeadm at a pinned release
(ADR-029). k3s stays in CI because it starts fast; the demonstration
uses what most people run. If the store runs as
several nodes (item 11), one store member runs on each machine. The
presenter's laptop runs Windows and holds `kubectl.exe`, a kubeconfig and
`kubectl-quanty.exe`, nothing else: no SSH session is part of the
demonstration.

## Must

Without all of these there is no demonstration.

1. QuantyKube serves the etcd v3 gRPC API that an unmodified
   kube-apiserver uses, at a pinned release recorded in `docs/DEMO.md`,
   and its `/readyz` reports ok.
2. From the laptop, `kubectl get nodes` reports all three machines
   Ready, and a deployment spreads its pods across w1 and w2.
3. A ConfigMap can be created, read back, changed and deleted with
   `kubectl`, and `kubectl get configmaps --watch` in a second window
   shows each change as it happens.
4. QuantyKube is killed with SIGKILL and restarted. The API server
   reconnects without being restarted and every object is still there.
5. Undo, from the laptop: the deleted ConfigMap from item 3 is listed
   with `kubectl quanty history` and brought back with `kubectl quanty
   restore`. The history is served through the Kubernetes API (an
   aggregated API, ADR-028), so the laptop needs nothing but its
   kubeconfig, and access follows RBAC.
6. Pods on w1 run on `quantykube runtime`: one process for the runtime
   and none per pod, images pulled over HTTPS by the runtime itself.
7. Measurements, because the talk announces honest benchmarks, shown
   whichever way they come out (ADR-002):
   - the store against etcd: memory, request latency, write and read
     throughput, one member against one and, if item 11 passes, three
     against three on the same machines;
   - w1 against w2 with the same deployment: pod start time and memory
     per pod at 10, 30 and 100 pods, and a cold image pull;
   - every known difference that favours one side named next to the
     number (BENCHMARKS.md), in particular any confinement one side
     applies and the other does not.
8. Everything the store and the runtime say is correct is correct under
   the API server's use: no lost or reordered watch events, no response
   whose revision disagrees with its data, no write acknowledged that is
   not on disk. The issues found in the review of 20 October are fixed
   with a test each before anything new is built on the store.

## Should, each with a cut off on 7 November

What is not ready by then is not shown running; the talk shows the plan
and says why.

9. Pods on w1 run under an AppArmor profile applied by the runtime, the
   RuntimeDefault profile and Localhost profiles, compiled by QuantyKube's
   own compiler. If that compiler is not ready, the runtime loads its
   profile through the host's `apparmor_parser`, and the talk says so.
10. The installer (ADR-009): on a kubeadm cluster that runs on etcd
    and containerd, one `kubectl apply` of a published manifest moves
    the store to QuantyKube and w1 to `quantykube runtime`, node by
    node, and an undo puts both back. Only kubeadm is in scope for this
    date (ADR-029).
11. Several store nodes with Raft (ADR-027): three members, one per
    machine. A member is killed on stage and the cluster keeps working;
    it comes back and catches up. Before it is shown, a deterministic
    simulator with crashes, partitions and delays has run in CI without
    finding a lost acknowledged write or two leaders in one term.
12. `etcdctl get`, `put` and `watch` work against QuantyKube for the keys
    the API server writes, from cp, for showing the raw store.

## Not part of this date

Managed clusters (EKS, GKE, AKS), installers for distributions other than
kubeadm, Windows nodes, user namespaces in pods, authentication on the etcd
API beyond client certificates, and any RPC the API server does not call.

## How it is run on the day

- The laptop reaches cp's API server over the venue network if it allows
  it, otherwise over a phone hotspot. The API server also listens on
  port 443 for networks that allow nothing else.
- `docs/DEMO.md` contains the exact commands, and they are run verbatim
  from the laptop at least five times before the day. A step that is
  only ever run from memory is not part of the demonstration.
- A recording of one complete, successful run exists by 10 November.
  Items 9 to 11 are in it only if they passed their cut off; the
  recording is redone once if they pass later. If anything fails live,
  the recording is shown instead.
