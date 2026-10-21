# The rented cluster

The setup in ACCEPTANCE.md: three servers with Ubuntu 26.04 and cgroup
v2, upstream Kubernetes v1.37.1 set up by kubeadm (ADR-029), containerd
on every node to begin with. Nothing here has run on the servers yet.
What CI already runs on one machine is marked; the first pass on the
servers turns the rest into commands that did, and DEMO.md takes them
from there.

| Machine | Runs |
| --- | --- |
| cp | the control plane, first on etcd, then on `quantykube-store` |
| w1 | a worker, its kubelet moved onto `quantykube runtime` |
| w2 | a worker with containerd, unchanged, for comparison |

The laptop runs Windows and holds only `kubectl.exe`, a kubeconfig for
cp and `kubectl-quanty.exe` (HISTORY.md). Nothing in the demonstration
needs a shell on a server.

## Every node

As kubeadm's documentation asks: swap off, `br_netfilter` loaded,
`net.ipv4.ip_forward=1`, containerd with `SystemdCgroup = true`, and
kubeadm, kubelet and kubectl v1.37.1. CI does the same on a runner
(`.github/workflows/ci.yml`, job kubeadm).

## cp

```
kubeadm init --pod-network-cidr 10.244.0.0/16 \
  --apiserver-cert-extra-sans <public address of cp>
kubectl apply -f <flannel manifest, pinned>
```

The laptop gets `/etc/kubernetes/admin.conf` with the server address
changed to cp's public one.

Moving the store, on cp as root (ADR-035):

```
quantykube install store
```

It stops the API server, copies every key from etcd with its revisions,
starts the store under systemd on 127.0.0.1:2479 and the API server on
it. etcd keeps running untouched; `quantykube undo store` goes back to
it. CI runs both on a one-node kubeadm cluster
(`scripts/kubeadm.sh move`), and a cluster set up on the store from the
start (`scripts/kubeadm.sh external`).

## w1 and w2

```
kubeadm join <cp>:6443 --token <token> --discovery-token-ca-cert-hash <hash>
```

w1 then moves its kubelet onto `quantykube runtime`. On a kubeadm node
that means kube-proxy and the CNI's DaemonSet run under the runtime too,
and both want the host's network, and kube-proxy a privileged
container, which the runtime refuses today (ADR-018). Until that is
decided and built, w1 stays on containerd.

## What is measured

On both workers, with the same deployment pinned to each in turn: pod
start from `kubectl apply` to Ready, the memory of the runtime and its
shims per node at 10, 30 and 100 pods, and a cold image pull. The store
benchmarks from BENCHMARKS.md run again on cp, against etcd on the same
machine.
