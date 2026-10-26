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
kubeadm, kubelet and kubectl v1.37.1: `scripts/kubeadm-node.sh`, which
CI runs on its runners too.

## cp

```
kubeadm init --pod-network-cidr 10.244.0.0/16 \
  --apiserver-cert-extra-sans <public address of cp>
kubectl apply -f <flannel manifest, pinned>
```

The laptop gets `/etc/kubernetes/admin.conf` with the server address
changed to cp's public one.

## w1 and w2

```
kubeadm join <cp>:6443 --token <token> --discovery-token-ca-cert-hash <hash>
```

On w1, kube-proxy and the CNI's DaemonSet run on the runtime too, on
the host's network, kube-proxy privileged (ADR-036).

## Onto QuantyKube

From the laptop, once w1 and w2 have joined (ADR-040):

```
kubectl label node w1 install.quantyroot.io/runtime=quantykube
kubectl apply -f https://raw.githubusercontent.com/QuantyRoot/QuantyKube/<commit>/deploy/quantykube.yaml
kubectl get installation quantykube -w
```

The operator moves the API server from etcd onto the store (ADR-035),
then w1's kubelet from containerd onto `quantykube runtime` (ADR-038):
cordon, drain, the move, Ready on the new runtime, uncordon. w2 is not
labelled and stays on containerd. Back to how it was:

```
kubectl patch installation quantykube --type merge -p '{"spec":{"undo":true}}'
```

etcd and containerd keep running untouched the whole time; the undo
copies what was written on the store back into etcd. CI runs all of it
on a one-node kubeadm cluster with the published image of each commit
(job kubeadm-apply), and the pieces on their own: the store as etcd
from the start, every pod on the runtime, the store moved and back, the
kubelet moved and back (`scripts/kubeadm.sh external|runtime|move|node`).

## What is measured

On both workers, with the same deployment pinned to each in turn: pod
start from `kubectl apply` to Ready, the memory of the runtime and its
shims per node at 10, 30 and 100 pods, and a cold image pull. The store
benchmarks from BENCHMARKS.md run again on cp, against etcd on the same
machine.
