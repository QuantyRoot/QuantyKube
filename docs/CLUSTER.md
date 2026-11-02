# The rented cluster

The setup in ACCEPTANCE.md: three servers with Ubuntu 26.04 and cgroup
v2, upstream Kubernetes v1.37.1 set up by kubeadm (ADR-029), containerd
on every node to begin with. Nothing here has run on the servers yet.
What CI already runs on one machine is marked; the first pass on the
servers turns the rest into commands that did, and DEMO.md takes them
from there.

| Machine | Name | Runs |
| --- | --- | --- |
| cp | cp.knet1.quantyverse.net | the control plane, first on etcd, then on `quantykube-store`; Prometheus and Grafana |
| w1 | w1.knet1.quantyverse.net | a worker, its kubelet moved onto `quantykube runtime` |
| w2 | w2.knet1.quantyverse.net | a worker with containerd, unchanged, for comparison |

Grafana is at https://grafana.knet1.quantyverse.net.

The laptop runs Windows and holds only `kubectl.exe`, a kubeconfig for
cp and `kubectl-quanty.exe` (HISTORY.md). Nothing in the demonstration
needs a shell on a server.

## Every node

As root, on each fresh server:

```
hostnamectl set-hostname cp    # w1, w2 on the others
git clone https://github.com/QuantyRoot/QuantyKube && cd QuantyKube
scripts/kubeadm-node.sh
```

It installs what kubeadm's documentation asks for, kept across a
reboot: containerd with `SystemdCgroup = true`, kubeadm, kubelet,
kubectl and crictl v1.37.1 and the CNI plugins, pinned by hash, swap
off, `br_netfilter` and `net.ipv4.ip_forward=1`. CI runs the same.

## cp

```
scripts/kubeadm-cp.sh 62.129.150.40 cp.knet1.quantyverse.net
```

kubeadm init with the public address and name in the API server's
certificate, Flannel v0.28.9 pinned by hash, and
`/root/laptop.kubeconfig` with the name in it, for the laptop. It
prints the join command.

## w1 and w2

The join command cp printed, as root. On w1, once it is moved, kube-proxy
and Flannel run on QuantyKube's runtime too, on the host's network,
kube-proxy privileged and Flannel with NET_ADMIN (ADR-036).

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

## Monitoring

Outside the cluster, so that moving the cluster does not move it:

```
scripts/monitoring.sh node                       # on every node
scripts/monitoring.sh cp grafana.knet1.quantyverse.net \
    cp=62.129.150.40 w1=62.129.150.64 w2=62.129.150.83   # on cp
```

node_exporter and process-exporter on every node count memory and CPU
per program; Prometheus on cp scrapes them every five seconds and
listens only there; Grafana behind Caddy with HTTPS shows the dashboard
from `deploy/grafana`, with a random admin password in
`/root/grafana-admin-password` on cp.

## What is measured

On both workers, with the same deployment pinned to each in turn: pod
start from `kubectl apply` to Ready, the memory of the runtime and its
shims per node at 10, 30 and 100 pods, and a cold image pull. The store
benchmarks from BENCHMARKS.md run again on cp, against etcd on the same
machine.
