# The rented cluster

The plan for the setup in ACCEPTANCE.md: three servers, cgroup v2, k3s
v1.37.1. Nothing here has run yet; the first pass on the servers turns
each step into commands that did, and DEMO.md takes them from there.

The cluster on stage is kubeadm on Ubuntu 26.04 (ADR-029). The steps
below are written for k3s and are rewritten for kubeadm before the
servers exist; what they check stays the same.

| Machine | Runs |
| --- | --- |
| cp | `quantykube serve`, `k3s server --disable-agent` on it |
| w1 | `k3s agent`, its kubelet on `quantykube runtime` |
| w2 | `k3s agent` with its own containerd, unchanged |

## cp

```
quantykube serve --data /var/lib/quantykube/db --socket /run/quantykube.sock &
k3s server --disable-agent --token <token> \
  --datastore-endpoint=unix:///run/quantykube.sock \
  --tls-san <public address of cp>
```

The laptop gets the kubeconfig from `/etc/rancher/k3s/k3s.yaml`, with the
server address changed to cp's.

## w1, on QuantyKube's runtime

k3s's agent runs flannel and writes its CNI configuration and plugins
under its data directory; the runtime takes them from there and finds
the configuration once flannel has written it (ADR-022).

```
quantykube runtime --root /var/lib/quantykube-runtime \
  --socket /run/quantykube-cri.sock \
  --cni-conf /var/lib/rancher/k3s/agent/etc/cni/net.d \
  --cni-bin /var/lib/rancher/k3s/data/current/bin &
k3s agent --server https://<cp>:6443 --token <token> \
  --container-runtime-endpoint unix:///run/quantykube-cri.sock \
  --kubelet-arg cgroup-driver=cgroupfs
```

To check first, in this order: that the two CNI paths are where k3s
v1.37.1 puts them; and that the agent leaves out its own containerd
when given an endpoint. Images are pulled over HTTPS by the runtime
itself; `quantykube import` from an OCI layout is the fallback.

## w2, the comparison

```
k3s agent --server https://<cp>:6443 --token <token>
```

## What is measured

On both workers, with the same deployment pinned to each in turn: pod
start from `kubectl apply` to Ready, the memory of the runtime and its
shims per node at 10, 30 and 100 pods, and a cold image pull. The store
benchmarks from BENCHMARKS.md run again on cp.
