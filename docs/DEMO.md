# The demonstration

The commands for 17 November, in order. ACCEPTANCE.md says they are run
verbatim from a clean machine before the day; this file is what gets run.
Everything below has been run as written against k3s v1.37.1 (control
plane only) and etcd v3.6.15's `etcdctl`, which `scripts/e2e.sh` also
uses.

The talk itself runs against knet1, the rented cluster (CLUSTER.md),
from the Windows laptop: the first section. The local demonstration
after it is the fallback when knet1 cannot be reached from the room.

## On stage: knet1 from the laptop

Everything in PowerShell on the laptop, with the kubeconfig for knet1
as the current context, except where it says cp. The manifest is the
one pinned for the day: the tag `meetup-2026-11-17`, whose
`deploy/meetup.yaml` names the image by its commit, so nothing that is
pushed afterwards changes what runs.

### The evening before, and again an hour before

On cp, as root, in the checkout:

```
export KUBECONFIG=/etc/kubernetes/admin.conf
kubectl get installation quantykube
```

If it says anything but `Undone` or nothing at all, undo first:

```
kubectl patch installation quantykube --type merge -p 'spec: {undo: true}'
kubectl get installation quantykube -w
```

Then the cluster as it was before QuantyKube, without a trace of the
installer, so that `kubectl apply` on stage is the first one; and the
load:

```
kubectl delete -f deploy/quantykube.yaml --ignore-not-found --wait=true
kubectl get nodes -o wide
scripts/load.sh up w1 w2
```

Every node shows `containerd://`, and the API server is on etcd:

```
grep etcd-servers /etc/kubernetes/manifests/kube-apiserver.yaml
```

says `https://127.0.0.1:2379`. On the laptop:

```
kubectl get nodes -o wide
kubectl quanty history configmap/default/important
```

The first works; the second fails, since there is no history API yet.
That is right. Open the dashboard "QuantyKube on stage" on
https://grafana.knet1.quantyverse.net, last 15 minutes, and leave it
for ten minutes so that the "before" level is on the screen.
`kubectl-quanty.exe` comes from the CI artifact `kubectl-quanty-windows`
and lies next to `kubectl.exe` on the PATH.

### 1. The cluster as it is

```
kubectl get nodes -o wide
kubectl get pods -A --field-selector spec.nodeName=w1 | Measure-Object -Line
```

Three nodes on containerd, the API server on etcd, 60 pods on each
worker and 30 more coming and going. On the dashboard: what containerd
needs for that, the processes it runs, the store's memory, and how long
30 pods take to start.

### 2. One kubectl apply

```
kubectl label node w1 w2 install.quantyroot.io/runtime=quantykube
kubectl apply -f https://raw.githubusercontent.com/QuantyRoot/QuantyKube/meetup-2026-11-17/deploy/meetup.yaml
kubectl get installation quantykube -w
```

The operator moves the API server from etcd onto the store, then w1
and w2 one after the other: cordon, drain, the new runtime, uncordon.
The API server is away for a few seconds during the store's move;
`-w` then prints an error and has to be started again. Talk over the
slides meanwhile; the whole move took MOVE_TIME in the rehearsals.
`Done` ends it. On the dashboard the lines fall once the pods are back
on each node.

### 3. Undo of a single object

```
kubectl create configmap important --from-literal=password=hunter2
kubectl patch configmap important -p 'data: {password: correct-horse}'
kubectl delete configmap important
kubectl get configmap important
kubectl quanty history configmap/default/important
```

The patches are YAML, which kubectl takes as well as JSON, so that no
shell on Windows mangles their quotes. The history
lists create, update and delete. With the revision of the update:

```
kubectl quanty restore --rev <rev> configmap/default/important
kubectl get configmap important -o jsonpath='{.data.password}'
```

says `correct-horse`.

### 4. The way back

```
kubectl patch installation quantykube --type merge -p 'spec: {undo: true}'
kubectl get installation quantykube -w
```

Everything as it was: etcd with what was written on the store, every
node on containerd. It takes about as long as the move; the talk ends
while it runs, and the dashboard shows the lines climbing back.

### After the talk

On cp: `scripts/load.sh down`.

### If something fails

- `kubectl` from the laptop gets no answer: the room's network blocks
  port 6443. Phone hotspot; else `ssh root@62.129.150.40` and the same
  commands there; else the recording.
- The installation says `Failed`: its message says why. The undo in
  step 4 puts everything back; then the recording.
- `kubectl quanty history` says the server could not find the resource:
  the history API is not registered. Show the store's own view on cp
  (`quantykube history --socket /run/quantykube/store.sock
  /registry/configmaps/default/important`), then go on.

## Pinned versions

| What | Version | Where from |
| --- | --- | --- |
| k3s | v1.37.1+k3s1 | github.com/k3s-io/k3s releases |
| etcdctl | v3.6.15 | github.com/etcd-io/etcd releases |
| QuantyKube | the tagged commit of the day | this repository |

## The fallback: on the laptop alone

### Before going on stage

```
export D=/tmp/qk-demo
rm -rf $D && mkdir -p $D
cargo build --release
```

## 1. A store that is not etcd

```
target/release/quantykube serve --data $D/db --socket $D/qk.sock &
etcdctl --endpoints=unix://$D/qk.sock endpoint status -w table
```

It answers as an etcd 3.6 member: the API server checks the version to
decide which features it may use.

## 2. A real Kubernetes control plane on top

```
sudo k3s server --disable-agent --token demo --data-dir $D/k3s \
  --datastore-endpoint=unix://$D/qk.sock --write-kubeconfig $D/k.yaml \
  --bind-address 127.0.0.1 \
  --disable=traefik,servicelb,metrics-server,local-storage,coredns \
  --disable-network-policy --flannel-backend=none --disable-kube-proxy \
  --disable-cloud-controller --disable-helm-controller &
export KUBECONFIG=$D/k.yaml
kubectl get --raw /readyz
kubectl get namespaces
etcdctl --endpoints=unix://$D/qk.sock get /registry/namespaces --prefix --keys-only
```

Ready in about two seconds here. The last command shows the same objects
as raw keys in QuantyKube.

## 3. Watch works

In a second terminal:

```
kubectl get configmaps --watch
```

Then:

```
kubectl create configmap important --from-literal=password=hunter2
kubectl patch configmap important -p '{"data":{"password":"correct horse"}}'
```

## 4. Kill the store

```
kill -9 %1
target/release/quantykube serve --data $D/db --socket $D/qk.sock &
kubectl get configmap important -o jsonpath='{.data.password}'
```

The API server reconnects on its own. Nothing was lost: every answer the
store gave was on disk before it was given.

## 5. Undo

```
kubectl delete configmap important
kubectl get configmap important
export QUANTYKUBE_SOCKET=$D/qk.sock
sudo ln -sf "$PWD/target/release/quantykube" /usr/local/bin/kubectl-quanty
kubectl quanty history configmap/default/important
```

The history lists the create, the update and the delete with their
revisions and times. Take the revision of the update:

```
kubectl quanty restore --rev <rev> configmap/default/important
kubectl get configmap important -o jsonpath='{.data.password}'
```

The deleted object is back, and the watch from step 3 shows it arriving.
This works after the API server has compacted etcd, for as long as
`--retain` keeps history (ADR-011). `--ago 5m` instead of `--rev` picks
the state five minutes ago.

## 6. The other half: the node runtime

The control plane from step 2 runs without a node. This adds one whose
kubelet starts containers through `quantykube runtime`; it needs cgroup
v2 and the CNI plugins (v1.8.0) under `/opt/cni/bin`. The k3s from step
2 (job 2 of this shell) stops first, since two k3s on one machine
collide (BENCHMARKS.md); the store keeps running, so the cluster's
objects stay.

```
kill %2; wait %2
sudo mkdir -p $D/net.d
sudo cp scripts/demo-net.conflist $D/net.d/10-demo.conflist
sudo target/release/quantykube runtime --root $D/rt --socket $D/cri.sock \
  --cni-conf $D/net.d &
sudo k3s server --token demo --data-dir $D/k3s \
  --datastore-endpoint=unix://$D/qk.sock --write-kubeconfig $D/k.yaml \
  --container-runtime-endpoint=unix://$D/cri.sock \
  --kubelet-arg cgroup-driver=cgroupfs \
  --disable=traefik,servicelb,metrics-server,local-storage,coredns \
  --disable-network-policy --flannel-backend=none --disable-kube-proxy \
  --disable-cloud-controller --disable-helm-controller &
kubectl get nodes
kubectl run hello --image=mirror.gcr.io/library/busybox:1.36 \
  -- sh -c 'echo hello from QuantyKube; exec sleep 3600'
kubectl get pod hello -o wide
kubectl logs hello
ps -o pid,rss,cmd -C quantykube
```

One process for the runtime and none per pod: no shim, no pause
container. The runtime pulled the image itself, over HTTPS. `scripts/cluster.sh` runs the same in CI; these exact lines
have not been run yet, which this machine (cgroup v1) cannot, and are
the first thing to run on the rented servers.

## 7. The numbers

The benchmark tables from `docs/BENCHMARKS.md`, whichever way they came
out.

## If something fails live

Stop, say so, and play the recording (ACCEPTANCE.md).
