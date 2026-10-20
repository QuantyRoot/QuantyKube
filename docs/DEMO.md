# The demonstration

The commands for 17 November, in order. ACCEPTANCE.md says they are run
verbatim from a clean machine before the day; this file is what gets run.
Everything below has been run as written against k3s v1.37.1 (control
plane only) and etcd v3.6.15's `etcdctl`, which `scripts/e2e.sh` also
uses.

The real cluster on rented servers (ACCEPTANCE.md item 9) gets its own
section once the servers exist.

## Pinned versions

| What | Version | Where from |
| --- | --- | --- |
| k3s | v1.37.1+k3s1 | github.com/k3s-io/k3s releases |
| etcdctl | v3.6.15 | github.com/etcd-io/etcd releases |
| QuantyKube | the tagged commit of the day | this repository |

## Before going on stage

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
ln -sf "$PWD/target/release/quantykube" /usr/local/bin/kubectl-quanty
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
v2 and the CNI plugins (v1.8.0) under `/opt/cni/bin`. Stop the k3s from
step 2 first, two k3s on one machine collide (BENCHMARKS.md).

```
skopeo copy docker://busybox:1.36 oci:$D/busybox
sudo target/release/quantykube import --root $D/rt $D/busybox test/busybox:1
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
kubectl run hello --image=test/busybox:1 --image-pull-policy=Never \
  -- sh -c 'echo hello from QuantyKube; exec sleep 3600'
kubectl get pod hello -o wide
kubectl logs hello
ps -o pid,rss,cmd -C quantykube
```

One process for the runtime and none per pod: no shim, no pause
container. `scripts/cluster.sh` runs the same in CI; these exact lines
have not been run yet, which this machine (cgroup v1) cannot, and are
the first thing to run on the rented servers.

## 7. The numbers

The benchmark tables from `docs/BENCHMARKS.md`, whichever way they came
out.

## If something fails live

Stop, say so, and play the recording (ACCEPTANCE.md).
