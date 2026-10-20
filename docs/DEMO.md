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
target/release/quantykube history --data $D/db /registry/configmaps/default/important
```

The history lists the create, the update and the delete with their
revisions and times. Take the revision of the update:

```
target/release/quantykube get --data $D/db --rev <rev> \
  /registry/configmaps/default/important \
  | etcdctl --endpoints=unix://$D/qk.sock put /registry/configmaps/default/important
kubectl get configmap important -o jsonpath='{.data.password}'
```

The deleted object is back, and the watch from step 3 shows it arriving.
This works after the API server has compacted etcd, for as long as
`--retain` keeps history (ADR-011).

## 6. The numbers

The benchmark tables from `docs/BENCHMARKS.md`, whichever way they came
out.

## If something fails live

Stop, say so, and play the recording (ACCEPTANCE.md).
