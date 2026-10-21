#!/usr/bin/env bash
# Upstream Kubernetes set up by kubeadm on this machine.
#
# external: QuantyKube is its external etcd from the start: the control
# plane as kubeadm makes it, a pod reached through a Service by its DNS
# name, a deleted object restored from the history, and the store killed
# with SIGKILL under the running cluster and started again.
#
# move: kubeadm sets it up on its own etcd, `quantykube install store`
# moves the API server onto QuantyKube with every object at its
# resourceVersion, the cluster goes on, and `quantykube undo store` puts
# it back on etcd.
#
# Needs root, systemd, containerd with its CRI plugin, kubeadm, kubelet
# and kubectl on the PATH and the CNI plugins in /opt/cni/bin. It changes
# this machine for good: for a CI runner or a server set up to be wiped.
#
#   [QK=target/release/quantykube] scripts/kubeadm.sh external|move
set -euo pipefail

mode=${1:?external or move}

QK=$(realpath "${QK:-target/release/quantykube}")
pki=/etc/kubernetes/pki
data=/var/lib/quantykube
sock=/run/quantykube/store.sock
work=$(mktemp -d)
export KUBECONFIG=/etc/kubernetes/admin.conf
image=mirror.gcr.io/library/busybox@sha256:
image+=b7f3d86d6e84fc17718c48bcde1450807faa2d56704205c697b4bd5df7b9e29f
qk=
trap '[ -z "$qk" ] || kill "$qk" 2>/dev/null || true' EXIT

fail() {
    printf 'FAIL: %s\n' "$*"
    kubectl get nodes,pods -A -o wide 2>&1 || true
    kubectl get events -A --sort-by=.lastTimestamp 2>&1 | tail -30 || true
    for p in $(kubectl get pods -A --no-headers 2>/dev/null |
        awk '{split($3, r, "/"); if (r[1] != r[2]) print $1 "/" $2}'); do
        printf '\n--- %s\n' "$p"
        kubectl -n "${p%%/*}" logs "${p#*/}" --tail=20 2>&1 || true
    done
    printf '\n--- kube-proxy\n'
    kubectl -n kube-system logs -l k8s-app=kube-proxy --tail=20 2>&1 || true
    printf '\n--- store\n'
    tail -40 "$work/qk.log" 2>/dev/null ||
        journalctl -u quantykube-store --no-pager | tail -40
    printf '\n--- kubelet\n'; journalctl -u kubelet --no-pager | tail -60
    exit 1
}

# retry <seconds> <command>...
retry() {
    local n=$1; shift
    for _ in $(seq "$n"); do
        "$@" >/dev/null 2>&1 && return 0
        sleep 1
    done
    fail "$* did not succeed in ${n}s"
}

start() {
    mkdir -p "$data" "$(dirname "$sock")"
    "$QK" serve --data "$data/db" --socket "$sock" --listen 127.0.0.1:2379 \
        --cert "$pki/etcd/server.crt" --key "$pki/etcd/server.key" \
        --client-ca "$pki/etcd/ca.crt" 2>>"$work/qk.log" &
    qk=$!
    retry 30 curl -sf --cacert "$pki/etcd/ca.crt" \
        --cert "$pki/apiserver-etcd-client.crt" \
        --key "$pki/apiserver-etcd-client.key" https://127.0.0.1:2379/health
}

cat >"$work/kubeadm.yaml" <<EOF
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
nodeRegistration:
  criSocket: unix:///run/containerd/containerd.sock
---
apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
networking:
  podSubnet: 10.244.0.0/16
EOF
if [ "$mode" = external ]; then
    cat >>"$work/kubeadm.yaml" <<EOF
etcd:
  external:
    endpoints: [https://127.0.0.1:2379]
    caFile: $pki/etcd/ca.crt
    certFile: $pki/apiserver-etcd-client.crt
    keyFile: $pki/apiserver-etcd-client.key
EOF
    # the store takes the certificates kubeadm makes for a local etcd
    kubeadm init phase certs etcd-ca
    kubeadm init phase certs etcd-server
    kubeadm init phase certs apiserver-etcd-client
    start
fi
# one node, so a bridge is the whole pod network
mkdir -p /etc/cni/net.d
cat >/etc/cni/net.d/10-bridge.conflist <<'EOF'
{
  "cniVersion": "1.0.0",
  "name": "pods",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "isDefaultGateway": true,
      "ipam": {"type": "host-local", "ranges": [[{"subnet": "10.244.0.0/24"}]]}
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
EOF
kubeadm init --config "$work/kubeadm.yaml" >"$work/init.log" 2>&1 ||
    { cat "$work/init.log"; fail "kubeadm init"; }
kubectl taint nodes --all node-role.kubernetes.io/control-plane- >/dev/null
kubectl wait --for=condition=Ready node --all --timeout=180s >/dev/null ||
    fail "the node is not ready"
kubectl -n kube-system rollout status deploy/coredns --timeout=180s \
    >/dev/null || fail "CoreDNS is not ready"
echo "kubeadm init ($mode): ok"

if [ "$mode" = move ]; then
    manifest=/etc/kubernetes/manifests/kube-apiserver.yaml
    cp "$manifest" "$work/original.yaml"
    for i in $(seq 20); do
        kubectl create configmap "before-$i" --from-literal=v="$i" >/dev/null
    done
    objects() {
        local cols=K:.kind,NS:.metadata.namespace,N:.metadata.name
        cols+=,RV:.metadata.resourceVersion
        kubectl get configmaps,deployments,services,namespaces -A \
            -o custom-columns="$cols" \
            --no-headers | awk '{print $1 "/" $2 "/" $3, $4}' | sort
    }
    objects >"$work/before"
    "$QK" install store >"$work/install.log" 2>&1 ||
        { cat "$work/install.log"; fail "install store"; }
    grep -q -- '--etcd-servers=https://127.0.0.1:2479' "$manifest" ||
        fail "the API server is not pointed at the store"
    objects >"$work/after"
    # what the controllers wrote since may differ; every object from
    # before is there at its resourceVersion or a newer one
    while read -r name rv; do
        now=$(awk -v n="$name" '$1 == n {print $2}' "$work/after" | head -1)
        [ -n "$now" ] && [ "$now" -ge "$rv" ] ||
            fail "$name was at $rv and is now at '$now'"
    done <"$work/before"
    for i in 1 20; do
        v=$(kubectl get configmap "before-$i" -o jsonpath='{.data.v}')
        [ "$v" = "$i" ] || fail "before-$i is '$v'"
    done
    "$QK" install store >/dev/null 2>&1 &&
        fail "a second install was not refused"
    kubectl create configmap after-move --from-literal=v=x >/dev/null
    kubectl -n kube-system scale deploy coredns --replicas=3 >/dev/null
    kubectl -n kube-system rollout status deploy/coredns --timeout=120s \
        >/dev/null || fail "the controllers did not act on the store"
    on_store=$(kubectl get configmap after-move \
        -o jsonpath='{.metadata.resourceVersion}')
    echo "install store: ok"

    "$QK" undo store >"$work/undo.log" 2>&1 ||
        { cat "$work/undo.log"; fail "undo store"; }
    cmp -s "$manifest" "$work/original.yaml" ||
        fail "the manifest is not the original"
    v=$(kubectl get configmap before-20 -o jsonpath='{.data.v}')
    [ "$v" = 20 ] || fail "before-20 after the undo is '$v'"
    # what was written on the store is copied back, under revisions
    # past the store's, so no client holds a resourceVersion from the
    # future
    v=$(kubectl get configmap after-move -o jsonpath='{.data.v}')
    [ "$v" = x ] || fail "after-move is not back in etcd: '$v'"
    rv=$(kubectl get configmap after-move \
        -o jsonpath='{.metadata.resourceVersion}')
    [ "$rv" -gt "$on_store" ] ||
        fail "after-move is at $rv in etcd, $on_store on the store"
    n=$(kubectl -n kube-system get deploy coredns -o jsonpath='{.spec.replicas}')
    [ "$n" = 3 ] || fail "the scale on the store is not in etcd: $n"
    systemctl is-active -q quantykube-store && fail "the store still runs"
    echo "undo store: ok"
    exit 0
fi

serve='mkdir -p /www && echo hello-from-web >/www/index.html &&
    exec httpd -f -p 8080 -h /www'
kubectl create deployment web --image="$image" -- sh -c "$serve" >/dev/null
kubectl expose deployment web --port 80 --target-port 8080 >/dev/null
kubectl rollout status deploy/web --timeout=120s >/dev/null ||
    fail "web is not ready"
kubectl run probe --image="$image" --restart=Never -- \
    wget -qO- http://web.default.svc.cluster.local >/dev/null
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/probe \
    --timeout=120s >/dev/null || fail "the probe pod did not succeed"
got=$(kubectl logs probe)
[ "$got" = hello-from-web ] || fail "the probe read '$got'"
echo "a Service by its DNS name: ok"

kubectl create configmap important --from-literal=v=1 >/dev/null
key=/registry/configmaps/default/important
rev=$("$QK" history --socket "$sock" "$key" | awk 'NR==1 {print $1}')
kubectl delete configmap important >/dev/null
"$QK" restore --socket "$sock" --rev "$rev" "$key" >/dev/null
retry 30 kubectl get configmap important
[ "$(kubectl get configmap important -o jsonpath='{.data.v}')" = 1 ] ||
    fail "the restored configmap is not the one deleted"
echo "a deleted object restored from the history: ok"

version() { kubectl get deploy web -o jsonpath='{.metadata.resourceVersion}'; }
before=$(version)
kill -9 "$qk"
wait "$qk" 2>/dev/null || true
start
retry 120 kubectl get --raw /readyz
[ "$(version)" = "$before" ] ||
    fail "the deployment changed across the restart"
kubectl create configmap after-restart --from-literal=v=2 >/dev/null
kubectl scale deploy web --replicas=2 >/dev/null
kubectl rollout status deploy/web --timeout=120s >/dev/null ||
    fail "the controllers did not act after the restart"
echo "the store killed and started under the cluster: ok"
