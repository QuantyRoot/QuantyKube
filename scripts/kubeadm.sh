#!/usr/bin/env bash
# Upstream Kubernetes set up by kubeadm on this machine, with QuantyKube
# as its external etcd: the control plane as kubeadm makes it, a pod
# reached through a Service by its DNS name, a deleted object restored
# from the history, and the store killed with SIGKILL under the running
# cluster and started again.
#
# Needs root, systemd, containerd with its CRI plugin, kubeadm, kubelet
# and kubectl on the PATH and the CNI plugins in /opt/cni/bin. It changes
# this machine for good: for a CI runner or a server set up to be wiped.
#
#   [QK=target/release/quantykube] scripts/kubeadm.sh
set -euo pipefail

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
    printf '\n--- store\n'; tail -40 "$work/qk.log"
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

# the store takes the certificates kubeadm makes for a local etcd
kubeadm init phase certs etcd-ca
kubeadm init phase certs etcd-server
kubeadm init phase certs apiserver-etcd-client
start

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
etcd:
  external:
    endpoints: [https://127.0.0.1:2379]
    caFile: $pki/etcd/ca.crt
    certFile: $pki/apiserver-etcd-client.crt
    keyFile: $pki/apiserver-etcd-client.key
EOF
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
echo "kubeadm init on the store: ok"

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
