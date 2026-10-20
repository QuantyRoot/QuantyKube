#!/usr/bin/env bash
# A one node k3s cluster with both halves of QuantyKube: the store behind
# the API server and the runtime behind the kubelet. Starts a pod with an
# exec probe and a ConfigMap volume, checks it runs, then deletes it.
# Needs root, cgroup v2, `ip`, and no other k3s on the machine.
#
#   K3S=/path/to/k3s IMAGE=<busybox image dir> CNI_BIN=<CNI plugins> \
#       scripts/cluster.sh
set -euo pipefail

QK=$(realpath "${QK:-target/release/quantykube}")
work=$(mktemp -d)
pids=()
cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    wait 2>/dev/null || true
    grep -o "$work[^ ]*" /proc/self/mounts | sort -r | xargs -r umount 2>/dev/null || true
    grep -o '/run/k3s[^ ]*\|/var/lib/kubelet[^ ]*' /proc/self/mounts | sort -r |
        xargs -r umount 2>/dev/null || true
    ip link del qkcluster0 2>/dev/null || true
    [ -n "${KEEP:-}" ] || rm -rf "$work"
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -20 "$work/k3s.log" "$work/runtime.log"
    exit 1
}
until_ok() {
    local what=$1 i
    shift
    for i in $(seq 120); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
    fail "$what"
}

"$QK" serve --data "$work/db" --socket "$work/store.sock" 2>"$work/store.log" &
pids+=($!)
"$QK" import --root "$work/runtime" "$IMAGE" test/busybox:1 >/dev/null
mkdir "$work/net.d"
cat >"$work/net.d/10-cluster.conflist" <<JSON
{"cniVersion": "1.0.0", "name": "cluster", "plugins": [{"type": "bridge",
 "bridge": "qkcluster0", "isGateway": true, "ipMasq": true,
 "ipam": {"type": "host-local", "ranges": [[{"subnet": "10.42.0.0/24"}]],
 "dataDir": "$work/ipam"}}]}
JSON
"$QK" runtime --root "$work/runtime" --socket "$work/cri.sock" \
    --cni-conf "$work/net.d" --cni-bin "$CNI_BIN" 2>"$work/runtime.log" &
pids+=($!)
until_ok "store and runtime sockets" test -S "$work/store.sock" -a -S "$work/cri.sock"

"$K3S" server --token cluster --data-dir "$work/k3s" \
    --datastore-endpoint "unix://$work/store.sock" \
    --container-runtime-endpoint "unix://$work/cri.sock" \
    --write-kubeconfig "$work/kubeconfig" --write-kubeconfig-mode 600 \
    --flannel-backend none --disable-network-policy --disable-kube-proxy \
    --disable traefik,servicelb,metrics-server,local-storage,coredns \
    --disable-cloud-controller --disable-helm-controller \
    --kubelet-arg cgroup-driver=cgroupfs ${K3S_ARGS:-} >"$work/k3s.log" 2>&1 &
pids+=($!)
export KUBECONFIG=$work/kubeconfig
k() { "$K3S" kubectl "$@"; }
until_ok "kubeconfig" test -s "$work/kubeconfig"
until_ok "node ready" sh -c "'$K3S' kubectl get nodes 2>/dev/null | grep -qw Ready"

until_ok "default service account" "$K3S" kubectl get serviceaccount default
k apply -f - >/dev/null <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata: {name: greeting}
data: {text: hello from a ConfigMap}
---
apiVersion: v1
kind: Pod
metadata: {name: hello}
spec:
  automountServiceAccountToken: true
  containers:
  - name: hello
    image: test/busybox:1
    imagePullPolicy: Never
    command: [sh, -c, 'cat /config/text; echo; id -u; touch /tmp/alive; exec sleep 3600']
    securityContext: {runAsUser: 1000, runAsGroup: 1000, allowPrivilegeEscalation: false}
    volumeMounts: [{name: config, mountPath: /config}]
    resources: {limits: {memory: 64Mi, cpu: 100m}}
    livenessProbe:
      exec: {command: [test, -e, /tmp/alive]}
      periodSeconds: 2
  volumes: [{name: config, configMap: {name: greeting}}]
YAML
until_ok "pod running" sh -c "'$K3S' kubectl get pod hello -o jsonpath='{.status.phase}' | grep -qx Running"
until_ok "pod logs" sh -c "'$K3S' kubectl logs hello | grep -qx 'hello from a ConfigMap'"
[ "$(k logs hello | tail -1)" = 1000 ] || fail "runAsUser: $(k logs hello)"
ip=$(k get pod hello -o jsonpath='{.status.podIP}')
case $ip in 10.42.0.*) ;; *) fail "pod address: $ip" ;; esac
sleep 6
[ "$(k get pod hello -o jsonpath='{.status.containerStatuses[0].restartCount}')" = 0 ] ||
    fail "the liveness probe failed"
k delete pod hello --wait=true --timeout=60s >/dev/null || fail "delete"
echo "cluster: ok"
