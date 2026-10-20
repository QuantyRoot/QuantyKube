#!/usr/bin/env bash
# Runs QuantyKube against the real etcdctl and, if K3S is set, a real
# Kubernetes API server (k3s with its agent disabled). Exits non-zero on the
# first answer that differs from what etcd would give.
#
#   ETCDCTL=/path/to/etcdctl [K3S=/path/to/k3s] scripts/e2e.sh
set -euo pipefail

: "${ETCDCTL:?set ETCDCTL to an etcdctl binary}"
QK=${QK:-target/release/quantykube}
work=$(mktemp -d)
sock=$work/qk.sock
pids=()
cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

start() {
    rm -f "$sock"
    "$QK" serve --data "$work/db" --socket "$sock" 2>>"$work/qk.log" &
    qk=$!
    pids+=("$qk")
    for _ in $(seq 50); do [ -S "$sock" ] && return; sleep 0.1; done
    echo "quantykube did not start"; cat "$work/qk.log"; exit 1
}

e() { "$ETCDCTL" --endpoints="unix://$sock" "$@"; }
# etcdctl logs a retry warning before printing the error itself
last() { "$@" 2>&1 | tail -1; }

expect() {
    local want=$1; shift
    local got
    got=$("$@" 2>&1) || true
    if [ "$got" != "$want" ]; then
        printf 'FAIL: %s\nwant:\n%s\ngot:\n%s\n' "$*" "$want" "$got"
        for log in "$work"/*.log; do
            printf '\n--- %s\n' "$log"
            tail -40 "$log"
        done
        exit 1
    fi
}

start
expect OK e put /a hello
expect OK e put /a again
expect $'/a\nagain' e get /a
expect $'/a\nhello' e get /a --rev=2
expect 1 e del /a
expect "" e get /a
expect $'SUCCESS\n\nOK' e txn <<<$'mod("/b") = "0"\n\nput /b created\n\n'
expect $'FAILURE\n\n/b\ncreated' e txn <<<$'mod("/b") = "0"\n\nput /b again\n\nget /b\n'

e watch /w --prefix >"$work/watch" 2>&1 &
pids+=($!)
sleep 0.5
e put /w/1 x >/dev/null
e del /w/1 >/dev/null
sleep 0.5
expect $'PUT\n/w/1\nx\nDELETE\n/w/1' cat "$work/watch"

expect $'PUT\n/a\nhello\nPUT\n/a\nagain\nDELETE\n/a' \
    timeout 2 "$ETCDCTL" --endpoints="unix://$sock" watch /a --rev=1
e put /last v >/dev/null
rev=$(e get /last -w json | sed 's/.*"revision":\([0-9]*\).*/\1/')
expect $'PUT\n/last\nv' timeout 1 "$ETCDCTL" --endpoints="unix://$sock" watch /last --rev="$rev"
rev=$(e get /b -w json | sed 's/.*"revision":\([0-9]*\).*/\1/')
expect "compacted revision $rev" e compaction "$rev"
expect "Error: etcdserver: mvcc: required revision has been compacted" last e get /a --rev=2

kill -9 "$qk"
wait "$qk" 2>/dev/null || true
start
expect $'/b\ncreated' e get /b
echo "etcdctl: ok"

[ -n "${K3S:-}" ] || exit 0
"$K3S" server --disable-agent --token e2e --data-dir "$work/k3s" \
    --datastore-endpoint="unix://$sock" --write-kubeconfig "$work/k.yaml" \
    --bind-address 127.0.0.1 \
    --disable=traefik,servicelb,metrics-server,local-storage,coredns \
    --disable-network-policy --flannel-backend=none --disable-kube-proxy \
    --disable-cloud-controller --disable-helm-controller >"$work/k3s.log" 2>&1 &
pids+=($!)
k() { "$K3S" kubectl --kubeconfig "$work/k.yaml" "$@"; }
ready() {
    for _ in $(seq 120); do
        k get --raw /readyz >/dev/null 2>&1 && return
        sleep 1
    done
}
ready
expect ok k get --raw /readyz
expect $'default\nkube-node-lease\nkube-public\nkube-system' \
    k get namespaces -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'
k create configmap e2e --from-literal=v=1 >/dev/null
k patch configmap e2e -p '{"data":{"v":"2"}}' >/dev/null
expect 2 k get configmap e2e -o 'jsonpath={.data.v}'
kill -9 "$qk"
wait "$qk" 2>/dev/null || true
start
ready
expect 2 k get configmap e2e -o 'jsonpath={.data.v}'
k delete configmap e2e >/dev/null
expect 'Error from server (NotFound): configmaps "e2e" not found' last k get configmap e2e

key=/registry/configmaps/default/e2e
rev=$("$QK" history --data "$work/db" "$key" | awk '$4 == "update" {print $1}')
"$QK" get --data "$work/db" --rev "$rev" "$key" 2>/dev/null | e put "$key" >/dev/null
expect 2 k get configmap e2e -o 'jsonpath={.data.v}'
echo "kubernetes: ok"
