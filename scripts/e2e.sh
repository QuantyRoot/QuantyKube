#!/usr/bin/env bash
# Runs QuantyKube against the real etcdctl, then against an unmodified
# kube-apiserver if KUBE_APISERVER and KUBECTL are set, then against a k3s
# control plane if K3S is set. Exits non-zero on the first answer that
# differs from what etcd would give.
#
#   ETCDCTL=... [KUBE_APISERVER=... KUBECTL=...] [K3S=...] scripts/e2e.sh
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
expect 600 stat -c %a "$sock"
expect OK e put /a hello
expect OK e put /a again
expect $'/a\nagain' e get /a
expect $'/a\nhello' e get /a --rev=2
expect 1 e del /a
expect "" e get /a
expect $'SUCCESS\n\nOK' e txn <<<$'mod("/b") = "0"\n\nput /b created\n\n'
expect $'FAILURE\n\n/b\ncreated' e txn <<<$'mod("/b") = "0"\n\nput /b again\n\nget /b\n'

"$ETCDCTL" --endpoints="unix://$sock" watch /w --prefix >"$work/watch" 2>&1 &
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

if [ -n "${KUBE_APISERVER:-}" ]; then
    openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
        -addext subjectAltName=IP:127.0.0.1 \
        -keyout "$work/tls.key" -out "$work/tls.crt" 2>/dev/null
    echo "e2e-token,admin,admin,system:masters" >"$work/tokens.csv"
    "$KUBE_APISERVER" --etcd-servers="unix://$sock" --etcd-prefix=/upstream \
        --bind-address=127.0.0.1 --secure-port=6555 \
        --tls-cert-file="$work/tls.crt" --tls-private-key-file="$work/tls.key" \
        --service-account-issuer=https://kubernetes.default.svc \
        --service-account-key-file="$work/sa.key" \
        --service-account-signing-key-file="$work/sa.key" \
        --token-auth-file="$work/tokens.csv" --authorization-mode=RBAC \
        --service-cluster-ip-range=10.96.0.0/16 --cert-dir="$work" \
        >"$work/kube-apiserver.log" 2>&1 &
    pids+=($!)
    up() {
        "$KUBECTL" --server=https://127.0.0.1:6555 --token=e2e-token \
            --insecure-skip-tls-verify "$@"
    }
    for _ in $(seq 60); do
        up get --raw /readyz >/dev/null 2>&1 && break
        sleep 1
    done
    expect ok up get --raw /readyz
    for i in $(seq 25); do
        printf -- '---\napiVersion: v1\nkind: ConfigMap\n'
        printf 'metadata: {name: cm-%s, namespace: default}\n' "$i"
    done >"$work/many.yaml"
    up apply --server-side -f "$work/many.yaml" >/dev/null
    expect 25 bash -c "$(declare -f up); KUBECTL=$KUBECTL; up get configmaps --chunk-size=4 -o name | wc -l"
    up delete configmaps --all >/dev/null
    expect 0 bash -c "$(declare -f up); KUBECTL=$KUBECTL; up get configmaps -o name | wc -l"
    kill "${pids[-1]}"
    echo "kube-apiserver: ok"
fi

[ -n "${K3S:-}" ] || exit 0
"$K3S" server --disable-agent --token e2e --data-dir "$work/k3s" \
    --datastore-endpoint="unix://$sock" --write-kubeconfig "$work/k.yaml" \
    --bind-address 127.0.0.1 --https-listen-port 16443 \
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
rev=$("$QK" history --socket "$sock" "$key" | awk '$4 == "update" {print $1}')
"$QK" restore --socket "$sock" --rev "$rev" "$key" >/dev/null
expect 2 k get configmap e2e -o 'jsonpath={.data.v}'
echo "kubernetes: ok"
