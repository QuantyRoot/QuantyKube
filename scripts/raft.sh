#!/usr/bin/env bash
# Three QuantyKube store nodes on this machine, joined by Raft, against
# the real etcdctl and, if KUBE_APISERVER and KUBECTL are set, an
# unmodified kube-apiserver given all three. The leader is killed with
# SIGKILL under both, and comes back.
#
#   ETCDCTL=... [KUBE_APISERVER=... KUBECTL=...] scripts/raft.sh
set -euo pipefail

: "${ETCDCTL:?set ETCDCTL to an etcdctl binary}"
QK=${QK:-target/release/quantykube}
work=$(mktemp -d)
declare -A pid
others=()
cleanup() {
    for p in "${pid[@]}" "${others[@]}"; do kill "$p" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

pki=$work/pki
mkdir -p "$pki"
ca() {
    openssl ecparam -name prime256v1 -genkey -noout -out "$pki/$1.key" 2>/dev/null
    openssl req -x509 -new -key "$pki/$1.key" -subj "/CN=$1" -days 1 \
        -out "$pki/$1.pem" 2>/dev/null
}
# cert <name> <ca> <extensions>
cert() {
    openssl ecparam -name prime256v1 -genkey -noout 2>/dev/null |
        openssl pkcs8 -topk8 -nocrypt -out "$pki/$1.key"
    openssl req -new -key "$pki/$1.key" -subj "/CN=$1" -out "$pki/$1.csr" 2>/dev/null
    printf '%s\nbasicConstraints=CA:FALSE\n' "$3" >"$pki/$1.ext"
    openssl x509 -req -in "$pki/$1.csr" -CA "$pki/$2.pem" -CAkey "$pki/$2.key" \
        -CAcreateserial -days 1 -extfile "$pki/$1.ext" -out "$pki/$1.pem" 2>/dev/null
}
ca client-ca
ca peer-ca
cert server client-ca $'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth'
cert client client-ca 'extendedKeyUsage=clientAuth'
cert peer peer-ca $'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth'

peers=1=localhost:23901,2=localhost:23902,3=localhost:23903
start() {
    local n=$1
    mkdir -p "$work/n$n"
    "$QK" serve --data "$work/n$n/db" --socket "$work/n$n/s.sock" \
        --listen "127.0.0.1:2381$n" --cert "$pki/server.pem" \
        --key "$pki/server.key" --client-ca "$pki/client-ca.pem" \
        --node "$n" --peers "$peers" --peer-cert "$pki/peer.pem" \
        --peer-key "$pki/peer.key" --peer-ca "$pki/peer-ca.pem" \
        2>>"$work/n$n.log" &
    pid[$n]=$!
}

e() {
    local n=$1; shift
    "$ETCDCTL" --endpoints="https://127.0.0.1:2381$n" --cacert "$pki/client-ca.pem" \
        --cert "$pki/client.pem" --key "$pki/client.key" --command-timeout=10s "$@"
}

expect() {
    local want=$1; shift
    local got
    got=$("$@" 2>&1) || true
    if [ "$got" != "$want" ]; then
        printf 'FAIL: %s\nwant:\n%s\ngot:\n%s\n' "$*" "$want" "$got"
        for log in "$work"/*.log; do
            printf '\n--- %s\n' "$log"
            tail -20 "$log"
        done
        exit 1
    fi
}

leader() {
    for n in 1 2 3; do
        [ -n "${pid[$n]:-}" ] || continue
        e "$n" endpoint status -w json 2>/dev/null |
            grep -o '"leader":[0-9]*' | head -1 | cut -d: -f2 && return
    done
}

for n in 1 2 3; do start "$n"; done
for _ in $(seq 100); do
    e 1 put /ready yes >/dev/null 2>&1 && break
    sleep 0.2
done
expect OK e 1 put /a one
expect $'/a\none' e 3 get /a
e 2 watch /w --prefix >"$work/watch" 2>&1 &
others+=($!)
sleep 0.5
expect OK e 3 put /w/x from-3
sleep 0.5
expect $'PUT\n/w/x\nfrom-3' cat "$work/watch"
# the same revision on every node
r1=$(e 1 get /a -w json | grep -o '"mod_revision":[0-9]*')
expect "$r1" bash -c "$(declare -f e); ETCDCTL=$ETCDCTL pki=$pki; e 2 get /a -w json | grep -o '\"mod_revision\":[0-9]*'"
echo "three nodes: ok"

# kill the leader; the other two go on
id=$(leader)
n=$(( (id & 0xff) - 1 ))
kill -9 "${pid[$n]}"
wait "${pid[$n]}" 2>/dev/null || true
unset 'pid[$n]'
rest=()
for m in 1 2 3; do [ "$m" = "$n" ] || rest+=("$m"); done
for _ in $(seq 50); do
    e "${rest[0]}" put /b without-leader >/dev/null 2>&1 && break
    sleep 0.2
done
expect $'/b\nwithout-leader' e "${rest[1]}" get /b
echo "without the leader: ok"

# it comes back and catches up
start "$n"
for _ in $(seq 100); do
    got=$(e "$n" get /b --consistency=s 2>/dev/null || true)
    [ "$got" = $'/b\nwithout-leader' ] && break
    sleep 0.2
done
expect $'/b\nwithout-leader' e "$n" get /b --consistency=s
echo "the old leader caught up: ok"

# and holds what the others hold, at the same revisions: nothing it had
# applied before the kill was applied twice after it
expect OK e "${rest[0]}" put /c after-restart
rev() {
    e "$1" get /c --consistency=s -w json 2>/dev/null | grep -o '"revision":[0-9]*' | head -1
}
want=$(rev "${rest[0]}")
for _ in $(seq 50); do [ "$(rev "$n")" = "$want" ] && break; sleep 0.2; done
expect "$want" rev "$n"
expect "$want" rev "${rest[1]}"
echo "the same revisions after the restart: ok"

[ -n "${KUBE_APISERVER:-}" ] || exit 0
openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
    -addext subjectAltName=IP:127.0.0.1 \
    -keyout "$work/tls.key" -out "$work/tls.crt" 2>/dev/null
echo "e2e-token,admin,admin,system:masters" >"$work/tokens.csv"
"$KUBE_APISERVER" \
    --etcd-servers=https://127.0.0.1:23811,https://127.0.0.1:23812,https://127.0.0.1:23813 \
    --etcd-cafile="$pki/client-ca.pem" --etcd-certfile="$pki/client.pem" \
    --etcd-keyfile="$pki/client.key" \
    --bind-address=127.0.0.1 --secure-port=6556 \
    --tls-cert-file="$work/tls.crt" --tls-private-key-file="$work/tls.key" \
    --service-account-issuer=https://kubernetes.default.svc \
    --service-account-key-file="$work/sa.key" \
    --service-account-signing-key-file="$work/sa.key" \
    --token-auth-file="$work/tokens.csv" --authorization-mode=RBAC \
    --service-cluster-ip-range=10.96.0.0/16 --cert-dir="$work" \
    >"$work/kube-apiserver.log" 2>&1 &
others+=($!)
k() {
    "$KUBECTL" --server=https://127.0.0.1:6556 --token=e2e-token \
        --insecure-skip-tls-verify "$@"
}
for _ in $(seq 90); do
    k get --raw /readyz >/dev/null 2>&1 && break
    sleep 1
done
expect ok k get --raw /readyz
k create configmap before --from-literal=v=1 >/dev/null
id=$(leader)
n=$(( (id & 0xff) - 1 ))
kill -9 "${pid[$n]}"
wait "${pid[$n]}" 2>/dev/null || true
unset 'pid[$n]'
for _ in $(seq 30); do
    k create configmap after --from-literal=v=2 >/dev/null 2>&1 && break
    sleep 1
done
expect 1 k get configmap before -o 'jsonpath={.data.v}'
expect 2 k get configmap after -o 'jsonpath={.data.v}'
echo "kube-apiserver on three nodes, without the leader: ok"
