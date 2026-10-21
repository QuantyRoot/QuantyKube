#!/usr/bin/env bash
# A cluster moved from etcd to QuantyKube: an unmodified kube-apiserver
# writes into a real etcd, is stopped, `quantykube migrate` copies the
# keys, and the API server started again on QuantyKube finds every
# object at its resourceVersion, and a new write gets a resourceVersion
# newer than any before.
#
#   ETCD=... KUBE_APISERVER=... KUBECTL=... scripts/migrate.sh
set -euo pipefail

: "${ETCD:?set ETCD to an etcd binary}"
: "${KUBE_APISERVER:?set KUBE_APISERVER to a kube-apiserver binary}"
: "${KUBECTL:?set KUBECTL to a kubectl binary}"
QK=${QK:-target/release/quantykube}
work=$(mktemp -d)
pids=()
cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT

fail() {
    printf 'FAIL: %s\n' "$*"
    for log in "$work"/*.log; do
        printf '\n--- %s\n' "$log"
        tail -30 "$log"
    done
    exit 1
}

pki=$work/pki
mkdir -p "$pki"
openssl ecparam -name prime256v1 -genkey -noout -out "$pki/ca.key" 2>/dev/null
openssl req -x509 -new -key "$pki/ca.key" -subj /CN=etcd-ca -days 1 \
    -out "$pki/ca.pem" 2>/dev/null
cert() {
    openssl ecparam -name prime256v1 -genkey -noout 2>/dev/null |
        openssl pkcs8 -topk8 -nocrypt -out "$pki/$1.key"
    openssl req -new -key "$pki/$1.key" -subj "/CN=$1" -out "$pki/$1.csr" \
        2>/dev/null
    printf '%s\nbasicConstraints=CA:FALSE\n' "$2" >"$pki/$1.ext"
    openssl x509 -req -in "$pki/$1.csr" -CA "$pki/ca.pem" \
        -CAkey "$pki/ca.key" -CAcreateserial -days 1 \
        -extfile "$pki/$1.ext" -out "$pki/$1.pem" 2>/dev/null
}
cert server $'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth'
cert client 'extendedKeyUsage=clientAuth'
openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
    -addext subjectAltName=IP:127.0.0.1 \
    -keyout "$work/tls.key" -out "$work/tls.crt" 2>/dev/null
echo "admin-token,admin,admin,system:masters" >"$work/tokens.csv"

"$ETCD" --data-dir "$work/etcd" --name m \
    --listen-client-urls https://127.0.0.1:23891 \
    --advertise-client-urls https://127.0.0.1:23891 \
    --listen-peer-urls http://127.0.0.1:23892 \
    --initial-advertise-peer-urls http://127.0.0.1:23892 \
    --initial-cluster m=http://127.0.0.1:23892 \
    --cert-file "$pki/server.pem" --key-file "$pki/server.key" \
    --client-cert-auth --trusted-ca-file "$pki/ca.pem" \
    >"$work/etcd.log" 2>&1 &
etcd=$!
pids+=("$etcd")

apiserver() {
    env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy \
        "$KUBE_APISERVER" --etcd-servers="https://127.0.0.1:$1" \
        --etcd-cafile="$pki/ca.pem" --etcd-certfile="$pki/client.pem" \
        --etcd-keyfile="$pki/client.key" \
        --bind-address=127.0.0.1 --secure-port=6556 \
        --tls-cert-file="$work/tls.crt" \
        --tls-private-key-file="$work/tls.key" \
        --service-account-issuer=https://kubernetes.default.svc \
        --service-account-key-file="$work/sa.key" \
        --service-account-signing-key-file="$work/sa.key" \
        --token-auth-file="$work/tokens.csv" --authorization-mode=RBAC \
        --service-cluster-ip-range=10.96.0.0/16 --cert-dir="$work" \
        >>"$work/kube-apiserver.log" 2>&1 &
    api=$!
    pids+=("$api")
    for _ in $(seq 90); do
        k get --raw /readyz >/dev/null 2>&1 && return
        sleep 1
    done
    fail "kube-apiserver did not become ready on port $1"
}
k() {
    "$KUBECTL" --server=https://127.0.0.1:6556 --token=admin-token \
        --insecure-skip-tls-verify "$@"
}

apiserver 23891
for i in $(seq 30); do
    k create configmap "cm-$i" --from-literal=v="$i" >/dev/null
done
k create namespace moved >/dev/null
# events are kept under an etcd lease, so the copy has one to carry
k create -f - >/dev/null <<'YAML'
apiVersion: v1
kind: Event
metadata: {name: moved, namespace: default}
involvedObject: {kind: ConfigMap, name: cm-1, namespace: default}
reason: Moved
message: before the move
type: Normal
YAML
k -n moved create secret generic s --from-literal=k=v >/dev/null
k create configmap changing --from-literal=v=0 >/dev/null
k create configmap changing --from-literal=v=1 --dry-run=client -o yaml |
    k replace -f - >/dev/null
k get configmaps,secrets,namespaces,events -A \
    -o custom-columns=N:.metadata.name,RV:.metadata.resourceVersion \
    --no-headers | sort >"$work/before"
from=$(k get configmap changing -o jsonpath='{.metadata.resourceVersion}')

kill "$api"
wait "$api" 2>/dev/null || true
"$QK" migrate --from 127.0.0.1:23891 --etcd-ca "$pki/ca.pem" \
    --etcd-cert "$pki/client.pem" --etcd-key "$pki/client.key" \
    --data "$work/qk.db" >"$work/migrate.out" 2>"$work/migrate.log" ||
    fail "migrate"
grep -q '^copied [0-9]* keys and [1-9][0-9]* leases as of revision' \
    "$work/migrate.out" || fail "migrate said: $(cat "$work/migrate.out")"
"$QK" migrate --from 127.0.0.1:23891 --etcd-ca "$pki/ca.pem" \
    --etcd-cert "$pki/client.pem" --etcd-key "$pki/client.key" \
    --data "$work/qk.db" >/dev/null 2>&1 &&
    fail "migrate overwrote a store that exists"
# etcd stays as it was, untouched, for the way back
kill "$etcd"
"$QK" serve --data "$work/qk.db" --socket "$work/qk.sock" \
    --listen 127.0.0.1:23893 --cert "$pki/server.pem" \
    --key "$pki/server.key" --client-ca "$pki/ca.pem" \
    2>"$work/quantykube.log" &
pids+=($!)
apiserver 23893
k get configmaps,secrets,namespaces,events -A \
    -o custom-columns=N:.metadata.name,RV:.metadata.resourceVersion \
    --no-headers | sort >"$work/after"
diff "$work/before" "$work/after" >/dev/null ||
    fail "objects or resourceVersions differ: $(diff "$work/before" "$work/after")"
[ "$(k -n moved get secret s -o jsonpath='{.data.k}')" = dg== ] ||
    fail "the secret's data"
echo "every object at its resourceVersion: ok"

# revisions go on from etcd's: a client's resourceVersion from before
# the move is older than every new write, and a watch from it is told
# to list again, as after any restart of the API server
k create configmap after --from-literal=v=3 >/dev/null
newest=$(awk '{print $2}' "$work/before" | sort -n | tail -1)
rv=$(k get configmap after -o jsonpath='{.metadata.resourceVersion}')
[ "$rv" -gt "$newest" ] || fail "a new write got $rv, not after $newest"
q="watch=1&resourceVersion=$from&fieldSelector=metadata.name%3Dchanging"
timeout 5 "$KUBECTL" --server=https://127.0.0.1:6556 --token=admin-token \
    --insecure-skip-tls-verify get --raw "/api/v1/namespaces/default/configmaps?$q" \
    >"$work/watch" 2>&1 || true
grep -q '"code":410' "$work/watch" ||
    fail "a watch from before the move: $(cat "$work/watch")"
echo "revisions go on from etcd's: ok"
