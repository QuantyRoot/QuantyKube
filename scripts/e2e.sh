#!/usr/bin/env bash
# Runs QuantyKube against the real etcdctl, on the Unix socket and over
# TLS with client certificates, then against an unmodified kube-apiserver
# over TLS if KUBE_APISERVER and KUBECTL are set, then against a k3s
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

# a CA of the test's own, a certificate for the store and one for clients
pki=$work/pki
mkdir -p "$pki"
cert() {
    openssl ecparam -name prime256v1 -genkey -noout 2>/dev/null |
        openssl pkcs8 -topk8 -nocrypt -out "$pki/$1.key"
    openssl req -new -key "$pki/$1.key" -subj "/CN=$1" -out "$pki/$1.csr" 2>/dev/null
    printf '%s\nbasicConstraints=CA:FALSE\n' "$2" >"$pki/$1.ext"
    openssl x509 -req -in "$pki/$1.csr" -CA "$pki/ca.pem" -CAkey "$pki/ca.key" \
        -CAcreateserial -days 1 -extfile "$pki/$1.ext" -out "$pki/$1.pem" 2>/dev/null
}
openssl ecparam -name prime256v1 -genkey -noout -out "$pki/ca.key" 2>/dev/null
openssl req -x509 -new -key "$pki/ca.key" -subj /CN=e2e-ca -days 1 \
    -out "$pki/ca.pem" 2>/dev/null
cert server $'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth'
cert client 'extendedKeyUsage=clientAuth'
# the history API is served under the name kube-apiserver gives it, to
# the front proxy certificate kube-apiserver holds
cert api $'subjectAltName=DNS:quantykube.kube-system.svc\nextendedKeyUsage=serverAuth'
mv "$pki/ca.pem" "$pki/main-ca.pem"
mv "$pki/ca.key" "$pki/main-ca.key"
openssl ecparam -name prime256v1 -genkey -noout -out "$pki/ca.key" 2>/dev/null
openssl req -x509 -new -key "$pki/ca.key" -subj /CN=front-proxy-ca -days 1 \
    -out "$pki/ca.pem" 2>/dev/null
cert front-proxy-client 'extendedKeyUsage=clientAuth'
mv "$pki/ca.pem" "$pki/front-proxy-ca.pem"
mv "$pki/ca.key" "$pki/front-proxy-ca.key"
mv "$pki/main-ca.pem" "$pki/ca.pem"
mv "$pki/main-ca.key" "$pki/ca.key"
tls_port=23790
api_port=23791
# an endpoint of a Service may not be a loopback address
api_ip=$(hostname -I | awk '{print $1}')

start() {
    rm -f "$sock"
    "$QK" serve --data "$work/db" --socket "$sock" \
        --listen "127.0.0.1:$tls_port" --cert "$pki/server.pem" \
        --key "$pki/server.key" --client-ca "$pki/ca.pem" \
        --api-listen "$api_ip:$api_port" --api-cert "$pki/api.pem" \
        --api-key "$pki/api.key" --api-client-ca "$pki/front-proxy-ca.pem" \
        2>>"$work/qk.log" &
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

t() {
    "$ETCDCTL" --endpoints="https://127.0.0.1:$tls_port" --cacert "$pki/ca.pem" \
        --cert "$pki/client.pem" --key "$pki/client.key" "$@"
}
expect OK t put /tls over-tls
expect $'/tls\nover-tls' t get /tls
t watch /tw --prefix >"$work/tls-watch" 2>&1 &
pids+=($!)
sleep 0.5
for i in $(seq 20); do t put "/tw/$i" x >/dev/null; done
sleep 0.5
expect 20 grep -c '^PUT' "$work/tls-watch"
# without a client certificate nothing is answered
if "$ETCDCTL" --endpoints="https://127.0.0.1:$tls_port" --cacert "$pki/ca.pem" \
    --dial-timeout=2s --command-timeout=3s get /tls >/dev/null 2>&1; then
    echo "FAIL: a client without a certificate was answered"; exit 1
fi
echo "etcdctl over TLS: ok"

if [ -n "${KUBE_APISERVER:-}" ]; then
    openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 \
        -addext subjectAltName=IP:127.0.0.1 \
        -keyout "$work/tls.key" -out "$work/tls.crt" 2>/dev/null
    echo "e2e-token,admin,admin,system:masters" >"$work/tokens.csv"
    echo "viewer-token,viewer,viewer" >>"$work/tokens.csv"
    # its calls to the history API must not go through a proxy the
    # machine may have for the outside world
    env -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy \
        "$KUBE_APISERVER" --etcd-servers="https://127.0.0.1:$tls_port" \
        --requestheader-client-ca-file="$pki/front-proxy-ca.pem" \
        --requestheader-allowed-names=front-proxy-client \
        --requestheader-username-headers=X-Remote-User \
        --requestheader-group-headers=X-Remote-Group \
        --requestheader-extra-headers-prefix=X-Remote-Extra- \
        --proxy-client-cert-file="$pki/front-proxy-client.pem" \
        --proxy-client-key-file="$pki/front-proxy-client.key" \
        --enable-aggregator-routing=true \
        --etcd-cafile="$pki/ca.pem" --etcd-certfile="$pki/client.pem" \
        --etcd-keyfile="$pki/client.key" --etcd-prefix=/upstream \
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
    echo "kube-apiserver over TLS: ok"

    # history through the Kubernetes API (ADR-028)
    cabundle=$(base64 -w0 <"$pki/ca.pem")
    up apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Service
metadata: {name: quantykube, namespace: kube-system}
spec:
  ports: [{port: 443, targetPort: $api_port}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: quantykube
  namespace: kube-system
  labels: {kubernetes.io/service-name: quantykube}
addressType: IPv4
ports: [{port: $api_port}]
endpoints: [{addresses: ["$api_ip"]}]
---
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata: {name: v1alpha1.quanty.quantyroot.io}
spec:
  group: quanty.quantyroot.io
  version: v1alpha1
  service: {namespace: kube-system, name: quantykube, port: 443}
  caBundle: $cabundle
  groupPriorityMinimum: 1000
  versionPriority: 15
YAML
    for _ in $(seq 60); do
        up get apiservice v1alpha1.quanty.quantyroot.io \
            -o 'jsonpath={.status.conditions[0].status}' 2>/dev/null | grep -q True && break
        sleep 1
    done
    expect True up get apiservice v1alpha1.quanty.quantyroot.io \
        -o 'jsonpath={.status.conditions[0].status}'
    "$KUBECTL" config --kubeconfig="$work/kc" set-cluster e2e \
        --server=https://127.0.0.1:6555 --insecure-skip-tls-verify=true >/dev/null
    "$KUBECTL" config --kubeconfig="$work/kc" set-credentials admin --token=e2e-token >/dev/null
    "$KUBECTL" config --kubeconfig="$work/kc" set-credentials viewer --token=viewer-token >/dev/null
    "$KUBECTL" config --kubeconfig="$work/kc" set-context admin --cluster=e2e --user=admin >/dev/null
    "$KUBECTL" config --kubeconfig="$work/kc" set-context viewer --cluster=e2e --user=viewer >/dev/null
    "$KUBECTL" config --kubeconfig="$work/kc" use-context admin >/dev/null
    plugin() { KUBECONFIG="$work/kc" KUBECTL="$KUBECTL" "${QKP:-target/release/kubectl-quanty}" "$@"; }
    up create configmap hist --from-literal=v=1 >/dev/null
    up patch configmap hist -p '{"data":{"v":"2"}}' >/dev/null
    up delete configmap hist >/dev/null
    key=/upstream/configmaps/default/hist
    expect $'create\nupdate\ndelete' bash -c "$(declare -f plugin); work=$work KUBECTL=$KUBECTL; plugin history $key | awk '{print \$4}'"
    rev=$(plugin history "$key" | awk '$4 == "update" {print $1}')
    plugin restore --rev "$rev" "$key" >/dev/null
    expect 2 up get configmap hist -o 'jsonpath={.data.v}'
    # RBAC decides who may look: a user without a role may not
    if KUBECONFIG="$work/kc" "$KUBECTL" --context viewer get --raw \
        /apis/quanty.quantyroot.io/v1alpha1/logs?q= >/dev/null 2>&1; then
        echo "FAIL: a user without a role read history"; exit 1
    fi
    kill "${pids[-1]}"
    echo "history through the Kubernetes API: ok"
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
