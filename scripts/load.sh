#!/usr/bin/env bash
# The load on stage: the cluster full of pods, so that the move onto
# QuantyKube shows on the stage dashboard (deploy/grafana/stage.json).
# Runs on cp as root, after monitoring.sh.
#
#   scripts/load.sh up [node...]     default nodes: w1 w2
#   scripts/load.sh down
#
# up puts 60 sleeping pods on each node and leaves them there, and
# starts a loop, as the systemd unit qk-load so that it outlives the
# ssh session: on each node in turn, 30 more pods are started, timed
# from kubectl apply to all Ready, and removed again. The last time per
# node goes to node_exporter's textfile directory as
# quantykube_load_start_seconds{target="<node>"}, which the dashboard
# shows. A round that fails, as during a move when the API server is
# away, is written as nothing and the loop goes on.
#
# 60 + 30 pods, and the node's own, stay below the kubelet's 110.
set -euo pipefail

export KUBECONFIG=${KUBECONFIG:-/etc/kubernetes/admin.conf}
ns=qk-load
image=registry.k8s.io/e2e-test-images/busybox:1.36.1-1
steady=${STEADY:-60}
burst=${BURST:-30}
textfile=${TEXTFILE:-/var/lib/prometheus/node-exporter}
here=$(cd "$(dirname "$0")" && pwd)

# deployment <name> <node> <replicas>
deployment() {
    cat <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $1
  namespace: $ns
spec:
  replicas: $3
  selector:
    matchLabels: {app: $1}
  template:
    metadata:
      labels: {app: $1}
    spec:
      nodeSelector: {kubernetes.io/hostname: $2}
      terminationGracePeriodSeconds: 0
      containers:
        - name: sleep
          image: $image
          command: [sleep, "86400"]
          resources:
            requests: {cpu: 1m, memory: 4Mi}
YAML
}

# write <node> <seconds>: the gauge, replaced whole so that it is never
# read half written
write() {
    local f=$textfile/quantykube_load_$1.prom
    printf '# HELP quantykube_load_start_seconds Time for %s pods from kubectl apply to all Ready.\n# TYPE quantykube_load_start_seconds gauge\nquantykube_load_start_seconds{target="%s"} %s\n' \
        "$burst" "$1" "$2" >"$f.new"
    mv "$f.new" "$f"
}

# loop <node...>: what the unit runs
loop() {
    while :; do
        for n in "$@"; do
            t=$(date +%s%N)
            if deployment "burst-$n" "$n" "$burst" | kubectl apply -f - >/dev/null 2>&1 &&
                kubectl -n "$ns" rollout status "deploy/burst-$n" --timeout=120s >/dev/null 2>&1; then
                write "$n" "$(awk -v d=$(($(date +%s%N) - t)) 'BEGIN {printf "%.3f", d / 1e9}')"
            fi
            kubectl -n "$ns" scale "deploy/burst-$n" --replicas=0 >/dev/null 2>&1 || true
            for _ in $(seq 120); do
                [ -z "$(kubectl -n "$ns" get pods -l "app=burst-$n" -o name 2>/dev/null)" ] && break
                sleep 1
            done
            sleep 5
        done
    done
}

case ${1:-} in
up)
    shift
    nodes=("$@")
    [ ${#nodes[@]} -gt 0 ] || nodes=(w1 w2)
    [ -d "$textfile" ] || { echo "no $textfile: is node_exporter's textfile collector on?"; exit 1; }
    kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    for n in "${nodes[@]}"; do
        deployment "steady-$n" "$n" "$steady" | kubectl apply -f - >/dev/null
    done
    for n in "${nodes[@]}"; do
        kubectl -n "$ns" rollout status "deploy/steady-$n" --timeout=600s >/dev/null
        echo "$n: $steady pods running"
    done
    systemctl stop qk-load >/dev/null 2>&1 || true
    systemd-run --unit qk-load --collect --quiet \
        --setenv KUBECONFIG="$KUBECONFIG" --setenv BURST="$burst" --setenv TEXTFILE="$textfile" \
        bash "$here/load.sh" loop "${nodes[@]}"
    echo "the loop runs as qk-load: journalctl -u qk-load; stop with $0 down"
    ;;
loop)
    shift
    loop "$@"
    ;;
down)
    systemctl stop qk-load >/dev/null 2>&1 || true
    kubectl delete namespace "$ns" --wait=true >/dev/null 2>&1 || true
    rm -f "$textfile"/quantykube_load_*.prom
    echo "no load"
    ;;
*)
    echo "usage: $0 up [node...] | down"
    exit 1
    ;;
esac
