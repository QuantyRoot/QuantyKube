#!/usr/bin/env bash
# The numbers on stage, measured on the rented cluster as it is, once
# before the move onto QuantyKube and once after (CLUSTER.md). Runs on
# cp as root: kubectl with admin.conf, Prometheus from monitoring.sh on
# 127.0.0.1:9090.
#
#   [RUNS=3] scripts/measure.sh <label> [node...]   default nodes: w1 w2
#
# RUNS times (default 3), for each node, with the same pods pinned to it:
#   start   20 single pods one after another, kubectl apply to Ready as
#           kubectl sees it; the API server and scheduler are the same
#           for every node, so the difference is the node
#   N pods  a deployment of 10, 30 and 100 pods: time to all Ready, then
#           after 30 s the resident memory of the runtime and everything
#           it starts besides the containers (containerd, its shims,
#           runc, pause; or quantykube runtime), averaged over 30 s
# and once for the store on cp (etcd or quantykube-store).
#
# Results go to measure/<label>-<time>/: one CSV per kind and
# summary.md, which gives the median over the runs and, in brackets,
# the lowest and highest run. The image is pulled once per node before
# anything is timed.
set -euo pipefail
shopt -s inherit_errexit

label=${1:?label, e.g. before or after}
runs=${RUNS:-3}
shift
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || nodes=(w1 w2)
export KUBECONFIG=${KUBECONFIG:-/etc/kubernetes/admin.conf}
prom=${PROM:-http://127.0.0.1:9090}
image=registry.k8s.io/e2e-test-images/busybox:1.36.1-1
ns=qk-measure
out=measure/$label-$(date +%Y%m%d-%H%M%S)
mkdir -p "$out"

ms() { echo $((($(date +%s%N) - $1) / 1000000)); }

cleanup() { kubectl delete namespace "$ns" --wait=true >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
kubectl create namespace "$ns" >/dev/null

# pod <name> <node>: a busybox sleeping, pinned to node
pod() {
    cat <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $ns
  labels: {app: measure}
spec:
  nodeName: $2
  terminationGracePeriodSeconds: 0
  containers:
    - name: sleep
      image: $image
      command: [sleep, "3600"]
      resources:
        requests: {cpu: 1m, memory: 4Mi}
YAML
}

# deployment <node> <replicas>
deployment() {
    cat <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: many
  namespace: $ns
spec:
  replicas: $2
  selector:
    matchLabels: {app: many}
  template:
    metadata:
      labels: {app: many}
    spec:
      nodeName: $1
      terminationGracePeriodSeconds: 0
      containers:
        - name: sleep
          image: $image
          command: [sleep, "3600"]
          resources:
            requests: {cpu: 1m, memory: 4Mi}
YAML
}

# promq <query>: the single value of an instant query, or "nan"
promq() {
    curl -fsS --get "$prom/api/v1/query" --data-urlencode "query=$1" |
        python3 -c 'import json,sys
r=json.load(sys.stdin)["data"]["result"]
print(r[0]["value"][1] if r else "nan")'
}

runtime='containerd|containerd-shim|runc|pause|quantykube-runtime'
# mem <node>: MiB of the runtime and what it starts, over the last 30 s
mem() {
    promq "sum(avg_over_time(namedprocess_namegroup_memory_bytes{memtype=\"resident\",node=\"$1\",groupname=~\"$runtime\"}[30s])) / 1048576"
}
# procs <node>: how many such processes
procs() {
    promq "sum(namedprocess_namegroup_num_procs{node=\"$1\",groupname=~\"$runtime\"})"
}
# runtime_of <node>: the runtime the kubelet reports
runtime_of() {
    kubectl get node "$1" -o jsonpath='{.status.nodeInfo.containerRuntimeVersion}'
}

echo "round,node,runtime,run,ms" >"$out/start.csv"
echo "round,node,runtime,pods,ready_ms,runtime_mib,runtime_procs" >"$out/many.csv"

for round in $(seq 1 "$runs"); do
for n in "${nodes[@]}"; do
    rt=$(runtime_of "$n")
    echo "== run $round of $runs: $n ($rt)"
    pod warm "$n" | kubectl apply -f - >/dev/null
    kubectl -n "$ns" wait --for=condition=Ready pod/warm --timeout=300s >/dev/null
    kubectl -n "$ns" delete pod warm --wait=true >/dev/null

    for i in $(seq 1 20); do
        t=$(date +%s%N)
        pod "p$i" "$n" | kubectl apply -f - >/dev/null
        kubectl -n "$ns" wait --for=condition=Ready "pod/p$i" --timeout=120s >/dev/null
        echo "$round,$n,$rt,$i,$(ms "$t")" >>"$out/start.csv"
        kubectl -n "$ns" delete pod "p$i" --wait=true >/dev/null
    done

    for count in 10 30 100; do
        t=$(date +%s%N)
        deployment "$n" "$count" | kubectl apply -f - >/dev/null
        kubectl -n "$ns" rollout status deploy/many --timeout=600s >/dev/null
        ready=$(ms "$t")
        sleep 60
        echo "$round,$n,$rt,$count,$ready,$(mem "$n"),$(procs "$n")" >>"$out/many.csv"
        echo "   $count pods ready in $ready ms"
        kubectl -n "$ns" delete deploy many --wait=true >/dev/null
        kubectl -n "$ns" wait --for=delete pod -l app=many --timeout=300s \
            >/dev/null 2>&1 || true
    done
done
done

store=$(promq "sum(avg_over_time(namedprocess_namegroup_memory_bytes{memtype=\"resident\",node=\"cp\",groupname=~\"etcd|quantykube-store\"}[5m])) / 1048576")
storename=$(curl -fsS --get "$prom/api/v1/query" --data-urlencode \
    'query=namedprocess_namegroup_num_procs{node="cp",groupname=~"etcd|quantykube-store"} > 0' |
    python3 -c 'import json,sys
r=json.load(sys.stdin)["data"]["result"]
print("+".join(sorted(x["metric"]["groupname"] for x in r)))')
echo "store,mib_5m" >"$out/store.csv"
echo "$storename,$store" >>"$out/store.csv"

python3 - "$out" "$label" <<'PY'
import csv, statistics, sys
out, label = sys.argv[1], sys.argv[2]
start = list(csv.DictReader(open(f"{out}/start.csv")))
many = list(csv.DictReader(open(f"{out}/many.csv")))
store = list(csv.DictReader(open(f"{out}/store.csv")))
rounds = len(set(r["round"] for r in start))

def spread(values, fmt="{:.0f}"):
    """The median of the runs, and the lowest and highest run."""
    m = statistics.median(values)
    if len(values) == 1:
        return fmt.format(m)
    return f"{fmt.format(m)} ({fmt.format(min(values))} to {fmt.format(max(values))})"

lines = [f"# {label}", "", f"{rounds} runs; the median of the runs, in brackets the lowest and highest run.", "",
         "## Pod start, apply to Ready, 20 pods one by one per run", "",
         "| node | runtime | median ms | p90 ms |", "| --- | --- | --- | --- |"]
for node in dict.fromkeys(r["node"] for r in start):
    rt = next(r["runtime"] for r in start if r["node"] == node)
    med, p90 = [], []
    for rd in dict.fromkeys(r["round"] for r in start):
        v = sorted(int(r["ms"]) for r in start if r["node"] == node and r["round"] == rd)
        med.append(statistics.median(v))
        p90.append(v[min(len(v) - 1, int(len(v) * 0.9))])
    lines.append(f"| {node} | {rt} | {spread(med)} | {spread(p90)} |")
lines += ["", "## Many pods", "",
          "| node | pods | all Ready ms | runtime MiB | runtime processes |",
          "| --- | --- | --- | --- | --- |"]
for node in dict.fromkeys(r["node"] for r in many):
    for pods in dict.fromkeys(r["pods"] for r in many):
        rs = [r for r in many if r["node"] == node and r["pods"] == pods]
        lines.append(f"| {node} | {pods} | {spread([int(r['ready_ms']) for r in rs])} | "
                     f"{spread([float(r['runtime_mib']) for r in rs], '{:.1f}')} | "
                     f"{spread([float(r['runtime_procs']) for r in rs])} |")
lines += ["", "## Store on cp, resident memory over 5 minutes", ""]
for r in store:
    lines.append(f"{r['store']}: {float(r['mib_5m']):.1f} MiB")
open(f"{out}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY
echo "results in $out"
