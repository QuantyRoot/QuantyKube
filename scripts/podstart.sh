#!/usr/bin/env bash
# Times pod start through crictl against any CRI endpoint: runp alone, and
# runp through a busybox `sleep 3600` container that crictl reports running.
# Teardown is not timed. Then holds 10 such pods and sums the RSS of the
# processes whose command line matches PROCS. Needs test/busybox:1 loaded.
#
#   CRICTL=/path/to/crictl ENDPOINT=unix:///path/to.sock PROCS='<regex>' \
#       scripts/podstart.sh [N]
set -euo pipefail
shopt -s inherit_errexit

n=${1:-30}
work=$(mktemp -d)
cleanup() {
    for p in $(c pods -q --label bench=podstart); do
        c stopp "$p" >/dev/null || true
        c rmp "$p" >/dev/null || true
    done
    rm -rf "$work"
}
trap cleanup EXIT

cat >"$work/crictl.yaml" <<YAML
runtime-endpoint: $ENDPOINT
image-endpoint: $ENDPOINT
timeout: 30
YAML
c() { "$CRICTL" -c "$work/crictl.yaml" "$@" 2>/dev/null; }

pod_json() {
    cat >"$work/pod.json" <<JSON
{"metadata": {"name": "bench-$1", "namespace": "default", "uid": "bench-$1"},
 "labels": {"bench": "podstart"}, "log_directory": "$work/logs/$1",
 "linux": {"security_context": {"namespace_options": {"pid": 1}}}}
JSON
}
cat >"$work/ctr.json" <<'JSON'
{"metadata": {"name": "sleep"}, "image": {"image": "test/busybox:1"},
 "command": ["sleep", "3600"], "log_path": "sleep.log"}
JSON

ms() { echo $((($(date +%s%N) - $1) / 1000000)); }

# start_pod <name>: prints "<pod> <ctr> <runp ms> <total ms>"
start_pod() {
    pod_json "$1"
    local t0 pod ctr runp
    t0=$(date +%s%N)
    pod=$(c runp "$work/pod.json")
    runp=$(ms "$t0")
    ctr=$(c create "$pod" "$work/ctr.json" "$work/pod.json")
    c start "$ctr" >/dev/null
    local state=
    until [ "$state" = CONTAINER_RUNNING ]; do
        state=$(c inspect -o go-template --template '{{.status.state}}' "$ctr")
        case $state in CONTAINER_EXITED | "") echo "$ctr: $state" >&2; return 1 ;; esac
    done
    echo "$pod $ctr $runp $(ms "$t0")"
}

stop_pod() {
    c stop -t 0 "$2" >/dev/null
    c rm "$2" >/dev/null
    c stopp "$1" >/dev/null
    c rmp "$1" >/dev/null
}

stats() {
    sort -n | awk -v name="$1" '{ v[NR] = $1 }
        END { i = int((NR * 9 + 9) / 10)
              printf "%-22s median %5d ms  p90 %5d ms\n", name, v[int((NR + 1) / 2)], v[i] }'
}

# one untimed round so both runtimes start from warm caches
r=$(start_pod warm)
read -r pod ctr _ <<<"$r"
stop_pod "$pod" "$ctr"

for i in $(seq "$n"); do
    r=$(start_pod "$i")
    read -r pod ctr runp total <<<"$r"
    echo "$runp $total" >>"$work/times"
    stop_pod "$pod" "$ctr"
done
echo "$n rounds"
cut -d' ' -f1 "$work/times" | stats "runp"
cut -d' ' -f2 "$work/times" | stats "runp to running"

for i in $(seq 10); do start_pod "held-$i" >/dev/null; done
pids=$(pgrep -f "$PROCS" | grep -vx "$$" || true)
kb=0
for p in $pids; do
    r=$(awk '/^VmRSS/ { print $2 }' "/proc/$p/status" 2>/dev/null || true)
    kb=$((kb + ${r:-0}))
done
echo "RSS with 10 pods: $((kb / 1024)) MiB in $(echo $pids | wc -w) processes"
