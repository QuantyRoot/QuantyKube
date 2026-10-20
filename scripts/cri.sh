#!/usr/bin/env bash
# Takes a pod through its life with crictl, the CRI client the Kubernetes
# project ships, against `quantykube runtime`. Needs root.
#
#   CRICTL=/path/to/crictl IMAGE=<dir with manifest.json and blobs/> \
#       scripts/cri.sh
set -euo pipefail

QK=$(realpath "${QK:-target/release/quantykube}")
work=$(mktemp -d)
cleanup() {
    [ -z "${rt:-}" ] || kill "$rt" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL: $*"; exit 1; }

"$QK" import --root "$work/root" "$IMAGE" test/busybox:1 >/dev/null
"$QK" runtime --root "$work/root" --socket "$work/cri.sock" 2>"$work/rt.log" &
rt=$!
for _ in $(seq 100); do [ -S "$work/cri.sock" ] && break; sleep 0.05; done
cat >"$work/crictl.yaml" <<YAML
runtime-endpoint: unix://$work/cri.sock
image-endpoint: unix://$work/cri.sock
timeout: 10
YAML
c() { "$CRICTL" -c "$work/crictl.yaml" "$@" 2>/dev/null; }

[ "$(c version | grep RuntimeName)" = "RuntimeName:  quantykube" ] ||
    fail "version"
c images | grep -q 'test/busybox' || fail "image not listed"

cat >"$work/pod.json" <<JSON
{"metadata": {"name": "web", "namespace": "default", "uid": "1"},
 "hostname": "web-0", "log_directory": "$work/logs",
 "linux": {"security_context": {"namespace_options": {"pid": 1}}}}
JSON
cat >"$work/ctr.json" <<'JSON'
{"metadata": {"name": "hello"}, "image": {"image": "test/busybox:1"},
 "command": ["sh", "-c", "echo $(hostname) $GREETING; while true; do sleep 1; done"],
 "envs": [{"key": "GREETING", "value": "hi"}], "log_path": "hello.log"}
JSON
pod=$(c runp "$work/pod.json") || fail "runp"
ctr=$(c create "$pod" "$work/ctr.json" "$work/pod.json") || fail "create"
c start "$ctr" >/dev/null || fail "start"
for _ in $(seq 100); do
    [ "$(c logs "$ctr")" = "web-0 hi" ] && break
    sleep 0.05
done
[ "$(c logs "$ctr")" = "web-0 hi" ] || fail "logs: $(c logs "$ctr")"
c ps -q --state running | grep -qx "$ctr" || fail "not running"

# a shell as PID 1 ignores SIGTERM, so this takes the SIGKILL
c stop -t 1 "$ctr" >/dev/null
c inspect "$ctr" | grep -q '"exitCode": 137' || fail "exit code"
c rm "$ctr" >/dev/null && c stopp "$pod" >/dev/null && c rmp "$pod" >/dev/null ||
    fail "removal"
[ -z "$(c pods -q)" ] || fail "pod left behind"
! grep -q "$work" /proc/self/mounts || fail "mount left behind"

sed 's/"pid": 1/"network": 2, "pid": 1/' "$work/pod.json" >"$work/host.json"
! c runp "$work/host.json" >/dev/null || fail "a host network pod ran"
echo "cri: ok"
