#!/usr/bin/env bash
# Takes a pod through its life with crictl, the CRI client the Kubernetes
# project ships, against `quantykube runtime`. Needs root.
#
#   CRICTL=/path/to/crictl IMAGE=<dir with manifest.json and blobs/> \
#       [CNI_BIN=<dir of CNI plugins>] scripts/cri.sh
#
# With CNI_BIN, pods get a bridge network on 10.88.77.0/24.
set -euo pipefail

QK=$(realpath "${QK:-target/release/quantykube}")
work=$(mktemp -d)
cleanup() {
    [ -z "${rt:-}" ] || kill "$rt" 2>/dev/null || true
    grep -o "$work[^ ]*" /proc/self/mounts | sort -r | xargs -r umount 2>/dev/null || true
    [ -z "${CNI_BIN:-}" ] || ip link del qktest0 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL: $*"; exit 1; }

"$QK" import --root "$work/root" "$IMAGE" test/busybox:1 >/dev/null
mkdir "$work/net.d"
if [ -n "${CNI_BIN:-}" ]; then
    cat >"$work/net.d/10-test.conflist" <<JSON
{"cniVersion": "1.0.0", "name": "test", "plugins": [{"type": "bridge",
 "bridge": "qktest0", "isGateway": true, "ipam": {"type": "host-local",
 "ranges": [[{"subnet": "10.88.77.0/24"}]], "dataDir": "$work/ipam"}},
 {"type": "portmap", "capabilities": {"portMappings": true}}]}
JSON
fi
"$QK" runtime --root "$work/root" --socket "$work/cri.sock" \
    --cni-conf "$work/net.d" --cni-bin "${CNI_BIN:-/nonexistent}" 2>"$work/rt.log" &
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
 "dns_config": {"servers": ["10.0.0.10"], "searches": ["svc.test"]},
 "port_mappings": [{"container_port": 8080, "host_port": 18080}],
 "linux": {"security_context": {"namespace_options": {"pid": 1}}}}
JSON
cat >"$work/ctr.json" <<'JSON'
{"metadata": {"name": "hello"}, "image": {"image": "test/busybox:1"},
 "command": ["sh", "-c", "echo $(hostname) $GREETING; echo kept > /data/f; httpd -p 8080 -h /etc; while true; do sleep 1; done"],
 "mounts": [{"container_path": "/data", "host_path": "VOLUME"}],
 "envs": [{"key": "GREETING", "value": "hi"}], "log_path": "hello.log"}
JSON
mkdir "$work/volume"
sed -i "s|VOLUME|$work/volume|" "$work/ctr.json"
pod=$(c runp "$work/pod.json") || fail "runp: $(tail -1 "$work/rt.log")"
ctr=$(c create "$pod" "$work/ctr.json" "$work/pod.json") || fail "create"
c start "$ctr" >/dev/null || fail "start"
for _ in $(seq 100); do
    [ "$(c logs "$ctr")" = "web-0 hi" ] && break
    sleep 0.05
done
[ "$(c logs "$ctr")" = "web-0 hi" ] || fail "logs: $(c logs "$ctr")"
[ "$(cat "$work/volume/f")" = kept ] || fail "volume"
c ps -q --state running | grep -qx "$ctr" || fail "not running"

# exec joins the container: its environment, and its PID namespace
got=$(c exec -s "$ctr" sh -c 'echo $GREETING; [ -d /proc/$$ ] && echo own pid') || true
[ "$got" = "$(printf 'hi\nown pid')" ] || fail "exec: $got"
if [ -n "${CNI_BIN:-}" ]; then
    ip=$(c inspectp -o go-template --template '{{.status.network.ip}}' "$pod")
    case $ip in 10.88.77.*) ;; *) fail "pod address: $ip" ;; esac
    c exec -s "$ctr" ping -c1 -W2 10.88.77.1 >/dev/null || fail "no route out of the pod"
    # the portmap plugin got the mapping: it forwards the host port
    iptables -t nat -S 2>/dev/null | grep -q -- "--dport 18080 -j DNAT --to-destination $ip:8080" ||
        fail "no forwarding for host port 18080"
fi
[ "$(c exec -s "$ctr" cat /etc/hostname /etc/resolv.conf)" = \
    "$(printf 'web-0\nsearch svc.test\nnameserver 10.0.0.10')" ] || fail "pod files"
start=$(date +%s)
! c exec -s --timeout 1 "$ctr" sleep 30 >/dev/null || fail "exec timeout"
[ $(($(date +%s) - start)) -lt 5 ] || fail "exec outlived its timeout"

# a shell as PID 1 ignores SIGTERM, so this takes the SIGKILL
c stop -t 1 "$ctr" >/dev/null
c inspect "$ctr" | grep -q '"exitCode": 137' || fail "exit code"
c rm "$ctr" >/dev/null && c stopp "$pod" >/dev/null && c rmp "$pod" >/dev/null ||
    fail "removal"
[ -z "$(c pods -q)" ] || fail "pod left behind"
if [ -n "${CNI_BIN:-}" ]; then
    ! ls "$work/ipam/test" | grep -q '^10\.' || fail "address not released"
    ! iptables -t nat -S 2>/dev/null | grep -q -- "--dport 18080" || fail "host port still forwarded"
fi
! grep -q "$work" /proc/self/mounts || fail "mount left behind"

sed 's/"pid": 1/"network": 2, "pid": 1/' "$work/pod.json" >"$work/host.json"
! c runp "$work/host.json" >/dev/null || fail "a host network pod ran"
echo "cri: ok"
