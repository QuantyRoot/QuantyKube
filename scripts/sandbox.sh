#!/usr/bin/env bash
# Checks `quantykube sandbox` against what a container must look like from
# the inside, and with `runc` set, times it against runc on the same
# root filesystem. Needs root.
#
#   [RUNC=/path/to/runc] scripts/sandbox.sh
set -euo pipefail

QK=$(realpath "${QK:-target/release/quantykube}")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
root=$work/rootfs
mkdir -p "$root"/{bin,proc,dev,sys,tmp,etc}
for b in /bin/sh /bin/cat /bin/true /bin/hostname; do
    cp "$b" "$root/bin/"
    for lib in $(ldd "$b" | grep -o '/[^ ]*'); do
        mkdir -p "$root$(dirname "$lib")"
        cp --update=none "$lib" "$root$lib"
    done
done

expect() {
    local want=$1; shift
    local got
    got=$("$@" 2>&1) || true
    if [ "$got" != "$want" ]; then
        printf 'FAIL: %s\nwant: %s\ngot:  %s\n' "$*" "$want" "$got"
        exit 1
    fi
}

s() { "$QK" sandbox --rootfs "$root" "$@"; }
expect 1 s -- /bin/sh -c 'echo $$'
expect box s --hostname box -- /bin/hostname
expect "/bin/sh: 1: cannot create /tmp/x: Read-only file system" \
    s --readonly -- /bin/sh -c 'echo x > /tmp/x'
s -- /bin/sh -c 'exit 7' && code=0 || code=$?
[ "$code" = 7 ] || { echo "FAIL: exit status $code, want 7"; exit 1; }
expect "quantykube: setup failed at Exec" s -- /missing
[ "$(hostname)" != box ] || { echo "FAIL: hostname leaked to the host"; exit 1; }
expect $'CapEff:\t00000000a80425fb\nCapBnd:\t00000000a80425fb\nNoNewPrivs:\t1\nSeccomp:\t2' \
    s -- /bin/sh -c 'while read -r l; do case $l in CapEff*|CapBnd*|NoNewPrivs*|Seccomp:*) echo "$l";; esac; done < /proc/self/status'
expect "full null ptmx pts random shm stderr stdin stdout tty urandom zero fd" \
    s -- /bin/sh -c 'cd /dev; echo full null ptmx pts random shm stderr stdin stdout tty urandom zero fd; for f in *; do case " full null ptmx pts random shm stderr stdin stdout tty urandom zero fd " in *" $f "*) ;; *) echo "extra /dev/$f";; esac; done'
expect "/bin/sh: 1: cannot create /sys/x: Read-only file system" s -- /bin/sh -c 'echo > /sys/x'
if [ -e /sys/fs/cgroup/cgroup.controllers ]; then
    expect "0::/quantykube/check" s --cgroup check --pids 20 -- /bin/cat /proc/self/cgroup
fi
echo "sandbox: ok"

[ -n "${RUNC:-}" ] || exit 0
bundle=$work/bundle
mkdir -p "$bundle"
cp -a "$root" "$bundle/rootfs"
(cd "$bundle" && "$RUNC" spec)
python3 -I - "$bundle/config.json" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
c["process"]["args"] = ["/bin/true"]
c["process"]["terminal"] = False
c["root"]["readonly"] = True
json.dump(c, open(p, "w"))
PY
python3 -I - "$QK" "$root" "$RUNC" "$bundle" <<'PY'
import statistics, subprocess, sys, time
qk, root, runc, bundle = sys.argv[1:]
def timed(cmd):
    t = time.perf_counter()
    r = subprocess.run(cmd, capture_output=True)
    if r.returncode != 0:
        sys.exit(f"{cmd[0]} failed: {r.stderr.decode(errors='replace')}")
    return (time.perf_counter() - t) * 1000
runs = 50
ours = [timed([qk, "sandbox", "--rootfs", root, "--readonly", "--", "/bin/true"]) for _ in range(runs)]
theirs = [timed([runc, "run", "--bundle", bundle, f"t{i}"]) for i in range(runs)]
for name, ts in (("quantykube sandbox", ours), ("runc run", theirs)):
    ts.sort()
    print(f"{name:20} median {statistics.median(ts):6.2f} ms  p90 {ts[int(runs * 0.9)]:6.2f} ms")
PY
