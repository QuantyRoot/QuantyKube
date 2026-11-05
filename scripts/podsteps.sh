#!/usr/bin/env bash
# What a pod's start on this node is made of, from the times the runtime
# writes to its journal: per step, how often, and the fastest, median,
# 90th percentile and slowest in milliseconds. Runs on a node on
# QuantyKube's runtime.
#
#   scripts/podsteps.sh [since [until]]     e.g. "16:54" "16:56"
set -euo pipefail

since=${1:--30min}
args=(-u quantykube-runtime --since "$since" -o cat --no-pager)
[ $# -ge 2 ] && args+=(--until "$2")
journalctl "${args[@]}" |
    sed -n 's/^quantykube: \(.*\) \([0-9][0-9]*\) ms$/\1\t\2/p' |
    sort -t "$(printf '\t')" -k1,1 -k2,2n |
    awk -F '\t' '
    function flush(   p) {
        if (n == 0) return
        p = int(n * 0.9 + 0.5)
        if (p < 1) p = 1
        printf "%-16s %5d  %6d %6d %6d %6d\n", step, n, v[1],
            v[int((n + 1) / 2)], v[p], v[n]
    }
    BEGIN { printf "%-16s %5s  %6s %6s %6s %6s\n", "step", "n", "min", "median", "p90", "max" }
    {
        if ($1 != step) { flush(); step = $1; n = 0 }
        v[++n] = $2
    }
    END { flush() }'
