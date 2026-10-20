#!/usr/bin/env bash
# etcd against QuantyKube on the same machine and disk, with etcd's own
# benchmark tool. docs/BENCHMARKS.md has the results and how to read them.
#
#   BIN=<dir with etcd, etcdctl, k3s> scripts/bench.sh [setup|micro|fsync|k8s|report|all]
#
# Work files go to $B, bench-out by default. The k8s phase runs k3s and
# needs root.
set -u
B=${B:-$PWD/bench-out}
BIN=${BIN:?set BIN to a directory holding etcd, etcdctl and k3s}
QK=${QK:-$PWD/target/release/quantykube}
ETCD=$BIN/etcd
ETCDCTL=$BIN/etcdctl
K3S=$BIN/k3s
BENCH=$B/benchmark
GO=${GO:-/usr/local/go/bin/go}
REPS=${REPS:-3}
RUN=$B/run          # data dirs (same filesystem for both stores)
OUT=$B/results      # raw logs + CSVs
mkdir -p "$B" "$RUN" "$OUT/raw"
CSV=$OUT/micro.csv
MEMCSV=$OUT/mem_b.csv
K8SCSV=$OUT/k8s.csv
FAILS=$OUT/failures.log

log() { echo "[$(date +%T)] $*" >&2; }

setup() {
  if [ ! -x "$BENCH" ]; then
    [ -d "$B/etcd-src" ] || git clone --depth 1 --branch v3.6.15 https://github.com/etcd-io/etcd "$B/etcd-src"
    (cd "$B/etcd-src/tools/benchmark" && GOPROXY=https://proxy.golang.org,direct "$GO" build -o "$BENCH" .)
  fi
  {
    echo "date: $(date -Is)"
    echo "kernel: $(uname -r)"
    echo "nproc: $(nproc)"
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "mem_total: $(grep MemTotal /proc/meminfo | awk '{print $2" kB"}')"
    echo "df -T $RUN:"; df -T "$RUN"
    echo "mount options:"; findmnt -no SOURCE,FSTYPE,OPTIONS -T "$RUN"
    echo "etcd: $($ETCD --version | head -1)"
    echo "k3s: $($K3S --version | head -1)"
    echo "quantykube sha256: $(sha256sum "$QK" | cut -d' ' -f1)"
    echo "benchmark built from etcd v3.6.15 tools/benchmark, go: $($GO version)"
  } > "$OUT/machine.txt"
}

# start_store <etcd|qk> <dir>  -> sets SPID, EP (endpoint), DATA (data path)
start_store() {
  local s=$1 d=$2
  rm -rf "$d"; mkdir -p "$d"
  if [ "$s" = etcd ]; then
    DATA=$d/etcd-data; EP=unix://$d/etcd.sock
    "$ETCD" --name default --data-dir "$DATA" \
      --listen-client-urls "unix://$d/etcd.sock,http://127.0.0.1:2379" --advertise-client-urls http://127.0.0.1:2379 \
      --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 \
      --initial-cluster default=http://127.0.0.1:2380 > "$d/server.log" 2>&1 &
  else
    DATA=$d/qk.db; EP=unix://$d/qk.sock
    "$QK" serve --data "$DATA" --socket "$d/qk.sock" > "$d/server.log" 2>&1 &
  fi
  SPID=$!
  for _ in $(seq 1 100); do
    if "$ETCDCTL" --endpoints "$EP" --command-timeout 1s endpoint health >/dev/null 2>&1; then return 0; fi
    kill -0 "$SPID" 2>/dev/null || break
    sleep 0.2
  done
  log "store $s failed to start"; cat "$d/server.log" >&2; return 1
}
stop_pid() {  # stop_pid <pid>: TERM, wait up to 30 s, then KILL
  local p=$1
  kill "$p" 2>/dev/null || return 0
  for _ in $(seq 1 150); do kill -0 "$p" 2>/dev/null || break; sleep 0.2; done
  kill -0 "$p" 2>/dev/null && { log "pid $p did not exit, SIGKILL"; kill -9 "$p"; }
  wait "$p" 2>/dev/null || true
}
vm() { awk -v k="$2:" '$1==k{print $2}' "/proc/$1/status" 2>/dev/null; }  # vm <pid> VmHWM -> kB
dsize() { du -sk "$1" | cut -f1; }          # kB
dsizeh() { du -sh "$1" | cut -f1; }
count_keys() { "$ETCDCTL" --endpoints "$EP" get "" --prefix --count-only -w fields 2>&1 | awk '/"Count"/{print $3}'; }

# parse <file> <section-regex or ''> -> "rps avg_ms p99_ms"
parse() {
  python3 -I - "$1" "$2" <<'EOF'
import sys,re
t=open(sys.argv[1],errors='replace').read()
sec=sys.argv[2]
if sec:
    i=t.find(sec); t=t[i:] if i>=0 else ''
    j=t.find('Summary:',t.find('Summary:')+1)  # stop at next summary
    if j>0: t=t[:j]
def g(p):
    m=re.search(p,t); return m.group(1) if m else 'NA'
rps=g(r'Requests/sec:\s*([\d.]+)')
avg=g(r'Average:\s*([\d.]+) secs'); p99=g(r'\n\s*99% in ([\d.]+) secs')
f=lambda x: x if x=='NA' else '%.3f'%(float(x)*1000)
print(rps,f(avg),f(p99))
EOF
}

# workload definitions: name | global flags | command
W_a='--clients 1 --conns 1|put --total 10000 --val-size 256 --sequential-keys --key-space-size 10000'
W_b='--clients 16 --conns 4|put --total 20000 --val-size 256 --sequential-keys --key-space-size 20000'
W_c='--clients 16 --conns 4|range benchkey --total 50000 --consistency l'
W_cs='--clients 16 --conns 4|range benchkey --total 50000 --consistency s'
W_d='--clients 16 --conns 4|txn-put --total 20000 --val-size 256 --key-space-size 20000 --txn-ops 1'
W_e='--clients 1 --conns 1|watch-latency --streams 10 --watchers-per-stream 10 --put-total 1000 --put-rate 100 --key-size 32 --val-size 32'
EXPECT_a=10000; EXPECT_b=20000; EXPECT_d=20000

micro() {
  [ -f "$CSV" ] || echo "workload,store,rep,metric,rps,avg_ms,p99_ms,errors,keys_after,expected_keys,exit" > "$CSV"
  [ -f "$MEMCSV" ] || echo "store,rep,vmhwm_kb,vmrss_kb,data_kb,data_h" > "$MEMCSV"
  for w in ${WORKLOADS:-a b c cs d e}; do
    local def; eval "def=\$W_$w"
    local gflags=${def%%|*} cmd=${def#*|}
    for rep in $(seq 1 "$REPS"); do
      for s in etcd qk; do     # interleave stores to spread drift
        local d=$RUN/micro-$s
        start_store "$s" "$d" || { echo "$w $s rep$rep: server start failed" >> "$FAILS"; continue; }
        if [ "$w" = c ] || [ "$w" = cs ]; then "$ETCDCTL" --endpoints "$EP" put benchkey "$(head -c 256 /dev/zero | tr '\0' v)" >/dev/null; fi
        local raw=$OUT/raw/$w-$s-$rep.txt
        log "workload $w store $s rep $rep"
        # shellcheck disable=SC2086
        timeout 600 "$BENCH" --precise --endpoints "$EP" $gflags $cmd > "$raw" 2>&1
        local rc=$?
        # benchmark prints "Error distribution:" followed by "  [N]\t<error>" lines
        local errs; errs=$(awk '/Error distribution/{f=1;next} f&&match($0,/^[ \t]*\[[0-9]+\]/){gsub(/[^0-9]/,"",$1); s+=$1} END{print s+0}' "$raw")
        local keys=NA expect=NA
        eval "expect=\${EXPECT_$w:-NA}"
        [ "$expect" != NA ] && keys=$(count_keys)
        if [ "$w" = e ]; then
          echo "$w,$s,$rep,put,$(parse "$raw" 'Put summary' | tr ' ' ,),$errs,$keys,$expect,$rc" >> "$CSV"
          echo "$w,$s,$rep,watch_event,$(parse "$raw" 'Watch events summary' | tr ' ' ,),$errs,$keys,$expect,$rc" >> "$CSV"
        else
          echo "$w,$s,$rep,req,$(parse "$raw" '' | tr ' ' ,),$errs,$keys,$expect,$rc" >> "$CSV"
        fi
        if [ "$rc" != 0 ] || [ "$errs" != 0 ] || { [ "$expect" != NA ] && [ "$keys" != "$expect" ]; }; then
          { echo "=== FAIL workload $w store $s rep $rep exit=$rc errors=$errs keys=$keys expected=$expect"
            tail -40 "$raw"; echo "--- server stderr:"; tail -40 "$d/server.log"; } >> "$FAILS"
        fi
        if [ "$w" = b ]; then
          sync
          echo "$s,$rep,$(vm "$SPID" VmHWM),$(vm "$SPID" VmRSS),$(dsize "$DATA"),$(dsizeh "$DATA")" >> "$MEMCSV"
        fi
        stop_pid "$SPID"
        cp "$d/server.log" "$OUT/raw/$w-$s-$rep.server.log"
      done
    done
  done
}

fsync_check() {
  local f=$OUT/fsync.txt; : > "$f"
  for s in etcd qk; do
    local d=$RUN/fsync-$s; rm -rf "$d"; mkdir -p "$d"
    if [ "$s" = etcd ]; then
      strace -f -c -e trace=fsync,fdatasync,sync_file_range,msync -o "$d/strace.txt" "$ETCD" --name default --data-dir "$d/etcd-data" \
        --listen-client-urls "unix://$d/etcd.sock,http://127.0.0.1:2379" --advertise-client-urls http://127.0.0.1:2379 \
        --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 \
        --initial-cluster default=http://127.0.0.1:2380 > "$d/server.log" 2>&1 &
      EP=unix://$d/etcd.sock
    else
      strace -f -c -e trace=fsync,fdatasync,sync_file_range,msync -o "$d/strace.txt" "$QK" serve --data "$d/qk.db" --socket "$d/qk.sock" > "$d/server.log" 2>&1 &
      EP=unix://$d/qk.sock
    fi
    local sp=$!; sleep 5
    for c in "1 1" "16 4"; do
      set -- $c
      "$BENCH" --endpoints "$EP" --clients "$1" --conns "$2" put --total 1000 --val-size 256 --sequential-keys --key-space-size 1000 >/dev/null 2>&1
    done
    stop_pid "$(ps -o pid= --ppid "$sp" | tr -d ' ')"; wait "$sp" 2>/dev/null
    { echo "== $s: 2000 puts (1000 with 1 client, then 1000 with 16 clients), plus startup"; cat "$d/strace.txt"; } >> "$f"
  done
}

K3SFLAGS="--disable-agent --token bench --bind-address 127.0.0.1 --disable=traefik,servicelb,metrics-server,local-storage,coredns --disable-network-policy --flannel-backend=none --disable-kube-proxy --disable-cloud-controller --disable-helm-controller"
descendants() { local p; for p in $(ps -o pid= --ppid "$1"); do echo "$p"; descendants "$p"; done; }
tsec() { local t0 t1; t0=$(date +%s.%N); "$@"; local rc=$?; t1=$(date +%s.%N); LAST_T=$(echo "$t1 - $t0" | bc); return $rc; }

k8s() {
  [ -f "$K8SCSV" ] || echo "store,rep,ready_s,idle_rss_kb,create_s,create_ok,get_s,get_lines,delete_s,after_rss_kb,after_hwm_kb,data_kb,data_h,k3s_leftovers" > "$K8SCSV"
  local yaml=$OUT/cm500.yaml
  python3 -I -c '
for i in range(500):
    print("---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: bench-cm-%d\n  namespace: default\ndata:\n  k: \"%s\"" % (i, "x"*256))
' > "$yaml"
  for rep in $(seq 1 "${K8SREPS:-2}"); do
  # alternate which store goes first (odd reps: etcd first, even reps: qk first)
  local order="etcd qk"; [ $((rep % 2)) = 0 ] && order="qk etcd"
  for s in ${K8SORDER:-$order}; do
    local d=$RUN/k8s-$s
    start_store "$s" "$d/store" || { echo "k8s $s: store start failed" >> "$FAILS"; continue; }
    local kc=$d/kubeconfig kd=$d/k3s
    mkdir -p "$kd"
    local t0; t0=$(date +%s.%N)
    "$K3S" server $K3SFLAGS --data-dir "$kd" --datastore-endpoint="$EP" --write-kubeconfig "$kc" > "$d/k3s.log" 2>&1 &
    local kp=$! ready=NA
    for _ in $(seq 1 600); do
      if [ -f "$kc" ] && [ "$("$K3S" kubectl --kubeconfig "$kc" get --raw /readyz 2>/dev/null)" = ok ]; then
        ready=$(echo "$(date +%s.%N) - $t0" | bc); break; fi
      kill -0 "$kp" 2>/dev/null || break
      sleep 0.5
    done
    log "k8s $s ready after $ready s"
    if [ "$ready" = NA ]; then
      { echo "=== FAIL k8s $s: k3s never became ready"; tail -60 "$d/k3s.log"; echo "--- store stderr:"; tail -60 "$d/store/server.log"; } >> "$FAILS"
    fi
    sleep 60
    local idle; idle=$(vm "$SPID" VmRSS)
    local K="$K3S kubectl --kubeconfig $kc"
    tsec $K apply -f "$yaml" > "$d/apply.log" 2>&1; local crc=$? ct=$LAST_T
    local ok; ok=$(grep -c 'created' "$d/apply.log")
    [ "$crc" != 0 ] && { echo "=== FAIL k8s $s apply exit=$crc"; tail -30 "$d/apply.log"; echo "--- store stderr:"; tail -40 "$d/store/server.log"; } >> "$FAILS"
    tsec bash -c "$K get configmaps -A > $d/get.txt 2>&1"; local gt=$LAST_T
    local gl; gl=$(wc -l < "$d/get.txt")
    tsec $K delete -f "$yaml" > "$d/delete.log" 2>&1; local drc=$? dt=$LAST_T
    [ "$drc" != 0 ] && { echo "=== FAIL k8s $s delete exit=$drc"; tail -30 "$d/delete.log"; echo "--- store stderr:"; tail -40 "$d/store/server.log"; } >> "$FAILS"
    local left; left=$($K get configmaps -n default --no-headers 2>/dev/null | grep -c bench-cm)
    [ "$left" != 0 ] && echo "=== FAIL k8s $s: $left bench ConfigMaps left after delete" >> "$FAILS"
    sync
    local arss ahwm dk dh; arss=$(vm "$SPID" VmRSS); ahwm=$(vm "$SPID" VmHWM); dk=$(dsize "$DATA"); dh=$(dsizeh "$DATA")
    local kids; kids=$(descendants "$kp" | tr '\n' ' ')
    stop_pid "$kp"
    local leftovers=0
    for p in $kids; do kill -0 "$p" 2>/dev/null && { leftovers=$((leftovers+1)); log "killing k3s leftover $p"; stop_pid "$p"; }; done
    stop_pid "$SPID"
    echo "$s,$rep,$ready,$idle,$ct,$ok,$gt,$gl,$dt,$arss,$ahwm,$dk,$dh,$leftovers" >> "$K8SCSV"
    cp "$d/k3s.log" "$OUT/raw/k8s-$s-$rep.k3s.log"; cp "$d/store/server.log" "$OUT/raw/k8s-$s-$rep.server.log"
  done
  done
}

report() {
  python3 -I - "$OUT" <<'EOF'
import csv,sys,statistics as st,os
o=sys.argv[1]
def med(xs):
    xs=[float(x) for x in xs if x not in ('NA','')]
    return '%.3f'%st.median(xs) if xs else 'NA'
rows=list(csv.DictReader(open(o+'/micro.csv')))
names={'a':'a) put, 1 client/1 conn, 10000 req, 256 B, sequential keys',
 'b':'b) put, 16 clients/4 conns, 20000 req, 256 B, sequential keys',
 'c':'c) range 1 key, 16 clients/4 conns, 50000 req, linearizable (tool default)',
 'cs':'c2) range 1 key, 16 clients/4 conns, 50000 req, serializable',
 'd':'d) txn-put, 16 clients/4 conns, 20000 txns, 1 put/txn, 256 B',
 'e':'e) watch-latency, 10 streams x 10 watchers, 1000 puts @100/s'}
for w in ['a','b','c','cs','d','e']:
    rs=[r for r in rows if r['workload']==w]
    if not rs: continue
    print('\n### '+names[w]+'\n')
    print('| store | metric | req/s (median) | avg ms (median) | p99 ms (median) | runs | per-run req/s | errors | keys after (expected) |')
    print('|---|---|---|---|---|---|---|---|---|')
    for s in ['etcd','qk']:
        for m in sorted(set(r['metric'] for r in rs)):
            x=[r for r in rs if r['store']==s and r['metric']==m]
            if not x: continue
            print('| %s | %s | %s | %s | %s | %d | %s | %s | %s |'%('etcd' if s=='etcd' else 'QuantyKube',m,med([r['rps'] for r in x]),med([r['avg_ms'] for r in x]),med([r['p99_ms'] for r in x]),len(x),' / '.join('%.0f'%float(r['rps']) if r['rps']!='NA' else 'NA' for r in x),','.join(r['errors'] for r in x),','.join(r['keys_after'] for r in x)+' ('+x[0]['expected_keys']+')'))
if os.path.exists(o+'/mem_b.csv'):
    m=list(csv.DictReader(open(o+'/mem_b.csv')))
    print('\n### Memory and disk after workload b\n\n| store | VmHWM MiB (median) | VmRSS MiB (median) | data size (median) | per-run du -sh |\n|---|---|---|---|---|')
    for s in ['etcd','qk']:
        x=[r for r in m if r['store']==s]
        if x: print('| %s | %.1f | %.1f | %.1f MiB | %s |'%('etcd' if s=='etcd' else 'QuantyKube',float(med([r['vmhwm_kb'] for r in x]))/1024,float(med([r['vmrss_kb'] for r in x]))/1024,float(med([r['data_kb'] for r in x]))/1024,' / '.join(r['data_h'] for r in x)))
if os.path.exists(o+'/k8s.csv'):
    k=list(csv.DictReader(open(o+'/k8s.csv')))
    print('\n### Kubernetes (k3s control plane, 500 ConfigMaps)\n\n| store | rep | ready s | idle store RSS MiB (60 s) | apply 500 CM s | created | get -A s | get lines | delete s | store RSS after MiB | store VmHWM MiB | data after | k3s leftovers |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    for r in k:
        f=lambda v: '%.1f'%(float(v)/1024) if v not in ('','NA') else 'NA'
        g=lambda v: '%.2f'%float(v) if v not in ('','NA') else 'NA'
        print('| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |'%('etcd' if r['store']=='etcd' else 'QuantyKube',r['rep'],g(r['ready_s']),f(r['idle_rss_kb']),g(r['create_s']),r['create_ok'],g(r['get_s']),r['get_lines'],g(r['delete_s']),f(r['after_rss_kb']),f(r['after_hwm_kb']),r['data_h'],r['k3s_leftovers']))
EOF
}

case "${1:-all}" in
  setup) setup ;;
  micro) micro ;;
  fsync) fsync_check ;;
  k8s) k8s ;;
  report) report ;;
  all) rm -f "$CSV" "$MEMCSV" "$K8SCSV" "$FAILS"; setup; micro; fsync_check; k8s; report > "$OUT/tables.md"; cat "$OUT/tables.md" ;;
esac
