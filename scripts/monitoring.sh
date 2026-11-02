#!/usr/bin/env bash
# What the comparison on stage is measured with, outside the cluster so
# that moving the cluster does not move it (CLUSTER.md).
#
#   node: node_exporter and process-exporter on this machine, which
#         count memory and CPU per program: etcd against the store,
#         containerd and its shims against the runtime.
#   cp <name>=<address>...: also Prometheus, scraping those nodes, and Grafana
#         with the dashboard from deploy/grafana, both reachable only
#         from this machine (ssh -L 3000:localhost:3000).
#
#   sudo scripts/monitoring.sh node
#   sudo scripts/monitoring.sh cp cp=62.129.150.40 w1=62.129.150.64 w2=62.129.150.83
set -euo pipefail

role=${1:?node or cp}
shift
here=$(cd "$(dirname "$0")/.." && pwd)
export DEBIAN_FRONTEND=noninteractive

apt-get update -q >/dev/null
apt-get install -y -q prometheus-node-exporter prometheus-process-exporter >/dev/null

# comm is cut at 15 characters: kube-controller-manager is
# kube-controller, containerd-shim-runc-v2 containerd-shim
cat >/etc/default/prometheus-process-exporter <<'CONF'
ARGS="-config.path /etc/prometheus-process-exporter.yml"
CONF
cat >/etc/prometheus-process-exporter.yml <<'CONF'
process_names:
  - name: quantykube-store
    cmdline: ['quantykube\s.*\bserve\b']
  - name: quantykube-runtime
    cmdline: ['quantykube\s.*\bruntime\b']
  - name: "{{.Comm}}"
    comm:
      - etcd
      - kube-apiserver
      - kube-controller
      - kube-scheduler
      - kubelet
      - containerd
      - containerd-shim
      - runc
      - kube-proxy
      - flanneld
      - coredns
      - pause
CONF
systemctl restart prometheus-node-exporter prometheus-process-exporter

if [ "$role" = node ]; then
    echo "exporters on :9100 and :9256"
    exit 0
fi
[ "$role" = cp ] || { echo "node or cp"; exit 1; }
[ $# -gt 0 ] || { echo "cp needs name=address for each node"; exit 1; }

apt-get install -y -q prometheus >/dev/null
# job <name> <port>: one target per node, labelled with its name
job() {
    printf '  - job_name: %s\n    static_configs:\n' "$1"
    for n in "${nodes[@]}"; do
        printf '      - targets: ["%s:%s"]\n        labels: {node: %s}\n' \
            "${n#*=}" "$2" "${n%%=*}"
    done
}
nodes=("$@")
{
    printf 'global:\n  scrape_interval: 5s\nscrape_configs:\n'
    job node 9100
    job process 9256
} >/etc/prometheus/prometheus.yml
# only this machine: Grafana asks it, the laptop goes through ssh
cat >/etc/default/prometheus <<'CONF'
ARGS="--web.listen-address=127.0.0.1:9090 --storage.tsdb.retention.time=30d"
CONF
systemctl restart prometheus

if ! command -v grafana-server >/dev/null; then
    install -d /etc/apt/keyrings
    curl -fsSL https://apt.grafana.com/gpg.key | gpg --dearmor --yes \
        -o /etc/apt/keyrings/grafana.gpg
    echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
        >/etc/apt/sources.list.d/grafana.list
    apt-get update -q >/dev/null
    apt-get install -y -q grafana >/dev/null
fi
sed -i 's/^;\?http_addr =.*/http_addr = 127.0.0.1/' /etc/grafana/grafana.ini
mkdir -p /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards \
    /var/lib/grafana/dashboards
cat >/etc/grafana/provisioning/datasources/prometheus.yaml <<'CONF'
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    url: http://127.0.0.1:9090
    isDefault: true
CONF
cat >/etc/grafana/provisioning/dashboards/quantykube.yaml <<'CONF'
apiVersion: 1
providers:
  - name: quantykube
    folder: QuantyKube
    options:
      path: /var/lib/grafana/dashboards
CONF
cp "$here/deploy/grafana/quantykube.json" /var/lib/grafana/dashboards/
chown -R grafana:grafana /var/lib/grafana/dashboards
systemctl enable --now grafana-server >/dev/null 2>&1
systemctl restart grafana-server
echo "Prometheus on 127.0.0.1:9090, Grafana on 127.0.0.1:3000 (admin/admin,"
echo "changed on first login); from the laptop: ssh -L 3000:localhost:3000 root@<cp>"
