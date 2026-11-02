#!/usr/bin/env bash
# What the comparison on stage is measured with, outside the cluster so
# that moving the cluster does not move it (CLUSTER.md).
#
#   node: node_exporter and process-exporter on this machine, which
#         count memory and CPU per program: etcd against the store,
#         containerd and its shims against the runtime.
#   cp <domain> <name>=<address>...: also Prometheus, scraping those
#         nodes, only on this machine, and Grafana with the dashboard
#         from deploy/grafana, behind Caddy with HTTPS on <domain>, with
#         a random admin password in /root/grafana-admin-password.
#
#   sudo scripts/monitoring.sh node
#   sudo scripts/monitoring.sh cp grafana.knet1.quantyverse.net \
#       cp=62.129.150.40 w1=62.129.150.64 w2=62.129.150.83
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
domain=${1:?cp needs the domain for Grafana}
shift
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
# reachable only through Caddy, which holds the certificate
cat >/etc/grafana/grafana.ini <<CONF
[paths]
data = /var/lib/grafana
logs = /var/log/grafana
plugins = /var/lib/grafana/plugins
provisioning = /etc/grafana/provisioning
[server]
http_addr = 127.0.0.1
domain = $domain
root_url = https://$domain/
[users]
allow_sign_up = false
[auth.anonymous]
enabled = false
[analytics]
reporting_enabled = false
check_for_updates = false
CONF
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
# never admin/admin on a public address: a random password, once
if [ ! -s /root/grafana-admin-password ]; then
    head -c 24 /dev/urandom | base64 | tr -d '/+=' >/root/grafana-admin-password
    chmod 600 /root/grafana-admin-password
fi
for _ in $(seq 30); do
    curl -fs http://127.0.0.1:3000/api/health >/dev/null && break
    sleep 1
done
# with the service's configuration and data, or it sets the password
# in a database of its own that nothing reads
grafana cli --homepath /usr/share/grafana --config /etc/grafana/grafana.ini \
    --configOverrides cfg:default.paths.data=/var/lib/grafana \
    admin reset-admin-password "$(cat /root/grafana-admin-password)" >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' -u "admin:$(cat /root/grafana-admin-password)" \
    http://127.0.0.1:3000/api/user)
[ "$code" = 200 ] || { echo "the admin password did not take ($code)"; exit 1; }

apt-get install -y -q caddy >/dev/null
cat >/etc/caddy/Caddyfile <<CONF
$domain {
    reverse_proxy 127.0.0.1:3000
}
CONF
ufw allow 80/tcp >/dev/null
ufw allow 443/tcp >/dev/null
systemctl enable --now caddy >/dev/null 2>&1
systemctl reload caddy
echo "Grafana on https://$domain, user admin, password in /root/grafana-admin-password"
