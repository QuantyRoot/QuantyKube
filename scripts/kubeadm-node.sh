#!/usr/bin/env bash
# Prepares this machine as kubeadm expects a node: kubeadm, kubelet,
# kubectl and crictl at pinned versions, the CNI plugins, containerd with
# the systemd cgroup driver, and the kubelet's unit as kubeadm's packages
# install it. For a CI runner or a server set up to be wiped.
#
#   sudo scripts/kubeadm-node.sh
set -euo pipefail

work=$(mktemp -d)
cd "$work"
for b in kubeadm kubelet kubectl; do
    curl -fsSLO "https://dl.k8s.io/release/v1.37.1/bin/linux/amd64/$b"
done
curl -fsSLo cni.tgz https://github.com/containernetworking/plugins/releases/download/v1.8.0/cni-plugins-linux-amd64-v1.8.0.tgz
curl -fsSLo crictl.tgz https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.34.0/crictl-v1.34.0-linux-amd64.tar.gz
sha256sum -c - <<'SUMS'
087b6340dcea496f2cde2b3888ecd414354c08a0aceabd6b7ca195cccb66ecd3  kubeadm
cee008c1640c3b41820ef31d35b3cffc132b0c4b78c288be2b8022ebc7aa8293  kubelet
65691ff77eb6fa44c908b77a1082c9f092c3b9733b5cefabec0d1104890e21a8  kubectl
ab3bda535f9d90766cccc90d3dddb5482003dd744d7f22bcf98186bf8eea8be6  cni.tgz
a8ff2a3edb37a98daf3aba7c3b284fe0aa5bff24166d896ab9ef64c8913c9f51  crictl.tgz
SUMS
tar -xzf crictl.tgz
install -m 755 kubeadm kubelet kubectl crictl /usr/local/bin/
mkdir -p /opt/cni/bin && tar -xzf cni.tgz -C /opt/cni/bin

apt-get install -y conntrack >/dev/null
swapoff -a
modprobe br_netfilter
sysctl -qw net.ipv4.ip_forward=1 net.bridge.bridge-nf-call-iptables=1
# Docker on a CI runner drops forwarded packets, pods' among them
iptables -P FORWARD ACCEPT
mkdir -p /etc/containerd
containerd config default >/etc/containerd/config.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
grep -q 'SystemdCgroup = true' /etc/containerd/config.toml
systemctl restart containerd

cat >/etc/systemd/system/kubelet.service <<'UNIT'
[Unit]
Wants=network-online.target
After=network-online.target
[Service]
ExecStart=/usr/local/bin/kubelet
Restart=always
StartLimitInterval=0
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
mkdir -p /etc/systemd/system/kubelet.service.d
cat >/etc/systemd/system/kubelet.service.d/10-kubeadm.conf <<'UNIT'
[Service]
Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
ExecStart=
ExecStart=/usr/local/bin/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS
UNIT
systemctl daemon-reload
systemctl enable kubelet
rm -rf "$work"
