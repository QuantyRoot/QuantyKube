#!/usr/bin/env bash
# Sets up the control plane on this node, after scripts/kubeadm-node.sh:
# kubeadm init with the public address in the API server's certificate,
# Flannel at a pinned version as the pod network, and a kubeconfig for a
# laptop with that address in it. Prints what the workers run to join.
#
#   sudo scripts/kubeadm-cp.sh <public address of this node>
set -euo pipefail

public=${1:?the public address of this node}
kubeadm init --pod-network-cidr 10.244.0.0/16 \
    --apiserver-cert-extra-sans "$public" >/root/kubeadm-init.log
export KUBECONFIG=/etc/kubernetes/admin.conf

work=$(mktemp -d)
curl -fsSLo "$work/kube-flannel.yml" \
    https://github.com/flannel-io/flannel/releases/download/v0.28.9/kube-flannel.yml
echo "1c06a15a771009c263bdb1f51c928af46f7606fe969f3963652167938d536aaa  $work/kube-flannel.yml" |
    sha256sum -c - >/dev/null
kubectl apply -f "$work/kube-flannel.yml" >/dev/null
rm -rf "$work"

# for the laptop: the same admin credentials, the public address
sed "s|server: https://.*:6443|server: https://$public:6443|" \
    /etc/kubernetes/admin.conf >/root/laptop.kubeconfig
chmod 600 /root/laptop.kubeconfig

kubectl wait --for=condition=Ready node --all --timeout=300s >/dev/null
echo "control plane ready; on w1 and w2, as root:"
kubeadm token create --print-join-command
echo "the laptop's kubeconfig: /root/laptop.kubeconfig"
