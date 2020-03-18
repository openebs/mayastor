#!/bin/bash
set -e
sudo kubeadm init --config /tmp/kubeadm_config.yaml \
  --ignore-preflight-errors=Swap,NumCPU

[ -d "$HOME"/.kube ] || mkdir -p "$HOME"/.kube
ln -s /etc/kubernetes/admin.conf "$HOME"/.kube/config

while ! nc -z localhost 6443; do
  echo "...Waiting on k8s API server to give a sign of life"
  sleep 5
done

sudo kubectl apply -f https://raw.githubusercontent.com/cloudnativelabs/kube-router/master/daemonset/kubeadm-kuberouter.yaml
