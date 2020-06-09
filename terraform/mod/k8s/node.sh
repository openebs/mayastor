#!/bin/bash
set -ex

echo ${nr_hugepages} | sudo tee  /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
sudo modprobe nbd
sudo modprobe xfs

case ${modprobe_nvme} in
    ubuntu-20.04-server-cloudimg-amd64.img)
        echo "loading nvme kernel modules"
        # for ubuntu-20.04-server-cloudimg-amd64.img we need to
        # install linux-modules-extra-<kernel-release>
        # so that we can load kernel module nvme-tcp
        sudo apt -y install `apt search linux-modules-extra | fgrep \`uname -r\` | sed -e "s/,.*//"`
        sudo modprobe nvme-tcp
        sudo modprobe nvmet
        ;;
    *)
        echo "nvme kernel modules not loaded!"
        ;;
esac

until $(nc -z ${master_ip} 6443); do
  echo "Waiting for API server to respond"
  sleep 5
done

sudo kubeadm join --token=${token} ${master_ip}:6443 \
  --discovery-token-unsafe-skip-ca-verification \
  --ignore-preflight-errors=Swap,SystemVerification
