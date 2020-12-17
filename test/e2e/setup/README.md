
This directory contains the scripts to bring up and environment ready for testing
(for example by CI).


## Pre-requisites

### Step 1

Install libvirt plugin
```
# From https://github.com/vagrant-libvirt/vagrant-libvirt#installation
# apt-get build-dep vagrant ruby-libvirt # Usually not necessary
sudo apt-get install -y qemu libvirt-daemon-system libvirt-clients ebtables dnsmasq-base ruby-libvirt
sudo apt-get install -y libxslt-dev libxml2-dev libvirt-dev zlib1g-dev ruby-dev
vagrant plugin install vagrant-libvirt
```

### Step 2

Clone kubespray (tested at v2.14.2):
```
git clone --branch v2.14.2 git@github.com:kubernetes-sigs/kubespray.git
```

NB You may need to update $KUBESPRAY_REPO in bringup-cluster.sh to
point to the location where you cloned kubespray.

### Step 3

Install ansible (tested at v2.9.6).
https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html

### Step 4 (Optional)

If you're going to want to push images from a host to the insecure registry in the cluster,
You'll need to add the following to your `/etc/docker/daemon.json`:
```
{
    "insecure-registries" : ["172.18.8.101:30291"]
}
```
The IP is static and set by the Vagrantfile, so shouldn't require changing between deployments.
The port is set in test-registry.yaml, and also shouldn't vary.

## Bringing up the cluster

```
./bringup-cluster.sh
```


## Cleanup

From the kubespray repo:
```
vagrant destroy -f
```

## Cluster Spec

The created cluster is based on the defaults provided by the kubespray repo, with
some values are overwritten in by the config.rb file created by bringup-cluster.sh.

In summary, the cluster consists of 3 nodes (1 master, 2 workers).
All 3 are labelled for use by mayasotr (ie including the master), to try
to still get 3 copies of mayastor but reduce resource usage a little.
Each node has 2 2G data disks (sda, sdb) available, as well as 4 CPUs and 6GiB of RAM.
