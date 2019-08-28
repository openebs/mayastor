# Moac

Moac is a proof of concept of control plane for mayastor. It is written
in nodejs and makes use of kubernetes-client library to interact with
k8s api server. In a nutshell it is responsible for following things:

- _node operator_: Keeping track of nodes with running mayastor instances
- [storage pool operator](/doc/pool-operator.md): Creating/deleting storage pools on mayastor nodes as requested by admin by means of custom resource records
- _CSI controller_: Provisioning of volumes on mayastor nodes.

## Requirements

- required k8s version is 1.14
- nodejs v10 (see below)
- npm dependencies (`npm install`)

## Build it

Nodejs v10 is not available in default package repository on ubuntu.
New package source has to be added and nodejs package installed from there:

```bash
curl -sL https://deb.nodesource.com/setup_10.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt install nodejs
```

Following command needs to be run just once to download and install npm
dependencies of moac:

```bash
npm install
```

## Run it

### Inside k8s cluster

It is the most straightforward way to run moac, however the least convenient
for debugging issues. Use [k8s yaml file](/deploy/moac-deployment.yaml) for
deploying moac in k8s cluster as you would any other application.

### Outside k8s cluster

When developing or fixing bugs in moac it is necessary to execute it along
with k8s cluster but outside of k8s cluster. We assume following environment:

- configured k8s cluster accessible from dev box (with kubeconfig file)
- mayastor daemonset properly deployed in k8s cluster

Kubeconfig file solves the access of moac to k8s api server. However moac
needs to access mayastor grpc server exposed by mayastor. For that we edit
mayastor daemonset yaml file and change args of mayastor-client from:

```
         - "--address=$(MY_POD_IP)"
```

to

```
         - "--address=127.0.0.1"
```

This will cause moac to connect to loopback interface instead of mayastor's
pod IP. Now we just need to redirect traffic comming to localhost port 10124
to mayastor pod inside the cluster. Run following command on your dev box:

```bash
kubectl port-forward pods/<mayastor-pod-name> 10124:10124
```

Obviously this workaround can be used only with a single mayastor instance.
To start moac type:

```bash
./index.js --kubeconfig
```

## Troubleshooting

Running moac with trace log level enabled (`-vv`) prints all details about
incoming/outgoing CSI messages, watcher events, etc.

## History

The name moac is acronym from "Mother of All CASes".
