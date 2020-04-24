# CSI in Mayastor

CSI is a storage provider gRPC interface independent of storage vendor and
cloud environment. The reader should be familiar with CSI spec before reading this
document. The following picture highlights relations between k8s and mayastor
components:

![CSI architecture](img/csi-arch.jpg)

Notes:
* arrows depict client-server relation (not a direction of message flow)
* blue components belong to k8s implementation
* red components describe our implementation of mayastor
* CSI controller grpc service is missing in the picture (sorry)

Basic workflow starting from registration is as follows:

1. csi-node-driver-registrar retrieves information about csi plugin (mayastor) using csi identity service.
2. csi-node-driver-registrar registers csi plugin with kubelet passing plugin's csi endpoint as parameter.
3. kubelet uses csi identity and node services to retrieve information about the plugin (including plugin's ID string).
4. kubelet creates a custom resource (CR) "csi node info" for the csi plugin.
5. moac watches the csi node info resources and detects a new csi plugin called "csi-mayastor".
6. moac parses grpc endpoint from plugin's ID string stored previously by kubelet to CR record.
7. moac can communicate directly with mayastor-grpc to accomplish following tasks:
  * create/destroy storage pools
  * create/destroy volumes
  * create/destroy replicas (storage backend for volumes)
