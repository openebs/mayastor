# Mayastor

[![Releases](https://img.shields.io/github/release/openebs/Mayastor/all.svg?style=flat-square)](https://github.com/openebs/Mayastor/releases)
[![CI-basic](https://mayastor-ci.mayadata.io/buildStatus/icon?job=Mayastor%2Fdevelop)](https://mayastor-ci.mayadata.io/blue/organizations/jenkins/Mayastor/activity/)
[![Slack](https://img.shields.io/badge/JOIN-SLACK-blue)](https://kubernetes.slack.com/messages/openebs)
[![built with nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)

## :warning: Breaking Changes

On develop, there are breaking changes to the etcd key space which is used by the control-plane components and the
io-engine to retain information about replicas which contain the correct volume data.
An upgrade strategy is yet to be devised, but you may try these steps at your own risk:
1. Stop the [mayastor-control-plane](https://github.com/openebs/mayastor-control-plane) components
1. When the pool operator is no longer running, remove the finalyzers and delete all `MayastorPool` CR's
1. Enable compatibility mode with the mayastor data replica information
   ```bash
   $> ETCDCTL_API=3 etcdctl put "/openebs.io/mayastor/apis/v0/clusters/$KUBE_SYSTEM_UID/namespaces/$NAMESPACE/CoreRegistryConfig/db98f8bb-4afc-45d0-85b9-24c99cc443f2"
   '{"id":"db98f8bb-4afc-45d0-85b9-24c99cc443f2","registration":"Automatic", "mayastor_compat_v1": true}'
   ```
1. Move etcd data from `/namespace/$NAMESPACE/control-plane/` to `/openebs.io/mayastor/apis/v0/clusters/$KUBE_SYSTEM_UID/namespaces/$NAMESPACE/`
1. Install the new version of mayastor-io-engine and [mayastor-control-plane](https://github.com/openebs/mayastor-control-plane) from [mayastor-extensions](https://github.com/openebs/mayastor-extensions)
1. Recreate the `MayastorPools` with the new CRD type `DiskPool`

Table of contents:
==================
- [Quickly deploy it on K8s and get started](https://mayastor.gitbook.io)
    - [Deploying on microk8s](/doc/microk8s.md)
- [High-level overview](#overview)
    - [The Nexus CAS module](#Nexus)
    - [Local storage](#local-storage)
    - [Exporting a Nexus](#exporting-the-nexus)
- [Building from source](/doc/build.md)
- [Examples of the Nexus module](/doc/mcli.md)
- [Frequently asked questions](/doc/FAQ.md)

<p align="justify">
<strong>Mayastor</strong> is a cloud-native declarative data plane written in <strong>Rust.</strong>
Our goal is to abstract storage resources and their differences through the data plane such that users only need to
supply the <strong>what</strong> and do not have to worry about the <strong>how</strong> so that individual teams stay in control.

We also try to be as unopinionated as possible. What this means is that we try to work with the existing storage systems
 you might already have and unify them as abstract resources instead of swapping them out whenever the resources are local
 or remote.
<br>
<br>
</p>

Some targeted use cases are:

 - Low latency workloads for converged and segregated storage by leveraging NVMe/NVMe over Fabrics (NVMe-oF)
 - Micro-VM based containers like [Firecracker microVMs](https://github.com/firecracker-microvm/firecracker) and [Kata Containers](https://github.com/kata-containers/kata-containers) by providing storage over vhost-user
 - Programmatic based storage access, i.e write to block devices from within your application instead of making system calls
 - Storage unification to lift barriers so that you can start deploying cloud native apps on your existing storage without painful data gravity barriers that prevent progress and innovation

# User Documentation

The official user documentation for the Mayastor Project is published here in GitBook format: [mayastor.gitbook.io](https://mayastor.gitbook.io/)

## Overview

At a high-level, Mayastor consists of two major components.

### **Control plane:**

 * A microservices patterned control plane, centered around a core agent which publicly exposes a RESTful API.  This is extended by a dedicated operator responsible
 for managing the life cycle of "Mayastor Pools" (an abstraction for devices supplying the cluster with persistent backing storage) and a CSI compliant external provisioner (controller).
 Source code for the control plane components is located in its [own repository](https://github.com/openebs/mayastor-control-plane)

 * A _per_ node instance *mayastor-csi* plugin which implements the identity and node grpc services from CSI protocol.

### **Data plane:**

* Each node you wish to use for storage or storage services will have to run a Mayastor daemon set. Mayastor itself has three major components: the Nexus, a local storage component, and the mayastor-csi plugin.

## Nexus

<p align="justify">
The Nexus is responsible for attaching to your storage resources and making it available to the host that is
selected to run your k8s workload. We call these from the Nexus' point of view its "children".

The goal we envision the Nexus to provide here, as it sits between the storage systems and PVCs, is loose coupling.

A practical example: Once you are up and running with persistent workloads in a container, you need to move your data because
the storage system that stores your PVC goes EOL.  You now can control how this impacts your team without getting into storage
migration projects, which are always painful and complicated.  In reality, the individual storage volumes per team/app are
relatively small, but today it is not possible for individual teams to handle their own storage needs. The Nexus provides the
abstraction over the resources such that the developer teams stay in control.

The reason we think this can work is because applications have changed, and the way they are built allows us to rethink
they way we do things. Moreover, due to hardware [changes](https://searchstorage.techtarget.com/tip/NVMe-performance-challenges-expose-the-CPU-chokepoint)
we in fact are forced to think about it.

Based on storage URIs the Nexus knows how to connect to the resources and will make these resources available as
a single device to a protocol standard protocol. These storage URIs are generated automatically by MOAC and it keeps
track of what resources belong to what Nexus instance and subsequently to what PVC.

You can also directly use the nexus from within your application code. For example:
</p>

```rust
use io_engine::descriptor::{Descriptor, DmaBuf};
use io_engine::bdev::nexus::nexus_bdev::nexus_create;

let children = vec![
      "aio:////disk1.img?blk_size=512".to_string(),
      // it is assumed these hosts are reachable over the network
      "nvmf://fooo/nqn.2019-05.io-openebs:disk0".into(),
      "nvmf://barr/nqn.2019-05.io-openebs:disk0".into()
];

// if no UUID given, one will be generated for you
let uuid = "b6565df-af19-4645-9f98-e6a8b8c13b58".to_string();

// create the nexus using the vector of child devices
let nexus = nexus_create("mynexus", 4096, 131_027, Some(uuid),  &children).await.unwrap();

// open a block descriptor
let bd = Descriptor::open(&nexus, true).unwrap();

// only use DMA buffers to issue IO, as its a member of the opened device
// alignment is handled implicitly
let mut buf = bd.dma_zmalloc(4096).unwrap();

// fill the buffer with a know value
buf.fill(0xff);

// write out the buffer to the nexus, all child devices will receive the
// same IO. Put differently. A single IO becomes three IOs
bd.write_at(0, &mut buf).await.unwrap();

// fill the buffer with zeros and read back the data
buf.fill(0x00);
bd.read_at(0, &mut buf).await.unwrap();

// verify that the buffer is filled with what wrote previously
buf.as_slice().into_iter().map(|b| assert_eq!(b, 0xff)).for_each(drop);
```

<p align="justify">

We think this can help a lot of database projects as well, where they typically have all the smarts in their database engine
and they want the most simple (but fast) storage device. For a more elaborate example see some of the tests in mayastor/tests.

To communicate with the children, the Nexus uses industry standard protocols. The Nexus supports direct access to local storage and remote storage using NVMe-oF TCP. Another advantage of the implementation is that if you were to remove
the Nexus from the data path, you would still be able to access your data as if Mayastor was not there.

The Nexus itself does not store any data and in its most simplistic form the Nexus is a proxy towards real storage
devices where the transport may vary. It can however, as mentioned, "transform" the data, which makes it possible to
store copies of your data within different cloud systems. One of the other ideas we have is to write a block device on top
of a S3 bucket such that you can create PVCs from [Minio](https://min.io/), AWS or any other compatible S3 bucket. This
simplifies the replication model for the Nexus itself somewhat but creates a bit more on the buffering side of things.
What model fits best for you? You get to decide!

<br>
</p>

## Local storage

<p align="justify">
If you do not have a storage system, and just have local storage, i.e block devices attached to your system, we can
consume these and make a "storage system" out of these local devices such that
you can leverage features like snapshots, clones, thin provisioning, and the likes. Our K8s tutorial does that under
the water today. Currently, we are working on exporting your local storage implicitly when needed, such that you can
share storage between nodes. This means that your application, when re-scheduled, can still connect to your local storage
except for the fact that it is not local anymore.

Similarly, if you do not want to use anything other than local storage, you can still use Mayastor to provide you with
additional functionality that otherwise would require you setup kernel specific features like LVM for example.
<br>
</p>

## Exporting the Nexus

<p align="justify">
The primary focus of development is using NVMe as a transport protocol. The Nexus uses
NVMe-oF to replicate a volume's data to multiple devices on multiple nodes (if required).

<br>
</p>

## Client

<p align="justify">
Although a client for gRPC server is not required for the product,
it is important for testing and troubleshooting. The client
allows you to manage storage pools and replicas and just use `--help`
option if you are not sure how to use it. CSI services are not covered by the client.

<p align="justify">
In following example of a client session is assumed that mayastor has been
started and is running:

```
$ dd if=/dev/zero of=/tmp/disk bs=1024 count=102400
102400+0 records in
102400+0 records out
104857600 bytes (105 MB, 100 MiB) copied, 0.235195 s, 446 MB/s
$ sudo losetup /dev/loop8 /tmp/disk
$ io-engine-client pool create tpool /dev/loop8
$ io-engine-client pool list
NAME                 STATE        CAPACITY         USED   DISKS
tpool                0            96.0 MiB          0 B   tpool
$ io-engine-client replica create tpool replica1 --size=10
$ io-engine-client replica create tpool replica2 --size=1000 --thin
$ io-engine-client replica list
POOL                 NAME                 THIN           SIZE
tpool                replica1             false       10.0 MiB
tpool                replica2             true         1.0 GiB
$ io-engine-client replica destroy tpool replica1
$ io-engine-client replica destroy tpool replica2
$ io-engine-client replica list
No replicas have been created
$ io-engine-client pool destroy tpool
```

## Links

- [Our bindings to spdk in the spdk-sys crate](https://github.com/openebs/spdk-sys)
- [Our vhost-user implementation](https://github.com/openebs/vhost-user)

## License

Mayastor is developed under Apache 2.0 license at the project level. Some components of the project are derived from
other open source projects and are distributed under their respective licenses.

```http://www.apache.org/licenses/LICENSE-2.0```

### Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in Mayastor by you, as defined in the Apache-2.0 license, licensed as above,
without any additional terms or conditions.
