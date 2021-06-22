# MayaStor

[![Releases](https://img.shields.io/github/release/openebs/Mayastor/all.svg?style=flat-square)](https://github.com/openebs/Mayastor/releases)
[![CI-basic](https://mayastor-ci.mayadata.io/buildStatus/icon?job=Mayastor%2Fmaster)](https://mayastor-ci.mayadata.io/blue/organizations/jenkins/Mayastor/activity/)
[![Slack](https://img.shields.io/badge/JOIN-SLACK-blue)](https://kubernetes.slack.com/messages/openebs)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor?ref=badge_shield)
[![built with nix](https://builtwithnix.org/badge.svg)](https://builtwithnix.org)

<img width="300" align="right" alt="OpenEBS Logo" src="https://raw.githubusercontent.com/cncf/artwork/master/projects/openebs/stacked/color/openebs-stacked-color.png" xmlns="http://www.w3.org/1999/html">

<p align="justify">
<strong>MayaStor</strong> is a cloud-native declarative data plane written in <strong>Rust.</strong>
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

# Project Status

Mayastor is currently beta software.  From Wikipedia: "it (beta software) will generally have many more bugs in it than completed software and speed or performance issues, and may still cause crashes or data loss."

The project's maintainers operate a live issue tracking dashboard for defects which they have under active triage and investigation. It can be accessed [here](https://mayadata.atlassian.net/secure/Dashboard.jspa?selectPageId=10015). You are strongly encouraged to familisarise yourself with the issues identified there before deploying Mayastor and/or raising issue reports.

Table of contents:
==================
- [Quickly deploy it on K8s and get started](/deploy/README.md)
    - [Deploying on microk8s](/doc/microk8s.md)
- [High-level overview](#overview)
    - [The Nexus CAS module](#Nexus)
    - [Local storage](#local-storage)
    - [Exporting a Nexus](#exporting-the-nexus)
- [Building from source](/doc/build.md)
- [Examples of the Nexus module](/doc/mcli.md)
- [Frequently asked questions](/doc/FAQ.md)

## Overview

At a high-level, MayaStor consists of two major components.

### **Control plane:**

 * A single instance K8s controller which implements the [CSI](https://github.com/container-storage-interface/spec/blob/master/spec.md) controller spec but also private interfaces that otherwise would be implemented by your storage system.  This is called Mother Of All Containers native storage or [MOAC](https://github.com/openebs/moac) for short; it runs as a k8s deployment.

 * A _per_ node instance *mayastor-csi* plugin which implements the identity and node grpc services from CSI protocol.

### **Data plane:**

* Each node you wish to use for storage or storage services will have to run a MayaStor daemon set. MayaStor itself has three major components: the Nexus, a local storage component, and the mayastor-csi plugin.

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
use mayastor::descriptor::{Descriptor, DmaBuf};
use mayastor::bdev::nexus::nexus_bdev::nexus_create;

let children = vec![
      "aio:////disk1.img?blk_size=512".to_string(),
      // it is assumed these hosts are reachable over the network
      "iscsi://foobar/iqn.2019-05.io.openebs:disk0".into(),
      "nvmf://fooo/nqn.2019-05.io-openebs:disk0".into()
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

To communicate with the children, the Nexus uses industry standard protocols. Currently, the Nexus has support for
direct access to local storage and remote storage using NVMF or iSCSI. The other advantage is that if you were to remove
the Nexus out of the data path, you would still be able to access your data as if Mayastor was not there.

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
Our current main focus of development is on NVMe and vhost-user. Vhost-user allows developers to expose virtio devices
implemented as a user space process that the hyper-visor can use to submit IO to. This means that our Nexus can be exposed as a
vhost-user device such that a micro-vm (which typically does not have a feature rich kernel with drivers) can submit IO
to the Nexus.

In turn, the Nexus can then use nvmf to replicate (if needed) the data to multiple devices and or nodes. Our
vhost-user code can be seen in the link section (still in C).

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
$ mayastor-client pool create tpool /dev/loop8
$ mayastor-client pool list
NAME                 STATE        CAPACITY         USED   DISKS
tpool                0            96.0 MiB          0 B   tpool
$ mayastor-client replica create tpool replica1 --size=10
$ mayastor-client replica create tpool replica2 --size=1000 --thin
$ mayastor-client replica list
POOL                 NAME                 THIN           SIZE
tpool                replica1             false       10.0 MiB
tpool                replica2             true         1.0 GiB
$ mayastor-client replica destroy tpool replica1
$ mayastor-client replica destroy tpool replica2
$ mayastor-client replica list
No replicas have been created
$ mayastor-client pool destroy tpool
```

## Links

- [Our bindings to spdk in the spdk-sys crate](https://github.com/openebs/spdk-sys)
- [Our vhost-user implementation](https://github.com/openebs/vhost-user)

## License

Mayastor is developed under Apache 2.0 license at the project level. Some components of the project are derived from
other open source projects and are distributed under their respective licenses.

```http://www.apache.org/licenses/LICENSE-2.0```


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor?ref=badge_large)

### Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in Mayastor by you, as defined in the Apache-2.0 license, licensed as above,
without any additional terms or conditions.
