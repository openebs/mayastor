# MayaStor

some change

[![Slack](https://img.shields.io/badge/JOIN-SLACK-blue)]( https://openebs-community.slack.com)
[![Build Status](https://travis-ci.org/openebs/MayaStor.svg?branch=master)](https://travis-ci.org/openebs/MayaStor)
[![pipeline status](https://gitlab.com/mayastor/MayaStor/badges/master/pipeline.svg)](https://gitlab.com/mayastor/MayaStor/commits/master)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fopenebs%2FMayaStor?ref=badge_shield)

<img width="300" align="right" alt="OpenEBS Logo" src="https://raw.githubusercontent.com/cncf/artwork/master/projects/openebs/stacked/color/openebs-stacked-color.png" xmlns="http://www.w3.org/1999/html">

<p align="justify">
<strong>MayaStor</strong> is a cloud-native declarative data plane written in <strong>RUST.</strong>
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

Table of contents:
==================
- [Quickly deploy it on K8s and get started](/doc/quick.md)
    - [Deploying on microk8s](/doc/microk8s.md)
- [High-level overview](#overview)
    - [The Nexus CAS module](#Nexus)
    - [Local storage](#local-storage)
    - [Exporting a Nexus](#exporting-the-nexus)
- [Building from source](/doc/build.md)
- [Examples of the Nexus module](/doc/mcli.md)
- [Frequently asked questions](/doc/FAQ.md)

## Overview

At a high-level, MayaStor consists out of two major components.

### **Control plane:**

 * A single instance K8s controller which implements the [CSI](https://github.com/container-storage-interface/spec/blob/master/spec.md)
 controller spec but also private interfaces that otherwise, would be implemented by your storage system which we
 call Mother Of All Containers native storage or *MAOC*  for short which runs as deployment.

 * A _per_ node instance *mayastor-agent* which handles the per node CSI related aspects as well as private a set of private API's.

### **Data plane:**

* For each node, you wish to use for storage or storage services will have to run a MayaStor daemon set. MayaStor itself,
has yet again, three major components, the Nexus, a local storage component and the mayastor-agent.

## Nexus

<p align="justify">
The Nexus is responsible for attaching to your storage resources and make it available to the host that is
selected to run your k8s workload. We call these from the Nexus its point of view children.

The goal we envision the Nexus to provide here is, as it sits between the storage systems and PVCs, is loose coupling.

A practical example: Once you are up and running with persistent workloads in a containers, you need to move your data because
the storage system that stores your PVC goes EOL, you now can control how this impacts your team without getting amid storage
migration projects, which are always painful and complicated. In reality, the individual storage volumes per team/app are
relatively small, but today, its not possible for individual teams to handle their own storage needs. The nexus provides the
abstraction over the resources such that the developer teams stay in control.

The reason we think this can work is because applications have changed, they way they are built allows us to rethink
they way we do things. Moreover, due to hardware [changes](https://searchstorage.techtarget.com/tip/NVMe-performance-challenges-expose-the-CPU-chokepoint)
we even get forced to think about it.

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

// fill the buffer with zeroes and read back the data
buf.fill(0x00);
bd.read_at(0, &mut buf).await.unwrap();

// verify that the buffer is filled with what wrote previously
buf.as_slice().into_iter().map(|b| assert_eq!(b, 0xff)).for_each(drop);
```

<p align="justify">

We think this can help a lot of database projects as well, where they typically have all the smarts in their database engine
and they want the most simple (but fast) storage device. For a more elaborate example see some of the tests in mayastor/tests.

To communicate with the children, the Nexus uses industry standard protocols. Currently, the Nexus has support for
direct access to local storage and remote storage using NVMF or iSCSI. The other advantage is that if where to remove
the Nexus out of the data path, you can still access your data like if Mayastor was never there.

The Nexus itself does not store any data and in it's most simplistic form the Nexus is a proxy towards real storage
devices where the transport may vary. It can, however, as mentioned, "transform" the data which makes it possible to
store copies of your data within different cloud systems. One of the other ideas we have is to write block device on top
of a S3 bucket such that you can create PVCs from [Minio](https://min.io/), AWS or any other compatible S3 bucket. This
simplifies the replication model for the Nexus itself somewhat but creates a bit more on the buffering side of things.
What model fits best for you? You get to decide!

<br>
</p>

## Local storage

<p align="justify">
If you do not have a storage system, and just have local storage, i.e device attached to your system that
shows up as block devices, we can consume these devices and make a "storage system" out of these local devices such that
you can leverage features like snapshots, clones, thin provisioning, and the likes. Our K8s tutorial does that under
the watter today. Currently, we are working on exporting your local storage implicitly when needed, such that you can
share storage between nodes. This means that your application when re-scheduled, can still connect to your local storage
except for the fact that it is not local anymore.

Similarly, if you do not want to use anything else then local storage, you can still use Mayastor to provide you with
additional functionality that otherwise would require you setup, kernel specific features like LVM for example.
<br>
</p>

## Exporting the Nexus

<p align="justify">
Our current main focus of development is on NVMe and vhost-user. Vhost-user allows developers to expose virtio devices
that are implemented as a user space process that the hyper-visor can use to submit IO too. This means that our Nexus can be exposed as a
vhost-user device such that a micro-vm (who typically does not have a feature rich kernel with drivers) can submit IO
to the Nexus.

In turn, the Nexus can then use nvmf to replicate (if needed) the data to multiple devices and or nodes. Our
vhost-user code can be seen in the link section (still in C).

<br>
</p>

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
