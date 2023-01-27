# Zoned Storage Support
Mayastor supports zoned storage in the form of PCIe ZNS devices and zoned SPDK uring block devices.

## Overview
Zoned storage is a class of storage that divides its address space into zones. These zones come with a sequential write constraint. Therefore, writes can just be issued to the zones write pointer, which will be advanced with a successful write operation. If the zone's capacity is reached, the zone is being transferred to the 'Full' state by the device controller and can not be rewritten until the zone is actively reset by the user. As of now zoned storage is available in the form of SMR HDDs and ZNS SSDs. This proposal focuses on ZNS SSDs.
For more information about zoned storage visit [zonedstorage.io](https://zonedstorage.io/).

Zoned Namespace (ZNS) NVMe SSDs are defined as part of a NVMe Command Set (see 'NVM Express Zoned Namespace Command Set Specification' in the [NVMe Command Set Specifications](https://nvmexpress.org/developers/nvme-command-set-specifications/)) and is supported since Linux kernel v5.9. SPDK supports zoned storage since v20.10.

Because ZNS SSDs align their flash media with zones, no on device garbage collection is needed. This results in better throughput, predictable latency and higher capacities per dollar (because over provisioning and DRAM for page mapping is not needed) in comparison to conventional SSDs.

The concept of ZNS SSDs and its advantages are discussed in depth in the ['ZNS: Avoiding the Block Interface Tax for Flash-based SSDs'](https://www.usenix.org/conference/atc21/presentation/bjorling) paper.

[RocksDB](https://github.com/facebook/rocksdb) and [TerarkDB](https://github.com/bytedance/terarkdb) are example applications of end to end integration with zoned storage through [ZenFS](https://github.com/westerndigitalcorporation/zenfs).
POSIX file systems like f2fs and btrfs also have zone support.

## Requirements for Mayastor
Initially the ZNS support in Mayastor is targeting the non-replicated volume I/O path with a disabled volume partitioning.
Replication and volume partitioning can be addressed later on as those features require special care in regards to the sequential write constrain and the devices max active zones and max open zones restrictions.

The NexusChild of a non-replicated Nexus should allow ZNS NVMe devices via the PCIe URI scheme as well as zoned SPDK uring devices via the uring URI scheme. This results automatically in a zoned nexus which is exposed to the user as a raw zoned NVMe-oF target or formated with btrfs.

## Prerequisites
- Linux kernel v5.15.68 or higher is needed because of the patch [nvmet: fix mar and mor off-by-one errors](https://lore.kernel.org/lkml/20220906073929.3292899-1-Dennis.Maisenbacher@wdc.com/)
- SPDK 23.01 is needed because of [ZNS support for NVMe-oF](https://review.spdk.io/gerrit/c/spdk/spdk/+/16044/7)
