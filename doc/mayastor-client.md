# Overview

This section shows a couple of examples of what you already can do with Mayastor today:

- [Overview](#overview)
  - [io-engine-client](#io-engine-client)
  - [Local Storage](#local-storage)
  - [Use Case: Mirroring over NVMF](#use-case-mirroring-over-nvmf)

## io-engine-client

`io-engine-client` is a small tool to interact with Mayastor and its Nexuses, pools and replicas. It currently does not
interact with the local provisioner but you _can_ use local storage with it already. As of this writing we have
added support for sharing the Nexus over NBD and NVMf.

```bash
> io-engine-client --help
Mayastor CLI 0.1
CLI utility for Mayastor

USAGE:
    io-engine-client [FLAGS] [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -q, --quiet      Do not print any output except for list records
    -V, --version    Prints version information
    -v, --verbose    Verbose output

OPTIONS:
    -a, --address <HOST>    IP address of mayastor instance [default: 127.0.0.1]
    -p, --port <NUMBER>     Port number of mayastor server [default: 10124]
    -u, --units <BASE>
            Output with large units: i for kiB, etc. or d for kB, etc.


SUBCOMMANDS:
    bdev       Block device management
    help       Prints this message or the help of the given subcommand(s)
    nexus      Nexus device management
    pool       Storage pool management
    replica    Replica management
```

To get more information specific to a subcommand, just execute the subcomand without any additional parameters,
or by using the `-h` flag, for example:

```bash
> io-engine-client nexus -h
io-engine-client-nexus
Nexus device management

USAGE:
    io-engine-client nexus <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    add          add a child
    children     list nexus children
    create       Create a new nexus device
    destroy      destroy the nexus with given name
    help         Prints this message or the help of the given subcommand(s)
    list         list all nexus devices
    publish      publish the nexus
    remove       remove a child
    unpublish    unpublish the nexus
```

## Local Storage

There are a lot of cases where you might have a workload configured to make use of the storage of the node
it is configured on. This makes certain things more simple, but at the same time eliminates certain degrees
of freedom as well. With Mayastor, we attempt to solve this transparently and determine based on declarative
intent what is best to do. Let us start with an example.

Let's assume we have a local disk `/dev/sdb` and we want to make use of it. 
By making use of the `io-engine-client` we can specify
a URI to the resource and we can start using it.

```bash
> io-engine-client nexus create `uuidgen -r` 1GiB aio:///dev/sdb
4db90841-5ee8-4b7d-a4e9-13be1043bcb3
```

Tip: To find out what the arguments are, simply append the `-h` flag to any command.

Now that was easy! Let us inspect the nexus:

```bash
> io-engine-client nexus list -c
NAME                                 PATH     SIZE STATE  REBUILDS CHILDREN
4db90841-5ee8-4b7d-a4e9-13be1043bcb3      15728640 online        0 aio:///dev/sdb
```

Now this is not all that exciting, but as we you can see in [pool.rs](../mayastor/src/pool.rs) we can
actually thin provision volumes out of the disks. You can also have a look into our test case that demonstrates
that [here](../test/grpc/test_cli.js). We can also add files to the mix and the Nexus would be
fine writing to it as it were a local disk.

```bash
> fallocate -l 2GiB /data/file.img
> io-engine-client nexus create `uuidgen -r` 1GiB 'aio:///data/file.img?blk_size=512 aio:///dev/sdb'
d0c47a07-d104-48e6-8f36-bfdb47e8e766
```

Notice the added query parameter `blk_size`, required as files do not have block sizes.

```bash
> io-engine-client nexus list -c
NAME                                 PATH     SIZE STATE  REBUILDS CHILDREN
d0c47a07-d104-48e6-8f36-bfdb47e8e766      15728640 online        0 aio:///data/file.img?blk_size=512,aio:///dev/sdb
```

As a foundation for rebuilding, we needed to add support for adding and removing devices. You can try this out
yourself by running fio on top of the NBD device.

```bash
> io-engine-client nexus remove d0c47a07-d104-48e6-8f36-bfdb47e8e766 'aio:///data/file.img?blk_size=512'
aio:///data/file.img?blk_size=512
```

In the logs of mayastor you will see something like:

```bash
[2020-07-20T15:24:48.202874271Z DEBUG mayastor_grpc.rs:293] remove_child_nexus; self=MayastorSvc request=Request { metadata: MetadataMap { headers: {"te": "trailers", "content-type": "application/grpc"} }, message: RemoveChildNexusRequest { uuid: "d0c47a07-d104-48e6-8f36-bfdb47e8e766", uri: "aio:///data/file.img?blk_size=512" }, extensions: Extensions }
[2020-07-20T15:24:48.202934811Z DEBUG mayastor_grpc.rs:301] Removing child aio:///data/file.img?blk_size=512 from nexus d0c47a07-d104-48e6-8f36-bfdb47e8e766 ...
[2020-07-20T15:24:48.204115021Z INFO nexus_bdev.rs:438] nexus-d0c47a07-d104-48e6-8f36-bfdb47e8e766: Dynamic reconfiguration event: ChildRemove started
[2020-07-20T15:24:48.205461951Z INFO nexus_bdev.rs:447] nexus-d0c47a07-d104-48e6-8f36-bfdb47e8e766: Dynamic reconfiguration event: ChildRemove completed Ok(0)
[2020-07-20T15:24:48.206991361Z INFO mayastor_grpc.rs:305] Removed child from nexus d0c47a07-d104-48e6-8f36-bfdb47e8e766
```

Now we can add the device again:

```bash
> io-engine-client nexus add d0c47a07-d104-48e6-8f36-bfdb47e8e766 'aio:///data/file.img?blk_size=512'
aio:///data/file.img?blk_size=512
```

Both the nexus and the newly added children are now degraded. The child is degraded because it needs to be rebuilt
and the nexus is degraded because at least one of its children is degraded: (todo: add rebuild documentation)

```bash
> io-engine-client nexus children d0c47a07-d104-48e6-8f36-bfdb47e8e766
NAME                               STATE
aio:///dev/sdb                     online
aio:///data/file.img?blk_size=512  degraded

> io-engine-client nexus list -c
NAME                                 PATH       SIZE STATE    REBUILDS CHILDREN
d0c47a07-d104-48e6-8f36-bfdb47e8e766      1073741824 degraded        1 aio:///dev/sdb,aio:///data/file.img?blk_size=512
```

After some time, the rebuild should complete and you should see something similar to this in the logs:

```bash
[2020-07-20T15:30:06.855088153Z INFO rebuild_impl.rs:381] Rebuild job aio:///data/file.img?blk_size=512: changing state from Running to Completed
[2020-07-20T15:30:06.855117683Z INFO rebuild_impl.rs:480] State: completed, Src: aio:///dev/sdb, Dst: aio:///data/file.img?blk_size=512, range: 10240..204767, next: 204767, block_size: 512, segment_sz: 20, recovered_blks: 194527, progress: 100%
[2020-07-20T15:30:06.856241973Z INFO nexus_bdev_rebuild.rs:304] nexus nexus-d0c47a07-d104-48e6-8f36-bfdb47e8e766 received notify_rebuild from job aio:///data/file.img?blk_size=512
[2020-07-20T15:30:06.856265613Z INFO nexus_bdev_rebuild.rs:235] Child aio:///data/file.img?blk_size=512 has been rebuilt successfully
```

## Use Case: Mirroring over NVMF

Within this example we will show you how, currently the Nexus works by using the CLI tool `io-engine-client`.

We have setup an NVMF target over TCP on a local 1 GbE network, nothing to fancy as the purpose is to illustrate
the working of the Nexus.

```bash
uname -r
sudo modprobe nvme_tcp
sudo nvme discover -t tcp -a 192.168.1.2  -s 4420

Discovery Log Number of Records 2, Generation counter 7
=====Discovery Log Entry 0======
trtype:  tcp
adrfam:  ipv4
subtype: nvme subsystem
treq:    not specified
portid:  0
trsvcid: 4420
subnqn:  nqn.2019-05.io.openebs:cnode1
traddr:  192.168.1.2
sectype: none
=====Discovery Log Entry 1======
trtype:  tcp
adrfam:  ipv4
subtype: nvme subsystem
treq:    not specified
portid:  1
trsvcid: 4420
subnqn:  nqn.2019-05.io.openebs:cnode2
traddr:  192.168.1.2
sectype: none
```

Now that we can see the block devices, we will connect to them and perform some IO to one of the devices.

```bash
sudo nvme connect-all -t tcp -a 192.168.1.2 -s 4420
```

We can verify the connection has been made by looking at dmesg for some output:

```bash
[17251.205183] nvme nvme1: new ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery", addr 192.168.1.2:4420
[17251.206576] nvme nvme1: Removing ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery"
[17251.245350] nvme nvme1: creating 4 I/O queues.
[17251.281562] nvme nvme1: mapped 4/0 default/read queues.
[17251.284471] nvme nvme1: new ctrl: NQN "nqn.2019-05.io.openebs:cnode1", addr 192.168.1.2:4420
[17251.297755] nvme nvme2: creating 4 I/O queues.
[17251.332165] nvme nvme2: mapped 4/0 default/read queues.
[17251.341883] nvme nvme2: new ctrl: NQN "nqn.2019-05.io.openebs:cnode2", addr 192.168.1.2:4420
```

Using the following fio config:

```bash
[global]
bs=4k
iodepth=32
time_based=1
runtime=30
ioengine=linuxaio
group_reporting=1
[job2]
numjobs=4
rw=randrw
filename=/dev/nvme1n1
```

We get:

```bash
job2: (g=0): rw=randrw, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=32
...
fio-3.12
Starting 4 processes
Jobs: 4 (f=4): [m(4)][100.0%][r=11.2MiB/s,w=10.9MiB/s][r=2880,w=2783 IOPS][eta 00m:00s]
job2: (groupid=0, jobs=4): err= 0: pid=92279: Mon Aug 26 15:15:10 2019
  read: IOPS=11.1k, BW=43.2MiB/s (45.3MB/s)(1296MiB/30002msec)
    slat (nsec): min=1638, max=17961k, avg=275475.84, stdev=553702.70
    clat (usec): min=4, max=658461, avg=5632.67, stdev=27928.99
     lat (usec): min=111, max=658463, avg=5908.56, stdev=27977.53
    clat percentiles (usec):
     |  1.00th=[   117],  5.00th=[   120], 10.00th=[   123], 20.00th=[   130],
     | 30.00th=[   174], 40.00th=[  1106], 50.00th=[  2057], 60.00th=[  3720],
     | 70.00th=[  5997], 80.00th=[  8848], 90.00th=[ 12649], 95.00th=[ 15795],
     | 99.00th=[ 21365], 99.50th=[ 23725], 99.90th=[624952], 99.95th=[633340],
     | 99.99th=[650118]
   bw (  KiB/s): min=  432, max=43240, per=25.50%, avg=11276.20, stdev=10811.81, samples=234
   iops        : min=  108, max=10810, avg=2819.02, stdev=2702.95, samples=234
  write: IOPS=11.1k, BW=43.2MiB/s (45.3MB/s)(1296MiB/30002msec); 0 zone resets
    slat (usec): min=2, max=8760, avg= 5.81, stdev=36.40
    clat (usec): min=109, max=658831, avg=5578.97, stdev=26966.53
     lat (usec): min=111, max=658833, avg=5585.00, stdev=26966.83
    clat percentiles (usec):
     |  1.00th=[   117],  5.00th=[   120], 10.00th=[   123], 20.00th=[   130],
     | 30.00th=[   174], 40.00th=[  1123], 50.00th=[  2073], 60.00th=[  3785],
     | 70.00th=[  6063], 80.00th=[  8848], 90.00th=[ 12780], 95.00th=[ 15795],
     | 99.00th=[ 21365], 99.50th=[ 23725], 99.90th=[624952], 99.95th=[633340],
     | 99.99th=[650118]
   bw (  KiB/s): min=  376, max=42888, per=25.50%, avg=11281.71, stdev=10774.02, samples=234
   iops        : min=   94, max=10722, avg=2820.40, stdev=2693.49, samples=234
  lat (usec)   : 10=0.01%, 250=33.57%, 500=1.61%, 750=2.08%, 1000=1.89%
  lat (msec)   : 2=10.43%, 4=11.77%, 10=22.03%, 20=15.05%, 50=1.38%
  lat (msec)   : 750=0.19%
  cpu          : usr=1.38%, sys=3.83%, ctx=109316, majf=0, minf=70
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=100.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.1%, 64=0.0%, >=64=0.0%
     issued rwts: total=331704,331887,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=32

Run status group 0 (all jobs):
   READ: bw=43.2MiB/s (45.3MB/s), 43.2MiB/s-43.2MiB/s (45.3MB/s-45.3MB/s), io=1296MiB (1359MB), run=30002-30002msec
  WRITE: bw=43.2MiB/s (45.3MB/s), 43.2MiB/s-43.2MiB/s (45.3MB/s-45.3MB/s), io=1296MiB (1359MB), run=30002-30002msec
```

We are maxing out at roughly 90MB, as this a 1 GbE network that is to be expected.

Now, let's disconnect it and create a Nexus that that consumes one of the NVMe targets and rerun the test:

```bash
nvme disconnect -d {/dev/nvme1,/dev/nvme2}
io-engine-client nexus create `uuidgen -r` 64MiB 'nvmf://192.168.1.2/nqn.2019-05.io.openebs:cnode2 nvmf://192.168.1.2/nqn.2019-05.io.openebs:cnode1'
```

Ok we now have created a nexus that consists out of 2 replica's:

```bash
> io-engine-client nexus list -c
NAME                                 PATH       SIZE STATE  REBUILDS CHILDREN
787f82e7-e7d8-4ae1-8a25-5d48ead4f4cd        67108864 online        0 nvmf://192.168.1.2/nqn.2019-05.io.openebs:cnode2n2,nvmf://192.168.1.2/nqn.2019-05.io.openebs:cnode1n1
```

We can share the Nexus to this local machine rather simply and we will use the NBD protocol.
This is something you typically would not do but it we in the near future, can exchange NBD for virtio,
and NVMF

```bash
> io-engine-client nexus publish 787f82e7-e7d8-4ae1-8a25-5d48ead4f4cd
file:///dev/nbd0
```

And the results:

```bash
job2: (g=0): rw=randrw, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=32
...
fio-3.12
Starting 4 processes
Jobs: 4 (f=4): [m(4)][100.0%][r=15.3MiB/s,w=15.5MiB/s][r=3912,w=3956 IOPS][eta 00m:00s]
job2: (groupid=0, jobs=4): err= 0: pid=98478: Mon Aug 26 15:31:57 2019
  read: IOPS=7899, BW=30.9MiB/s (32.4MB/s)(926MiB/30003msec)
    slat (nsec): min=1627, max=14789k, avg=420094.63, stdev=812020.68
    clat (usec): min=3, max=659761, avg=7842.58, stdev=28291.63
     lat (usec): min=114, max=659763, avg=8263.11, stdev=28401.15
    clat percentiles (usec):
     |  1.00th=[   121],  5.00th=[   126], 10.00th=[   129], 20.00th=[   145],
     | 30.00th=[   233], 40.00th=[  1860], 50.00th=[  3490], 60.00th=[  5932],
     | 70.00th=[  9110], 80.00th=[ 13435], 90.00th=[ 19006], 95.00th=[ 22938],
     | 99.00th=[ 29492], 99.50th=[ 32375], 99.90th=[633340], 99.95th=[641729],
     | 99.99th=[650118]
   bw (  KiB/s): min=   16, max=39272, per=26.19%, avg=8275.07, stdev=9837.56, samples=229
   iops        : min=    4, max= 9818, avg=2068.73, stdev=2459.39, samples=229
  write: IOPS=7907, BW=30.9MiB/s (32.4MB/s)(927MiB/30003msec); 0 zone resets
    slat (usec): min=2, max=11536, avg= 6.85, stdev=63.47
    clat (usec): min=111, max=661809, avg=7842.54, stdev=27572.86
     lat (usec): min=114, max=661812, avg=7849.61, stdev=27573.15
    clat percentiles (usec):
     |  1.00th=[   122],  5.00th=[   126], 10.00th=[   130], 20.00th=[   143],
     | 30.00th=[   235], 40.00th=[  1876], 50.00th=[  3589], 60.00th=[  5932],
     | 70.00th=[  9372], 80.00th=[ 13566], 90.00th=[ 19006], 95.00th=[ 23200],
     | 99.00th=[ 29754], 99.50th=[ 32900], 99.90th=[633340], 99.95th=[641729],
     | 99.99th=[650118]
   bw (  KiB/s): min=   16, max=39696, per=26.08%, avg=8248.55, stdev=9780.93, samples=230
   iops        : min=    4, max= 9924, avg=2062.10, stdev=2445.24, samples=230
  lat (usec)   : 4=0.01%, 10=0.01%, 50=0.01%, 250=30.71%, 500=2.84%
  lat (usec)   : 750=0.76%, 1000=1.33%
  lat (msec)   : 2=5.84%, 4=11.03%, 10=19.45%, 20=19.29%, 50=8.57%
  lat (msec)   : 100=0.01%, 750=0.18%
  cpu          : usr=1.07%, sys=3.12%, ctx=93934, majf=0, minf=63
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=100.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.1%, 64=0.0%, >=64=0.0%
     issued rwts: total=237004,237254,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=32

Run status group 0 (all jobs):
   READ: bw=30.9MiB/s (32.4MB/s), 30.9MiB/s-30.9MiB/s (32.4MB/s-32.4MB/s), io=926MiB (971MB), run=30003-30003msec
  WRITE: bw=30.9MiB/s (32.4MB/s), 30.9MiB/s-30.9MiB/s (32.4MB/s-32.4MB/s), io=927MiB (972MB), run=30003-30003msec

Disk stats (read/write):
  nbd0: ios=58805/9242, merge=1/851440, ticks=94557/281694, in_queue=264124, util=99.33%
```

Note, the performance has dropped to 64MB, however we are **writing the data twice!** Let's see if we can prove that is
the case. We will create a filesystem on top of the block device and write some data to it. We will then decouple
the Nexus from the the block device and verify the data is indeed written to both block devices.

```bash
sudo mkfs.ext4 /dev/nbd0
mke2fs 1.44.6 (5-Mar-2019)
Discarding device blocks: done
Creating filesystem with 65536 1k blocks and 16384 inodes
Filesystem UUID: d3c2fe6f-901d-4c87-a11d-ac5cf3bd54ca
Superblock backups stored on blocks:
        8193, 24577, 40961, 57345

Allocating group tables: done
Writing inode tables: done
Creating journal (4096 blocks): done
Writing superblocks and filesystem accounting information: done

sudo mount /dev/nbd0 /mnt
echo "hello nexus" | sudo tee  /mnt/nexus
hello nexus
cat /mnt/nexus
hello nexus
md5sum /mnt/nexus
37e970093ada39803b8e7b3b08f2371c  /mnt/nexus
umount /mnt/nexus
io-engine-client nexus unpublish 787f82e7-e7d8-4ae1-8a25-5d48ead4f4cd
Nexus 787f82e7-e7d8-4ae1-8a25-5d48ead4f4cd unpublished
```

We will attach the devices directly to the host without the Nexus in between. We expect to see that both block devices
will have the same data on its filesystem, and have the same content including a matching md5.

```bash
sudo nvme connect-all -t tcp -a 192.168.1.2 -s 4420
sudo mkdir /{disk1,disk2}
sudo mount /dev/nvme1n1 /disk1
sudo mount /dev/nvme2n1 /disk2

cd /disk1/
cat nexus
hello nexus
md5sum nexus
37e970093ada39803b8e7b3b08f2371c  nexus

cd ..
cd disk2/
cat nexus
hello nexus
md5sum nexus
37e970093ada39803b8e7b3b08f2371c  nexus
```

What this demonstrates is that indeed -- we write the data twice. If you where to add a third child, we would write to
that device all the same. What this also shows, is how we are transparent to the actual block devices. When we are removed
from the data path, the data is still accessible without any special purpose tools or software.
