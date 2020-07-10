# Summary

The tests use nix to construct VMs and then runs tests against
these VMs. I'm not sure what a nice way is to layout the tests
themselves, but the thinking is: `<catagory/basic>` and for example
`<catagory/io_handeling>`, etc..etc

The sequence of execution should be chosen such that the io tests
(for example) only start running when basic tests have succeeded.

It is also possible to preserve the state of the VMs and use them in
subsequent testing, but I suggest we leave that for later.


TODO:
[ ] We should write some high-level python code that we can reuse in the tests.
[ ] We need to figure out how many tests we want to run within a single run
[ ] We will need more gRPC methods to build more sophisticated tests
[ ] A test where we set up and deploy, k8s first, and then install mayastor would be very nice but for sure should be done last.



Note, this will only work after a "cargo build --all" as it uses the
mayastor-adhoc package, which simply copies whats in target/debug.

# Run the test

In order to run the test:
```
cd path/to/test
nix-build default.nix -A fio_nvme_basic
```

To get the interactive driver (with emacs!)

```
nix-build default.nix -A fio_nvme_basic.driver
```

and run `./result/bin/<name>`


The output shows something like:

```
{
  "bdevs": [
    {
      "blk_size": 512,
      "claimed": true,
      "claimed_by": "NVMe-oF Target",
      "name": "aio:///dev/vdb",
      "num_blocks": 1048576,
      "uuid": "00000000-76b6-4fcf-864d-1027d4038756"
    }
  ]
}

(0.11 seconds)
should be able to discover the target
initiator: must succeed: nvme discover -a 192.168.0.1 -t tcp -s 8420
initiator # [    6.554240] nvme nvme0: queue_size 128 > ctrl sqsize 64, clamping down
initiator # [    6.554997] nvme nvme0: new ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery", addr 192.168.0.1:8420
initiator # [    6.562521] nvme nvme0: Removing ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery"
(0.05 seconds)

Discovery Log Number of Records 1, Generation counter 4
=====Discovery Log Entry 0======
trtype:  tcp
adrfam:  ipv4
subtype: nvme subsystem
treq:    not required
portid:  0
trsvcid: 8430
subnqn:  nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756
traddr:  192.168.0.1
sectype: none

(0.05 seconds)
should be able to connecto to the target
initiator: must succeed: nvme connect-all -a 192.168.0.1 -t tcp -s 8420
initiator # [    6.593168] nvme nvme0: queue_size 128 > ctrl sqsize 64, clamping down
initiator # [    6.593786] nvme nvme0: new ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery", addr 192.168.0.1:8420
initiator # [    6.601565] nvme nvme0: Removing ctrl: NQN "nqn.2014-08.org.nvmexpress.discovery"
initiator # [    6.636751] nvme nvme0: queue_size 128 > ctrl sqsize 64, clamping down
initiator # [    6.639529] nvme nvme0: creating 1 I/O queues.
initiator # [    6.640734] nvme nvme0: mapped 1/0/0 default/read/poll queues.
initiator # [    6.646345] nvme nvme0: new ctrl: NQN "nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756", addr 192.168.0.1:8430
(0.08 seconds)

(0.08 seconds)
should be able to run FIO with verify=crc32
initiator: must succeed: fio --thread=1 --ioengine=libaio --direct=1 --bs=4k --iodepth=1 --rw=randrw --verify=crc32 --numjobs=1 --group_reporting=1 --runtime=15 --name=job --filename=/dev/nvme0n1
(15.74 seconds)
job: (g=0): rw=randrw, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=1
fio-3.20
Starting 1 thread
Jobs: 1 (f=1): [m(1)][100.0%][r=880KiB/s,w=868KiB/s][r=220,w=217 IOPS][eta 00m:00s]
job: (groupid=0, jobs=1): err= 0: pid=697: Mon Jul 13 21:51:40 2020
  read: IOPS=215, BW=860KiB/s (881kB/s)(12.6MiB/15002msec)
    slat (usec): min=29, max=112, avg=38.63, stdev= 7.99
    clat (usec): min=1520, max=2976, avg=2230.59, stdev=73.00
     lat (usec): min=1629, max=3025, avg=2277.39, stdev=70.91
    clat percentiles (usec):
     |  1.00th=[ 2147],  5.00th=[ 2180], 10.00th=[ 2180], 20.00th=[ 2212],
     | 30.00th=[ 2212], 40.00th=[ 2212], 50.00th=[ 2212], 60.00th=[ 2245],
     | 70.00th=[ 2245], 80.00th=[ 2245], 90.00th=[ 2278], 95.00th=[ 2278],
     | 99.00th=[ 2638], 99.50th=[ 2671], 99.90th=[ 2868], 99.95th=[ 2868],
     | 99.99th=[ 2966]
   bw (  KiB/s): min=  768, max=  968, per=99.53%, avg=855.93, stdev=55.26, samples=29
   iops        : min=  192, max=  242, avg=213.97, stdev=13.82, samples=29
  write: IOPS=222, BW=891KiB/s (912kB/s)(13.0MiB/15002msec); 0 zone resets
    slat (usec): min=43, max=137, avg=56.50, stdev=10.74
    clat (usec): min=1585, max=5462, avg=2202.96, stdev=84.77
     lat (usec): min=1648, max=5547, avg=2269.14, stdev=83.71
    clat percentiles (usec):
     |  1.00th=[ 2114],  5.00th=[ 2147], 10.00th=[ 2180], 20.00th=[ 2180],
     | 30.00th=[ 2180], 40.00th=[ 2180], 50.00th=[ 2212], 60.00th=[ 2212],
     | 70.00th=[ 2212], 80.00th=[ 2245], 90.00th=[ 2245], 95.00th=[ 2278],
     | 99.00th=[ 2311], 99.50th=[ 2343], 99.90th=[ 2835], 99.95th=[ 2900],
     | 99.99th=[ 5473]
   bw (  KiB/s): min=  784, max=  984, per=100.00%, avg=897.03, stdev=55.05, samples=29
   iops        : min=  196, max=  246, avg=224.24, stdev=13.77, samples=29
  lat (msec)   : 2=0.79%, 4=99.19%, 10=0.02%
  cpu          : usr=0.25%, sys=2.07%, ctx=13174, majf=0, minf=80
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=3226,3340,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
   READ: bw=860KiB/s (881kB/s), 860KiB/s-860KiB/s (881kB/s-881kB/s), io=12.6MiB (13.2MB), run=15002-15002msec
  WRITE: bw=891KiB/s (912kB/s), 891KiB/s-891KiB/s (912kB/s-912kB/s), io=13.0MiB (13.7MB), run=15002-15002msec

Disk stats (read/write):
  nvme0n1: ios=3257/3317, merge=0/0, ticks=7362/7448, in_queue=13097, util=44.71%

(15.74 seconds)
should be able to disconnect from the target
initiator: must succeed: nvme disconnect-all
initiator # [   22.396334] nvme nvme0: Removing ctrl: NQN "nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756"
(0.02 seconds)

(0.02 seconds)
(23.04 seconds)
test script finished in 23.06s
cleaning up
killing initiator (pid 9)
killing target (pid 25)
(0.00 seconds)
/nix/store/0024692x0a7mpbqkzblnyam97w6y77ja-vm-test-run-fio_against_nvmf_target

```

 #  Rerun the tests

 Once the test has completed succesfully and you want to re-run it,
 the output has to be destroyed. Simple way to do this is:

 ```
 nix-store --delete ./result --ignore-liveness
 ```

 Running the driver manually should always be possible regardless
 of result directory