## Why user space?

As we target cloud native workloads, depending on kernel features is not possible. For example, the Ubuntu image
you run on AWS is not the same as the Ubuntu you run on your laptop. Its still Ubuntu of course, but certain features
might be disabled to optimize for the masses. This is just a difference between the same distro, being able to
support multiple distro's compounds to the problem. Likewise, when running K8s in the cloud, the kernels are mostly
stripped of nonessential features.

By running in user space, it does not matter what kernel you use its "Just a container".

## What is NVMe?

NVMe is a protocol that specifies how data is moved between the CPU and a PCIe device. This protocol was needed
because CPUs in terms of speed is relatively stable, but the number of cores keeps increasing.
Also with SSD devices, the CPUs are starting to become the bottleneck.

In short, it meant that  a whole new design was needed (which is not completely new as it borrows a lot
from InfiniBand).

## Can NVMe only be used with NVMe devices?

No, it can be used with any device as it is just a protocol. It is perfectly fine to export your 5400RPM 2.5inch
SATA SSD with NVMe.

## Why is NVMe faster?

By design, NVMe is implemented by making use lockless ring buffer. This does not make the devices fast but
it makes the protocol fast. Each CPU gets its own buffer such that it does not have to wait on any locks
being held by other CPUs. As CPUs get higher in terms of core counts, locking in the hot path is costly.

This technique is not limited to NVMe by any way, as you can see [here](https://lwn.net/Articles/776703/) and
[here](https://lwn.net/Articles/789603/) is an approach taken more often.

## Huge pages

Huge pages, are a feature in the Linux kernel, used in order to reduce TLB misses. See [here](https://lwn.net/Articles/374424/)
for a more in depth introduction and explanation. Because of certain properties of huge pages, like the ability to pin
them, they can be used for DMA which in turn is needed to make the system go achieve low latency performance. During
start-up, a pool of huge pages is allocated upfront so that they do not need to be allocated during run time.

## Isolating cores

To get optimal performance, it advised to isolate one or more CPUs. This will prevent the kernel to schedule any other
user space tasks. The reason for this is that you might not want your storage IOs to be, for example, put on hold while
due to some other un related user space application. If you do not isolate a core, you will notice erratic latency but
depending on the workload that might be fine. A CPU can only be really isolated during system boot so it must be added
during boot time like for example:

```bash
GRUB_CMDLINE_LINUX_DEFAULT=isolcpu="1,3"
```

The same set of CPU's should be passed on to Mayastor and during startup the it will pin itself to the cores given. Initially
this might look cumbersome, but in turns out in practice, due to many core systems these days, it actually provides a very
predictable and scaling model.

