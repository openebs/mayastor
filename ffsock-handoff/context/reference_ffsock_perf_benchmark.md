---
name: reference_ffsock_perf_benchmark
description: How to run a VALID ffsock (F-Stack) vs posix NVMe/TCP perf comparison — hardware rig + methodology (net_tap cannot produce a real number)
metadata:
  node_type: memory
  type: reference
  originSessionId: d923c46f-b7ce-48d7-a6e3-28c15bbfb240
  modified: 2026-07-20T06:47:32.816Z
---

For the [[project_mtcp_spdk_integration]] ffsock work. Defined 2026-07-20 when a perf comparison was requested but is NOT achievable on the current cluster/dev box.

**WHY net_tap gives an INVALID result:** F-Stack's whole value is not touching the kernel on the data path. net_tap routes every packet back through the kernel TUN/TAP, so ffsock-over-net_tap does strictly MORE work than posix-over-kernel → ffsock always LOSES on net_tap. That measures net_tap overhead, not F-Stack. Do NOT present net_tap numbers as an F-Stack verdict. A valid test needs F-Stack to own a real NIC via DPDK (true kernel bypass).

**HARDWARE REQUIRED (none available now — cluster VMs are single-NIC + no IOMMU):**
- A DPDK-bindable NIC SEPARATE from management eth0: either an SR-IOV VF (bind VF to vfio-pci, keep PF on kernel) or a second physical NIC. 25GbE+ recommended (so the run is CPU-bound, where bypass wins; a slow NIC hides the advantage).
- IOMMU enabled (intel_iommu=on/amd_iommu=on + VT-d/AMD-Vi) for the safe vfio-pci driver. Current cluster VMs expose NO IOMMU → must recreate with IOMMU passthrough or use bare metal.
- Ideally 2 hosts (target io-engine + initiator) on the dedicated link; hugepages present; pin to 1 core both arms (ffsock is single-lcore — see task 10 in [[project_mtcp_spdk_integration]]).

**RIG:** target io-engine serves a replica backed by a MALLOC/NULL bdev (isolates the network/TCP path, not disk) over NVMe/TCP; initiator = kernel nvme-tcp or SPDK perf. A/B on the SAME NIC + SAME core: run posix (NIC in kernel mode), then rebind NIC to vfio-pci and run ffsock. One variable = the sock impl.

**METRICS that reflect kernel-bypass (keep it CPU-bound: 4KiB + high QD on a fast NIC):**
- IOPS/core (4KiB randread, high QD) — the key metric; expect higher.
- cycles-per-IOP (target-side, IOPS ÷ core-busy via mpstat -P <core>) — expect lower.
- QD=1 4KiB latency — expect lower (saved syscall+interrupt+ctx-switch).
- p99/p99.9 tail — expect tighter (no interrupt jitter).
- max single-core Gbps (128KiB) — higher until NIC-bound.
fio: `--ioengine=libaio --direct=1 --rw=randread --bs=4k --iodepth=128 --numjobs=1 --ramp_time=10 --runtime=60 --time_based`; sweep bs {4k,64k,128k}, iodepth {1,8,32,128,256}.

**ffsock arm config (using what's built):** io-engine single-core (-m <one-core>), env MAYASTOR_FFSOCK=1 + FFSOCK_CONF=/etc/f-stack.conf; --env-context gives DPDK the REAL NIC PCI addr (allowlist), NOT --vdev net_tap; f-stack.conf port0 = the dedicated NIC (IP/netmask/gw), lcore_mask = the io-engine core; `dpdk-devbind.py --bind=vfio-pci <pci>`. posix arm: same core, NIC on kernel driver, no MAYASTOR_FFSOCK.

**Honest expectation:** literature (userspace-TCP NVMe-oF) ~1.5–3x IOPS/core and ~30–50% lower QD=1 latency in the CPU-bound small-IO regime — must be measured on the actual rig. Caveats stay: single-core only (multi-core = task 10, large effort), and needs the dedicated-NIC+IOMMU hardware above.

**How to apply:** when a node with SR-IOV VF or spare NIC + IOMMU is provisioned, wire ffsock's f-stack.conf to the real NIC and drive both arms per above. Until then, no valid perf number is possible — say so rather than reporting net_tap results.
