# ffsock (SPDK + F-Stack + DPDK) — handoff note

**Purpose of this branch:** transport the ffsock experiment from the dev box
(`ubuntu-24gb-nbg1-1-susobhan`) to the cluster **master node**, which has a
worker fleet where **each worker has a dedicated NIC** (separate from the mgmt
iface). The two hosts can't reach each other directly, so this branch + draft PR
is the ferry. On the master: fetch this PR, read this note, apply the bundle
(below), then do the real perf work the dev box can't (dedicated-NIC, kernel-bypass).

**Read `context/` first** — `project_mtcp_spdk_integration.md` is the full running
log (every gotcha + fix); the two `reference_*` files are the perf-benchmark method
and the custom-SPDK build flow. This note is the summary + resume plan.

---

## What ffsock is (one paragraph)

An `ffsock` SPDK `spdk_net_impl` sock module backed by **F-Stack** (userspace
FreeBSD TCP/IP over **DPDK**), giving the Mayastor io-engine a **kernel-bypass
NVMe/TCP** data path. SPDK does NVMe framing; F-Stack owns the NIC + TCP; they
share **one DPDK EAL**, reactor-driven (an SPDK poller pumps F-Stack via
`ff_run_once`). Goal: lower CPU-per-IOP than the posix (kernel) sock path.

---

## Status — DONE and validated

| Milestone | State |
|---|---|
| Spike (control-loop inversion + one round-trip) | ✅ built, ran, cluster-validated (node-0) |
| Phase 1 — module builds/registers, opt-in | ✅ runtime-verified in `nvmf_tgt` |
| Phase 2 — carries NVMe/TCP, single shared EAL, reactor-driven | ✅ 1MiB write/read/compare PASS |
| Task 6 — built into the mayastor io-engine | ✅ full symbol chain in binary |
| Task 8 — ffsock selected + F-Stack inits inside io-engine | ✅ single-core |
| Task 9 — ffsock io-engine deployed to cluster node-2 | ✅ non-destructive, dormant (safe) |
| **Re-validation (this session, single reactor)** | ✅ see below |

**Re-validation result (dev box, today):** `nvmftcp-validate.sh` — `nvmf_tgt`
single core (`-m 0x8`), net_tap, `-S ffsock` listener, kernel nvme-tcp initiator:
**PASS** — connected `/dev/nvme0n1` (SPDK_Controller1), 1MiB O_DIRECT
write/read/compare matched, **1 admin + 12 io qpairs**, clean exit, box left
clean (hugepages 3072/3072, no fftap0, no residue). The single-reactor path is
**known-good** — this is the baseline for the apples-to-apples comparison.

**Safety:** ffsock is **dormant unless `FFSOCK_CONF` is set** (registered but
opt-in; posix stays default). A multi-core io-engine that merely links ffsock is
unaffected. Do NOT set `MAYASTOR_FFSOCK=1` on a multi-core node (see Task 10).

---

## Task 10 (multi-core) — feasibility VERDICT: not an in-process fix

Investigated deeply this session. **In-process N-reactor multi-core with F-Stack
is a data-race trap, not a config away.** Evidence:

- **Single stack instance per address space.** VNET/VIMAGE **not compiled**
  (`f-stack/lib/opt/opt_global.h` has no `VIMAGE`) → every `V_*` is one global:
  one `tcbinfo` (PCB hash), one UMA/mbuf zone set, one callout wheel
  (`ff_kern_timeout.c:180` — `cc_cpu` is a scalar), one `pcpu` (`curcpu==0`).
- **The decider: stack locks are stubbed to no-ops** (`ff_lock.c:326-366`, etc. —
  `sx`/`rm`/`mtx`/`rw` compiled away). F-Stack assumes exactly one thread touches
  the stack.
- So making `lcore_conf`/`veth_ctx`/`ff_rss_tbl` `__thread` fixes only the DPDK
  datapath; N reactors would still mutate one PCB hash / UMA / timer wheel with
  **zero mutual exclusion** → corruption. (Also: the comment at
  `ff_dpdk_if.c:2604` claiming `lcore_conf` is thread-local is **false** — it is a
  plain global. Trap for the next person.)

**Why posix gets multi-core for free:** posix delegates to the OS kernel — a
genuinely SMP-safe TCP/IP stack (real per-inpcb locks, per-CPU slab caches,
RSS→per-queue NAPI). SPDK reactors are just clients doing syscalls; SPDK's posix
impl even steers a socket's poll group to the CPU the kernel delivers its packets
on (`module/sock/posix/posix.c:1801,1851` — placement_id via `SO_INCOMING_CPU`).
ffsock can't participate (its `group_impl_get_optimal` returns NULL). **ffsock
trades multi-core-for-free for lower per-core cost** — which is exactly what the
single-core perf test is meant to measure.

**Real paths to multi-core (all large, none this-session):**
1. **VNET/VIMAGE** — N independent stack instances in one address space. Major
   F-Stack rebuild it doesn't support out of the box.
2. **`dlmopen` link-map namespaces** — N private libfstack copies in one process.
   Real isolation but fights DPDK's singleton EAL. Research-grade.
3. **Network-lcore confinement** — keep F-Stack on ONE reactor; other reactors
   return NULL from `group_impl_create` (stay posix). Correct + safe under a
   multi-core io-engine, but TCP throughput stays single-core. Smallest real step;
   recommended if a "safe under multi-core" milestone is wanted before the big lift.

F-Stack's own multi-core answer is **multi-process** (proc_id → own RX queue,
symmetric RSS), i.e. shared-nothing across address spaces — which is why nothing
is thread-safe.

---

## NEXT STEP on the master — the apples-to-apples perf number

The dev box could only ever do **functional** validation: net_tap routes every
packet back through the kernel, so ffsock-over-tap does strictly MORE work than
posix and always loses — **net_tap numbers are not an F-Stack verdict.** The real
comparison needs F-Stack to own a real NIC via DPDK (true bypass). That's what the
master's dedicated-NIC workers provide.

**Method (full detail in `context/reference_ffsock_perf_benchmark.md`):**
- Target io-engine serves a replica on a **malloc/null bdev** (isolate the TCP path,
  not disk) over NVMe/TCP. Initiator = kernel nvme-tcp or SPDK `perf`.
- A/B on the **same NIC + same one core**: posix (NIC in kernel mode) vs ffsock
  (NIC rebound to `vfio-pci`). One variable = the sock impl.
- Metrics that reflect bypass, kept CPU-bound (4KiB, high QD, fast NIC):
  **IOPS/core**, **cycles-per-IOP** (target-side), **QD=1 latency**, **p99/p99.9**,
  **max single-core Gbps** (128KiB). fio: `--ioengine=libaio --direct=1
  --rw=randread --bs=4k --iodepth=128 --numjobs=1 --ramp_time=10 --runtime=60
  --time_based`; sweep bs {4k,64k,128k}, iodepth {1,8,32,128,256}.
- Honest expectation: literature ~1.5–3× IOPS/core and ~30–50% lower QD=1 latency
  in the small-IO CPU-bound regime — **must be measured on the rig.**

### tap → real NIC delta (NO ffsock/F-Stack source changes — config + one build item)

ffsock is device-agnostic; it talks to whatever port 0 F-Stack brings up.

1. **EAL args** (`--env-context`): drop `--vdev=net_tap0,iface=fftap0`; add the
   NIC's PCI allowlist `-a 0000:XX:00.0` (keep `--in-memory` on the live node).
2. **`f-stack.conf`** (`FFSOCK_CONF`): `[port0]` addr/netmask/gateway = the real
   NIC's network (not `10.99.0.x`); `lcore_mask` = the single pinned core.
3. **DPDK PMD (the only build change):** SPDK builds DPDK **driver-stripped** —
   only `net/tap` is enabled today (`dpdkbuild/Makefile` `DPDK_DRIVERS`, whole-
   archived in `lib/env_dpdk/env.mk` `DPDK_LIB_LIST`). Add the worker NIC's PMD
   (`net/mlx5` | `net/ice` | `net/i40e` | `net/ena` | `net/virtio` — check
   `lspci`) the same way, rebuild SPDK, relink io-engine. No source edits.
4. **Bind NIC to `vfio-pci`** (`dpdk-devbind.py`) — needs **IOMMU on**
   (`intel_iommu=on`/`amd_iommu=on` at boot). SR-IOV VF (keep PF on kernel) or a
   spare physical NIC; 25GbE+ so the run is CPU-bound.
5. **Drop the tap-only bits:** the net_tap MAC-collision fix
   (`ip link set fftap0 address …`) and the `fftap0` bring-up — no tap on a real NIC.

---

## Applying this bundle on the master

Assumes the master already has the mayastor codebase; the sibling SPDK/F-Stack
trees may or may not be present. Paths below match the dev box (`/root/mayastore/…`).

1. **spdk** (git repo, base `v25.05.x-mayastor` @ `349e7c8c3`):
   `git -C <spdk> apply .../patches/spdk.patch`
   then copy the new module: `cp -r spdk-module-sock-ff <spdk>/module/sock/ff`
   (files: `ff_sock.c`, `ff_sock.h`, `Makefile`).
2. **f-stack** (NOT a git repo): `tar xzf f-stack-lib.tar.gz -C <f-stack-parent>`
   → gives the durable `f-stack/lib` (patched glue sources + prebuilt `.o` +
   `libfstack.a`, ready to link). The patches applied: `ff_init_adopt_eal()`,
   `ff_run_once()` (in `ff_dpdk_if.c` + `ff_api.h`), bonding-PMD guard +
   `rte_timer_meta_init` stub (for vanilla DPDK). To rebuild from scratch (e.g.
   different DPDK), clone F-Stack `dev` and re-apply those; HOST_CFLAGS recipe is
   in the context memory.
3. **spdk-rs** (submodule): `git -C <mayastor>/spdk-rs apply
   .../patches/spdk-rs-build.rs.patch` (adds `mark_system("z")`).
4. **mayastor** own edits are already committed on THIS branch:
   `.cargo/config.toml` (the `-Wl,-z,nostart-stop-gc` link flag — REQUIRED, else
   gc drops F-Stack SYSINIT sets → null-deref crash) and
   `io-engine/src/subsys/config/mod.rs` (`MAYASTOR_FFSOCK=1` gate selecting ffsock
   as default before subsystem init).
5. **Durable build gotchas** (from context memory, easy to lose):
   - `CONFIG_FF_SOCK=y` lives in SPDK's `CONFIG` file (in `spdk.patch`) so it
     survives `./configure` (mk/config.mk-only gets wiped).
   - Build SPDK via `build_spdk.sh --without-fio ...` inside
     `nix-shell --argstr spdk-path <spdk>` (libfstack.a isn't -fPIC → can't go in
     SPDK's .so fio plugins; io-engine PIE is fine). See
     `context/reference_spdk_rs_custom_spdk.md`.
   - Then `cargo build --bin io-engine` (don't pipe through `| tail` — masks link rc).

**Reproduce the functional check** with `validate/nvmftcp-validate.sh` +
`validate/f-stack.conf` (adapt IPs/EAL for the real NIC per the delta above).

---

## Changed-file inventory

**mayastor** (committed on this branch): `.cargo/config.toml`,
`io-engine/src/subsys/config/mod.rs`.
**spdk** (`patches/spdk.patch` + `spdk-module-sock-ff/`): `CONFIG`,
`dpdkbuild/Makefile`, `lib/env_dpdk/env.mk`, `mk/spdk.common.mk`,
`mk/spdk.modules.mk`, `module/sock/Makefile`, new `module/sock/ff/{ff_sock.c,
ff_sock.h,Makefile}`.
**spdk-rs** (`patches/spdk-rs-build.rs.patch`): `build.rs`.
**f-stack** (`f-stack-lib.tar.gz`): patched `lib/` (`ff_dpdk_if.c`, `ff_init.c`,
`ff_api.h`, + prebuilt objects and `libfstack.a`).
