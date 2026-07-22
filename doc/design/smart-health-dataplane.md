| oep-number    | OEP 5198                                            |
| ------------- | --------------------------------------------------- |
| title         | disk-health-smart-support-in-io-engine              |
| authors       | @susobhandey                                        |
| owners        | @susobhandey                                        |
| editor        | TBD                                                 |
| creation-date | 2026-07-02                                          |
| last-updated  | 2026-07-22                                          |
| status        | provisional                                         |
| see-also      | N/A                                                 |
| replaces      | N/A                                                 |
| superseded-by | N/A                                                 |

# Disk Health (SMART) Support in io-engine

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Summary](#summary)
- [Motivation](#motivation)
  * [Goals](#goals)
  * [Non-Goals](#non-goals)
- [Proposal](#proposal)
  * [User Stories](#user-stories)
    + [Story 1](#story-1)
    + [Story 2](#story-2)
    + [Story 3](#story-3)
  * [Implementation Details/Notes/Constraints](#implementation-detailsnotesconstraints)
    + [Data Model](#data-model)
    + [The Common Method](#the-common-method)
    + [Branch Selection](#branch-selection)
    + [Common Start and End](#common-start-and-end)
    + [Path A — smartctl (kernel-attached disks)](#path-a--smartctl-kernel-attached-disks)
    + [Path B — NVMe SMART log page (VFIO-attached NVMe)](#path-b--nvme-smart-log-page-vfio-attached-nvme)
    + [Combined Flow](#combined-flow)
    + [gRPC / Protobuf Interface](#grpc--protobuf-interface)
    + [Error Handling](#error-handling)
    + [Concurrency and Locking](#concurrency-and-locking)
    + [File-Level Changes](#file-level-changes)
    + [Runtime Requirements](#runtime-requirements)
    + [Assumptions and Dependencies](#assumptions-and-dependencies)
  * [Risks and Mitigations](#risks-and-mitigations)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Infrastructure Needed](#infrastructure-needed)
- [Testing](#testing)

## Summary

The io-engine data plane currently has no way to report the health of the physical disks that back a Mayastor pool. Operators cannot see disk temperature, remaining life, media error counts, or vendor failure warnings for the devices io-engine has opened, which means degrading hardware is only noticed once it starts producing I/O errors.

This enhancement adds SMART (Self-Monitoring, Analysis and Reporting Technology) reporting to io-engine. A single method, `device_health()`, is added to the `BlockDevice` trait, so every device type answers the same question in its own way and callers never need to know how a disk is attached. Two implementations sit behind that method: kernel-attached disks (`aio://`, `uring://` — covering SATA, SAS, and NVMe not bound to VFIO) are read with `smartctl`, and VFIO-attached NVMe (`pcie://`) is read by issuing an NVMe `GET LOG PAGE` admin command for the SMART / Health Information log page (log ID `0x02`) through the admin-passthru path io-engine already uses for `identify`. Both paths normalise their output into one `DeviceHealth` struct.

The results are exposed through a new `GetPoolHealth` RPC on the existing pool gRPC service, which returns one entry per backing disk. Devices that genuinely cannot report health — a virtual cloud disk with no SMART support, for example — are returned with `supported = false` rather than failing the call, so a mixed pool always produces a usable answer. No changes to SPDK source are required; the only change outside io-engine is un-commenting an opcode constant that already exists in the `spdk-rs` bindings.

## Motivation

Storage administrators running Mayastor in production need early warning of disk failure. Today the only signal is I/O failure after the fact, at which point a replica is already degraded and a rebuild is required. Every enterprise SSD and HDD already tracks the information needed to predict this — wear level, spare capacity, media errors, unsafe shutdowns, temperature — but io-engine does not surface any of it.

The complication that motivates this specific design is that **how you read health depends on how the disk is attached, not on the disk brand**:

- **Kernel-attached disks** have a `/dev` name (for example `aio:///dev/sdb` or `uring:///dev/nvme4n1`). This covers SATA, SAS, and NVMe that has not been bound to VFIO. A standard userspace tool can reach these.
- **VFIO-attached NVMe** (for example `pcie://0000:04:00.0`) has no `/dev` name at all; SPDK drives the controller directly in userspace and the kernel driver has been detached. Nothing outside the io-engine process can talk to that device.

No single mechanism reaches both classes of device. A design that only shipped one method would silently exclude a large fraction of real deployments — either the performance-oriented VFIO deployments, or the far more common kernel-attached ones. The proposal therefore standardises the *interface* and lets each device type choose its own *mechanism*.

### Goals

- Provide one common method for asking any device for its health, so callers do not branch on device type.
- Produce one normalised health result that is identical in shape for every disk type.
- Read SATA, SAS, and kernel-attached NVMe through a single tool (`smartctl`).
- Read VFIO-attached NVMe through an NVMe SMART log page admin command.
- Expose the health of a pool's backing disks over the pool gRPC API.
- Require no changes to SPDK source.
- Degrade gracefully: a device that cannot report health returns `supported = false` instead of failing the request.

### Non-Goals

- **Health polling, history, or trend storage.** This proposal is strictly on-demand read. Periodic collection is deliberately deferred (see [Graduation Criteria](#graduation-criteria) and future work).
- **Control-plane and CSI integration.** Surfacing health through the control plane, `kubectl mayastor`, or CSI lives in other repositories and is out of scope here.
- **Metrics, alerting, or automatic remediation.** No Prometheus exporter, no threshold-based pool eviction, no automatic replica migration on a failure prediction.
- **Changes to SPDK source.** Only an already-present opcode constant in the `spdk-rs` bindings is enabled.
- **Vendor-specific SMART attributes.** Only the standard, cross-vendor fields are parsed and normalised.
- **Health for devices other than pool-backing disks** (for example NVMe-oF targets consumed as nexus children).

## Proposal

A new `device_health()` method is added to the `BlockDevice` trait in io-engine. The default implementation returns "not supported", so every existing device type compiles unchanged and simply reports no health until it implements the method. The concrete implementation in `bdev/device.rs` inspects the device's driver and name and dispatches to one of two readers, both of which return the same `DeviceHealth` struct.

A new `GetPoolHealth` RPC on `PoolRpc` resolves a pool by name, enumerates its backing disks, calls `device_health()` on each, and returns a list of per-disk results.

```
              user asks:  GetPoolHealth(pool)
                          |
                 io-engine finds the pool
                          |
                 get the pool's disk(s)
                          |
             for each disk -> device_health()
                          |
        +-----------------+------------------+
        |                                    |
   kernel disk                          VFIO NVMe
   (aio / uring, has /dev name)         (pcie, no /dev name)
        |                                    |
   run smartctl                        read NVMe log page 0x02
   (SATA / SAS / NVMe)                 (reuse existing admin code)
        |                                    |
        +-----------------+------------------+
                          |
                    health result
                          |
              send back to the user
```

The key design decisions and their rationale:

| # | Decision | Reason |
| - | -------- | ------ |
| 1 | One `device_health()` method on the device object | Callers stay simple; each disk type decides how to read its own health |
| 2 | Use `smartctl` for kernel-attached disks | One tool covers SATA, SAS, and kernel NVMe, including vendor quirk handling we would otherwise have to reimplement |
| 3 | Use the NVMe SMART log page for VFIO NVMe | These devices have no `/dev` name, so no external tool can reach them |
| 4 | Reuse the existing NVMe admin-passthru code | No new low-level code; the same mechanism already ships for `identify` |
| 5 | Do not change SPDK source | Avoids a long external review cycle; only enables an opcode already present in the bindings |
| 6 | Per-disk `supported` flag rather than a failed call | Mixed pools and SMART-less cloud disks still return useful data |

### User Stories

#### Story 1

As a platform SRE running Mayastor on bare metal, I want to query the health of a pool's backing disk so that I can replace a drive that is nearing end of life *before* it fails and forces a replica rebuild. I call `GetPoolHealth` on the pool and see `percentage_used`, `available_spare_percent`, and `media_errors` for the disk, and act when wear crosses my own threshold.

#### Story 2

As a support engineer debugging a node with VFIO-attached NVMe, I want the same health information I would get from `smartctl` on a kernel-attached disk — even though the device has been detached from the kernel driver and has no `/dev` node — so that I do not have to tear down the pool and rebind the device to the kernel just to read its SMART data.

#### Story 3

As an operator with a heterogeneous cluster — some SATA, some SAS, some VFIO NVMe, some cloud virtual disks with no SMART support at all — I want a single API call per pool that returns whatever is available for each disk and clearly marks the rest as unsupported, so that my tooling does not need per-device-type logic and does not break on the disks that cannot answer.

### Implementation Details/Notes/Constraints

#### Data Model

A new struct, `DeviceHealth`, is added in `io-engine/src/core/device_health.rs`. Fields are optional where a given device class does not report them.

| Field | Meaning |
| ----- | ------- |
| `critical_warning` | Vendor warning flags; `0` means good |
| `temperature_celsius` | Composite temperature in Celsius |
| `available_spare_percent` | Remaining spare capacity |
| `available_spare_threshold_percent` | Spare capacity warning level |
| `percentage_used` | Estimated life consumed |
| `data_units_read` / `data_units_written` | Volume read / written |
| `power_cycles` | Number of power cycles |
| `power_on_hours` | Hours powered on |
| `unsafe_shutdowns` | Number of unsafe shutdowns |
| `media_errors` | Media / data integrity errors |
| `num_error_log_entries` | Number of error log entries |

Small helpers are provided alongside it, for example `is_healthy()`, which folds `critical_warning` and the SMART overall-health verdict into a single boolean.

#### The Common Method

`device_health()` is added to the `BlockDevice` trait in `io-engine/src/core/block_device.rs`. The trait-level default returns "not supported", which keeps the change additive: no existing device implementation is forced to change, and SPDK is untouched.

#### Branch Selection

Inside `device_health()`:

- Driver is `aio` or `uring` **and** the name starts with `/dev/` → **Path A** (`smartctl`).
- Driver is `nvme` (VFIO, no `/dev` name) → **Path B** (NVMe log page).
- Anything else → return "not supported".

Note that a `/dev/disk/by-path/...` name also starts with `/dev/` and therefore takes Path A; `smartctl` resolves the symlink itself, so no extra handling is needed.

#### Common Start and End

**Start (both paths).** The client calls `GetPoolHealth` with a pool name. The handler in `grpc/v1/pool.rs` resolves the pool via `finder()`, obtains the backing disk list via `pool.disks()`, looks up the `BlockDevice` for each disk, and calls `device_health()` on it.

**End (both paths).** Each call yields either a `DeviceHealth` or an error. The handler builds a `DiskHealth { disk_uri, supported, health }` — `supported = true` with the health payload on success, `supported = false` on failure or when the device reports no SMART capability. All entries are collected into a `GetPoolHealthResponse`.

#### Path A — smartctl (kernel-attached disks)

Applies to SATA, SAS, and NVMe not bound to VFIO. Code lives in `bdev/device.rs` and `core/device_health.rs`.

```
User (grpcurl / client)
   |   GetPoolHealth("pool-node-0")
   v
gRPC server: PoolRpc.GetPoolHealth            [grpc/v1/pool.rs]
   |
   v
find the pool          finder()
   |
   v
get disk list          pool.disks()   ->  "aio:///dev/sdb"
   |
   v
find the device        device_lookup("/dev/sdb")  ->  BlockDevice
   |
   v
device_health()                               [bdev/device.rs]
   |   driver is "aio" or "uring", name is /dev/...
   v
read_device_health("/dev/sdb")                [core/device_health.rs]
   |
   v
run:  smartctl --json --all /dev/sdb
   |
   v
read the JSON  ->  fill DeviceHealth
   |
   v
back to handler  ->  DiskHealth { uri, supported, health }
   |
   v
GetPoolHealthResponse  ->  send to user
```

The JSON produced by `smartctl --json --all` is mapped as follows:

| JSON field | `DeviceHealth` field |
| ---------- | -------------------- |
| `smart_status.passed = false` | set the critical warning flag |
| `temperature.current` | temperature |
| `power_on_time.hours` | power on hours |
| `power_cycle_count` | power cycles |
| `nvme_smart_health_information_log` | all NVMe fields (kernel NVMe) |
| ATA attribute id 194 | temperature |
| ATA attribute id 5 | media errors |
| ATA attribute id 9 | power on hours |
| ATA attribute id 12 | power cycles |
| ATA attribute id 198 | error log entries |
| `scsi_grown_defect_list` | media errors (SAS) |

**Constraint:** `smartctl` returns a non-zero exit code in several non-fatal situations (for example when the overall-health self-assessment has failed, which is precisely the case we care about). The implementation therefore ignores the exit code and parses the JSON, treating unparseable output — not a non-zero exit — as the failure condition.

#### Path B — NVMe SMART log page (VFIO-attached NVMe)

Applies to NVMe attached over `pcie://`. There is no `/dev` name, so the SMART / Health Information log page (log ID `0x02`) is read directly through the NVMe admin command path that io-engine already uses.

```
User (grpcurl / client)
   |   GetPoolHealth("pool-node-0")
   v
gRPC server: PoolRpc.GetPoolHealth            [grpc/v1/pool.rs]
   |
   v
find the pool          finder()
   |
   v
get disk list          pool.disks()   ->  "pcie://...."
   |
   v
find the device        device_lookup(name)  ->  BlockDevice
   |
   v
device_health()                               [bdev/device.rs]
   |   driver is "nvme"  (no /dev name)
   v
open a handle, READ ONLY
   UntypedBdevHandle::open_with_bdev(bdev, false)
   |
   v
make a 512 byte buffer (DmaBuf)
   |
   v
nvme_get_smart(buffer)                         [core/handle.rs]
   |   set opcode = GET_LOG_PAGE (0x02)
   |   set log id = 0x02 (SMART), size = 512 bytes
   v
nvme_admin(cmd, buffer)          (this code already existed)
   |
   v
spdk_bdev_nvme_admin_passthru_ro(...)   ->  ask the NVMe disk
   |   wait for the answer (completion callback)
   v
the 512 byte SMART page comes back
   |
   v
DeviceHealth::from_nvme_smart(page)  ->  fill DeviceHealth
   |
   v
back to handler  ->  DiskHealth { uri, supported, health }
   |
   v
GetPoolHealthResponse  ->  send to user
```

**Log page parsing.** The SMART log page is a fixed 512-byte structure with each value at a defined offset (byte 0 is the critical warning bitmap, bytes 1–2 the composite temperature, byte 5 the percentage used, and so on). Each field is read from its fixed offset into `DeviceHealth`. Buffers shorter than 512 bytes are rejected rather than parsed partially.

The handle is opened **read-only** (`open_with_bdev(bdev, false)`) so that reading health cannot interfere with the pool that is already using the device, and the admin command uses the read-only passthru variant.

#### Combined Flow

```
                 GetPoolHealth (gRPC)
                        |
                 find pool + disks
                        |
                 device_health()
                        |
         +--------------+---------------+
         |                              |
   driver aio/uring               driver nvme (VFIO)
   name /dev/...                  no /dev name
         |                              |
   smartctl --json                open handle (read only)
         |                        nvme_get_smart()
   read JSON                      GET_LOG_PAGE 0x02
         |                        nvme_admin ->
         |                        spdk_bdev_nvme_admin_passthru_ro
         |                              |
         |                        read 512 byte page
         +--------------+---------------+
                        |
                   DeviceHealth
                        |
                    DiskHealth
                        |
              GetPoolHealthResponse -> user
```

#### gRPC / Protobuf Interface

One RPC is added to the existing `PoolRpc` service in `protobuf/v1/pool.proto`:

```protobuf
rpc GetPoolHealth (GetPoolHealthRequest) returns (GetPoolHealthResponse) {}
```

New messages:

- `GetPoolHealthRequest { name, uuid }` — pool name, with optional uuid for disambiguation.
- `DeviceHealth { ... }` — the fields listed in [Data Model](#data-model).
- `DiskHealth { disk_uri, supported, health }` — one per backing disk.
- `GetPoolHealthResponse { repeated DiskHealth disks }`.

The addition is backwards compatible: no existing message or RPC is modified, so older clients are unaffected.

#### Error Handling

If a disk cannot produce health data for any reason — `smartctl` missing from the image, the device does not implement SMART, the handle open fails, the admin command is rejected, or the output cannot be parsed — that disk's entry is returned with `supported = false` and no health payload. The overall RPC still succeeds. This is deliberate: a pool striped across a SMART-capable disk and a SMART-less cloud volume should still tell the operator what it knows about the first one.

The implementation never synthesises or defaults a health value. An absent field is absent, not zero.

#### Concurrency and Locking

Both paths are invoked from the gRPC handler, not from the I/O hot path. Path A spawns an external process and blocks only the calling task. Path B opens a **read-only** handle so it does not contend with the pool's existing use of the device, and issues a single admin command whose completion is awaited via the existing callback mechanism. Health reads are expected to be occasional — on operator demand — rather than continuous, which keeps the cost off the reactor's critical path.

#### File-Level Changes

**io-engine**

| File | Change |
| ---- | ------ |
| `core/device_health.rs` (new) | `DeviceHealth`, the `smartctl` reader, the JSON parser, the 512-byte log-page parser, unit tests |
| `core/block_device.rs` | Add `device_health()` to the `BlockDevice` trait with a default implementation |
| `core/handle.rs` | Add `nvme_get_smart()` (modelled on `nvme_identify_ctrlr`, reusing the existing `nvme_admin`) |
| `core/mod.rs` | Declare the module and re-export `DeviceHealth` |
| `bdev/device.rs` | Implement `device_health()` — `smartctl` path and VFIO log-page path |
| `grpc/v1/pool.rs` | `GetPoolHealth` handler, type conversions, `disks()` accessor |

**spdk-rs**

| File | Change |
| ---- | ------ |
| `src/nvme.rs` | Enable `GET_LOG_PAGE = 0x02` (previously commented out) |

**apis**

| File | Change |
| ---- | ------ |
| `protobuf/v1/pool.proto` | New RPC and messages |
| `src/v1.rs` | Re-export the new message names in the pool module |

#### Runtime Requirements

- `smartctl` (from `smartmontools`) must be present in the io-engine container image.
- The disk's `/dev` name must be visible inside the container.
- The container must hold the `SYS_RAWIO` capability in order to issue the underlying device commands.

#### Assumptions and Dependencies

- The pool's backing disk is already open by io-engine — that is, the pool is `Online`.
- For kernel-attached disks, the device genuinely supports SMART. Some cloud virtual disks do not; those correctly return `supported = false`.
- For VFIO NVMe, the controller responds to a standard SMART log page request. Log page `0x02` is mandatory in the NVMe specification, so this holds for any conformant device.

### Risks and Mitigations

| Risk | Impact | Mitigation |
| ---- | ------ | ---------- |
| `smartctl` missing from the image | Kernel-attached disk health cannot be read | Return `supported = false` rather than erroring; add `smartmontools` to the image as part of this change |
| Device does not implement SMART | No real values available | Return `supported = false`; never fabricate or default a value |
| Health read takes measurable time | Brief delay on a reactor core | Call health on demand only, off the I/O hot path; consider a timeout on the `smartctl` invocation |
| VFIO open path is new code | Possible surprises on real hardware | Test on real VFIO NVMe; open the handle read-only so it cannot clash with the running pool |
| `SYS_RAWIO` capability requirement | Widens the io-engine container's privileges | io-engine already runs privileged for SPDK device access, so this adds no new class of privilege; it should still be called out in deployment documentation |
| Admin passthru could in principle carry a harmful opcode | Device damage or data loss if misused | Only `GET LOG PAGE` is enabled, only log ID `0x02` is requested, and only the read-only passthru variant is used; the opcode is not exposed through any user-supplied parameter |
| Shelling out to an external binary | Process-spawn failures, parsing drift across `smartctl` versions | Parse the stable JSON output rather than human-readable text; treat parse failure as unsupported; pin the `smartmontools` version in the image |
| Health data exposed over gRPC | Serial numbers and wear data become visible to any gRPC client | The pool service is already an internal, node-local API with the same trust boundary; no new authentication surface is introduced |

## Graduation Criteria

*The phasing below was not specified in the source design documents and is proposed here for owner review.*

**Alpha**

- `device_health()` implemented for both paths, with unit tests passing on parsed fixtures for NVMe JSON, SATA attribute tables, and the raw 512-byte log page.
- `GetPoolHealth` available and callable via `grpcurl`.
- Verified manually on at least one kernel-attached SATA or SAS disk and one kernel-attached NVMe device.
- Documented as experimental; no control-plane consumer.

**Beta**

- Verified on real VFIO-attached NVMe hardware, including a read-only health read while the pool is `Online` and serving I/O, with no measurable impact on pool I/O.
- End-to-end test for `GetPoolHealth` running in CI.
- `smartmontools` present in the released image and the capability requirement documented.
- Behaviour confirmed on a pool mixing a SMART-capable and a SMART-less device.

**GA**

- The RPC has been available for at least one release with no reported regressions to pool I/O or stability.
- Field feedback gathered from operators on whether the normalised field set is sufficient.
- Consumers exist or are agreed upon — the control plane and CSI work is scoped, even though it is delivered separately.

## Implementation History

- **2026-07-02** — High-Level and Low-Level Design Documents drafted (v1.0).
- **2026-07-22** — Reformatted as an OEP for community review.
- *Pending* — `Summary` and `Motivation` sections merged, signalling owner acceptance.
- *Pending* — `Proposal` section merged, signalling agreement on the design.
- *Pending* — Implementation start.
- *Pending* — First OpenEBS release containing an initial version.
- *Pending* — Graduation to general availability.

## Drawbacks

- **It introduces a runtime dependency on an external binary.** Path A shells out to `smartctl`, which adds `smartmontools` to the container image, adds a process-spawn to the request path, and couples correctness to the JSON output of a tool we do not control. A pure-library implementation would avoid this, at considerable cost in per-vendor quirk handling.
- **Two mechanisms mean two code paths to maintain and test.** The normalised `DeviceHealth` struct hides this from callers, but it does not remove the maintenance burden, and it means field coverage differs subtly between paths (ATA devices simply do not report `percentage_used` or spare capacity).
- **The VFIO path exercises code that has not previously been used in production.** The admin-passthru mechanism ships today for `identify`, but issuing a log-page read against a device concurrently backing an online pool is new behaviour, and the blast radius of a mistake there is a live pool.
- **On its own, the feature is not yet actionable.** Without polling, history, metrics, or control-plane exposure, an operator must call the RPC manually to benefit. The value is only fully realised once the deferred follow-on work lands.
- **It widens the io-engine gRPC surface** for a feature that some deployments — for example, entirely cloud-virtual-disk-backed ones — can never use.

## Alternatives

**Use `smartctl` for everything.** Simplest to implement and maintain, single code path. Rejected because VFIO-attached NVMe devices have no `/dev` node and are detached from the kernel driver, so `smartctl` cannot reach them at all. This would exclude precisely the performance-oriented deployments most likely to care about drive wear.

**Use NVMe admin commands for everything.** Also a single code path, and no external binary dependency. Rejected because it only works for NVMe; SATA and SAS devices would be left with no health reporting, and those remain very common in Mayastor pools.

**Patch SPDK to expose a device-health abstraction.** Arguably the cleanest long-term home for this logic, and would benefit other SPDK consumers. Rejected for this proposal because it requires an upstream review cycle on a timeline we do not control, and would block the feature indefinitely. The chosen design deliberately requires no SPDK source change.

**Collect SMART data outside io-engine — a node-level DaemonSet, `node_exporter`'s smartmon collector, or similar.** Attractive because it keeps the data plane simple and reuses existing monitoring infrastructure. Rejected because such a collector cannot see VFIO-attached devices (the kernel no longer owns them), and because it has no knowledge of which disks back which Mayastor pool, so correlating a failing device with an affected pool would be left to the operator.

**Implement SMART parsing natively in Rust (`libatasmart` bindings or an equivalent crate) instead of invoking `smartctl`.** Would remove the external process and the image dependency. Rejected for the initial implementation because `smartmontools` encodes a large body of per-vendor attribute interpretation that would have to be reimplemented and maintained, and because its JSON output gives a stable machine-readable contract. This remains a reasonable follow-up if the process-spawn cost or image size proves problematic.

**Read health eagerly at pool import and cache it.** Would make the RPC cheap and constant-time. Rejected because health values are exactly the ones that change over time; a cached value is misleading precisely when it matters. On-demand reads with optional polling added later is the safer ordering.

## Infrastructure Needed

- **Container image change:** `smartmontools` must be added to the io-engine image build (Nix/Dockerfile) so that `smartctl` is available at runtime.
- **Capability and device-visibility review:** confirmation from the deployment/Helm owners that `SYS_RAWIO` and host `/dev` visibility are acceptable and correctly set in the shipped manifests.
- **CI hardware access:** the hardware-dependent tests need runners with (a) a real SATA or SAS disk, (b) a kernel-attached NVMe device, and (c) an NVMe device that can be bound to VFIO. Only the parsing tests can run on standard CI runners.
- **Cross-repository coordination:** changes span `io-engine`, `spdk-rs`, and `apis`, so the PRs will need to land in a coordinated order.

## Testing

**Unit tests (no hardware required)**

- Parse a captured NVMe `smartctl --json` payload and assert every mapped field.
- Parse a captured SATA attribute table and assert the mapped ATA attribute IDs (194, 5, 9, 12, 198).
- Parse a captured SAS payload and assert `scsi_grown_defect_list` maps to media errors.
- Parse a synthetic 512-byte NVMe SMART log page and assert every field offset.
- A log page shorter than 512 bytes is rejected rather than partially parsed.
- A failed SMART overall-health status maps to "not healthy".
- Malformed or empty `smartctl` output yields "not supported", not a panic.
- A non-zero `smartctl` exit code with valid JSON is still parsed successfully.

**Hardware and integration tests**

- Kernel-attached SATA or SAS disk via `aio://`.
- Kernel-attached NVMe via `uring://` and `aio://`.
- A device referenced by a `/dev/disk/by-path/...` symlink, confirming Path A selection.
- VFIO-attached NVMe via `pcie://`, including a read-only health read while the pool is `Online` and serving I/O, verifying no disruption to in-flight I/O.
- A device with no SMART support (for example a cloud virtual disk), confirming `supported = false`.
- io-engine running without `smartctl` in the image, confirming graceful degradation rather than a failed RPC.

**End-to-end tests**

- `GetPoolHealth` against a single-disk pool, asserting the response shape and populated values.
- `GetPoolHealth` against a multi-disk pool mixing a SMART-capable and a SMART-less device, asserting one entry per disk with the correct `supported` flags and that the call succeeds.
- `GetPoolHealth` against a non-existent pool, asserting the expected error status.
- Backwards compatibility: an older client using the pool service is unaffected by the new RPC.
