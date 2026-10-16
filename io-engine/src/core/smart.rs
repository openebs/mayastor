//! Device SMART data.
//!
//! This is implemented at the pool layer rather than inside the SPDK bdev
//! abstraction. Two disk attachments are covered, dispatched on by the caller
//! (see `SpdkBlockDevice::smart_data` in `bdev/device.rs`):
//!
//! - kernel block devices opened as `aio://` or `uring://` bdevs — including
//!   NVMe used without VFIO, e.g. `uring:///dev/nvme4n1` — which always have a
//!   `/dev` node. SMART data for such a device is obtained by invoking
//!   `smartctl --json` on that node ([`read_smart_data`]); this single path
//!   covers SAS, SATA and kernel NVMe uniformly.
//! - VFIO/SPDK-native NVMe (`pcie://`) has no `/dev` node, so `smartctl` can't
//!   be used. SMART data is instead read through the existing bdev NVMe admin
//!   passthru (`BdevHandle::nvme_get_smart`/`nvme_get_error_log`) and decoded
//!   here from the raw log pages ([`SmartData::from_nvme_smart`],
//!   [`parse_nvme_error_log`]).
//!
//! Runtime requirements for the kernel path: the `smartctl` binary must be
//! present in the engine image, the `/dev` node must be visible to the
//! process, and the process needs `CAP_SYS_RAWIO` (smartctl issues SG_IO/ATA
//! pass-through ioctls internally).
//!
//! NOTE: `smartctl` is a subprocess call that blocks until it completes.
//! [`read_smart_data`] is called from an SPDK reactor (the `ListPoolsSMART`
//! gRPC handler runs inside `spdk_submit!`), so it trampolines the actual
//! subprocess execution onto the tokio runtime (same off-reactor pattern as
//! `lvm::tokio_submit`/`tokio_run!`) rather than blocking the calling reactor
//! thread for the subprocess's duration.

use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    sync::Mutex,
};

use futures::channel::oneshot;
use nix::errno::Errno;
use once_cell::sync::Lazy;
use serde_json::Value;
use tokio::process::Command;

use crate::core::{runtime, CoreError, Reactor};

/// Static device identity/inventory data. Unlike [`SmartData`]'s counters,
/// this never changes for the device's lifetime, so callers should fetch it
/// once and cache it (see [`cached_nvme_identity`] for the VFIO path) rather
/// than re-reading it on every SMART poll. `Option` fields are unset when a
/// given device or transport does not report them.
#[derive(Debug, Default, Clone)]
pub struct DeviceIdentity {
    /// Model / product name.
    pub model: Option<String>,
    /// Model family, when the vendor's database recognises the model (ATA
    /// only).
    pub model_family: Option<String>,
    /// Device serial number.
    pub serial_number: Option<String>,
    /// Firmware revision string.
    pub firmware_revision: Option<String>,
    /// World Wide Name, when reported.
    pub wwn: Option<String>,
    /// Total user-addressable capacity, in bytes.
    pub capacity_bytes: Option<u64>,
    /// Logical block size, in bytes.
    pub logical_sector_size: Option<u32>,
    /// Physical block size, in bytes.
    pub physical_sector_size: Option<u32>,
    /// RPM, or `Some(0)` for non-rotating media (SSD/NVMe).
    pub rotation_rate: Option<u32>,
    /// Physical form factor (e.g. "2.5 inches"), ATA only.
    pub form_factor: Option<String>,
    /// Transport / interface protocol (e.g. "NVMe", "SATA", "PCIe").
    pub transport: Option<String>,
    /// Negotiated link speed, when reported by the transport.
    pub link_speed: Option<String>,
}

/// A single SMART attribute table entry (ATA devices only -- SAS/NVMe report
/// no attribute table since their equivalent thresholds are already
/// first-class fields on [`SmartData`]/[`NvmeSmart`]).
#[derive(Debug, Clone)]
pub struct SmartAttribute {
    /// Vendor-defined attribute ID.
    pub id: u8,
    /// Human-readable attribute name, when recognised.
    pub name: String,
    /// Normalised current value.
    pub value: u8,
    /// Worst (lowest) normalised value ever recorded.
    pub worst: u8,
    /// Failure threshold for the normalised value.
    pub threshold: u8,
    /// Vendor-specific raw counter backing this attribute.
    pub raw_value: u64,
}

/// A single NVMe Error Information Log entry (Log Page Identifier 01h),
/// decoded either from the raw 64-byte log entry (VFIO path) or from
/// `smartctl`'s `nvme_error_information_log.table` (kernel path). Only
/// non-empty slots are surfaced -- see [`parse_nvme_error_log`]. Surfaced
/// over `ListPoolsSMART`'s gRPC response as `NvmeSmart::error_log_entries`
/// (see `grpc/v1/pool.rs`'s `NvmeSmart`/`NvmeErrorLogEntry` `From` impls).
#[derive(Debug, Clone, Default)]
pub struct NvmeErrorLogEntry {
    /// Cumulative error counter for the controller (never zero for a valid
    /// entry -- unused log slots are zero-filled and skipped).
    pub error_count: u64,
    /// Submission queue the failing command was posted to.
    pub submission_queue_id: u16,
    /// Command identifier of the failing command, when reported.
    pub command_id: Option<u16>,
    /// Raw NVMe completion-queue Status Field for the failing command.
    pub status_field: u16,
    /// LBA associated with the error, when applicable.
    pub lba: Option<u64>,
    /// Namespace ID associated with the error, when applicable.
    pub namespace_id: Option<u32>,
}

/// Cache of [`DeviceIdentity`] for VFIO-bound NVMe devices, keyed by bdev
/// name. Identity requires a separate NVMe Identify Controller admin command
/// from the SMART log page, and — unlike the counters — never changes, so it's
/// fetched once per device and reused rather than re-issued on every
/// `ListPoolsSMART` call.
static NVME_IDENTITY_CACHE: Lazy<Mutex<HashMap<String, DeviceIdentity>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Look up a cached VFIO NVMe device identity, if one has already been
/// fetched for `name`.
pub fn cached_nvme_identity(name: &str) -> Option<DeviceIdentity> {
    NVME_IDENTITY_CACHE.lock().unwrap().get(name).cloned()
}

/// Cache a freshly-fetched VFIO NVMe device identity for `name`.
pub fn cache_nvme_identity(name: &str, identity: DeviceIdentity) {
    NVME_IDENTITY_CACHE
        .lock()
        .unwrap()
        .insert(name.to_string(), identity);
}

/// Evict a cached VFIO NVMe device identity for `name`, if one is present.
///
/// Must be called when the underlying bdev goes away (pool destroy/export, or
/// the device is hot-removed) -- otherwise the cache keeps growing, and a
/// later bdev that happens to reuse the same name (e.g. the same pool
/// recreated) would incorrectly be served the previous device's identity.
/// Called from `bdev_event_callback` on `DeviceEventType::DeviceRemoved`.
pub fn evict_nvme_identity(name: &str) {
    NVME_IDENTITY_CACHE.lock().unwrap().remove(name);
}

/// Device SMART data. The top-level fields are the values every device type
/// reports with the same meaning; the raw report for the device type is in
/// [`SmartData::protocol`]. `Option` fields are unset when a given device or
/// transport does not report them.
#[derive(Debug, Default, Clone)]
pub struct SmartData {
    /// The device's own overall health self-assessment. `None` when the
    /// device reports no verdict (it is not defaulted to "healthy").
    pub smart_status_passed: Option<bool>,
    /// Temperature in degrees Celsius.
    pub temperature_celsius: Option<i16>,
    /// Cumulative power-on time, in hours.
    pub power_on_hours: Option<u128>,
    /// Number of power-cycle events.
    pub power_cycles: Option<u128>,
    /// Remaining spare capacity, percent.
    pub available_spare_percent: Option<u8>,
    /// Spare threshold below which the device warns, percent.
    pub available_spare_threshold_percent: Option<u8>,
    /// Estimated device life used, percent (may exceed 100).
    pub percentage_used: Option<u8>,
    /// Static device identity/inventory data, see [`DeviceIdentity`].
    pub identity: Option<DeviceIdentity>,
    /// The raw report for the device type: NVMe or ATA. `None` for SAS.
    pub protocol: Option<SmartProtocol>,
}

/// Protocol specific SMART report.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum SmartProtocol {
    /// NVMe SMART / Health Information log and Error Information log.
    Nvme(NvmeSmart),
    /// ATA SMART attribute table.
    Ata(AtaSmart),
}

/// NVMe SMART / Health Information log (LID 02h) and Error Information log
/// (LID 01h) values. The temperature, power-on hours, power cycles and wear
/// indicators repeat the values in [`SmartData`] and are always equal to them.
#[derive(Debug, Default, Clone)]
pub struct NvmeSmart {
    /// Raw NVMe critical-warning bit flags (0 == no critical condition).
    pub critical_warning: u8,
    /// Temperature in degrees Celsius.
    pub temperature_celsius: Option<i16>,
    /// Remaining spare capacity, percent.
    pub available_spare_percent: Option<u8>,
    /// Spare threshold below which the device warns, percent.
    pub available_spare_threshold_percent: Option<u8>,
    /// Estimated device life used, percent (may exceed 100).
    pub percentage_used: Option<u8>,
    /// Number of 512-byte data units read, in thousands (i.e. total bytes
    /// read is approximately `data_units_read * 1000 * 512`).
    pub data_units_read: Option<u128>,
    /// Number of 512-byte data units written, in thousands (see
    /// `data_units_read`).
    pub data_units_written: Option<u128>,
    /// Number of host read commands completed.
    pub host_reads: Option<u128>,
    /// Number of host write commands completed.
    pub host_writes: Option<u128>,
    /// Time the controller was busy with I/O, in minutes.
    pub controller_busy_minutes: Option<u128>,
    /// Number of power-cycle events.
    pub power_cycles: Option<u128>,
    /// Cumulative power-on time, in hours.
    pub power_on_hours: Option<u128>,
    /// Number of unsafe shutdowns (power loss without a clean shutdown
    /// notification).
    pub unsafe_shutdowns: Option<u128>,
    /// Media / data-integrity (uncorrectable) error count.
    pub media_errors: Option<u128>,
    /// Number of entries in the error-information log.
    pub num_error_log_entries: Option<u128>,
    /// Minutes the composite temperature has been at or above the warning threshold.
    pub warning_temp_time: Option<u32>,
    /// Minutes the composite temperature has been at or above the critical threshold.
    pub critical_comp_time: Option<u32>,
    /// Most recent NVMe Error Information Log entries, bounded to a small
    /// window (see [`parse_nvme_error_log`]); empty when the device didn't
    /// report any errors, or for transports this isn't decoded for.
    pub error_log_entries: Vec<NvmeErrorLogEntry>,
}

/// ATA SMART data.
#[derive(Debug, Default, Clone)]
pub struct AtaSmart {
    /// SMART attribute table.
    pub attributes: Vec<SmartAttribute>,
}

impl NvmeSmart {
    /// Parse a 512-byte NVMe SMART / Health Information log page (LID 02h),
    /// as returned by a Get Log Page admin command. Returns `None` if too
    /// short.
    fn from_log_page(page: &[u8]) -> Option<Self> {
        if page.len() < 512 {
            return None;
        }
        let byte = |o: usize| page.get(o).copied();
        // Composite temperature is reported in Kelvin; convert to Celsius with
        // checked arithmetic so bogus/out-of-range device data (or a
        // misbehaving VFIO passthru) can't overflow the i16 subtraction and
        // panic -- it's just reported as absent instead.
        let temperature_celsius = le_u16(page, 1)
            .filter(|&temp_k| temp_k != 0)
            .and_then(|temp_k| i16::try_from(temp_k as i32 - 273).ok());
        Some(Self {
            critical_warning: byte(0)?,
            temperature_celsius,
            available_spare_percent: byte(3),
            available_spare_threshold_percent: byte(4),
            percentage_used: byte(5),
            data_units_read: le_u128(page, 32),
            data_units_written: le_u128(page, 48),
            host_reads: le_u128(page, 64),
            host_writes: le_u128(page, 80),
            controller_busy_minutes: le_u128(page, 96),
            power_cycles: le_u128(page, 112),
            power_on_hours: le_u128(page, 128),
            unsafe_shutdowns: le_u128(page, 144),
            media_errors: le_u128(page, 160),
            num_error_log_entries: le_u128(page, 176),
            // Warning / Critical Composite Temperature Time, in minutes.
            warning_temp_time: le_u32(page, 192),
            critical_comp_time: le_u32(page, 196),
            // Filled in by the caller from a separate Get Log Page(01h) read,
            // see `parse_nvme_error_log`.
            error_log_entries: Vec::new(),
        })
    }
}

impl SmartData {
    /// The NVMe report, if this is an NVMe device.
    pub fn nvme(&self) -> Option<&NvmeSmart> {
        match &self.protocol {
            Some(SmartProtocol::Nvme(n)) => Some(n),
            _ => None,
        }
    }

    /// Mutable access to the NVMe report, if this is an NVMe device.
    pub fn nvme_mut(&mut self) -> Option<&mut NvmeSmart> {
        match &mut self.protocol {
            Some(SmartProtocol::Nvme(n)) => Some(n),
            _ => None,
        }
    }

    /// Build the SMART data from a 512-byte NVMe SMART / Health Information log page (LID 02h),
    /// as returned by a Get Log Page admin command. Used for VFIO-bound NVMe,
    /// which has no `/dev` node for smartctl.
    /// Returns `None` if too short.
    pub fn from_nvme_smart(page: &[u8]) -> Option<Self> {
        let nvme = NvmeSmart::from_log_page(page)?;
        let mut data = Self {
            // No smartctl on this path: the verdict is the critical warning.
            smart_status_passed: Some(nvme.critical_warning == 0),
            // Identity comes from a separate Identify Controller admin
            // command; the caller (device.rs) fills this in from its own
            // cache, keyed by device name, since it never changes.
            identity: None,
            protocol: Some(SmartProtocol::Nvme(nvme)),
            ..Default::default()
        };
        data.sync_from_nvme();
        Some(data)
    }

    /// Set the normalized fields from the NVMe report, which takes precedence
    /// over any other source.
    /// No-op for non-NVMe devices.
    fn sync_from_nvme(&mut self) {
        if let Some(SmartProtocol::Nvme(n)) = &self.protocol {
            self.temperature_celsius = n.temperature_celsius.or(self.temperature_celsius);
            self.power_on_hours = n.power_on_hours.or(self.power_on_hours);
            self.power_cycles = n.power_cycles.or(self.power_cycles);
            self.available_spare_percent =
                n.available_spare_percent.or(self.available_spare_percent);
            self.available_spare_threshold_percent = n
                .available_spare_threshold_percent
                .or(self.available_spare_threshold_percent);
            self.percentage_used = n.percentage_used.or(self.percentage_used);
        }
    }

    /// Make the values repeated in the NVMe report equal to the normalized ones.
    /// No-op for non-NVMe devices.
    fn sync_to_nvme(&mut self) {
        if let Some(SmartProtocol::Nvme(n)) = &mut self.protocol {
            n.temperature_celsius = self.temperature_celsius;
            n.power_on_hours = self.power_on_hours;
            n.power_cycles = self.power_cycles;
            n.available_spare_percent = self.available_spare_percent;
            n.available_spare_threshold_percent = self.available_spare_threshold_percent;
            n.percentage_used = self.percentage_used;
        }
    }
}

/// Read `N` bytes at offset `o`, or `None` if they are out of bounds.
fn le_bytes<const N: usize>(buf: &[u8], o: usize) -> Option<[u8; N]> {
    buf.get(o..o.checked_add(N)?)?.try_into().ok()
}

fn le_u16(buf: &[u8], o: usize) -> Option<u16> {
    le_bytes(buf, o).map(u16::from_le_bytes)
}

fn le_u32(buf: &[u8], o: usize) -> Option<u32> {
    le_bytes(buf, o).map(u32::from_le_bytes)
}

fn le_u64(buf: &[u8], o: usize) -> Option<u64> {
    le_bytes(buf, o).map(u64::from_le_bytes)
}

fn le_u128(buf: &[u8], o: usize) -> Option<u128> {
    le_bytes(buf, o).map(u128::from_le_bytes)
}

/// Size in bytes of a single NVMe Error Information Log entry, per the NVMe
/// base spec. Shared with callers that need to size a buffer for the log
/// before it's parsed (see `SpdkBlockDevice::smart_data` in `bdev/device.rs`).
pub const NVME_ERROR_LOG_ENTRY_SIZE: usize = 64;

/// Parse a raw NVMe Error Information Log (Log Page Identifier 01h) buffer,
/// as returned by a Get Log Page admin command, into entries. Used for
/// VFIO-bound NVMe. The log is a fixed-size circular buffer of
/// [`NVME_ERROR_LOG_ENTRY_SIZE`]-byte entries; unused slots are zero-filled
/// per the NVMe base spec (a valid entry's `error_count` is never zero), so
/// those are filtered out.
pub fn parse_nvme_error_log(page: &[u8]) -> Vec<NvmeErrorLogEntry> {
    page.chunks_exact(NVME_ERROR_LOG_ENTRY_SIZE)
        .filter_map(|e| {
            let error_count = le_u64(e, 0)?;
            if error_count == 0 {
                return None;
            }
            Some(NvmeErrorLogEntry {
                error_count,
                submission_queue_id: le_u16(e, 8)?,
                command_id: Some(le_u16(e, 10)?),
                status_field: le_u16(e, 12)?,
                lba: Some(le_u64(e, 16)?),
                namespace_id: Some(le_u32(e, 24)?),
            })
        })
        .collect()
}

/// Parse a 4096-byte NVMe Identify Controller data structure (as returned by
/// an Identify admin command with CNS=1) into [`DeviceIdentity`]. Used for
/// VFIO-bound NVMe, which has no Identify-derived info surfaced any other way.
/// Layout is fixed by the NVMe base spec: SN at byte offset 4 (20 bytes), MN
/// at offset 24 (40 bytes), FR at offset 64 (8 bytes), all ASCII space-padded.
/// Returns `None` if the buffer is too short.
pub fn identity_from_nvme_identify(data: &[u8]) -> Option<DeviceIdentity> {
    if data.len() < 72 {
        return None;
    }
    let ascii_field = |start: usize, len: usize| -> Option<String> {
        let s = std::str::from_utf8(&data[start..start + len]).ok()?;
        let s = s.trim();
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    };
    Some(DeviceIdentity {
        model: ascii_field(24, 40),
        model_family: None,
        serial_number: ascii_field(4, 20),
        firmware_revision: ascii_field(64, 8),
        wwn: None,
        capacity_bytes: None,
        logical_sector_size: None,
        physical_sector_size: None,
        rotation_rate: Some(0), // NVMe is always non-rotating media.
        form_factor: None,
        transport: Some("PCIe".to_string()),
        link_speed: None,
    })
}

/// Read SMART data for a kernel block device by its `/dev` path, via
/// `smartctl --json --all -l devstat`.
///
/// Runs the subprocess off the calling SPDK reactor: when called from an SPDK
/// thread (the normal case -- `ListPoolsSMART` runs inside `spdk_submit!`),
/// the blocking `Command::output()` is trampolined onto the tokio runtime and
/// the result hopped back onto the primary reactor before returning, instead
/// of blocking the reactor for `smartctl`'s duration. See the module docs.
pub async fn read_smart_data(path: &str) -> Result<SmartData, CoreError> {
    let path = path.to_string();
    run_off_reactor(async move { run_smartctl(&path).await }).await
}

/// Run `future` off the calling SPDK reactor if currently on one (spawn onto
/// tokio, then hop the result back onto the primary reactor before
/// returning), otherwise just await it in place. Mirrors the pattern used by
/// `lvm::tokio_submit`/`tokio_run!` for the same reason: subprocess/blocking
/// work must not run on a reactor thread.
async fn run_off_reactor<F, T>(future: F) -> Result<T, CoreError>
where
    F: std::future::Future<Output = Result<T, CoreError>> + Send + 'static,
    T: Send + std::fmt::Debug + 'static,
{
    if !spdk_rs::Thread::is_spdk_thread() {
        return future.await;
    }

    let (tx, rx) = oneshot::channel();
    runtime::spawn(async move {
        let result = future.await;
        if let Ok(r) = Reactor::spawn_at_primary(async move {
            let _ = tx.send(result);
        }) {
            r.await.ok();
        }
    });
    rx.await
        .map_err(|_| CoreError::NotSupported { source: Errno::EIO })?
}

/// smartctl uses a bitmask exit status where non-zero frequently just signals
/// SMART warnings while still emitting complete JSON, so the exit code is
/// ignored and the JSON on stdout is parsed regardless.
async fn run_smartctl(path: &str) -> Result<SmartData, CoreError> {
    let output = Command::new("smartctl")
        // `--all` does not read the ATA Device Statistics log, which is where
        // the SATA SSD endurance (`endurance_used`) comes from, so ask for it
        // explicitly. The option is accepted for NVMe and SCSI devices too.
        .args(["--json", "--all", "-l", "devstat", path])
        .output()
        .await
        .map_err(|error| {
            // Capture the real error string into the returned error (not just a
            // log line) so it's visible to the ListPoolsSMART caller too, via
            // DiskSmart::error -- CoreError::NotSupported alone would only
            // carry the generic Errno, losing this detail.
            let reason = format!("failed to spawn smartctl for '{path}': {error}");
            warn!("{reason}");
            CoreError::SmartctlFailed {
                reason,
                source: Errno::ENOENT,
            }
        })?;

    let json: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        let reason = format!("failed to parse smartctl JSON output for '{path}': {error}");
        warn!("{reason}");
        CoreError::SmartctlFailed {
            reason,
            source: Errno::EIO,
        }
    })?;

    // If smartctl could not open / identify the device there is no usable data.
    // smartctl reports why in its own `messages` array (e.g. "Unable to detect
    // device type") -- surface that instead of a bare "not supported".
    if json.get("device").is_none() {
        let reason = json
            .pointer("/smartctl/messages/0/string")
            .and_then(Value::as_str)
            .map(|msg| format!("smartctl couldn't read '{path}': {msg}"))
            .unwrap_or_else(|| format!("smartctl produced no usable output for '{path}'"));
        warn!("{reason}");
        return Err(CoreError::SmartctlFailed {
            reason,
            source: Errno::ENXIO,
        });
    }

    Ok(parse_smartctl(&json))
}

/// Extract device identity/inventory data (model, serial, capacity, ...) from
/// smartctl JSON. Available uniformly across NVMe, SATA and SAS -- smartctl
/// normalises this regardless of transport, unlike the SMART counters below.
fn parse_smartctl_identity(j: &Value) -> DeviceIdentity {
    let str_field = |ptr: &str| j.pointer(ptr).and_then(Value::as_str).map(str::to_string);

    let wwn = j
        .pointer("/wwn")
        .and_then(|w| {
            let naa = w.get("naa").and_then(Value::as_u64)?;
            let oui = w.get("oui").and_then(Value::as_u64)?;
            let id = w.get("id").and_then(Value::as_u64)?;
            Some(format!("{naa:x}{oui:06x}{id:010x}"))
        })
        .or_else(|| {
            // Some SCSI devices (e.g. the scsi_debug test driver) report
            // identity as a single logical_unit_id string instead of the
            // structured naa/oui/id object above -- fall back to it,
            // stripped of its "0x" prefix to match the other format.
            j.pointer("/logical_unit_id")
                .and_then(Value::as_str)
                .map(|s| s.trim_start_matches("0x").to_string())
        });

    DeviceIdentity {
        // SAS drives report their identity under the `scsi_*` keys only.
        model: str_field("/model_name").or_else(|| str_field("/scsi_model_name")),
        model_family: str_field("/model_family"),
        serial_number: str_field("/serial_number"),
        firmware_revision: str_field("/firmware_version").or_else(|| str_field("/scsi_revision")),
        wwn,
        capacity_bytes: j.pointer("/user_capacity/bytes").and_then(Value::as_u64),
        logical_sector_size: j
            .pointer("/logical_block_size")
            .and_then(Value::as_u64)
            .map(|v| v as u32),
        physical_sector_size: j
            .pointer("/physical_block_size")
            .and_then(Value::as_u64)
            .map(|v| v as u32),
        rotation_rate: j
            .pointer("/rotation_rate")
            .and_then(Value::as_u64)
            .map(|v| v as u32),
        form_factor: str_field("/form_factor/name"),
        transport: str_field("/device/protocol"),
        link_speed: str_field("/interface_speed/current/string"),
    }
}

/// Convert a smartctl temperature to degrees Celsius. A reading of 0 is
/// treated as absent: devices without a real sensor (e.g. virtual SCSI disks)
/// report 0, which must not be shown as 0 °C. Out-of-range values are absent
/// too, rather than truncated.
fn celsius(v: i64) -> Option<i16> {
    if v == 0 {
        return None;
    }
    i16::try_from(v).ok()
}

/// Map smartctl JSON (NVMe / SATA / SAS) into [`SmartData`].
fn parse_smartctl(j: &Value) -> SmartData {
    let mut d = SmartData {
        identity: Some(parse_smartctl_identity(j)),
        // The device's own verdict, when it reports one.
        smart_status_passed: j.pointer("/smart_status/passed").and_then(Value::as_bool),
        ..Default::default()
    };

    // Temperature, power-on hours and power cycles are reported uniformly.
    if let Some(t) = j.pointer("/temperature/current").and_then(Value::as_i64) {
        d.temperature_celsius = celsius(t);
    }
    if let Some(v) = j.pointer("/power_on_time/hours").and_then(Value::as_u64) {
        d.power_on_hours = Some(v as u128);
    }
    if let Some(v) = j.pointer("/power_cycle_count").and_then(Value::as_u64) {
        d.power_cycles = Some(v as u128);
    }

    // Kernel NVMe: the SMART/health log is reported directly by smartctl. This
    // is the primary NVMe path (NVMe attached via uring/aio, not VFIO).
    if let Some(n) = j.get("nvme_smart_health_information_log") {
        let g = |k: &str| n.get(k).and_then(Value::as_u64);
        let nvme = NvmeSmart {
            critical_warning: g("critical_warning").unwrap_or(0) as u8,
            temperature_celsius: n
                .get("temperature")
                .and_then(Value::as_i64)
                .and_then(celsius),
            available_spare_percent: g("available_spare").map(|v| v as u8),
            available_spare_threshold_percent: g("available_spare_threshold").map(|v| v as u8),
            percentage_used: g("percentage_used").map(|v| v as u8),
            data_units_read: g("data_units_read").map(u128::from),
            data_units_written: g("data_units_written").map(u128::from),
            host_reads: g("host_reads").map(u128::from),
            host_writes: g("host_writes").map(u128::from),
            controller_busy_minutes: g("controller_busy_time").map(u128::from),
            power_cycles: g("power_cycles").map(u128::from),
            power_on_hours: g("power_on_hours").map(u128::from),
            unsafe_shutdowns: g("unsafe_shutdowns").map(u128::from),
            media_errors: g("media_errors").map(u128::from),
            num_error_log_entries: g("num_err_log_entries").map(u128::from),
            warning_temp_time: g("warning_temp_time").map(|v| v as u32),
            critical_comp_time: g("critical_comp_time").map(|v| v as u32),
            error_log_entries: Vec::new(),
        };
        d.protocol = Some(SmartProtocol::Nvme(nvme));
        d.sync_from_nvme();
    }

    // NVMe error log (Log Page 01h), surfaced by smartctl separately from the
    // SMART/health log above. `table` already omits zero-filled/unused slots,
    // so every entry here is real -- unlike the raw VFIO path, no filtering
    // is needed. Confirmed against live smartctl 7.5 JSON output.
    if let (Some(nvme), Some(table)) = (
        d.nvme_mut(),
        j.pointer("/nvme_error_information_log/table")
            .and_then(Value::as_array),
    ) {
        nvme.error_log_entries = table
            .iter()
            .filter_map(|e| {
                let error_count = e.get("error_count").and_then(Value::as_u64)?;
                Some(NvmeErrorLogEntry {
                    error_count,
                    submission_queue_id: e
                        .get("submission_queue_id")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as u16,
                    command_id: e
                        .get("command_id")
                        .and_then(Value::as_u64)
                        .map(|v| v as u16),
                    status_field: e
                        .pointer("/status_field/value")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as u16,
                    lba: e.pointer("/lba/value").and_then(Value::as_u64),
                    namespace_id: e.get("nsid").and_then(Value::as_u64).map(|v| v as u32),
                })
            })
            .collect();
    }

    // SATA: vendor attribute table. The attributes behind the normalized
    // fields only fill in what the cleaner top-level values above did not.
    if let Some(table) = j
        .pointer("/ata_smart_attributes/table")
        .and_then(Value::as_array)
    {
        let mut ata = AtaSmart::default();
        for attr in table {
            let id = attr.get("id").and_then(Value::as_u64).unwrap_or(0);
            let raw = attr.pointer("/raw/value").and_then(Value::as_u64);

            ata.attributes.push(SmartAttribute {
                id: id as u8,
                name: attr
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                value: attr.get("value").and_then(Value::as_u64).unwrap_or(0) as u8,
                worst: attr.get("worst").and_then(Value::as_u64).unwrap_or(0) as u8,
                threshold: attr.get("thresh").and_then(Value::as_u64).unwrap_or(0) as u8,
                raw_value: raw.unwrap_or(0),
            });

            match (id, raw) {
                (9, Some(v)) => {
                    d.power_on_hours = d.power_on_hours.or(Some(v as u128));
                }
                (12, Some(v)) => {
                    d.power_cycles = d.power_cycles.or(Some(v as u128));
                }
                (190 | 194, Some(v)) => {
                    d.temperature_celsius = d.temperature_celsius.or(celsius((v & 0xff) as i64));
                }
                _ => {}
            }
        }
        d.protocol = Some(SmartProtocol::Ata(ata));

        // SATA SSD wear indicators, from the ATA device statistics. Absent
        // for devices (e.g. spinning disks) that do not report them.
        let pct = |ptr: &str| {
            j.pointer(ptr)
                .and_then(Value::as_u64)
                .and_then(|v| u8::try_from(v).ok())
        };
        d.available_spare_percent = pct("/spare_available/current_percent");
        d.available_spare_threshold_percent = pct("/spare_available/threshold_percent");
        d.percentage_used = pct("/endurance_used/current_percent");
    }

    d.sync_to_nvme();
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real captures from physical devices, see `tests/data/smart/README.md`.
    const NVME_P4800X_SMART_LOG_PAGE: &[u8] =
        include_bytes!("../../tests/data/smart/nvme_p4800x_smart_log.bin");
    const NVME_P4800X_SMARTCTL_JSON: &str =
        include_str!("../../tests/data/smart/nvme_p4800x_smartctl_all.json");
    const NVME_970EVO_SMARTCTL_JSON: &str =
        include_str!("../../tests/data/smart/nvme_970evoplus_smartctl_all.json");
    const SAS_SSD_SMARTCTL_JSON: &str =
        include_str!("../../tests/data/smart/sas_ssd_smartctl_all.json");
    const SATA_SSD_SMARTCTL_JSON: &str =
        include_str!("../../tests/data/smart/sata_ssd_smartctl_x.json");

    #[test]
    fn real_nvme_log_page_matches_nvme_cli() {
        // Page captured with `nvme get-log --log-id=2 --raw-binary` on a Dell P4800X.
        // Expected values are what `nvme smart-log` printed for the same drive.
        let d = SmartData::from_nvme_smart(NVME_P4800X_SMART_LOG_PAGE).expect("512 byte page");
        assert_eq!(d.smart_status_passed, Some(true));
        assert_eq!(d.temperature_celsius, Some(38)); // 311 K
        assert_eq!(d.available_spare_percent, Some(100));
        assert_eq!(d.available_spare_threshold_percent, Some(0));
        assert_eq!(d.percentage_used, Some(0));
        assert_eq!(d.power_cycles, Some(800));
        assert_eq!(d.power_on_hours, Some(34634));

        let n = d.nvme().expect("nvme report");
        assert_eq!(n.critical_warning, 0);
        assert_eq!(n.data_units_read, Some(594770183));
        // These two moved while the drive was busy: values are the ones in the page itself.
        assert_eq!(n.data_units_written, Some(844447059));
        assert_eq!(n.host_writes, Some(94025498570));
        assert_eq!(n.host_reads, Some(68682906748));
        assert_eq!(n.controller_busy_minutes, Some(8382));
        assert_eq!(n.unsafe_shutdowns, Some(199));
        assert_eq!(n.media_errors, Some(0));
        assert_eq!(n.num_error_log_entries, Some(0));
        // Both are 0 on this drive, so offsets 192/196 are only checked against zero.
        assert_eq!(n.warning_temp_time, Some(0));
        assert_eq!(n.critical_comp_time, Some(0));
        assert!(n.error_log_entries.is_empty());
    }

    #[test]
    fn real_nvme_smartctl_json() {
        let j: Value = serde_json::from_str(NVME_P4800X_SMARTCTL_JSON).unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.smart_status_passed, Some(true));
        assert_eq!(d.temperature_celsius, Some(38));
        assert_eq!(d.power_cycles, Some(800));
        assert_eq!(d.power_on_hours, Some(34634));
        assert_eq!(d.available_spare_percent, Some(100));
        assert_eq!(d.available_spare_threshold_percent, Some(0));
        assert_eq!(d.percentage_used, Some(0));

        let id = d.identity.as_ref().expect("identity");
        assert_eq!(
            id.model.as_deref(),
            Some("Dell Express Flash NVMe P4800X 375GB SFF")
        );
        assert_eq!(id.firmware_revision.as_deref(), Some("E201DP35"));
        assert_eq!(id.transport.as_deref(), Some("NVMe"));
        assert_eq!(id.capacity_bytes, Some(375083606016));

        let n = d.nvme().expect("nvme report");
        assert_eq!(n.critical_warning, 0);
        assert_eq!(n.unsafe_shutdowns, Some(199));
        assert_eq!(n.media_errors, Some(0));
        assert_eq!(n.num_error_log_entries, Some(0));
        assert_eq!(n.controller_busy_minutes, Some(8382));
        // smartctl 7.4 does not emit the temperature times; 7.5 does, see
        // `real_nvme_smartctl_json_with_error_log`.
        assert_eq!(n.warning_temp_time, None);
        assert_eq!(n.critical_comp_time, None);
        // No errors logged: smartctl reports the log without a table.
        assert!(n.error_log_entries.is_empty());
        // Repeated values equal the normalized ones.
        assert_eq!(n.temperature_celsius, d.temperature_celsius);
        assert_eq!(n.power_on_hours, d.power_on_hours);
    }

    #[test]
    fn real_nvme_smartctl_json_with_error_log() {
        // Samsung 970 EVO Plus, smartctl 7.5: the drive has a logged error.
        let j: Value = serde_json::from_str(NVME_970EVO_SMARTCTL_JSON).unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.smart_status_passed, Some(true));
        assert_eq!(d.temperature_celsius, Some(42));
        assert_eq!(d.power_cycles, Some(1526));
        assert_eq!(d.power_on_hours, Some(6950));
        assert_eq!(d.available_spare_percent, Some(100));
        assert_eq!(d.available_spare_threshold_percent, Some(10));
        assert_eq!(d.percentage_used, Some(43));

        let id = d.identity.as_ref().expect("identity");
        assert_eq!(id.model.as_deref(), Some("Samsung SSD 970 EVO Plus 500GB"));
        assert_eq!(id.firmware_revision.as_deref(), Some("2B2QEXM7"));
        assert_eq!(id.transport.as_deref(), Some("NVMe"));
        assert_eq!(id.capacity_bytes, Some(500107862016));

        let n = d.nvme().expect("nvme report");
        assert_eq!(n.critical_warning, 0);
        assert_eq!(n.data_units_read, Some(122488427));
        assert_eq!(n.data_units_written, Some(678247827));
        assert_eq!(n.host_reads, Some(4615917444));
        assert_eq!(n.host_writes, Some(11797257812));
        assert_eq!(n.controller_busy_minutes, Some(15559));
        assert_eq!(n.unsafe_shutdowns, Some(212));
        assert_eq!(n.media_errors, Some(0));
        // smartctl 7.5 emits the temperature times (0 on this drive).
        assert_eq!(n.warning_temp_time, Some(0));
        assert_eq!(n.critical_comp_time, Some(0));
        // The lifetime error count equals the `error_count` of the newest logged entry.
        assert_eq!(n.num_error_log_entries, Some(2848));
        assert_eq!(n.error_log_entries.len(), 1);
        let e = &n.error_log_entries[0];
        assert_eq!(e.error_count, 2848);
        assert_eq!(e.submission_queue_id, 0);
        assert_eq!(e.command_id, Some(6));
        assert_eq!(e.status_field, 8194);
        assert_eq!(e.lba, Some(0));
        assert_eq!(e.namespace_id, Some(0));
    }

    #[test]
    fn real_sas_ssd_smartctl_json() {
        let j: Value = serde_json::from_str(SAS_SSD_SMARTCTL_JSON).unwrap();
        let d = parse_smartctl(&j);
        // SAS has neither an NVMe nor an ATA report.
        assert!(d.protocol.is_none());
        assert_eq!(d.smart_status_passed, Some(true));
        assert_eq!(d.temperature_celsius, Some(38));
        assert_eq!(d.power_on_hours, Some(34635));
        // smartctl gives only the manufacture date for the start-stop counter.
        assert_eq!(d.power_cycles, None);
        assert_eq!(d.available_spare_percent, None);

        // SAS drives report their identity under the `scsi_*` keys only.
        let id = d.identity.as_ref().expect("identity");
        assert_eq!(id.model.as_deref(), Some("TOSHIBA KPM5XVUG960G"));
        assert_eq!(id.firmware_revision.as_deref(), Some("B01C"));
        assert_eq!(id.capacity_bytes, Some(960197124096));
        assert_eq!(id.rotation_rate, Some(0));
        assert_eq!(id.form_factor.as_deref(), Some("2.5 inches"));
        assert_eq!(id.transport.as_deref(), Some("SCSI"));
        assert_eq!(id.wwn.as_deref(), Some("5000000000000000"));
    }

    #[test]
    fn real_sata_ssd_smartctl_json() {
        let j: Value = serde_json::from_str(SATA_SSD_SMARTCTL_JSON).unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.smart_status_passed, Some(true));
        // The top-level value (28) wins over attribute 194's raw value (26).
        assert_eq!(d.temperature_celsius, Some(28));
        assert_eq!(d.power_on_hours, Some(32959));
        assert_eq!(d.power_cycles, Some(2841));
        // From `endurance_used`, which smartctl derives from the ATA device statistics.
        assert_eq!(d.percentage_used, Some(40));
        // This model reports no spare capacity.
        assert_eq!(d.available_spare_percent, None);
        assert_eq!(d.available_spare_threshold_percent, None);

        let id = d.identity.as_ref().expect("identity");
        assert_eq!(id.model.as_deref(), Some("CT250MX500SSD1"));
        assert_eq!(
            id.model_family.as_deref(),
            Some("Crucial/Micron Client SSDs")
        );
        assert_eq!(id.firmware_revision.as_deref(), Some("M3CR023"));
        assert_eq!(id.transport.as_deref(), Some("ATA"));
        assert_eq!(id.capacity_bytes, Some(250059350016));
        assert_eq!(id.logical_sector_size, Some(512));
        assert_eq!(id.physical_sector_size, Some(4096));
        assert_eq!(id.rotation_rate, Some(0));
        assert_eq!(id.form_factor.as_deref(), Some("2.5 inches"));
        assert_eq!(id.link_speed.as_deref(), Some("6.0 Gb/s"));
        assert_eq!(id.wwn.as_deref(), Some("50012340000000001"));

        assert!(d.nvme().is_none());
        let Some(SmartProtocol::Ata(ata)) = &d.protocol else {
            panic!("expected ATA report");
        };
        assert_eq!(ata.attributes.len(), 23);
        // Attribute 5 stays in the table; it is not mapped to an NVMe-only counter.
        let a5 = ata
            .attributes
            .iter()
            .find(|a| a.id == 5)
            .expect("attribute 5");
        assert_eq!(a5.name, "Reallocate_NAND_Blk_Cnt");
        assert_eq!(
            (a5.value, a5.worst, a5.threshold, a5.raw_value),
            (100, 100, 10, 0)
        );
        let a202 = ata
            .attributes
            .iter()
            .find(|a| a.id == 202)
            .expect("attribute 202");
        assert_eq!(a202.raw_value, 40);
    }

    #[test]
    fn sata_attributes_are_only_a_fallback_for_normalized_fields() {
        let j: Value = serde_json::from_str(
            r#"{
              "device": {"name": "/dev/sda", "type": "sat"},
              "temperature": {"current": 38},
              "ata_smart_attributes": {"table": [
                {"id": 9,   "raw": {"value": 777}},
                {"id": 12,  "raw": {"value": 8}},
                {"id": 194, "raw": {"value": 99}}
              ]}
            }"#,
        )
        .unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.temperature_celsius, Some(38));
        assert_eq!(d.power_on_hours, Some(777));
        assert_eq!(d.power_cycles, Some(8));
    }

    #[test]
    fn parses_sata_ssd_wear_indicators() {
        let j: Value = serde_json::from_str(
            r#"{
              "device": {"name": "/dev/sda", "type": "sat"},
              "spare_available": {"current_percent": 97, "threshold_percent": 10},
              "endurance_used": {"current_percent": 4},
              "ata_smart_attributes": {"table": []}
            }"#,
        )
        .unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.available_spare_percent, Some(97));
        assert_eq!(d.available_spare_threshold_percent, Some(10));
        assert_eq!(d.percentage_used, Some(4));
        // An empty table still identifies an ATA device.
        assert!(matches!(d.protocol, Some(SmartProtocol::Ata(_))));
    }

    #[test]
    fn sensorless_temperature_is_unset() {
        // e.g. a virtual SCSI disk: reports 0 with no sensor behind it.
        let j: Value = serde_json::from_str(
            r#"{"device": {"name": "/dev/sdb", "type": "scsi"},
                "temperature": {"current": 0}}"#,
        )
        .unwrap();
        assert_eq!(parse_smartctl(&j).temperature_celsius, None);
    }

    #[test]
    fn parses_nvme_temperature_times() {
        let j: Value = serde_json::from_str(
            r#"{
              "device": {"name": "/dev/nvme0n1", "type": "nvme"},
              "nvme_smart_health_information_log": {
                "critical_warning": 0,
                "warning_temp_time": 7,
                "critical_comp_time": 2
              }
            }"#,
        )
        .unwrap();
        let d = parse_smartctl(&j);
        let n = d.nvme().unwrap();
        assert_eq!(n.warning_temp_time, Some(7));
        assert_eq!(n.critical_comp_time, Some(2));
    }

    #[test]
    fn parses_structured_wwn() {
        let (naa, oui, id_num) = (5u64, 20817u64, 12345678u64);
        let j: Value = serde_json::from_str(&format!(
            r#"{{"device": {{"name": "/dev/sda", "type": "sat"}},
                "wwn": {{"naa": {naa}, "oui": {oui}, "id": {id_num}}}}}"#,
        ))
        .unwrap();
        let id = parse_smartctl_identity(&j);
        assert_eq!(id.wwn, Some(format!("{naa:x}{oui:06x}{id_num:010x}")));
    }

    #[test]
    fn falls_back_to_logical_unit_id_when_wwn_absent() {
        // Some SCSI devices (e.g. the scsi_debug test driver, confirmed
        // live) report identity via a plain logical_unit_id string instead
        // of the structured naa/oui/id object -- see
        // parses_structured_wwn for that case.
        let j: Value = serde_json::from_str(
            r#"{"device": {"name": "/dev/sdb", "type": "scsi"},
                "logical_unit_id": "0x3333333000006d60"}"#,
        )
        .unwrap();
        let id = parse_smartctl_identity(&j);
        assert_eq!(id.wwn, Some("3333333000006d60".to_string()));
    }

    #[test]
    fn structured_wwn_takes_priority_over_logical_unit_id() {
        let j: Value = serde_json::from_str(
            r#"{"device": {"name": "/dev/sda", "type": "sat"},
                "wwn": {"naa": 5, "oui": 20817, "id": 12345678},
                "logical_unit_id": "0xdeadbeefdeadbeef"}"#,
        )
        .unwrap();
        let id = parse_smartctl_identity(&j);
        assert!(id.wwn.unwrap().starts_with('5'));
    }

    #[test]
    fn parses_nvme_identify_controller_data() {
        // Build the space-padded ASCII fields programmatically rather than by
        // hand-counting literal padding -- a hand-counted literal is exactly
        // how this test previously had a silent off-by-one (39 bytes copied
        // into a 40-byte field, panicking on any run).
        let pad = |s: &str, len: usize| -> Vec<u8> {
            let mut v = s.as_bytes().to_vec();
            v.resize(len, b' ');
            v
        };
        let mut buf = vec![0u8; 4096];
        buf[4..24].copy_from_slice(&pad("SERIAL1234", 20));
        buf[24..64].copy_from_slice(&pad("Intel Optane P4800X 375GB", 40));
        buf[64..72].copy_from_slice(b"E201DP35");

        let id = identity_from_nvme_identify(&buf).expect("should parse");
        assert_eq!(id.serial_number, Some("SERIAL1234".to_string()));
        assert_eq!(id.model, Some("Intel Optane P4800X 375GB".to_string()));
        assert_eq!(id.firmware_revision, Some("E201DP35".to_string()));
        assert_eq!(id.rotation_rate, Some(0));
        assert_eq!(id.transport, Some("PCIe".to_string()));
    }

    #[test]
    fn nvme_identify_too_short_returns_none() {
        assert!(identity_from_nvme_identify(&[0u8; 10]).is_none());
    }

    #[test]
    fn reports_failed_smart_status() {
        let j: Value = serde_json::from_str(
            r#"{"device":{"name":"/dev/sdb"},"smart_status":{"passed":false},
                "temperature":{"current":55}}"#,
        )
        .unwrap();
        let d = parse_smartctl(&j);
        assert_eq!(d.smart_status_passed, Some(false));
        // The verdict no longer leaks into an NVMe critical-warning bit.
        assert!(d.protocol.is_none());
        assert_eq!(d.temperature_celsius, Some(55));
    }

    #[test]
    fn nvme_smart_bogus_temperature_does_not_panic() {
        // Composite temperature is a raw u16 Kelvin value from the device; a
        // misbehaving/bogus device could report anything up to u16::MAX
        // (65535 K), which overflows i16 once converted to Celsius
        // (65535 - 273 = 65262 > i16::MAX). Must report absent, not panic.
        let mut page = vec![0u8; 512];
        page[1..3].copy_from_slice(&u16::MAX.to_le_bytes());
        let h = SmartData::from_nvme_smart(&page).expect("should parse");
        assert_eq!(h.temperature_celsius, None);
        assert_eq!(h.nvme().unwrap().temperature_celsius, None);
    }

    #[test]
    fn nvme_smart_low_temperature_is_not_treated_as_overflow() {
        // A Kelvin value below 273 legitimately yields a negative Celsius
        // reading (physically absurd for a real device, but not an overflow)
        // -- must not be conflated with the bogus-data case above.
        let mut page = vec![0u8; 512];
        page[1..3].copy_from_slice(&1u16.to_le_bytes());
        let h = SmartData::from_nvme_smart(&page).expect("should parse");
        assert_eq!(h.temperature_celsius, Some(-272));
    }

    // Synthetic page, not a capture: the only coverage of non-zero temperature times, since
    // the real page (see `real_nvme_log_page_matches_nvme_cli`) has zeros at those offsets.
    #[test]
    fn nvme_smart_log_page_fills_both_views() {
        let mut page = vec![0u8; 512];
        page[1..3].copy_from_slice(&314u16.to_le_bytes()); // 41 C
        page[3] = 100; // available spare
        page[4] = 10; // spare threshold
        page[5] = 3; // percentage used
        page[112..128].copy_from_slice(&12u128.to_le_bytes()); // power cycles
        page[128..144].copy_from_slice(&4500u128.to_le_bytes()); // power on hours
        page[192..196].copy_from_slice(&7u32.to_le_bytes()); // warning temp time
        page[196..200].copy_from_slice(&2u32.to_le_bytes()); // critical temp time

        let d = SmartData::from_nvme_smart(&page).expect("should parse");
        assert_eq!(d.smart_status_passed, Some(true));
        assert_eq!(d.temperature_celsius, Some(41));
        assert_eq!(d.available_spare_percent, Some(100));
        assert_eq!(d.available_spare_threshold_percent, Some(10));
        assert_eq!(d.percentage_used, Some(3));
        assert_eq!(d.power_cycles, Some(12));
        assert_eq!(d.power_on_hours, Some(4500));
        let n = d.nvme().unwrap();
        assert_eq!(n.temperature_celsius, Some(41));
        assert_eq!(n.available_spare_percent, Some(100));
        assert_eq!(n.power_cycles, Some(12));
        assert_eq!(n.power_on_hours, Some(4500));
        assert_eq!(n.warning_temp_time, Some(7));
        assert_eq!(n.critical_comp_time, Some(2));
    }

    #[test]
    fn short_nvme_pages_do_not_panic() {
        assert!(SmartData::from_nvme_smart(&[]).is_none());
        assert!(SmartData::from_nvme_smart(&[0u8; 511]).is_none());
        // A trailing partial entry is dropped, not a panic.
        assert!(parse_nvme_error_log(&[0xffu8; NVME_ERROR_LOG_ENTRY_SIZE - 1]).is_empty());
        assert_eq!(le_u32(&[1, 2, 3], 0), None);
        assert_eq!(le_u32(&[1, 2, 3, 4], usize::MAX), None);
        assert_eq!(le_u32(&[1, 2, 3, 4], 0), Some(0x04030201));
    }

    #[test]
    fn nvme_smart_critical_warning_fails_the_verdict() {
        let mut page = vec![0u8; 512];
        page[0] = 0x04;
        let d = SmartData::from_nvme_smart(&page).expect("should parse");
        assert_eq!(d.smart_status_passed, Some(false));
        assert_eq!(d.nvme().unwrap().critical_warning, 0x04);
    }

    #[test]
    fn parses_nvme_error_log_page_skips_empty_slots() {
        // Two 64-byte slots: the first populated, the second zero-filled
        // (unused, per spec) and expected to be skipped.
        let mut page = vec![0u8; 128];
        page[0..8].copy_from_slice(&7u64.to_le_bytes()); // error_count
        page[8..10].copy_from_slice(&1u16.to_le_bytes()); // sqid
        page[10..12].copy_from_slice(&42u16.to_le_bytes()); // cid
        page[12..14].copy_from_slice(&0x8006u16.to_le_bytes()); // status field
        page[16..24].copy_from_slice(&1234u64.to_le_bytes()); // lba
        page[24..28].copy_from_slice(&1u32.to_le_bytes()); // nsid

        let entries = parse_nvme_error_log(&page);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.error_count, 7);
        assert_eq!(e.submission_queue_id, 1);
        assert_eq!(e.command_id, Some(42));
        assert_eq!(e.status_field, 0x8006);
        assert_eq!(e.lba, Some(1234));
        assert_eq!(e.namespace_id, Some(1));
    }

    #[test]
    fn parses_kernel_nvme_error_log_from_smartctl_json() {
        // Shape confirmed against live smartctl 7.5 --json --all output.
        let j: Value = serde_json::from_str(
            r#"{
              "device": {"name": "/dev/nvme0n1", "type": "nvme"},
              "nvme_smart_health_information_log": {"critical_warning": 0},
              "nvme_error_information_log": {
                "size": 64,
                "read": 16,
                "unread": 0,
                "table": [
                  {
                    "error_count": 1,
                    "submission_queue_id": 0,
                    "status_field": {"value": 24582, "do_not_retry": true, "string": "Internal Error"},
                    "phase_tag": false,
                    "lba": {"value": 0}
                  }
                ]
              }
            }"#,
        )
        .unwrap();
        let d = parse_smartctl(&j);
        let h = d.nvme().expect("nvme report");
        assert_eq!(h.error_log_entries.len(), 1);
        let e = &h.error_log_entries[0];
        assert_eq!(e.error_count, 1);
        assert_eq!(e.submission_queue_id, 0);
        assert_eq!(e.status_field, 24582);
        assert_eq!(e.lba, Some(0));
    }
}
