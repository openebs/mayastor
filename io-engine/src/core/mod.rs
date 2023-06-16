//!
//! core contains the primary abstractions around the SPDK primitives.
use std::{fmt::Debug, sync::atomic::AtomicUsize};

use nix::errno::Errno;
use snafu::Snafu;

pub use bdev::{Bdev, BdevIter, UntypedBdev};
pub use block_device::{
    BlockDevice,
    BlockDeviceDescriptor,
    BlockDeviceHandle,
    BlockDeviceIoStats,
    DeviceIoController,
    DeviceTimeoutAction,
    IoCompletionCallback,
    IoCompletionCallbackArg,
    LbaRangeController,
    OpCompletionCallback,
    OpCompletionCallbackArg,
    ReadMode,
};
pub use cpu_cores::{Core, Cores};
pub use descriptor::{DescriptorGuard, UntypedDescriptorGuard};
pub use device_events::{
    DeviceEventDispatcher,
    DeviceEventListener,
    DeviceEventSink,
    DeviceEventType,
};
pub use device_monitor::{
    device_cmd_queue,
    device_monitor_loop,
    DeviceCommand,
};
pub use env::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    GLOBAL_RC,
    SIG_RECEIVED,
};
pub use handle::{BdevHandle, UntypedBdevHandle};
pub use io_device::IoDevice;
pub use logical_volume::LogicalVolume;
pub use reactor::{
    reactor_monitor_loop,
    Reactor,
    ReactorState,
    Reactors,
    REACTOR_LIST,
};

pub use lock::{
    ProtectedSubsystems,
    ResourceLockGuard,
    ResourceLockManager,
    ResourceLockManagerConfig,
    ResourceSubsystem,
};
pub use runtime::spawn;
pub(crate) use segment_map::SegmentMap;
pub use share::{Protocol, PtplProps, Share, ShareProps, UpdateProps};
pub use spdk_rs::{cpu_cores, GenericStatusCode, IoStatus, IoType, NvmeStatus, CommandSpecificStatusCode};
pub use thread::Mthread;

use crate::subsys::NvmfError;
pub use snapshot::{
    SnapshotDescriptor,
    SnapshotOps,
    SnapshotParams,
    SnapshotXattrs,
};

mod bdev;
mod block_device;
mod descriptor;
mod device_events;
mod device_monitor;
pub mod diagnostics;
mod env;
mod handle;
mod io_device;
pub mod io_driver;
pub mod lock;
pub mod logical_volume;
pub mod mempool;
mod nic;
pub mod partition;
mod reactor;
pub mod runtime;
pub(crate) mod segment_map;
mod share;
pub mod snapshot;
pub(crate) mod thread;
mod work_queue;

/// Obtain the full error chain
pub trait VerboseError {
    fn verbose(&self) -> String;
}

impl<T> VerboseError for T
where
    T: std::error::Error,
{
    /// loops through the error chain and formats into a single string
    /// containing all the lower level errors
    fn verbose(&self) -> String {
        let mut msg = format!("{self}");
        let mut opt_source = self.source();
        while let Some(source) = opt_source {
            msg = format!("{msg}: {source}");
            opt_source = source.source();
        }
        msg
    }
}

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum CoreError {
    #[snafu(display("bdev {} not found", name))]
    BdevNotFound {
        name: String,
    },
    #[snafu(display("failed to open bdev"))]
    OpenBdev {
        source: Errno,
    },
    #[snafu(display("bdev {} not found", name))]
    InvalidDescriptor {
        name: String,
    },
    #[snafu(display("failed to get IO channel for {}", name))]
    GetIoChannel {
        name: String,
    },
    InvalidOffset {
        offset: u64,
    },
    #[snafu(display(
        "Failed to dispatch write at offset {} length {}",
        offset,
        len
    ))]
    WriteDispatch {
        source: Errno,
        offset: u64,
        len: u64,
    },
    #[snafu(display(
        "Failed to dispatch read at offset {} length {}",
        offset,
        len
    ))]
    ReadDispatch {
        source: Errno,
        offset: u64,
        len: u64,
    },
    #[snafu(display("Failed to dispatch reset: {}", source))]
    ResetDispatch {
        source: Errno,
    },
    #[snafu(display("Failed to dispatch flush: {}", source))]
    FlushDispatch {
        source: Errno,
    },
    #[snafu(display(
        "Failed to dispatch NVMe Admin command {:x}h: {}",
        opcode,
        source
    ))]
    NvmeAdminDispatch {
        source: Errno,
        opcode: u16,
    },
    #[snafu(display(
        "Failed to dispatch unmap at offset {} length {}",
        offset,
        len
    ))]
    UnmapDispatch {
        source: Errno,
        offset: u64,
        len: u64,
    },
    #[snafu(display(
        "Failed to dispatch write-zeroes at offset {} length {}",
        offset,
        len
    ))]
    WriteZeroesDispatch {
        source: Errno,
        offset: u64,
        len: u64,
    },
    #[snafu(display(
        "Failed to dispatch NVMe IO passthru command {:x}h: {}",
        opcode,
        source
    ))]
    NvmeIoPassthruDispatch {
        source: Errno,
        opcode: u16,
    },
    #[snafu(display(
        "Write failed at offset {} length {} with status {:?}",
        offset,
        len,
        status
    ))]
    WriteFailed {
        status: NvmeStatus,
        offset: u64,
        len: u64,
    },
    #[snafu(display(
        "Read failed at offset {} length {} with status {:?}",
        offset,
        len,
        status
    ))]
    ReadFailed {
        status: NvmeStatus,
        offset: u64,
        len: u64,
    },
    #[snafu(display(
        "Attempt to read unallocated block failed at offset {} length {}",
        offset,
        len
    ))]
    ReadingUnallocatedBlock {
        offset: u64,
        len: u64,
    },
    #[snafu(display("Reset failed"))]
    ResetFailed {},
    #[snafu(display(
        "Write zeroes failed at offset {} length {}",
        offset,
        len
    ))]
    WriteZeroesFailed {
        offset: u64,
        len: u64,
    },
    #[snafu(display("NVMe Admin command {:x}h failed", opcode))]
    NvmeAdminFailed {
        opcode: u16,
    },
    #[snafu(display("NVMe IO Passthru command {:x}h failed", opcode))]
    NvmeIoPassthruFailed {
        opcode: u16,
    },
    #[snafu(display("failed to share"))]
    ShareNvmf {
        source: NvmfError,
    },
    #[snafu(display("failed to unshare"))]
    UnshareNvmf {
        source: NvmfError,
    },
    #[snafu(display("the operation is invalid for this bdev: {}", source))]
    NotSupported {
        source: Errno,
    },
    #[snafu(display("failed to configure reactor: {}", source))]
    ReactorConfigureFailed {
        source: Errno,
    },
    #[snafu(display("Failed to allocate DMA buffer of {} bytes", size))]
    DmaAllocationFailed {
        size: u64,
    },
    #[snafu(display("Failed to get I/O satistics for device: {}", source))]
    DeviceStatisticsFailed {
        source: Errno,
    },
    #[snafu(display("No devices available for I/O"))]
    NoDevicesAvailable {},
    #[snafu(display("Invalid NVMe device hanele: {}", msg))]
    InvalidNvmeDeviceHandle {
        msg: String,
    },

    #[snafu(display("errno: {} Device Flush {}", source, name))]
    DeviceFlush {
        source: Errno,
        name: String,
    },
    #[snafu(display(
        "NVMe persistence through power-loss failure: {}",
        reason
    ))]
    Ptpl {
        reason: String,
    },
    #[snafu(display("Failed to create device snapshot: {}", reason))]
    SnapshotCreate {
        reason: String,
    },
}

/// Transform error into errno code.
pub trait IntoErrno {
    fn into_errno(self) -> u32;
}

/// Map CoreError to errno code. For now assume any error is EIO
impl IntoErrno for CoreError {
    fn into_errno(self) -> u32 {
        libc::EIO as u32
    }
}

/// Logical volume layer failure.
#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum LvolFailure {
    NoSpace,
}

/// I/O submission failure.
#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum IoSubmissionFailure {
    Read,
    Write,
}

// Generic I/O completion status for block devices, which supports per-protocol
// error domains.
#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum IoCompletionStatus {
    Success,
    NvmeError(NvmeStatus),
    LvolError(LvolFailure),
    IoSubmissionError(IoSubmissionFailure),
    AdminCommandError,
}

impl From<NvmeStatus> for IoCompletionStatus {
    fn from(s: NvmeStatus) -> Self {
        if s == NvmeStatus::VendorSpecific(libc::ENOSPC) {
            IoCompletionStatus::LvolError(LvolFailure::NoSpace)
        } else {
            IoCompletionStatus::NvmeError(s)
        }
    }
}

pub fn is_zoned_nvme_error(status: IoCompletionStatus) -> bool {
    match status {
        IoCompletionStatus::NvmeError(NvmeStatus::CommandSpecific(cssc)) => match cssc {
            CommandSpecificStatusCode::ZonedBoundaryError => true,
            CommandSpecificStatusCode::ZoneIsFull => true,
            CommandSpecificStatusCode::ZoneIsReadOnly => true,
            CommandSpecificStatusCode::ZoneIsOffline => true,
            CommandSpecificStatusCode::ZoneInvalidWrite => true,
            CommandSpecificStatusCode::TooManyActiveZones => true,
            CommandSpecificStatusCode::TooManyOpenZones => true,
            CommandSpecificStatusCode::InvalidZoneStateTransition => true,
            _ => false,
        },
        _ => false,
    }
}

// TODO move this elsewhere ASAP
pub static PAUSING: AtomicUsize = AtomicUsize::new(0);
pub static PAUSED: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub struct MayastorFeatures {
    pub asymmetric_namespace_access: bool,
}
