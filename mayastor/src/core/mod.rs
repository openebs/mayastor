//!
//! core contains the primary abstractions around the SPDK primitives.
use std::sync::atomic::AtomicUsize;

pub use ::uuid::Uuid;
use nix::errno::Errno;
use snafu::Snafu;

pub use bdev::{Bdev, BdevIter};
pub use bio::{Bio, IoStatus, IoType};
pub use block_device::{
    BlockDevice,
    BlockDeviceDescriptor,
    BlockDeviceHandle,
    BlockDeviceIoStats,
    DeviceEventListener,
    DeviceEventType,
    DeviceIoController,
    DeviceTimeoutAction,
    IoCompletionCallback,
    IoCompletionCallbackArg,
    LbaRangeController,
    OpCompletionCallback,
    OpCompletionCallbackArg,
};
pub use channel::IoChannel;
pub use cpu_cores::{Core, Cores};
pub use descriptor::{Descriptor, RangeContext};
pub use dma::{DmaBuf, DmaError};
pub use env::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    GLOBAL_RC,
    SIG_RECEIVED,
};
pub use handle::BdevHandle;
pub use io_device::IoDevice;
pub use nvme::{
    nvme_admin_opc,
    nvme_nvm_opcode,
    GenericStatusCode,
    NvmeCommandStatus,
    NvmeStatus,
};
pub use reactor::{Reactor, ReactorState, Reactors, REACTOR_LIST};
pub use runtime::spawn;
pub use share::{Protocol, Share};
pub use thread::Mthread;

use crate::{subsys::NvmfError, target::iscsi};

mod bdev;
mod bio;
mod block_device;
mod channel;
mod cpu_cores;
mod descriptor;
mod dma;
mod env;
mod handle;
mod io_device;
pub mod io_driver;
pub mod mempool;
mod nvme;
pub mod poller;
mod reactor;
pub mod runtime;
mod share;
pub(crate) mod thread;
pub mod uuid;

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub")]
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
        "Failed to dispatch NVMe IO passthru command {:x}h: {}",
        opcode,
        source
    ))]
    NvmeIoPassthruDispatch {
        source: Errno,
        opcode: u16,
    },
    #[snafu(display("Write failed at offset {} length {}", offset, len))]
    WriteFailed {
        offset: u64,
        len: u64,
    },
    #[snafu(display("Read failed at offset {} length {}", offset, len))]
    ReadFailed {
        offset: u64,
        len: u64,
    },
    #[snafu(display("Reset failed"))]
    ResetFailed {},
    #[snafu(display("NVMe Admin command {:x}h failed", opcode))]
    NvmeAdminFailed {
        opcode: u16,
    },
    #[snafu(display("NVMe IO Passthru command {:x}h failed", opcode))]
    NvmeIoPassthruFailed {
        opcode: u16,
    },
    #[snafu(display("failed to share {}", source))]
    ShareNvmf {
        source: NvmfError,
    },
    #[snafu(display("failed to unshare {}", source))]
    UnshareNvmf {
        source: NvmfError,
    },
    #[snafu(display("failed to share {}", source))]
    ShareIscsi {
        source: iscsi::Error,
    },
    #[snafu(display("failed to unshare {}", source))]
    UnshareIscsi {
        source: iscsi::Error,
    },
    #[snafu(display("the operation is invalid for this bdev: {}", source))]
    NotSupported {
        source: Errno,
    },
    #[snafu(display("failed to configure reactor: {}", source))]
    ReactorError {
        source: Errno,
    },
    #[snafu(display("Failed to allocate DMA buffer of {} bytes", size))]
    DmaAllocationError {
        size: u64,
    },
    #[snafu(display("Failed to get I/O satistics for device: {}", source))]
    DeviceStatisticsError {
        source: Errno,
    },
    #[snafu(display("No devices available for I/O"))]
    NoDevicesAvailable {},
}

// Generic I/O completion status for block devices, which supports per-protocol
// error domains.
#[derive(Debug, Copy, Clone, Eq, PartialOrd, PartialEq)]
pub enum IoCompletionStatus {
    Success,
    NvmeError(NvmeCommandStatus),
}

pub static PAUSING: AtomicUsize = AtomicUsize::new(0);
pub static PAUSED: AtomicUsize = AtomicUsize::new(0);
type Nexus = String;
type Child = String;

pub enum Command {
    Retire(Nexus, Child),
}

pub static DEAD_LIST: once_cell::sync::Lazy<
    crossbeam::queue::SegQueue<Command>,
> = once_cell::sync::Lazy::new(crossbeam::queue::SegQueue::new);
