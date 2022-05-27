//!
//! core contains the primary abstractions around the SPDK primitives.
use std::{fmt::Debug, sync::atomic::AtomicUsize, time::Duration};

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
};
pub use channel::IoChannel;
pub use cpu_cores::{Core, Cores};
pub use descriptor::{Descriptor, RangeContext};
pub use device_events::{
    DeviceEventDispatcher,
    DeviceEventListener,
    DeviceEventSink,
    DeviceEventType,
};
pub use env::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    GLOBAL_RC,
    SIG_RECEIVED,
};
pub use handle::BdevHandle;
pub use io_device::IoDevice;
pub use reactor::{Reactor, ReactorState, Reactors, REACTOR_LIST};
pub use runtime::spawn;
pub use share::{Protocol, Share};
pub use spdk_rs::{
    cpu_cores,
    GenericStatusCode,
    IoStatus,
    IoType,
    NvmeCommandStatus,
    NvmeStatus,
};
pub use thread::Mthread;

use crate::subsys::NvmfError;

mod bdev;
mod block_device;
mod channel;
mod descriptor;
mod device_events;
mod env;
mod handle;
mod io_device;
pub mod io_driver;
pub mod mempool;
mod nic;
pub mod partition;
pub mod poller;
mod reactor;
pub mod runtime;
mod share;
pub(crate) mod thread;

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
    #[snafu(display("failed to share {}", source))]
    ShareNvmf {
        source: NvmfError,
    },
    #[snafu(display("failed to unshare {}", source))]
    UnshareNvmf {
        source: NvmfError,
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

// TODO move this elsewhere ASAP
pub static PAUSING: AtomicUsize = AtomicUsize::new(0);
pub static PAUSED: AtomicUsize = AtomicUsize::new(0);

/// TODO
pub async fn device_monitor() {
    let handle = Mthread::get_init();
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    loop {
        interval.tick().await;
        if let Some(w) = MWQ.take() {
            info!(?w, "executing command");
            match w {
                Command::RemoveDevice(nexus, child) => {
                    let rx = handle.spawn_local(async move {
                        if let Some(n) =
                            crate::bdev::nexus::nexus_lookup_mut(&nexus)
                        {
                            if let Err(e) = n.destroy_child(&child).await {
                                error!(?e, "destroy child failed");
                            }
                        }
                    });

                    match rx {
                        Err(e) => {
                            error!(?e, "failed to equeue removal request")
                        }
                        Ok(rx) => rx.await.unwrap(),
                    }
                }
            }
        }
    }
}

type Nexus = String;
type Child = String;

#[derive(Debug, Clone)]
pub enum Command {
    RemoveDevice(Nexus, Child),
}

#[derive(Debug)]
pub struct MayastorWorkQueue<T: Send + Debug> {
    incoming: crossbeam::queue::SegQueue<T>,
}

impl<T: Send + Debug> Default for MayastorWorkQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Debug> MayastorWorkQueue<T> {
    pub fn new() -> Self {
        Self {
            incoming: crossbeam::queue::SegQueue::new(),
        }
    }

    pub fn enqueue(&self, entry: T) {
        trace!(?entry, "enqueued");
        self.incoming.push(entry)
    }

    pub fn len(&self) -> usize {
        self.incoming.len()
    }

    pub fn is_empty(&self) -> bool {
        self.incoming.len() == 0
    }

    pub fn take(&self) -> Option<T> {
        if let Some(elem) = self.incoming.pop() {
            return Some(elem);
        }
        None
    }
}

pub static MWQ: once_cell::sync::Lazy<MayastorWorkQueue<Command>> =
    once_cell::sync::Lazy::new(MayastorWorkQueue::new);

#[derive(Debug, Clone)]
pub struct MayastorFeatures {
    pub asymmetric_namespace_access: bool,
}
