//!
//! core contains the primary abstractions around the SPDK primitives.
pub use ::uuid::Uuid;
use nix::errno::Errno;
use snafu::Snafu;

pub use bdev::{Bdev, BdevIter};
pub use channel::IoChannel;
pub use cpu_cores::{Core, Cores};
pub use descriptor::{Descriptor, RangeContext};
pub use dma::{DmaBuf, DmaError};
pub use env::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment};
pub use handle::BdevHandle;
pub use reactor::{Reactor, Reactors, REACTOR_LIST};
pub use thread::Mthread;

mod bdev;
mod channel;
mod cpu_cores;
mod descriptor;
mod dma;
mod env;
mod handle;
mod reactor;
mod thread;
mod uuid;

#[derive(Debug, Snafu)]
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
        len: usize,
    },
    #[snafu(display(
        "Failed to dispatch read at offset {} length {}",
        offset,
        len
    ))]
    ReadDispatch {
        source: Errno,
        offset: u64,
        len: usize,
    },
    #[snafu(display("Failed to dispatch reset",))]
    ResetDispatch {
        source: Errno,
    },
    #[snafu(display("Write failed at offset {} length {}", offset, len))]
    WriteFailed {
        offset: u64,
        len: usize,
    },
    #[snafu(display("Read failed at offset {} length {}", offset, len))]
    ReadFailed {
        offset: u64,
        len: usize,
    },
    #[snafu(display("Reset failed"))]
    ResetFailed {},
}
