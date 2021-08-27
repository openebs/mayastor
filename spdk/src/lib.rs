#[macro_use]
extern crate tracing;

mod bdev_module;
pub use bdev_module::{
    BdevModule,
    BdevModuleBuild,
    BdevModuleBuilder,
    BdevModuleConfigJson,
    BdevModuleError,
    BdevModuleFini,
    BdevModuleGetCtxSize,
    BdevModuleInit,
};

mod uuid;
pub use crate::uuid::Uuid;

pub mod cpu_cores;
pub mod ffihelper;

mod io_type;
pub use io_type::{IoStatus, IoType};

// -- tmp --
#[macro_export]
macro_rules! dbgln {
    ($cls:ident, $subcls:expr; $fmt:expr $(,$a:expr)*) => ({
        let p = format!("{: >2}| {: <20} | {: <64} |", Cores::current(),
            stringify!($cls), $subcls);
        let m = format!($fmt $(,$a)*);
        println!("{} {}", p, m);
    });
}

#[macro_use]
pub mod bdev;
pub use bdev::{Bdev, BdevBuilder, BdevOps};

#[macro_use]
mod io_device;
pub use io_device::IoDevice;

#[macro_use]
mod io_channel;
pub use io_channel::IoChannel;

#[macro_use]
mod bdev_io;
pub use bdev_io::BdevIo;

#[macro_use]
mod poller;
pub use poller::{Poller, PollerBuilder};
