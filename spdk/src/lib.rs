#[macro_use]
extern crate tracing;
extern crate serde;
extern crate serde_json;

mod bdev;
mod bdev_desc;
mod bdev_io;
mod bdev_iter;
mod bdev_module;
pub mod cpu_cores;
mod dma;
mod error;
pub mod ffihelper;
mod io_channel;
mod io_device;
mod io_device_traverse;
mod io_type;
mod json_write_context;
mod nvme;
mod poller;
mod thread;
mod uring;
mod uuid;

pub use crate::{
    bdev::{Bdev, BdevBuilder, BdevOps},
    bdev_desc::{BdevDesc, BdevEvent},
    bdev_io::BdevIo,
    bdev_iter::BdevIter,
    bdev_module::{
        BdevModule,
        BdevModuleBuild,
        BdevModuleBuilder,
        WithModuleConfigJson,
        WithModuleFini,
        WithModuleGetCtxSize,
        WithModuleInit,
    },
    dma::{DmaBuf, DmaError},
    error::{Result, SpdkError},
    io_channel::IoChannel,
    io_device::IoDevice,
    io_device_traverse::{ChannelTraverseStatus, IoDeviceChannelTraverse},
    io_type::{IoStatus, IoType},
    json_write_context::JsonWriteContext,
    nvme::{
        nvme_admin_opc,
        nvme_nvm_opcode,
        nvme_reservation_acquire_action,
        nvme_reservation_register_action,
        nvme_reservation_register_cptpl,
        nvme_reservation_type,
        GenericStatusCode,
        NvmeCommandStatus,
        NvmeStatus,
    },
    poller::{Poller, PollerBuilder},
    thread::Thread,
    uuid::Uuid,
};
