use async_trait::async_trait;

pub use dev::{device_create, device_destroy, device_lookup, device_open};
pub use device::{bdev_io_ctx_pool_init, SpdkBlockDevice};
pub use nexus::{Nexus, NexusInfo, NexusState};
pub use nvmx::{
    nvme_io_ctx_pool_init,
    NvmeController,
    NvmeControllerState,
    NVME_CONTROLLERS,
};

mod aio;
pub(crate) mod dev;
pub(crate) use dev::uri;
pub(crate) mod device;
mod loopback;
mod malloc;
pub mod nexus;
mod null;
pub mod null_ng;
mod nvme;
mod nvmf;
pub(crate) mod nvmx;
mod uring;
pub mod util;

pub trait BdevCreateDestroy: CreateDestroy + GetName + std::fmt::Debug {}

impl<T: CreateDestroy + GetName + std::fmt::Debug> BdevCreateDestroy for T {}

#[async_trait(?Send)]
/// Main trait that must be implemented for every supported device type.
/// Note also that the required methods are declared as async.
pub trait CreateDestroy {
    type Error;
    async fn create(&self) -> Result<String, Self::Error>;
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error>;
}

/// The following trait must also be implemented for every supported
/// device type.
pub trait GetName {
    fn get_name(&self) -> String;
}
