use async_trait::async_trait;

pub use dev::{device_create, device_destroy, device_lookup, device_open};
pub use device::{bdev_event_callback, bdev_io_ctx_pool_init, SpdkBlockDevice};
pub use nexus::{Nexus, NexusInfo, NexusState};
pub use nvmx::{
    nvme_io_ctx_pool_init,
    NvmeController,
    NvmeControllerState,
    NVME_CONTROLLERS,
};

mod aio;
pub(crate) mod dev;
use crate::core::{MayastorEnvironment, PtplProps};
pub(crate) use dev::uri;

pub(crate) mod device;
mod loopback;
mod malloc;
pub mod nexus;
mod null_bdev;
pub mod null_ng;
mod nvme;
mod nvmf;
pub(crate) mod nvmx;
mod nx;
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

/// Exposes functionality to prepare for persisting reservations in the event
/// of a power loss.
/// This can be implemented by each resource that deals with persistent nvme
/// reservations.
pub(crate) trait PtplFileOps {
    /// Create the necessary directory path roots.
    fn create(&self) -> Result<Option<PtplProps>, std::io::Error> {
        if let Some(path) = self.path() {
            if let Some(path) = path.parent() {
                std::fs::create_dir_all(path)?;
            }
            return Ok(Some(PtplProps::new(path)));
        }
        Ok(None)
    }
    /// Destroy the backing file/directory.
    fn destroy(&self) -> Result<(), std::io::Error>;
    /// Get the subpath to the persistent file (within the base_path).
    fn subpath(&self) -> std::path::PathBuf;

    /// Get the base path where all ptpl files are stored in.
    /// If this feature is disable, None is returned.
    fn base_path() -> Option<std::path::PathBuf> {
        MayastorEnvironment::global_or_default()
            .ptpl_dir()
            .map(std::path::PathBuf::from)
    }

    /// Get the actual path to the ptpl file.
    /// If this feature is disable, None is returned.
    fn path(&self) -> Option<std::path::PathBuf> {
        Self::base_path().map(|base| base.join(self.subpath()))
    }
}
