use async_trait::async_trait;

pub use dev::{device_create, device_destroy, device_lookup, device_open};
pub use device::SpdkBlockDevice;
pub use nexus::{
    nexus_bdev::{
        nexus_create,
        nexus_lookup,
        Nexus,
        NexusState,
        NexusStatus,
        VerboseError,
    },
    nexus_child::{lookup_nexus_child, ChildState, Reason},
    nexus_child_status_config,
    nexus_label::{GptEntry, GptGuid as Guid, GptHeader},
    nexus_metadata::{
        MetaDataChildEntry,
        MetaDataIndex,
        MetaDataObject,
        NexusMetaData,
    },
    nexus_persistence::{ChildInfo, NexusInfo},
};

mod aio;
pub(crate) mod dev;
pub(crate) mod device;
mod iscsi;
mod loopback;
mod malloc;
pub(crate) mod nexus;
mod null;
mod nvme;
mod nvmf;
mod nvmx;
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

pub struct Uri;
