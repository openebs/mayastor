use async_trait::async_trait;

pub use nexus::{
    nexus_bdev::{
        nexus_create,
        nexus_lookup,
        Nexus,
        NexusStatus,
        VerboseError,
    },
    nexus_child::ChildStatus,
    nexus_child_error_store::NexusErrStore,
    nexus_label::{GPTHeader, GptEntry},
    nexus_metadata_content::{
        NexusConfig,
        NexusConfigVersion1,
        NexusConfigVersion2,
        NexusConfigVersion3,
    },
};

pub trait BdevCreateDestroy: CreateDestroy + GetName {}

impl<T: CreateDestroy + GetName> BdevCreateDestroy for T {}

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

pub(crate) mod dev;
pub(crate) mod nexus;
pub mod util;
