use super::pool_backend::PoolBackend;
use crate::core::{LogicalVolume, Protocol, PtplProps, UpdateProps};

/// This interface defines the high level operations which can be done on a
/// `Pool` replica. Replica-Specific details should be hidden away in the
/// implementation as much as possible, though we can allow for extra pool
/// specific options to be passed as parameters.
/// A `Replica` is also a `LogicalVolume` and also has `Share` traits.
#[async_trait::async_trait(?Send)]
pub trait ReplicaOps: LogicalVolume {
    /// Shares the replica with the given properties.
    /// This handles the idempotence in case the replica is already shared on
    /// the same protocol.
    /// If the replica is shared on a different protocol then it must be first
    /// unshared.
    /// todo: handle != protocol
    async fn share(
        &mut self,
        props: crate::core::ShareProps,
    ) -> Result<String, crate::pool_backend::Error> {
        if self.shared() == Some(props.protocol()) {
            self.update_properties(props.into()).await?;
            //return Ok(self.share_uri().unwrap_or_default());
            return Ok(String::new());
        }

        let share = self.share_nvmf(props.into()).await?;
        Ok(share)
    }

    fn shared(&self) -> Option<Protocol>;
    fn create_ptpl(
        &self,
    ) -> Result<Option<PtplProps>, crate::pool_backend::Error>;

    /// Shares the replica via nvmf.
    async fn share_nvmf(
        &mut self,
        props: crate::core::NvmfShareProps,
    ) -> Result<String, crate::pool_backend::Error>;
    /// Unshare the replica.
    async fn unshare(&mut self) -> Result<(), crate::pool_backend::Error>;
    /// Update share properties of a currently shared replica.
    async fn update_properties(
        &mut self,
        props: UpdateProps,
    ) -> Result<(), crate::pool_backend::Error>;

    /// Resize the replica to the given new size.
    async fn resize(
        &mut self,
        size: u64,
    ) -> Result<(), crate::pool_backend::Error>;
    /// Set the replica's entity id.
    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), crate::pool_backend::Error>;
    /// Destroy the replica from its parent pool.
    /// # Warning
    /// Destroying implies unsharing, which might fail for some reason, example
    /// if the target is in a bad state, or if IOs are stuck.
    /// todo: return back `Self` in case of an error.
    async fn destroy(self: Box<Self>)
        -> Result<(), crate::pool_backend::Error>;
}

/// Find replica with filters.
#[derive(Debug, Default)]
pub struct ListReplicaArgs {
    /// Match the given replica name.
    pub name: Option<String>,
    /// Match the given replica uuid.
    pub uuid: Option<String>,
    /// Match the given pool name.
    pub pool_name: Option<String>,
    /// Match the given pool uuid.
    pub pool_uuid: Option<String>,
}

/// Find replica with filters.
#[derive(Debug, Clone)]
pub struct FindReplicaArgs {
    /// The replica uuid to find for.
    pub uuid: String,
}
impl FindReplicaArgs {
    /// Create `Self` with the replica uuid.
    pub fn new(uuid: &str) -> Self {
        Self {
            uuid: uuid.to_string(),
        }
    }
}

/// Interface for a replica factory which can be used for various
/// listing operations, for a specific backend type.
#[async_trait::async_trait(?Send)]
pub trait ReplicaFactory {
    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error>;
    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error>;
    fn backend(&self) -> PoolBackend;
}
