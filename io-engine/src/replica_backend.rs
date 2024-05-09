use crate::core::{LogicalVolume, Share, UpdateProps};

/// This interface defines the high level operations which can be done on a
/// `Pool` replica. Replica-Specific details should be hidden away in the
/// implementation as much as possible, though we can allow for extra pool
/// specific options to be passed as parameters.
/// A `Replica` is also a `LogicalVolume` and also has `Share` traits.
#[async_trait::async_trait(?Send)]
pub trait ReplicaOps: Share + LogicalVolume {
    type ReplError: Into<tonic::Status> + std::fmt::Display;

    /// Shares the replica with the given properties.
    /// This handles the idempotence in case the replica is already shared on
    /// the same protocol.
    /// If the replica is shared on a different protocol then it must be first
    /// unshared.
    /// todo: handle != protocol
    async fn share(
        &mut self,
        props: crate::core::ShareProps,
    ) -> Result<String, Self::ReplError> {
        if self.shared() == Some(props.protocol()) {
            self.update_properties(props).await?;
            return Ok(self.share_uri().unwrap_or_default());
        }

        self.share_nvmf(props.into()).await
    }

    /// Shares the replica via nvmf.
    async fn share_nvmf(
        &mut self,
        props: crate::core::NvmfShareProps,
    ) -> Result<String, Self::ReplError>;
    /// Unshare the replica.
    async fn unshare(&mut self) -> Result<(), Self::ReplError>;
    /// Update share properties of a currently shared replica.
    async fn update_properties<P: Into<UpdateProps>>(
        &mut self,
        props: P,
    ) -> Result<(), Self::ReplError>;

    /// Resize the replica to the given new size.
    async fn resize(&mut self, size: u64) -> Result<(), Self::ReplError>;
    /// Set the replica's entity id.
    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), Self::ReplError>;
    /// Destroy the replica from its parent pool.
    /// # Warning
    /// Destroying implies unsharing, which might fail for some reason, example
    /// if the target is in a bad state, or if IOs are stuck.
    /// todo: return back `Self` in case of an error.
    async fn destroy(self) -> Result<(), Self::ReplError>;
}
