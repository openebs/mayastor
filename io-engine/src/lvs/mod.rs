use crate::{
    core::{Share, UntypedBdev, UpdateProps},
    pool_backend::{PoolOps, ReplicaArgs},
    replica_backend::ReplicaOps,
};
pub use lvol_snapshot::LvolSnapshotIter;
pub use lvs_bdev::LvsBdev;
pub use lvs_error::{BsError, ImportErrorReason, LvsError};
pub use lvs_iter::{LvsBdevIter, LvsIter};
pub use lvs_lvol::{Lvol, LvsLvol, PropName, PropValue};
pub use lvs_store::Lvs;
use std::pin::Pin;

mod lvol_snapshot;
mod lvs_bdev;
mod lvs_error;
mod lvs_iter;
pub mod lvs_lvol;
mod lvs_store;

#[async_trait::async_trait(?Send)]
impl ReplicaOps for Lvol {
    type ReplError = LvsError;

    async fn share_nvmf(
        &mut self,
        props: crate::core::NvmfShareProps,
    ) -> Result<String, Self::ReplError> {
        Pin::new(self).share_nvmf(Some(props)).await
    }
    async fn unshare(&mut self) -> Result<(), Self::ReplError> {
        Pin::new(self).unshare().await
    }
    async fn update_properties<P: Into<UpdateProps>>(
        &mut self,
        props: P,
    ) -> Result<(), Self::ReplError> {
        Pin::new(self).update_properties(props.into()).await
    }

    async fn resize(&mut self, size: u64) -> Result<(), Self::ReplError> {
        self.resize_replica(size).await
    }

    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), Self::ReplError> {
        Pin::new(self).set(PropValue::EntityId(id)).await
    }

    async fn destroy(self) -> Result<(), Self::ReplError> {
        self.destroy_replica().await?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PoolOps for Lvs {
    type Replica = Lvol;
    type Error = LvsError;

    async fn replicas(&self) -> Result<Vec<Self::Replica>, Self::Error> {
        let Some(bdev) = UntypedBdev::bdev_first() else {
            return Ok(vec![]);
        };

        Ok(bdev.into_iter().filter_map(Lvol::ok_from).collect())
    }
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Self::Replica, Self::Error> {
        self.create_lvol(
            &args.name,
            args.size,
            Some(&args.uuid),
            args.thin,
            args.entity_id,
        )
        .await
    }
    async fn destroy(self) -> Result<(), Self::Error> {
        self.destroy().await
    }

    async fn export(self) -> Result<(), Self::Error> {
        self.export().await
    }
}
