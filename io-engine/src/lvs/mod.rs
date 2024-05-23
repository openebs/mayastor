use crate::{
    bdev::PtplFileOps,
    core::{LogicalVolume, Protocol, PtplProps, Share, UpdateProps},
    pool_backend::{
        Error,
        FindPoolArgs,
        IPoolProps,
        ListPoolArgs,
        PoolArgs,
        PoolBackend,
        PoolFactory,
        PoolOps,
        ReplicaArgs,
    },
    replica_backend::{
        FindReplicaArgs,
        ListReplicaArgs,
        ReplicaFactory,
        ReplicaOps,
    },
};
pub use lvol_snapshot::LvolSnapshotIter;
pub use lvs_bdev::LvsBdev;
pub use lvs_error::{BsError, ImportErrorReason, LvsError};
pub use lvs_iter::{LvsBdevIter, LvsIter};
pub use lvs_lvol::{Lvol, LvsLvol, PropName, PropValue};
pub use lvs_store::Lvs;
use std::{convert::TryFrom, pin::Pin};

mod lvol_snapshot;
mod lvs_bdev;
mod lvs_error;
mod lvs_iter;
pub mod lvs_lvol;
mod lvs_store;

#[async_trait::async_trait(?Send)]
impl ReplicaOps for Lvol {
    fn shared(&self) -> Option<Protocol> {
        self.as_bdev().shared()
    }

    fn create_ptpl(
        &self,
    ) -> Result<Option<PtplProps>, crate::pool_backend::Error> {
        let ptpl =
            self.ptpl().create().map_err(|source| LvsError::LvolShare {
                source: crate::core::CoreError::Ptpl {
                    reason: source.to_string(),
                },
                name: self.name(),
            })?;
        Ok(ptpl)
    }

    async fn share_nvmf(
        &mut self,
        props: crate::core::NvmfShareProps,
    ) -> Result<String, crate::pool_backend::Error> {
        Pin::new(self)
            .share_nvmf(Some(props))
            .await
            .map_err(Into::into)
    }
    async fn unshare(&mut self) -> Result<(), crate::pool_backend::Error> {
        Pin::new(self).unshare().await.map_err(Into::into)
    }
    async fn update_properties(
        &mut self,
        props: UpdateProps,
    ) -> Result<(), crate::pool_backend::Error> {
        Pin::new(self).update_properties(props).await?;
        Ok(())
    }

    async fn resize(
        &mut self,
        size: u64,
    ) -> Result<(), crate::pool_backend::Error> {
        self.resize_replica(size).await.map_err(Into::into)
    }

    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), crate::pool_backend::Error> {
        Pin::new(self).set(PropValue::EntityId(id)).await?;
        Ok(())
    }

    async fn destroy(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        self.destroy_replica().await?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PoolOps for Lvs {
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error> {
        let lvol = self
            .create_lvol(
                &args.name,
                args.size,
                Some(&args.uuid),
                args.thin,
                args.entity_id,
            )
            .await?;
        Ok(Box::new(lvol))
    }
    async fn destroy(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        (*self).destroy().await?;
        Ok(())
    }

    async fn export(self: Box<Self>) -> Result<(), crate::pool_backend::Error> {
        (*self).export().await?;
        Ok(())
    }
}

impl IPoolProps for Lvs {
    fn name(&self) -> &str {
        self.name()
    }

    fn uuid(&self) -> String {
        self.uuid()
    }

    fn disks(&self) -> Vec<String> {
        vec![self.base_bdev().bdev_uri_str().unwrap_or_else(|| "".into())]
    }

    fn used(&self) -> u64 {
        self.used()
    }

    fn committed(&self) -> u64 {
        self.committed()
    }

    fn capacity(&self) -> u64 {
        self.capacity()
    }

    fn pool_type(&self) -> PoolBackend {
        PoolBackend::Lvs
    }

    fn cluster_size(&self) -> u32 {
        self.blob_cluster_size() as u32
    }
}

/// A factory instance which implements LVS specific `PoolFactory`.
#[derive(Default)]
pub struct PoolLvsFactory {}
#[async_trait::async_trait(?Send)]
impl PoolFactory for PoolLvsFactory {
    async fn create(
        &self,
        args: PoolArgs,
    ) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
        let lvs = Lvs::create_or_import(args).await?;
        Ok(Box::new(lvs))
    }
    async fn import(
        &self,
        args: PoolArgs,
    ) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
        let lvs = Lvs::import_from_args(args).await?;
        Ok(Box::new(lvs))
    }
    async fn find(
        &self,
        args: &FindPoolArgs,
    ) -> Result<Option<Box<dyn PoolOps>>, crate::pool_backend::Error> {
        let lvs = match args {
            FindPoolArgs::Uuid(uuid) => Lvs::lookup_by_uuid(uuid),
            FindPoolArgs::UuidOrName(id) => {
                Lvs::lookup_by_uuid(id).or_else(|| Lvs::lookup(id))
            }
            FindPoolArgs::NameUuid {
                name,
                uuid,
            } => match uuid {
                Some(uuid) => match Lvs::lookup_by_uuid(uuid) {
                    Some(pool) if pool.name() == name => Some(pool),
                    Some(_) => None,
                    None => None,
                },
                None => Lvs::lookup(name),
            },
        };
        Ok(lvs.map(|lvs| Box::new(lvs) as _))
    }
    async fn list(
        &self,
        args: &ListPoolArgs,
    ) -> Result<Vec<Box<dyn PoolOps>>, crate::pool_backend::Error> {
        if matches!(args.backend, Some(p) if p != PoolBackend::Lvs) {
            return Ok(vec![]);
        }

        let mut pools = vec![];
        if let Some(name) = &args.name {
            if let Some(lvs) = Lvs::lookup(name) {
                pools.push(lvs);
            }
        } else if let Some(uuid) = &args.uuid {
            if let Some(lvs) = Lvs::lookup_by_uuid(uuid) {
                pools.push(lvs);
            }
        } else {
            pools.extend(Lvs::iter());
        }
        Ok(pools
            .into_iter()
            .map(|p| Box::new(p) as _)
            .collect::<Vec<_>>())
    }
    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvs
    }
}

/// A factory instance which implements LVS specific `ReplicaFactory`.
#[derive(Default)]
pub struct ReplLvsFactory {}
#[async_trait::async_trait(?Send)]
impl ReplicaFactory for ReplLvsFactory {
    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        let lvol = crate::core::Bdev::lookup_by_uuid_str(&args.uuid)
            .map(Lvol::try_from)
            .transpose()?;
        Ok(lvol.map(|l| Box::new(l) as _))
    }
    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, Error> {
        let Some(bdev) = crate::core::UntypedBdev::bdev_first() else {
            return Ok(vec![]);
        };
        let retain = |arg: Option<&String>, val: &String| -> bool {
            arg.is_none() || arg == Some(val)
        };

        let lvols = bdev.into_iter().filter_map(Lvol::ok_from);
        let lvols = lvols.filter(|lvol| {
            retain(args.pool_name.as_ref(), &lvol.pool_name())
                && retain(args.pool_uuid.as_ref(), &lvol.pool_uuid())
                && retain(args.name.as_ref(), &lvol.name())
                && retain(args.uuid.as_ref(), &lvol.uuid())
        });

        Ok(lvols.map(|lvol| Box::new(lvol) as _).collect::<Vec<_>>())
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvs
    }
}
