use crate::{
    bdev::PtplFileOps,
    core::{
        snapshot::SnapshotDescriptor,
        CloneParams,
        LogicalVolume,
        Protocol,
        PtplProps,
        Share,
        SnapshotParams,
        UpdateProps,
    },
    pool_backend::{
        Error,
        FindPoolArgs,
        IPoolFactory,
        IPoolProps,
        ListPoolArgs,
        PoolArgs,
        PoolBackend,
        PoolMetadataInfo,
        PoolOps,
        ReplicaArgs,
    },
    replica_backend::{
        FindReplicaArgs,
        IReplicaFactory,
        ListCloneArgs,
        ListReplicaArgs,
        ListSnapshotArgs,
        ReplicaOps,
        SnapshotOps,
    },
};
pub use lvol_snapshot::LvolSnapshotIter;
pub use lvs_bdev::LvsBdev;
pub use lvs_error::{BsError, ImportErrorReason, LvsError};
pub use lvs_iter::{LvsBdevIter, LvsIter};
pub use lvs_lvol::{Lvol, LvsLvol, PropName, PropValue};
pub use lvs_store::Lvs;
use std::{convert::TryFrom, pin::Pin};

mod lvol_iter;
mod lvol_snapshot;
mod lvs_bdev;
mod lvs_error;
mod lvs_iter;
pub mod lvs_lvol;
mod lvs_store;

use crate::{
    core::{BdevStater, BdevStats, CoreError, UntypedBdev},
    replica_backend::{FindSnapshotArgs, ReplicaBdevStats},
};
pub use lvol_snapshot::{LvolResult, LvolSnapshotDescriptor, LvolSnapshotOps};

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

    async fn create_snapshot(
        &mut self,
        params: SnapshotParams,
    ) -> Result<Box<dyn SnapshotOps>, Error> {
        let snapshot = LvolSnapshotOps::create_snapshot(self, params).await?;
        Ok(Box::new(snapshot))
    }

    fn try_as_bdev(&self) -> Result<UntypedBdev, Error> {
        Ok(self.as_bdev())
    }
}

#[async_trait::async_trait(?Send)]
impl BdevStater for Lvol {
    type Stats = ReplicaBdevStats;

    async fn stats(&self) -> Result<ReplicaBdevStats, CoreError> {
        let stats = self.as_bdev().stats().await?;
        Ok(ReplicaBdevStats::new(stats, self.entity_id()))
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        self.as_bdev().reset_stats().await
    }
}

#[async_trait::async_trait(?Send)]
impl SnapshotOps for Lvol {
    async fn destroy_snapshot(self: Box<Self>) -> Result<(), Error> {
        LvolSnapshotOps::destroy_snapshot(*self).await?;
        Ok(())
    }

    async fn create_clone(
        &self,
        params: CloneParams,
    ) -> Result<Box<dyn ReplicaOps>, Error> {
        let clone = LvolSnapshotOps::create_clone(self, params).await?;
        Ok(Box::new(clone))
    }

    fn descriptor(&self) -> Option<SnapshotDescriptor> {
        self.snapshot_descriptor(None)
    }
    fn discarded(&self) -> bool {
        self.is_discarded_snapshot()
    }
}

#[async_trait::async_trait(?Send)]
impl PoolOps for Lvs {
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error> {
        let lvol = self.create_lvol_with_opts(args).await?;
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

    async fn grow(&self) -> Result<(), crate::pool_backend::Error> {
        (*self).grow().await?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl BdevStater for Lvs {
    type Stats = BdevStats;

    async fn stats(&self) -> Result<BdevStats, CoreError> {
        let stats = self.base_bdev().stats_async().await?;
        Ok(BdevStats::new(self.name().to_string(), self.uuid(), stats))
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        self.base_bdev().reset_bdev_io_stats().await
    }
}

impl IPoolProps for Lvs {
    fn pool_type(&self) -> PoolBackend {
        PoolBackend::Lvs
    }

    fn name(&self) -> &str {
        self.name()
    }

    fn uuid(&self) -> String {
        self.uuid()
    }

    fn disks(&self) -> Vec<String> {
        vec![self.base_bdev().bdev_uri_str().unwrap_or_else(|| "".into())]
    }

    fn disk_capacity(&self) -> u64 {
        self.base_bdev().size_in_bytes()
    }

    fn cluster_size(&self) -> u32 {
        self.blob_cluster_size() as u32
    }

    fn page_size(&self) -> Option<u32> {
        Some(self.page_size() as u32)
    }

    fn capacity(&self) -> u64 {
        self.capacity()
    }

    fn used(&self) -> u64 {
        self.used()
    }

    fn committed(&self) -> u64 {
        self.committed()
    }

    fn md_props(&self) -> Option<PoolMetadataInfo> {
        Some(PoolMetadataInfo {
            md_page_size: self.page_size() as u32,
            md_pages: self.md_pages(),
            md_used_pages: self.md_used_pages(),
        })
    }
}

/// A factory instance which implements LVS specific `PoolFactory`.
#[derive(Default)]
pub struct PoolLvsFactory {}

#[async_trait::async_trait(?Send)]
impl IPoolFactory for PoolLvsFactory {
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
impl IReplicaFactory for ReplLvsFactory {
    fn bdev_as_replica(
        &self,
        bdev: crate::core::UntypedBdev,
    ) -> Option<Box<dyn ReplicaOps>> {
        let Some(lvol) = Lvol::ok_from(bdev) else {
            return None;
        };
        if lvol.is_snapshot() {
            return None;
        }
        Some(Box::new(lvol))
    }

    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        let lvol = crate::core::Bdev::lookup_by_uuid_str(&args.uuid)
            .map(Lvol::try_from)
            .transpose()?;
        Ok(lvol.map(|l| Box::new(l) as _))
    }

    async fn find_snap(
        &self,
        args: &FindSnapshotArgs,
    ) -> Result<Option<Box<dyn SnapshotOps>>, crate::pool_backend::Error> {
        let lvol = crate::core::Bdev::lookup_by_uuid_str(&args.uuid)
            .map(Lvol::try_from)
            .transpose()?;
        if let Some(lvol) = &lvol {
            // should this be an error?
            if !lvol.is_snapshot() {
                return Ok(None);
            }
        }
        Ok(lvol.map(|l| Box::new(l) as _))
    }

    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, Error> {
        let retain = |arg: Option<&String>, val: &String| -> bool {
            arg.is_none() || arg == Some(val)
        };

        let lvols = lvol_iter::LvolIter::new().filter(|lvol| {
            retain(args.pool_name.as_ref(), &lvol.pool_name())
                && retain(args.pool_uuid.as_ref(), &lvol.pool_uuid())
                && retain(args.name.as_ref(), &lvol.name())
                && retain(args.uuid.as_ref(), &lvol.uuid())
        });

        Ok(lvols.map(|lvol| Box::new(lvol) as _).collect::<Vec<_>>())
    }
    async fn list_snaps(
        &self,
        args: &ListSnapshotArgs,
    ) -> Result<Vec<SnapshotDescriptor>, crate::pool_backend::Error> {
        // if snapshot_uuid is input, get specific snapshot result
        Ok(if let Some(ref snapshot_uuid) = args.uuid {
            let lvol = match crate::core::UntypedBdev::lookup_by_uuid_str(
                snapshot_uuid,
            ) {
                Some(bdev) => Lvol::try_from(bdev)?,
                None => {
                    return Err(LvsError::Invalid {
                        source: BsError::LvolNotFound {},
                        msg: format!("Snapshot {snapshot_uuid} not found"),
                    }
                    .into())
                }
            };
            lvol.list_snapshot_by_snapshot_uuid()
        } else if let Some(ref replica_uuid) = args.source_uuid {
            let lvol = match crate::core::UntypedBdev::lookup_by_uuid_str(
                replica_uuid,
            ) {
                Some(bdev) => Lvol::try_from(bdev)?,
                None => {
                    return Err(LvsError::Invalid {
                        source: BsError::LvolNotFound {},
                        msg: format!("Replica {replica_uuid} not found",),
                    }
                    .into());
                }
            };
            lvol.list_snapshot_by_source_uuid()
        } else {
            Lvol::list_all_snapshots(None)
        })
    }

    async fn list_clones(
        &self,
        args: &ListCloneArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        let clones = if let Some(snapshot_uuid) = &args.snapshot_uuid {
            let snap_lvol = match crate::core::UntypedBdev::lookup_by_uuid_str(
                snapshot_uuid,
            ) {
                Some(bdev) => Lvol::try_from(bdev),
                None => Err(LvsError::Invalid {
                    source: BsError::LvolNotFound {},
                    msg: format!("Snapshot {snapshot_uuid} not found"),
                }),
            }?;
            snap_lvol.list_clones_by_snapshot_uuid()
        } else {
            Lvol::list_all_clones()
        };
        Ok(clones
            .into_iter()
            .map(|lvol| Box::new(lvol) as _)
            .collect::<Vec<_>>())
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvs
    }
}
