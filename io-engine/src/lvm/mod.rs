//! Logical Volume Manager (LVM) is a device mapper framework that provides
//! logical volume management for the Linux kernel.
//!  - PV (Physical Volume) is any block device that is configured to be used by
//!    lvm i.e. formatted withthe lvm2_member filesystem. Commands available
//!       - pvcreate -> to create a physical volume out of any block device
//!       - pvchange -> to make any change like adding tags
//!       - pvs -> to list the physical volumes with their attributes
//!       - pvremove -> to delete a PV which removes the lvm specific filesystem
//!         from the block device
//!  - VG (Volume Group) is a collection of PVs that is used as a store to
//!    provision volumes. Commands available
//!       - vgcreate -> to create a volume group with a specific name and
//!         mentioned physical volumes
//!       - vgchange -> to make any change like adding tags, activate/deactivate
//!         volume group
//!       - vgs -> to list the VGs with their attributes
//!       - vgremove -> removes the volume group
//!  - LV (Logical Volume) is a block device carved out of VG. Commands
//!    available
//!       - lvcreate -> to create a logical volume with a specific name on
//!         mentioned volume group
//!       - lvchange -> to make any change like adding tags, activate/deactivate
//!         logical volume
//!       - lvs -> to list the logical volumes with their attributes
//!       - lvremove -> removes the logical volume

/// Helps run LVM commands and decode their json output and reports.
mod cli;
mod error;
/// Logical Volume management.
mod lv_replica;
mod property;
/// Logical Volume Group management.
mod vg_pool;

/// Errors encountered whilst interacting with the LVM module.
pub(crate) use error::Error;

/// Query arguments used to lookup and filter LVM resources.
pub(crate) use cli::CmnQueryArgs;

/// A pool which is a Volume Group in LVM.
pub(crate) use vg_pool::VolumeGroup;

/// Logical volume and its query arguments.
pub(crate) use lv_replica::{LogicalVolume, QueryArgs};

use crate::{
    bdev::PtplFileOps,
    core::{
        snapshot::SnapshotDescriptor,
        BdevStater,
        BdevStats,
        CloneParams,
        CoreError,
        NvmfShareProps,
        Protocol,
        PtplProps,
        SnapshotParams,
        UntypedBdev,
        UpdateProps,
    },
    lvm::property::Property,
    pool_backend::{
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
        FindSnapshotArgs,
        IReplicaFactory,
        ListCloneArgs,
        ListReplicaArgs,
        ListSnapshotArgs,
        ReplicaBdevStats,
        ReplicaOps,
        SnapshotOps,
    },
};
use futures::channel::oneshot::Receiver;

pub(super) fn is_alphanumeric(name: &str, value: &str) -> Result<(), Error> {
    if value.chars().any(|c| {
        !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '+'))
    }) {
        return Err(Error::NotFound {
            query: format!(
                "{name}('{value}') invalid: must be [a-zA-Z0-9.-_+]"
            ),
        });
    }
    Ok(())
}

pub(crate) fn tokio_submit<F, R>(future: F) -> Receiver<Result<R, Error>>
where
    F: std::future::Future<Output = Result<R, Error>> + Send + 'static,
    R: Send + std::fmt::Debug + 'static,
{
    let (s, r) = futures::channel::oneshot::channel();

    crate::core::runtime::spawn(async move {
        let result = future.await;

        if let Ok(r) = crate::core::Reactor::spawn_at_primary(async move {
            s.send(result).ok();
        }) {
            r.await.ok();
        }
    });
    r
}

#[macro_export]
macro_rules! spdk_run {
    ($fut:expr) => {{
        $fut.await
    }};
}

#[macro_export]
macro_rules! tokio_run {
    ($fut:expr) => {{
        let r = $crate::lvm::tokio_submit($fut);
        r.await.map_err(|_| Error::ReactorSpawnChannel {})?
    }};
}

#[async_trait::async_trait(?Send)]
impl PoolOps for VolumeGroup {
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error> {
        let replica = LogicalVolume::create(
            self.uuid(),
            &args.name,
            args.size,
            &args.uuid,
            args.thin,
            &args.entity_id,
            Protocol::Off,
        )
        .await?;
        Ok(Box::new(replica))
    }

    async fn destroy(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        (*self).destroy().await?;
        Ok(())
    }

    async fn export(
        mut self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        VolumeGroup::export(&mut self).await?;
        Ok(())
    }

    async fn grow(&self) -> Result<(), crate::pool_backend::Error> {
        Err(Error::GrowNotSup {}.into())
    }
}

#[async_trait::async_trait(?Send)]
impl BdevStater for VolumeGroup {
    type Stats = BdevStats;

    async fn stats(&self) -> Result<BdevStats, CoreError> {
        Err(CoreError::NotSupported {
            source: nix::errno::Errno::ENOSYS,
        })
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: nix::errno::Errno::ENOSYS,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl ReplicaOps for LogicalVolume {
    async fn share_nvmf(
        &mut self,
        props: NvmfShareProps,
    ) -> Result<String, crate::pool_backend::Error> {
        self.share_nvmf(Some(props)).await.map_err(Into::into)
    }
    async fn unshare(&mut self) -> Result<(), crate::pool_backend::Error> {
        self.unshare().await.map_err(Into::into)
    }
    async fn update_properties(
        &mut self,
        props: UpdateProps,
    ) -> Result<(), crate::pool_backend::Error> {
        self.update_share_props(props).await?;
        Ok(())
    }

    async fn set_entity_id(
        &mut self,
        id: String,
    ) -> Result<(), crate::pool_backend::Error> {
        self.set_property(Property::LvEntityId(id)).await?;
        Ok(())
    }

    async fn resize(
        &mut self,
        size: u64,
    ) -> Result<(), crate::pool_backend::Error> {
        self.resize(size).await.map_err(Into::into)
    }

    async fn destroy(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        (*self).destroy().await.map_err(Into::into)
    }

    fn shared(&self) -> Option<Protocol> {
        self.share_proto()
    }

    fn create_ptpl(
        &self,
    ) -> Result<Option<PtplProps>, crate::pool_backend::Error> {
        self.ptpl()
            .create()
            .map_err(|source| crate::pool_backend::Error::Lvm {
                source: Error::BdevShare {
                    source: crate::core::CoreError::Ptpl {
                        reason: source.to_string(),
                    },
                },
            })
    }

    fn prepare_snap_config(
        &self,
        _snap_name: &str,
        _entity_id: &str,
        _txn_id: &str,
        _snap_uuid: &str,
    ) -> Option<SnapshotParams> {
        None
    }

    async fn create_snapshot(
        &mut self,
        _params: SnapshotParams,
    ) -> Result<Box<dyn SnapshotOps>, crate::pool_backend::Error> {
        Err(Error::SnapshotNotSup {}.into())
    }

    fn try_as_bdev(&self) -> Result<UntypedBdev, crate::pool_backend::Error> {
        let bdev = Self::bdev(self.bdev_opts()?.uri())?;
        Ok(bdev)
    }
}

#[async_trait::async_trait(?Send)]
impl BdevStater for LogicalVolume {
    type Stats = ReplicaBdevStats;

    async fn stats(&self) -> Result<ReplicaBdevStats, CoreError> {
        Err(CoreError::NotSupported {
            source: nix::errno::Errno::ENOSYS,
        })
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: nix::errno::Errno::ENOSYS,
        })
    }
}

#[async_trait::async_trait(?Send)]
impl SnapshotOps for LogicalVolume {
    async fn destroy_snapshot(
        self: Box<Self>,
    ) -> Result<(), crate::pool_backend::Error> {
        Err(Error::SnapshotNotSup {}.into())
    }

    fn prepare_clone_config(
        &self,
        _clone_name: &str,
        _clone_uuid: &str,
        _source_uuid: &str,
    ) -> Option<CloneParams> {
        None
    }
    async fn create_clone(
        &self,
        _params: CloneParams,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error> {
        Err(Error::SnapshotNotSup {}.into())
    }

    fn descriptor(&self) -> Option<SnapshotDescriptor> {
        None
    }
    fn discarded(&self) -> bool {
        false
    }
}

impl IPoolProps for VolumeGroup {
    fn pool_type(&self) -> PoolBackend {
        PoolBackend::Lvm
    }

    fn name(&self) -> &str {
        self.name()
    }

    fn uuid(&self) -> String {
        self.uuid().to_string()
    }

    fn disks(&self) -> Vec<String> {
        self.disks().clone()
    }

    fn disk_capacity(&self) -> u64 {
        self.capacity()
    }

    fn cluster_size(&self) -> u32 {
        self.cluster_size() as u32
    }

    fn page_size(&self) -> Option<u32> {
        None
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
        None
    }
}

/// A factory instance which implements LVM specific `PoolFactory`.
#[derive(Default)]
pub struct PoolLvmFactory {}
#[async_trait::async_trait(?Send)]
impl IPoolFactory for PoolLvmFactory {
    async fn create(
        &self,
        args: PoolArgs,
    ) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
        let pool = VolumeGroup::create(args).await?;
        Ok(Box::new(pool))
    }

    async fn import(
        &self,
        args: PoolArgs,
    ) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
        let pool = VolumeGroup::import(args).await?;
        Ok(Box::new(pool))
    }

    async fn find(
        &self,
        args: &FindPoolArgs,
    ) -> Result<Option<Box<dyn PoolOps>>, crate::pool_backend::Error> {
        if !crate::core::MayastorFeatures::get().lvm() {
            return Ok(None);
        }
        use CmnQueryArgs;

        let query = match args {
            FindPoolArgs::Uuid(uuid) => CmnQueryArgs::ours().uuid(uuid),
            FindPoolArgs::UuidOrName(uuid) => CmnQueryArgs::ours().uuid(uuid),
            FindPoolArgs::NameUuid {
                name,
                uuid,
            } => CmnQueryArgs::ours().named(name).uuid_opt(uuid),
        };
        match VolumeGroup::lookup(query).await {
            Ok(vg) => Ok(Some(Box::new(vg))),
            Err(Error::NotFound {
                ..
            }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    async fn list(
        &self,
        args: &ListPoolArgs,
    ) -> Result<Vec<Box<dyn PoolOps>>, crate::pool_backend::Error> {
        if !crate::core::MayastorFeatures::get().lvm() {
            return Ok(vec![]);
        }
        if matches!(args.backend, Some(p) if p != PoolBackend::Lvm) {
            return Ok(vec![]);
        }

        let vgs = VolumeGroup::list(
            &CmnQueryArgs::ours()
                .named_opt(&args.name)
                .uuid_opt(&args.uuid),
        )
        .await?;

        Ok(vgs
            .into_iter()
            .map(|p| Box::new(p) as _)
            .collect::<Vec<_>>())
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvm
    }
}

/// A factory instance which implements LVM specific `ReplicaFactory`.
#[derive(Default)]
pub struct ReplLvmFactory {}
#[async_trait::async_trait(?Send)]
impl IReplicaFactory for ReplLvmFactory {
    fn bdev_as_replica(
        &self,
        _bdev: crate::core::UntypedBdev,
    ) -> Option<Box<dyn ReplicaOps>> {
        None
    }
    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        let lookup = LogicalVolume::lookup(
            &QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(&args.uuid)),
        )
        .await;
        match lookup {
            Ok(repl) => Ok(Some(Box::new(repl) as _)),
            Err(Error::NotFound {
                ..
            }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    async fn find_snap(
        &self,
        _args: &FindSnapshotArgs,
    ) -> Result<Option<Box<dyn SnapshotOps>>, crate::pool_backend::Error> {
        Ok(None)
    }

    async fn list(
        &self,
        args: &ListReplicaArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        if !crate::core::MayastorFeatures::get().lvm() {
            return Ok(vec![]);
        }
        let replicas = LogicalVolume::list(
            &QueryArgs::new()
                .with_lv(
                    CmnQueryArgs::ours()
                        .named_opt(&args.name)
                        .uuid_opt(&args.uuid),
                )
                .with_vg(
                    CmnQueryArgs::ours()
                        .named_opt(&args.pool_name)
                        .uuid_opt(&args.pool_uuid),
                ),
        )
        .await?;
        let replicas = replicas.into_iter().map(|r| Box::new(r) as _);
        Ok(replicas.collect::<Vec<_>>())
    }
    async fn list_snaps(
        &self,
        _args: &ListSnapshotArgs,
    ) -> Result<Vec<SnapshotDescriptor>, crate::pool_backend::Error> {
        Ok(vec![])
    }
    async fn list_clones(
        &self,
        _args: &ListCloneArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        Ok(vec![])
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvm
    }
}
