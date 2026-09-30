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
/// Device Mapper setup and info.
pub mod dm_setup;
mod error;
/// Logical Volume management.
mod lv_replica;
/// Pool options parsed from the disks entries.
mod options;
mod property;
/// Logical Volume Group management.
mod vg_pool;

/// Errors encountered whilst interacting with the LVM module.
pub(crate) use error::Error;

/// Query arguments used to lookup and filter LVM resources.
pub(crate) use cli::CmnQueryArgs;

/// A pool which is a Volume Group in LVM.
pub use vg_pool::VolumeGroup;

/// Logical volume and its query arguments.
pub(crate) use lv_replica::{LogicalVolume, QueryArgs};

use crate::{
    bdev::PtplFileOps,
    core::{
        snapshot::{ISnapshotDescriptor, SnapshotDescriptor, SnapshotInfo},
        BdevStater, BdevStats, BlockDeviceIoStats, CloneParams, CoreError, NvmfShareProps,
        Protocol, PtplProps, SnapshotParams, ToErrno, UnshareProps, UntypedBdev, UpdateProps,
    },
    lvm::property::Property,
    pool_backend::{
        FindPoolArgs, IPoolFactory, IPoolProps, ListPoolArgs, PoolArgs, PoolBackend,
        PoolMetadataInfo, PoolOps, ReplicaArgs,
    },
    replica_backend::{
        FindReplicaArgs, FindSnapshotArgs, IReplicaFactory, ListCloneArgs, ListReplicaArgs,
        ListSnapshotArgs, ReplicaBdevStats, ReplicaOps, SnapshotOps,
    },
};
use futures::channel::oneshot::Receiver;

pub(super) fn is_alphanumeric(name: &str, value: &str) -> Result<(), Error> {
    if value
        .chars()
        .any(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '+')))
    {
        return Err(Error::NotFound {
            query: format!("{name}('{value}') invalid: must be [a-zA-Z0-9.-_+]"),
        });
    }
    Ok(())
}

/// LVM tag values accept a limited set of characters. Reject anything else
/// here, so callers get an invalid argument instead of an lvcreate failure.
pub(super) fn is_valid_tag_value(name: &str, value: &str) -> Result<(), Error> {
    if value
        .chars()
        .any(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '+' | '/' | ':')))
    {
        return Err(Error::InvalidTagValue {
            error: format!("{name} '{value}' has characters which LVM tags cannot store"),
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
        let replica = self.create_lvol(args).await?;
        Ok(Box::new(replica))
    }

    async fn destroy(self: Box<Self>) -> Result<(), crate::pool_backend::Error> {
        (*self).destroy().await?;
        Ok(())
    }

    async fn export(mut self: Box<Self>) -> Result<(), crate::pool_backend::Error> {
        VolumeGroup::export(&mut self).await?;
        Ok(())
    }

    async fn grow(&self) -> Result<(), crate::pool_backend::Error> {
        self.resize_pvs().await?;
        Ok(())
    }

    fn rescan(&self) -> Result<(), crate::pool_backend::Error> {
        Err(Error::RescanNotSup {}.into())
    }

    async fn reset_errors(&self) -> Result<(), crate::pool_backend::Error> {
        Err(Error::ResetErrNotSup {}.into())
    }

    async fn reset_stall_transitions(&self) -> Result<(), crate::pool_backend::Error> {
        Err(Error::ResetStallTransitionNotSup {}.into())
    }
}

#[async_trait::async_trait(?Send)]
impl BdevStater for VolumeGroup {
    type Stats = BdevStats;

    /// A volume group has no pool level device, so its io stats are the total
    /// of its replicas' bdev stats. A replica whose stats fail is left out.
    async fn stats(&self) -> Result<BdevStats, CoreError> {
        let mut total = BlockDeviceIoStats {
            tick_rate: self.tick_rate(),
            ..Default::default()
        };
        for replica in self.fetch_lvs().await.map_err(stats_error)? {
            match replica.bdev_stats().await {
                Ok(stats) => add_io_stats(&mut total, &stats),
                Err(error) => {
                    warn!(uuid = replica.uuid(), %error, "Failed to get replica io stats")
                }
            }
        }
        Ok(BdevStats::new(
            self.name().to_string(),
            self.uuid().to_string(),
            total,
        ))
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        for replica in self.fetch_lvs().await.map_err(stats_error)? {
            if let Err(error) = replica.reset_bdev_stats().await {
                warn!(uuid = replica.uuid(), %error, "Failed to reset replica io stats");
            }
        }
        Ok(())
    }
}

fn stats_error(error: Error) -> CoreError {
    CoreError::DeviceStatisticsFailed {
        source: error.to_errno(),
    }
}

/// Add one replica's io stats to a pool total.
/// Counters and latency sums add up. The max and min latencies keep the
/// extremes, where a min of 0 means the replica saw no io of that kind. The
/// tick rate is the same clock for every bdev and is set on the total.
fn add_io_stats(total: &mut BlockDeviceIoStats, stats: &BlockDeviceIoStats) {
    fn min_seen(a: u64, b: u64) -> u64 {
        match (a, b) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.min(b),
        }
    }
    total.num_read_ops = total.num_read_ops.saturating_add(stats.num_read_ops);
    total.num_write_ops = total.num_write_ops.saturating_add(stats.num_write_ops);
    total.bytes_read = total.bytes_read.saturating_add(stats.bytes_read);
    total.bytes_written = total.bytes_written.saturating_add(stats.bytes_written);
    total.num_unmap_ops = total.num_unmap_ops.saturating_add(stats.num_unmap_ops);
    total.bytes_unmapped = total.bytes_unmapped.saturating_add(stats.bytes_unmapped);
    total.read_latency_ticks = total
        .read_latency_ticks
        .saturating_add(stats.read_latency_ticks);
    total.write_latency_ticks = total
        .write_latency_ticks
        .saturating_add(stats.write_latency_ticks);
    total.unmap_latency_ticks = total
        .unmap_latency_ticks
        .saturating_add(stats.unmap_latency_ticks);
    total.max_read_latency_ticks = total
        .max_read_latency_ticks
        .max(stats.max_read_latency_ticks);
    total.max_write_latency_ticks = total
        .max_write_latency_ticks
        .max(stats.max_write_latency_ticks);
    total.max_unmap_latency_ticks = total
        .max_unmap_latency_ticks
        .max(stats.max_unmap_latency_ticks);
    total.min_read_latency_ticks =
        min_seen(total.min_read_latency_ticks, stats.min_read_latency_ticks);
    total.min_write_latency_ticks =
        min_seen(total.min_write_latency_ticks, stats.min_write_latency_ticks);
    total.min_unmap_latency_ticks =
        min_seen(total.min_unmap_latency_ticks, stats.min_unmap_latency_ticks);
}

#[async_trait::async_trait(?Send)]
impl ReplicaOps for LogicalVolume {
    async fn share_nvmf(
        &mut self,
        props: NvmfShareProps,
    ) -> Result<String, crate::pool_backend::Error> {
        self.share_nvmf(Some(props)).await.map_err(Into::into)
    }
    async fn unshare(
        &mut self,
        opts: Option<UnshareProps>,
    ) -> Result<(), crate::pool_backend::Error> {
        self.unshare(opts).await.map_err(Into::into)
    }
    async fn update_properties(
        &mut self,
        props: UpdateProps,
    ) -> Result<(), crate::pool_backend::Error> {
        self.update_share_props(props).await?;
        Ok(())
    }

    async fn set_entity_id(&mut self, id: String) -> Result<(), crate::pool_backend::Error> {
        self.set_property(Property::LvEntityId(id)).await?;
        Ok(())
    }

    async fn resize(&mut self, size: u64) -> Result<(), crate::pool_backend::Error> {
        self.resize(size).await.map_err(Into::into)
    }

    async fn destroy(self: Box<Self>) -> Result<(), crate::pool_backend::Error> {
        (*self).destroy().await.map_err(Into::into)
    }

    fn shared(&self) -> Option<Protocol> {
        self.share_proto()
    }

    fn create_ptpl(&self) -> Result<Option<PtplProps>, crate::pool_backend::Error> {
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

    async fn create_snapshot(
        &mut self,
        params: SnapshotParams,
    ) -> Result<Box<dyn SnapshotOps>, crate::pool_backend::Error> {
        let snapshot = LogicalVolume::snapshot(self, params).await?;
        Ok(Box::new(snapshot))
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
        // Report the replica's own name and uuid. The bdev is named after the
        // lv device path, which callers cannot match to a replica.
        let stats = BdevStats::new(
            self.name().clone().unwrap_or_default(),
            self.uuid().to_string(),
            self.bdev_stats().await?,
        );
        Ok(ReplicaBdevStats::new(
            stats,
            self.entity_id().cloned(),
            Some(self.vg_name().to_string()),
            Some(self.vg_uuid().to_string()),
        ))
    }

    async fn reset_stats(&self) -> Result<(), CoreError> {
        self.reset_bdev_stats().await
    }
}

#[async_trait::async_trait(?Send)]
impl SnapshotOps for LogicalVolume {
    async fn destroy_snapshot(self: Box<Self>) -> Result<(), crate::pool_backend::Error> {
        // dm-thin has no deferred destroy, so a snapshot with clones is refused.
        if self.clone_count().await? > 0 {
            return Err(Error::SnapshotHasClones {
                snapshot: self.uuid().to_string(),
            }
            .into());
        }
        (*self).destroy().await?;
        Ok(())
    }

    async fn create_clone(
        &self,
        params: CloneParams,
    ) -> Result<Box<dyn ReplicaOps>, crate::pool_backend::Error> {
        let clone = LogicalVolume::clone_snap(self, params).await?;
        Ok(Box::new(clone))
    }

    fn descriptor(&self) -> Option<SnapshotDescriptor> {
        if !self.is_snap() {
            return None;
        }
        let params = self.snap_params();
        let valid = params.name().is_some()
            && params.parent_id().is_some()
            && params.entity_id().is_some()
            && params.txn_id().is_some()
            && params.create_time().is_some();
        let info = SnapshotInfo::new(
            params.parent_id().unwrap_or_default(),
            self.allocated_bytes(),
            params,
            self.snap_clones(),
            valid,
        );
        Some(SnapshotDescriptor::new(self.clone(), info))
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

    fn encrypted(&self) -> bool {
        false
    }

    fn max_expandable_size(&self) -> Option<u64> {
        None
    }
}

/// A factory instance which implements LVM specific `PoolFactory`.
#[derive(Default)]
pub struct PoolLvmFactory {}
#[async_trait::async_trait(?Send)]
impl IPoolFactory for PoolLvmFactory {
    async fn create(&self, args: PoolArgs) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
        let pool = VolumeGroup::create(args).await?;
        Ok(Box::new(pool))
    }

    async fn import(&self, args: PoolArgs) -> Result<Box<dyn PoolOps>, crate::pool_backend::Error> {
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
            // The id may be either, so fall back to the name as Lvs does.
            FindPoolArgs::UuidOrName(id) => {
                match VolumeGroup::lookup(CmnQueryArgs::ours().uuid(id)).await {
                    Ok(vg) => return Ok(Some(Box::new(vg))),
                    Err(Error::NotFound { .. }) => CmnQueryArgs::ours().named(id),
                    Err(error) => return Err(error.into()),
                }
            }
            FindPoolArgs::NameUuid { name, uuid } => {
                CmnQueryArgs::ours().named(name).uuid_opt(uuid)
            }
        };
        match VolumeGroup::lookup(query).await {
            Ok(vg) => Ok(Some(Box::new(vg))),
            Err(Error::NotFound { .. }) => Ok(None),
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
    fn bdev_as_replica(&self, bdev: crate::core::UntypedBdev) -> Option<Box<dyn ReplicaOps>> {
        let volume = LogicalVolume::imported(&bdev.uuid_as_string())?;
        if volume.is_snap() {
            return None;
        }
        Some(Box::new(volume))
    }
    async fn find(
        &self,
        args: &FindReplicaArgs,
    ) -> Result<Option<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        let lookup =
            LogicalVolume::lookup(&QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(&args.uuid)))
                .await;
        match lookup {
            Ok(repl) => Ok(Some(Box::new(repl) as _)),
            Err(Error::NotFound { .. } | Error::LvNotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    async fn find_snap(
        &self,
        args: &FindSnapshotArgs,
    ) -> Result<Option<Box<dyn SnapshotOps>>, crate::pool_backend::Error> {
        let fetch =
            LogicalVolume::fetch(&QueryArgs::new().with_lv(CmnQueryArgs::ours().uuid(&args.uuid)))
                .await;
        match fetch.map(|volumes| volumes.into_iter().next()) {
            Ok(Some(mut snapshot)) if snapshot.is_snap() => {
                let clones = snapshot.clone_count().await?;
                snapshot.set_snap_clones(clones);
                Ok(Some(Box::new(snapshot) as _))
            }
            Ok(_) => Ok(None),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
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
        args: &ListSnapshotArgs,
    ) -> Result<Vec<SnapshotDescriptor>, crate::pool_backend::Error> {
        if !crate::core::MayastorFeatures::get().lvm() {
            return Ok(vec![]);
        }
        let volumes = LogicalVolume::fetch(
            &QueryArgs::new()
                .with_lv(CmnQueryArgs::ours())
                .with_vg(CmnQueryArgs::ours()),
        )
        .await?;
        Ok(volumes
            .iter()
            .filter(|lv| lv.is_snap())
            .filter(|lv| args.uuid.as_deref().is_none_or(|uuid| lv.uuid() == uuid))
            .filter(|lv| {
                args.source_uuid
                    .as_deref()
                    .is_none_or(|source| lv.snap_parent_id().as_deref() == Some(source))
            })
            .filter_map(|lv| {
                let mut snapshot = lv.clone();
                snapshot.set_snap_clones(LogicalVolume::count_clones(&volumes, lv.uuid()));
                SnapshotOps::descriptor(&snapshot)
            })
            .collect())
    }
    async fn list_clones(
        &self,
        args: &ListCloneArgs,
    ) -> Result<Vec<Box<dyn ReplicaOps>>, crate::pool_backend::Error> {
        if !crate::core::MayastorFeatures::get().lvm() {
            return Ok(vec![]);
        }
        // Clones are replicas, so they are listed with their bdev like any other.
        let volumes = LogicalVolume::list(
            &QueryArgs::new()
                .with_lv(CmnQueryArgs::ours())
                .with_vg(CmnQueryArgs::ours()),
        )
        .await?;
        Ok(volumes
            .into_iter()
            .filter(|lv| match args.snapshot_uuid.as_deref() {
                Some(uuid) => lv.snapshot_uuid_prop().as_deref() == Some(uuid),
                None => lv.snapshot_uuid_prop().is_some(),
            })
            .map(|lv| Box::new(lv) as _)
            .collect())
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvm
    }
}
