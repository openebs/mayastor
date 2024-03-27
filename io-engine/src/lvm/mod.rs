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

use futures::channel::oneshot::Receiver;

/// The LVM code currently uses an async executor which is not runnable within
/// the spdk reactor, and as such we need a trampoline in order to use spdk
/// functionality within the LVM code.
/// This methods spawns a future on the primary reactor and collects its result
/// with a oneshot channel.
pub(crate) fn spdk_submit<F, R>(
    future: F,
) -> Result<Receiver<Result<R, Error>>, Error>
where
    F: std::future::Future<Output = Result<R, Error>> + 'static,
    R: Send + std::fmt::Debug + 'static,
{
    crate::core::Reactor::spawn_at_primary(future)
        .map_err(|_| Error::ReactorSpawn {})
}

#[macro_export]
macro_rules! spdk_run {
    ($fut:expr) => {{
        let r = $crate::lvm::spdk_submit($fut)?;
        r.await.map_err(|_| Error::ReactorSpawnChannel {})?
    }};
}
