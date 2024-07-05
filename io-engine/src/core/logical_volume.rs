use crate::{core::Protocol, pool_backend::PoolBackend};

/// LogicalVolume Trait Provide all the Generic Interface for a Logical Volume
/// on any backend type.
pub trait LogicalVolume: std::fmt::Debug {
    /// Returns the name of the Logical Volume.
    fn name(&self) -> String;

    /// Returns the UUID of the Logical Volume.
    fn uuid(&self) -> String;

    /// Returns the pool name of the Logical Volume.
    fn pool_name(&self) -> String;

    /// Returns the pool uuid of the Logical Volume.
    fn pool_uuid(&self) -> String;

    /// Returns entity id of the Logical Volume.
    fn entity_id(&self) -> Option<String>;

    /// Returns a boolean indicating if the Logical Volume is thin provisioned.
    fn is_thin(&self) -> bool;

    /// Returns a boolean indicating if the Logical Volume is read-only.
    fn is_read_only(&self) -> bool;

    /// Return the size of the Logical Volume in bytes.
    fn size(&self) -> u64;
    /// Return the committed size of the Logical Volume in bytes.
    fn committed(&self) -> u64;
    /// Return the allocated size of the Logical Volume in bytes.
    fn allocated(&self) -> u64;

    /// Returns Lvol disk space usage.
    fn usage(&self) -> LvolSpaceUsage;
    /// Returns the backend type which owns this Logical Volume.
    fn backend(&self) -> PoolBackend;

    /// Returns a boolean indication if the Logical Volume is a snapshot.
    fn is_snapshot(&self) -> bool;
    /// Returns a boolean indication if the Logical Volume is a clone.
    fn is_clone(&self) -> bool;

    /// Returns its parent snapshot uuid if the Logical Volume is a clone.
    fn snapshot_uuid(&self) -> Option<String>;

    /// Return the share protocol of this Logical Volume.
    fn share_protocol(&self) -> Protocol;
    /// Return the bdev share URI of this Logical Volume.
    fn bdev_share_uri(&self) -> Option<String>;
    /// Return the NVMf allowed hosts of this Logical Volume.
    fn nvmf_allowed_hosts(&self) -> Vec<String>;
}

/// Lvol space usage.
#[derive(Default, Copy, Clone, Debug)]
pub struct LvolSpaceUsage {
    /// Lvol size in bytes.
    pub capacity_bytes: u64,
    /// Amount of actually allocated disk space for this replica in bytes.
    pub allocated_bytes: u64,
    /// Cluster size in bytes.
    pub cluster_size: u64,
    /// Total number of clusters.
    pub num_clusters: u64,
    /// Number of actually allocated clusters.
    pub num_allocated_clusters: u64,
    /// Amount of disk space allocated by snapshots of this volume.
    pub allocated_bytes_snapshots: u64,
    /// Number of clusters allocated by snapshots of this volume.
    pub num_allocated_clusters_snapshots: u64,
    /// Actual Amount of disk space allocated by snapshot which is created from
    /// clone.
    pub allocated_bytes_snapshot_from_clone: Option<u64>,
}
