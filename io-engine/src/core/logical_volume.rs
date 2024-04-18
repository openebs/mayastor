///  LogicalVolume Trait Provide all the Generic Interface for Volume
pub trait LogicalVolume {
    /// Returns the name of the Logical Volume
    fn name(&self) -> String;

    /// Returns the UUID of the Logical Volume
    fn uuid(&self) -> String;

    /// Returns the pool name of the Logical Volume
    fn pool_name(&self) -> String;

    /// Returns the pool uuid of the Logical Volume
    fn pool_uuid(&self) -> String;

    /// Returns entity id of the Logical Volume.
    fn entity_id(&self) -> Option<String>;

    /// Returns a boolean indicating if the Logical Volume is thin provisioned
    fn is_thin(&self) -> bool;

    /// Returns a boolean indicating if the Logical Volume is read-only
    fn is_read_only(&self) -> bool;

    /// Return the size of the Logical Volume in bytes
    fn size(&self) -> u64;

    /// Return the committed size of the Logical Volume in bytes.
    fn committed(&self) -> u64;

    /// Returns Lvol disk space usage
    fn usage(&self) -> LvolSpaceUsage;
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
