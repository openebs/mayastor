/// create or import a pool specified by name
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateOrImportPoolArgs {
    /// name of the pool to import or create if it does not exist
    pub name: String,
    /// the block devices to use
    pub disks: Vec<String>,
    /// the block_size of the underlying block devices
    pub block_size: Option<u32>,
}

/// destroy the pool by name
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DestroyPoolArgs {
    /// name of the pool to destroy
    pub name: String,
}

/// representation of a storage pool
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pool {
    /// name of the pool to be created
    pub name: String,
    /// the block devices to use
    pub disks: Vec<String>,
    /// the state of the pool (TODO: make enum)
    pub state: String,
    /// the capacity in bytes
    pub capacity: u64,
    /// the used capacity in bytes
    pub used: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ShareProtocol {
    None,
    Nvmf,
    Iscsi,
}

/// create replica arguments
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateReplicaArgs {
    /// uuid and name of the replica to create
    pub uuid: String,
    /// name of the storage pool to create replica from
    pub pool: String,
    /// thin provision
    pub thin_provision: bool,
    /// protocol used for exposing the replica
    pub share: ShareProtocol,
    /// size of the replica in bytes
    pub size: u64,
}

/// destroy replica arguments
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DestroyReplicaArgs {
    /// uuid and name of the replica to destroy
    pub uuid: String,
}

/// representation of a replica
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Replica {
    pub uuid: String,
    pub pool: String,
    pub size: u64,
    pub thin_provision: bool,
    pub share: ShareProtocol,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stats {
    pub uuid: String,
    pub pool: String,
    pub num_read_ops: u64,
    pub num_write_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

// the underlying fields will be removed shortly

#[derive(Clone, Debug, Serialize)]
pub struct GetBdevsArgs {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Bdev {
    pub name: String,
    pub aliases: Vec<String>,
    pub product_name: String,
    pub block_size: u32,
    pub num_blocks: u64,
    pub uuid: Option<String>,
    // ... other fields which are not used by us (i.e. uuid, qos, etc.)
    pub driver_specific: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct StartNbdDiskArgs {
    pub bdev_name: String,
    pub nbd_device: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StopNbdDiskArgs {
    pub nbd_device: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NbdDisk {
    pub nbd_device: String,
    pub bdev_name: String,
}
