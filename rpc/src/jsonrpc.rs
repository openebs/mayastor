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

impl ShareProtocol {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(ShareProtocol::None),
            1 => Some(ShareProtocol::Nvmf),
            2 => Some(ShareProtocol::Iscsi),
            _ => None,
        }
    }
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
