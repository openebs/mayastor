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
