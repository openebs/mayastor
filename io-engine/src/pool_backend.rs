/// PoolArgs is used to translate the input for the grpc
/// Create/Import requests which contains name, uuid & disks.
/// This help us avoid importing grpc structs in the actual lvs mod
#[derive(Clone, Debug, Default)]
pub struct PoolArgs {
    pub name: String,
    pub disks: Vec<String>,
    pub uuid: Option<String>,
    pub cluster_size: Option<u32>,
    pub backend: PoolBackend,
}

/// PoolBackend is the type of pool underneath Lvs, Lvm, etc
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum PoolBackend {
    #[default]
    Lvs,
    Lvm,
}
