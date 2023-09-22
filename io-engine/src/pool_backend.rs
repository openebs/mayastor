use std::convert::TryFrom;

/// PoolArgs is used to translate the input for the grpc
/// Create/Import requests which contains name, uuid & disks.
/// This help us avoid importing grpc structs in the actual lvs mod
#[derive(Clone, Debug)]
pub struct PoolArgs {
    pub name: String,
    pub disks: Vec<String>,
    pub uuid: Option<String>,
    pub cluster_size: Option<u32>,
}

/// PoolBackend is the type of pool underneath Lvs, Lvm, etc
pub enum PoolBackend {
    Lvs,
}

impl TryFrom<i32> for PoolBackend {
    type Error = std::io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Lvs),
            _ => Err(Self::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid pool type {value}"),
            )),
        }
    }
}
