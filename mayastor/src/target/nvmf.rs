//! Methods for creating nvmf targets

use crate::{
    core::Bdev,
    subsys::{NvmfError, NvmfSubsystem},
};

/// Export given bdev over nvmf target.
pub async fn share<T>(uuid: &str, bdev: &Bdev<T>) -> Result<(), NvmfError>
where
    T: spdk_rs::BdevOps,
{
    if let Some(ss) = NvmfSubsystem::nqn_lookup(uuid) {
        assert_eq!(bdev.name(), ss.bdev().unwrap().name());
        return Ok(());
    };

    let ss = NvmfSubsystem::try_from(bdev)?;
    ss.start().await?;

    Ok(())
}

/// Un-export given bdev from nvmf target.
/// Unsharing a replica which is not shared is not an error.
pub async fn unshare(uuid: &str) -> Result<(), NvmfError> {
    if let Some(ss) = NvmfSubsystem::nqn_lookup(uuid) {
        ss.stop().await?;
        ss.destroy();
    }
    Ok(())
}

pub fn get_uri(uuid: &str) -> Option<String> {
    if let Some(ss) = NvmfSubsystem::nqn_lookup(uuid) {
        // for now we only pop the first but we can share a bdev
        // over multiple nqn's
        ss.uri_endpoints().unwrap().pop()
    } else {
        None
    }
}
