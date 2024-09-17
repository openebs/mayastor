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
    ss.start(false).await?;

    Ok(())
}

/// Un-export given bdev from nvmf target.
/// Unsharing a replica which is not shared is not an error.
pub async fn unshare(uuid: &str) -> Result<(), NvmfError> {
    if let Some(ss) = NvmfSubsystem::nqn_lookup(uuid) {
        ss.stop().await?;
        unsafe {
            ss.shutdown_unsafe();
        }
    }
    Ok(())
}

pub fn get_uri(uuid: &str) -> Option<String> {
    if let Some(ss) = NvmfSubsystem::nqn_lookup(uuid) {
        // If there is rdma capable uri available, return that. Otherwise,
        // for now we only pop the most relevant, but we can share a bdev
        // over multiple nqn's.
        let mut uris = ss.uri_endpoints().expect("no uri endpoints");
        let rdma_uri = uris
            .iter()
            .find(|u| u.starts_with("nvmf+rdma+tcp"))
            .cloned();

        rdma_uri.or(uris.pop())
    } else {
        None
    }
}
