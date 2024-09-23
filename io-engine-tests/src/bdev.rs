use super::compose::rpc::v1::{
    bdev::{Bdev, CreateBdevRequest, ListBdevOptions},
    SharedRpcHandle,
    Status,
};

/// Creates a bdev.
pub async fn create_bdev(
    rpc: SharedRpcHandle,
    uri: &str,
) -> Result<Bdev, Status> {
    rpc.lock()
        .await
        .bdev
        .create(CreateBdevRequest {
            uri: uri.to_string(),
        })
        .await
        .map(|r| r.into_inner().bdev.unwrap())
}

/// Lists bdevs.
pub async fn list_bdevs(rpc: SharedRpcHandle) -> Result<Vec<Bdev>, Status> {
    rpc.lock()
        .await
        .bdev
        .list(ListBdevOptions {
            name: None,
        })
        .await
        .map(|r| r.into_inner().bdevs)
}

/// Finds a bdev by its name.
pub async fn find_bdev_by_name(
    rpc: SharedRpcHandle,
    name: &str,
) -> Option<Bdev> {
    match list_bdevs(rpc).await {
        Err(_) => None,
        Ok(nn) => nn.into_iter().find(|p| p.name == name),
    }
}
