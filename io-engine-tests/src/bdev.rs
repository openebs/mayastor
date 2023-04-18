use super::compose::rpc::v1::{
    bdev::{Bdev, ListBdevOptions},
    SharedRpcHandle,
    Status,
};

pub async fn list_bdevs(rpc: &SharedRpcHandle) -> Result<Vec<Bdev>, Status> {
    rpc.lock()
        .await
        .bdev
        .list(ListBdevOptions {
            name: None,
        })
        .await
        .map(|r| r.into_inner().bdevs)
}
