use super::compose::rpc::v1::{
    bdev::{Bdev, ListBdevOptions},
    RpcHandle,
    Status,
};

pub async fn list_bdevs(rpc: &mut RpcHandle) -> Result<Vec<Bdev>, Status> {
    rpc.bdev
        .list(ListBdevOptions {
            name: None,
        })
        .await
        .map(|r| r.into_inner().bdevs)
}
