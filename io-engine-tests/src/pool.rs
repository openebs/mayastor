use super::compose::rpc::v1::{
    pool::{CreatePoolRequest, ListPoolOptions, Pool},
    RpcHandle,
    Status,
};

#[derive(Default, Clone, Debug)]
pub struct PoolBuilder {
    pub name: Option<String>,
    pub uuid: Option<String>,
    pub bdev: Option<String>,
}

impl PoolBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    pub fn with_bdev(mut self, bdev: &str) -> Self {
        self.bdev = Some(bdev.to_owned());
        self
    }

    pub async fn create(&self, rpc: &mut RpcHandle) -> Result<Pool, Status> {
        rpc.pool
            .create_pool(CreatePoolRequest {
                name: self.name.as_ref().unwrap().clone(),
                uuid: Some(self.uuid.as_ref().unwrap().clone()),
                pooltype: 0,
                disks: vec![self.bdev.as_ref().unwrap().clone()],
            })
            .await
            .map(|r| r.into_inner())
    }
}

pub async fn list_pools(rpc: &mut RpcHandle) -> Result<Vec<Pool>, Status> {
    rpc.pool
        .list_pools(ListPoolOptions {
            name: None,
            pooltype: None,
        })
        .await
        .map(|r| r.into_inner().pools)
}
