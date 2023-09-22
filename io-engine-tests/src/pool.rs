pub use super::compose::rpc::v1::pool::Pool;
use super::{
    compose::rpc::v1::{
        pool::{CreatePoolRequest, ListPoolOptions},
        replica::{ListReplicaOptions, Replica},
        SharedRpcHandle,
        Status,
    },
    generate_uuid,
};
use tonic::Code;

#[derive(Clone)]
pub struct PoolBuilder {
    rpc: SharedRpcHandle,
    name: Option<String>,
    uuid: Option<String>,
    bdev: Option<String>,
}

impl PoolBuilder {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            name: None,
            uuid: None,
            bdev: None,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    pub fn with_new_uuid(self) -> Self {
        self.with_uuid(&generate_uuid())
    }

    pub fn with_bdev(mut self, bdev: &str) -> Self {
        self.bdev = Some(bdev.to_owned());
        self
    }

    pub fn with_malloc(self, bdev_name: &str, size_mb: u64) -> Self {
        let bdev = format!("malloc:///{bdev_name}?size_mb={size_mb}");
        self.with_bdev(&bdev)
    }

    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }

    pub fn name(&self) -> String {
        self.name.as_ref().expect("Pool name must be set").clone()
    }

    pub fn uuid(&self) -> String {
        self.uuid.as_ref().expect("Pool UUID must be set").clone()
    }

    pub fn bdev(&self) -> String {
        self.bdev.as_ref().expect("Pool Bdev must be set").clone()
    }

    pub async fn create(&mut self) -> Result<Pool, Status> {
        self.rpc()
            .lock()
            .await
            .pool
            .create_pool(CreatePoolRequest {
                name: self.name(),
                uuid: Some(self.uuid()),
                pooltype: 0,
                disks: vec![self.bdev.as_ref().unwrap().clone()],
                cluster_size: None,
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn get_pool(&self) -> Result<Pool, Status> {
        let uuid = self.uuid();
        list_pools(self.rpc())
            .await?
            .into_iter()
            .find(|p| p.uuid == uuid)
            .ok_or_else(|| {
                Status::new(Code::NotFound, format!("Pool '{uuid}' not found"))
            })
    }

    pub async fn get_replicas(&self) -> Result<Vec<Replica>, Status> {
        self.rpc()
            .lock()
            .await
            .replica
            .list_replicas(ListReplicaOptions {
                name: None,
                poolname: None,
                uuid: None,
                pooluuid: self.uuid.clone(),
                query: None,
            })
            .await
            .map(|r| r.into_inner().replicas)
    }
}

pub async fn list_pools(rpc: SharedRpcHandle) -> Result<Vec<Pool>, Status> {
    rpc.lock()
        .await
        .pool
        .list_pools(ListPoolOptions {
            name: None,
            pooltype: None,
            uuid: None,
        })
        .await
        .map(|r| r.into_inner().pools)
}

/// Tests that all given pools report the same usage statistics.
pub async fn validate_pools_used_space(pools: &[PoolBuilder]) {
    let mut used_space: Option<u64> = None;
    for p in pools {
        let pool = p.get_pool().await.unwrap();
        if let Some(first) = &used_space {
            assert_eq!(
                *first, pool.used,
                "Used space of pool '{}' is different",
                pool.name
            );
        } else {
            used_space = Some(pool.used);
        }
    }
}
