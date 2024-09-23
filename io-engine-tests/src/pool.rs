pub use super::compose::rpc::v1::pool::Pool;
use super::{
    compose::rpc::v1::{
        pool::{CreatePoolRequest, GrowPoolRequest, ListPoolOptions},
        replica::{ListReplicaOptions, Replica},
        SharedRpcHandle,
        Status,
    },
    generate_uuid,
};
use io_engine::{core::Reactor, lvs, pool_backend::PoolArgs};
use std::ops::Deref;
use tonic::Code;

#[derive(Clone, Default)]
pub struct PoolBuilderOpts {
    name: Option<String>,
    uuid: Option<String>,
    bdev: Option<String>,
}

pub type PoolBuilder = PoolBuilderRpc;
#[derive(Clone)]
pub struct PoolBuilderRpc {
    rpc: SharedRpcHandle,
    builder: PoolBuilderOpts,
}

#[derive(Clone, Default)]
pub struct PoolBuilderLocal {
    builder: PoolBuilderOpts,
}

#[async_trait::async_trait(?Send)]
pub trait PoolOps {
    type Replica;
    async fn get_replicas(&self) -> Result<Vec<Self::Replica>, Status>;
    async fn create_repl(
        &self,
        name: &str,
        size: u64,
        uuid: Option<&str>,
        thin: bool,
        entity_id: Option<String>,
    ) -> Result<Self::Replica, Status>;
    async fn destroy(self) -> Result<(), Status>;
}

#[async_trait::async_trait(?Send)]
impl PoolOps for PoolLocal {
    type Replica = lvs::Lvol;

    async fn get_replicas(&self) -> Result<Vec<Self::Replica>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn create_repl(
        &self,
        name: &str,
        size: u64,
        uuid: Option<&str>,
        thin: bool,
        entity_id: Option<String>,
    ) -> Result<Self::Replica, Status> {
        let Some(lvs) = self.lvs.as_ref() else {
            return Err(Status::internal("deleted"));
        };
        let lvol = lvs.create_lvol(name, size, uuid, thin, entity_id).await?;
        Ok(lvol)
    }

    async fn destroy(mut self) -> Result<(), Status> {
        if let Some(pool) = self.lvs.take() {
            pool.destroy().await?;
        }
        Ok(())
    }
}

pub struct PoolLocal {
    lvs: Option<lvs::Lvs>,
    cleanup: bool,
}

impl Deref for PoolBuilderRpc {
    type Target = PoolBuilderOpts;
    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}
impl Deref for PoolBuilderLocal {
    type Target = PoolBuilderOpts;
    fn deref(&self) -> &Self::Target {
        &self.builder
    }
}

impl PoolBuilderRpc {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            builder: PoolBuilderOpts::default(),
        }
    }
}

impl PoolBuilderOpts {
    pub fn with_name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_uuid(&mut self, uuid: &str) -> &mut Self {
        self.uuid = Some(uuid.to_owned());
        self
    }

    pub fn with_new_uuid(&mut self) -> &mut Self {
        self.with_uuid(&generate_uuid())
    }

    pub fn with_bdev(&mut self, bdev: &str) -> &mut Self {
        self.bdev = Some(bdev.to_owned());
        self
    }

    pub fn with_malloc(&mut self, bdev_name: &str, size_mb: u64) -> &mut Self {
        let bdev = format!("malloc:///{bdev_name}?size_mb={size_mb}");
        self.with_bdev(&bdev)
    }

    pub fn with_malloc_blk_size(
        &mut self,
        bdev_name: &str,
        size_mb: u64,
        blk_size: u64,
    ) -> &mut Self {
        let bdev = format!(
            "malloc:///{bdev_name}?size_mb={size_mb}&blk_size={blk_size}"
        );
        self.with_bdev(&bdev)
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
}

impl PoolBuilderRpc {
    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }
    pub fn with_name(mut self, name: &str) -> Self {
        self.builder.with_name(name);
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.builder.with_uuid(uuid);
        self
    }

    pub fn with_new_uuid(self) -> Self {
        self.with_uuid(&generate_uuid())
    }

    pub fn with_bdev(mut self, bdev: &str) -> Self {
        self.builder.with_bdev(bdev);
        self
    }

    pub fn with_malloc(mut self, bdev_name: &str, size_mb: u64) -> Self {
        self.builder.with_malloc(bdev_name, size_mb);
        self
    }

    pub fn with_malloc_blk_size(
        mut self,
        bdev_name: &str,
        size_mb: u64,
        blk_size: u64,
    ) -> Self {
        self.builder
            .with_malloc_blk_size(bdev_name, size_mb, blk_size);
        self
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
                md_args: None,
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn grow(&mut self) -> Result<(Pool, Pool), Status> {
        self.rpc()
            .lock()
            .await
            .pool
            .grow_pool(GrowPoolRequest {
                name: self.name(),
                uuid: Some(self.uuid()),
            })
            .await
            .map(|r| {
                let t = r.into_inner();
                (t.previous_pool.unwrap(), t.current_pool.unwrap())
            })
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
                pooltypes: vec![],
            })
            .await
            .map(|r| r.into_inner().replicas)
    }
}

impl PoolBuilderLocal {
    pub async fn malloc(name: &str, size_mb: u64) -> Result<PoolLocal, Status> {
        let lvs = PoolBuilderLocal::default()
            .with_builder(|b| {
                b.with_name(name).with_new_uuid().with_malloc(name, size_mb)
            })
            .create()
            .await?;
        Ok(PoolLocal {
            lvs: Some(lvs),
            cleanup: true,
        })
    }

    pub fn with_builder<F>(&mut self, builder: F) -> &mut Self
    where
        F: Fn(&mut PoolBuilderOpts) -> &mut PoolBuilderOpts,
    {
        builder(&mut self.builder);
        self
    }

    pub async fn create(&mut self) -> Result<lvs::Lvs, Status> {
        let lvs = lvs::Lvs::create_or_import(PoolArgs {
            name: self.name(),
            uuid: Some(self.uuid()),
            disks: vec![self.bdev.as_ref().unwrap().clone()],
            cluster_size: None,
            md_args: None,
            backend: Default::default(),
        })
        .await?;
        Ok(lvs)
    }

    pub async fn destroy(&mut self) -> Result<(), Status> {
        let pool = self.get_pool().await?;
        pool.destroy().await?;
        Ok(())
    }

    pub async fn get_pool(&self) -> Result<lvs::Lvs, Status> {
        let uuid = self.uuid();
        lvs::Lvs::lookup_by_uuid(&uuid).ok_or_else(|| {
            Status::new(Code::NotFound, format!("Pool '{uuid}' not found"))
        })
    }
}

impl Drop for PoolLocal {
    fn drop(&mut self) {
        if self.cleanup {
            if let Some(lvs) = self.lvs.take() {
                Reactor::block_on(async move {
                    lvs.destroy().await.ok();
                });
            }
        }
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
pub async fn validate_pools_used_space(pools: &[PoolBuilderRpc]) {
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
