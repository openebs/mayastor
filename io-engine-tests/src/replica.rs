use super::{
    compose::rpc::v1::SharedRpcHandle,
    generate_uuid,
    nvmf::{test_devices_identical, NvmfLocation},
    pool::PoolBuilder,
};
use io_engine::{constants::NVME_NQN_PREFIX, subsys::make_subsystem_serial};
use io_engine_api::v1::replica::{
    destroy_replica_request,
    CreateReplicaRequest,
    DestroyReplicaRequest,
    ListReplicaOptions,
    Replica,
    ResizeReplicaRequest,
    ShareReplicaRequest,
};

use tonic::{Code, Status};

#[derive(Clone)]
pub struct ReplicaBuilder {
    pub rpc: SharedRpcHandle,
    pub pool_uuid: Option<String>,
    pub name: Option<String>,
    pub uuid: Option<String>,
    pub bdev: Option<String>,
    pub size: Option<u64>,
    pub thin: bool,
    pub share: i32,
    pub shared_uri: Option<String>,
    pub serial: Option<String>,
}

impl ReplicaBuilder {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            pool_uuid: None,
            name: None,
            uuid: None,
            bdev: None,
            size: None,
            thin: false,
            share: 0,
            shared_uri: None,
            serial: None,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn with_uuid(mut self, uuid: &str) -> Self {
        self.uuid = Some(uuid.to_owned());
        let u = uuid::Uuid::parse_str(uuid).unwrap();
        self.serial = Some(make_subsystem_serial(u.as_bytes()));
        self
    }

    pub fn with_new_uuid(self) -> Self {
        self.with_uuid(&generate_uuid())
    }

    pub fn with_pool(mut self, p: &PoolBuilder) -> Self {
        self.pool_uuid = Some(p.uuid());
        self
    }

    pub fn with_size_kb(mut self, size_kb: u64) -> Self {
        self.size = Some(size_kb * 1024);
        self
    }

    pub fn with_size_b(mut self, size_b: u64) -> Self {
        self.size = Some(size_b);
        self
    }

    pub fn with_size_mb(mut self, size_mb: u64) -> Self {
        self.size = Some(size_mb * 1024 * 1024);
        self
    }

    pub fn with_thin(mut self, thin: bool) -> Self {
        self.thin = thin;
        self
    }

    pub fn with_nvmf(mut self) -> Self {
        self.share = io_engine_api::v1::common::ShareProtocol::Nvmf as i32;
        self
    }

    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }

    pub fn name(&self) -> String {
        self.name
            .as_ref()
            .expect("Replica name must be set")
            .clone()
    }

    pub fn uuid(&self) -> String {
        self.uuid
            .as_ref()
            .expect("Replica UUID must be set")
            .clone()
    }

    pub fn nqn(&self) -> String {
        format!("{}:{}", NVME_NQN_PREFIX, self.name.as_ref().unwrap())
    }

    pub fn bdev(&self) -> String {
        format!("bdev:///{}?uuid={}", self.name(), self.uuid())
    }

    pub fn shared_uri(&self) -> String {
        self.shared_uri
            .as_ref()
            .expect("Replica must be shared")
            .clone()
    }

    pub fn serial(&self) -> String {
        self.serial.as_ref().unwrap().clone()
    }

    pub fn nvmf_location(&self) -> NvmfLocation {
        NvmfLocation {
            addr: self.rpc().endpoint(),
            nqn: self.nqn(),
            serial: self.serial(),
        }
    }

    pub async fn create(&mut self) -> Result<Replica, Status> {
        self.rpc()
            .lock()
            .await
            .replica
            .create_replica(CreateReplicaRequest {
                name: self.name(),
                uuid: self.uuid(),
                pooluuid: self.pool_uuid.as_ref().unwrap().clone(),
                size: self.size.unwrap(),
                thin: self.thin,
                share: self.share,
                ..Default::default()
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn destroy(&mut self) -> Result<(), Status> {
        let pool = self
            .pool_uuid
            .clone()
            .map(destroy_replica_request::Pool::PoolUuid);
        self.rpc()
            .lock()
            .await
            .replica
            .destroy_replica(DestroyReplicaRequest {
                uuid: self.uuid(),
                pool,
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn share(&mut self) -> Result<Replica, Status> {
        let r = self
            .rpc()
            .lock()
            .await
            .replica
            .share_replica(ShareReplicaRequest {
                uuid: self.uuid(),
                share: 1,
                ..Default::default()
            })
            .await
            .map(|r| r.into_inner())?;
        self.shared_uri = Some(r.uri.clone());
        Ok(r)
    }

    pub async fn resize(&mut self, req_size: u64) -> Result<Replica, Status> {
        let r = self
            .rpc()
            .lock()
            .await
            .replica
            .resize_replica(ResizeReplicaRequest {
                uuid: self.uuid(),
                requested_size: req_size,
            })
            .await
            .map(|r| r.into_inner())?;
        self.size = Some(r.size);
        Ok(r)
    }

    pub async fn get_replica(&self) -> Result<Replica, Status> {
        let uuid = self.uuid();
        list_replicas(self.rpc())
            .await?
            .into_iter()
            .find(|p| p.uuid == uuid)
            .ok_or_else(|| {
                Status::new(
                    Code::NotFound,
                    format!("Replica '{uuid}' not found"),
                )
            })
    }
}

pub async fn list_replicas(
    rpc: SharedRpcHandle,
) -> Result<Vec<Replica>, Status> {
    rpc.lock()
        .await
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: None,
            pooltypes: vec![],
        })
        .await
        .map(|r| r.into_inner().replicas)
}

pub async fn find_replica_by_uuid(
    rpc: SharedRpcHandle,
    uuid: &str,
) -> Result<Replica, Status> {
    rpc.lock()
        .await
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: Some(uuid.to_owned()),
            pooluuid: None,
            query: None,
            pooltypes: vec![],
        })
        .await
        .map(|r| r.into_inner().replicas)?
        .into_iter()
        .find(|n| n.uuid == uuid)
        .ok_or_else(|| {
            Status::new(Code::NotFound, format!("Replica '{uuid}' not found"))
        })
}

/// Reads all given replicas and checks if all them contain the same data.
pub async fn validate_replicas(replicas: &[ReplicaBuilder]) {
    let ls: Vec<NvmfLocation> =
        replicas.iter().map(|r| r.nvmf_location()).collect();
    test_devices_identical(&ls).await.unwrap();
}
