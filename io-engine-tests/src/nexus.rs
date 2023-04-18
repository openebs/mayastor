use super::{
    compose::rpc::v1::{
        nexus::{
            AddChildNexusRequest,
            Child,
            ChildAction,
            ChildOperationRequest,
            ChildState,
            ChildStateReason,
            CreateNexusRequest,
            InjectNexusFaultRequest,
            InjectedFault,
            ListInjectedNexusFaultsRequest,
            ListNexusOptions,
            Nexus,
            PublishNexusRequest,
            RebuildHistoryRecord,
            RebuildHistoryRequest,
            RemoveInjectedNexusFaultRequest,
        },
        SharedRpcHandle,
        Status,
    },
    file_io::DataSize,
    fio::Fio,
    generate_uuid,
    nvmf::{test_fio_to_nvmf, test_write_to_nvmf, NvmfLocation},
    replica::ReplicaBuilder,
};
use io_engine::{constants::NVME_NQN_PREFIX, subsys::make_subsystem_serial};
use std::time::{Duration, Instant};
use tonic::Code;

#[derive(Clone)]
pub struct NexusBuilder {
    rpc: SharedRpcHandle,
    name: Option<String>,
    uuid: Option<String>,
    size: Option<u64>,
    min_cntl_id: u32,
    max_cntl_id: u32,
    resv_key: u64,
    preempt_key: u64,
    resv_type: Option<i32>,
    children: Option<Vec<String>>,
    nexus_info_key: Option<String>,
    serial: Option<String>,
}

impl NexusBuilder {
    pub fn new(rpc: SharedRpcHandle) -> Self {
        Self {
            rpc,
            name: None,
            uuid: None,
            size: None,
            min_cntl_id: 1,
            max_cntl_id: 1,
            resv_key: 1,
            preempt_key: 0,
            resv_type: None,
            children: None,
            nexus_info_key: None,
            serial: None,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self.nexus_info_key = Some(name.to_owned());
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

    pub fn with_size_kb(mut self, size_kb: u64) -> Self {
        self.size = Some(size_kb * 1024);
        self
    }

    pub fn with_size_mb(mut self, size_mb: u64) -> Self {
        self.size = Some(size_mb * 1024 * 1024);
        self
    }

    pub fn with_children(mut self, bdevs: Vec<String>) -> Self {
        self.children = Some(bdevs);
        self
    }

    pub fn with_bdev(mut self, bdev: &str) -> Self {
        if self.children.is_none() {
            self.children = Some(vec![]);
        }
        self.children.as_mut().unwrap().push(bdev.to_owned());
        self
    }

    pub fn with_replica(self, r: &ReplicaBuilder) -> Self {
        let bdev = self.replica_uri(r);
        self.with_bdev(&bdev)
    }

    pub fn with_local_replica(self, r: &ReplicaBuilder) -> Self {
        if r.rpc() != self.rpc() {
            panic!("Replica is not local");
        }
        self.with_bdev(&r.bdev())
    }

    fn replica_uri(&self, r: &ReplicaBuilder) -> String {
        if r.rpc() == self.rpc() {
            r.bdev()
        } else {
            r.shared_uri()
        }
    }

    pub fn rpc(&self) -> SharedRpcHandle {
        self.rpc.clone()
    }

    pub fn name(&self) -> String {
        self.name.as_ref().expect("Nexus name must be set").clone()
    }

    pub fn uuid(&self) -> String {
        self.uuid.as_ref().expect("Nexus UUID must be set").clone()
    }

    pub fn nqn(&self) -> String {
        format!("{}:{}", NVME_NQN_PREFIX, self.name.as_ref().unwrap())
    }

    /// Returns NVMe serial for this Nexus.
    /// Serial is generated from Nexus UUID.
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

    pub async fn create(&mut self) -> Result<Nexus, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .create_nexus(CreateNexusRequest {
                name: self.name(),
                uuid: self.uuid(),
                size: self.size.unwrap(),
                min_cntl_id: self.min_cntl_id,
                max_cntl_id: self.max_cntl_id,
                resv_key: self.resv_key,
                preempt_key: self.preempt_key,
                children: self.children.as_ref().unwrap().clone(),
                nexus_info_key: self.nexus_info_key.as_ref().unwrap().clone(),
                resv_type: self.resv_type,
                preempt_policy: 0,
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn publish(&self) -> Result<Nexus, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .publish_nexus(PublishNexusRequest {
                uuid: self.uuid(),
                key: String::new(),
                share: 1,
                ..Default::default()
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn add_child(
        &self,
        bdev: &str,
        norebuild: bool,
    ) -> Result<Nexus, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .add_child_nexus(AddChildNexusRequest {
                uuid: self.uuid(),
                uri: bdev.to_owned(),
                norebuild,
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn add_replica(
        &self,
        r: &ReplicaBuilder,
        norebuild: bool,
    ) -> Result<Nexus, Status> {
        self.add_child(&self.replica_uri(r), norebuild).await
    }

    pub async fn online_child_bdev(&self, bdev: &str) -> Result<Nexus, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .child_operation(ChildOperationRequest {
                nexus_uuid: self.uuid(),
                uri: bdev.to_owned(),
                action: ChildAction::Online as i32,
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn online_child_replica(
        &self,
        r: &ReplicaBuilder,
    ) -> Result<Nexus, Status> {
        self.online_child_bdev(&self.replica_uri(r)).await
    }

    pub async fn offline_child_bdev(
        &self,
        bdev: &str,
    ) -> Result<Nexus, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .child_operation(ChildOperationRequest {
                nexus_uuid: self.uuid(),
                uri: bdev.to_owned(),
                action: ChildAction::Offline as i32,
            })
            .await
            .map(|r| r.into_inner().nexus.unwrap())
    }

    pub async fn offline_child_replica(
        &self,
        r: &ReplicaBuilder,
    ) -> Result<Nexus, Status> {
        self.offline_child_bdev(&self.replica_uri(r)).await
    }

    pub async fn inject_nexus_fault(
        &self,
        inj_uri: &str,
    ) -> Result<(), Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .inject_nexus_fault(InjectNexusFaultRequest {
                uuid: self.uuid(),
                uri: inj_uri.to_owned(),
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn remove_injected_nexus_fault(
        &self,
        inj_uri: &str,
    ) -> Result<(), Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .remove_injected_nexus_fault(RemoveInjectedNexusFaultRequest {
                uuid: self.uuid(),
                uri: inj_uri.to_owned(),
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn list_injected_faults(
        &self,
    ) -> Result<Vec<InjectedFault>, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .list_injected_nexus_faults(ListInjectedNexusFaultsRequest {
                uuid: self.uuid(),
            })
            .await
            .map(|r| r.into_inner().injections)
    }

    pub async fn get_rebuild_history(
        &self,
    ) -> Result<Vec<RebuildHistoryRecord>, Status> {
        self.rpc()
            .lock()
            .await
            .nexus
            .get_rebuild_history(RebuildHistoryRequest {
                uuid: self.uuid(),
            })
            .await
            .map(|r| r.into_inner().records)
    }

    pub async fn get_nexus(&self) -> Result<Nexus, Status> {
        let uuid = self.uuid();
        list_nexuses(self.rpc())
            .await?
            .into_iter()
            .find(|p| p.uuid == uuid)
            .ok_or_else(|| {
                Status::new(Code::NotFound, format!("Nexus '{uuid}' not found"))
            })
    }

    pub async fn get_nexus_replica_child(
        &self,
        r: &ReplicaBuilder,
    ) -> Result<Child, Status> {
        let child_uri = self.replica_uri(r);
        let n = find_nexus_by_uuid(self.rpc(), &self.uuid()).await?;
        n.children
            .into_iter()
            .find(|c| c.uri == child_uri)
            .ok_or_else(|| {
                Status::new(
                    Code::NotFound,
                    format!(
                        "Child '{}' not found on nexus '{}'",
                        child_uri,
                        self.uuid()
                    ),
                )
            })
    }

    pub async fn wait_children_online(
        &self,
        timeout: Duration,
    ) -> Result<(), Status> {
        let start = Instant::now();

        loop {
            let n = find_nexus_by_uuid(self.rpc(), &self.uuid()).await?;
            if n.children
                .iter()
                .all(|c| c.state == ChildState::Online as i32)
            {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if Instant::now() - start > timeout {
                return Err(Status::new(
                    Code::Cancelled,
                    "Waiting for children to get online timed out",
                ));
            }
        }
    }

    pub async fn wait_replica_state(
        &self,
        r: &ReplicaBuilder,
        state: ChildState,
        reason: Option<ChildStateReason>,
        timeout: Duration,
    ) -> Result<(), Status> {
        let start = Instant::now();

        loop {
            let c = self.get_nexus_replica_child(r).await?;
            if c.state == state as i32 {
                if let Some(r) = reason {
                    if c.state_reason == r as i32 {
                        return Ok(());
                    }
                }
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if start.elapsed() > timeout {
                return Err(Status::new(
                    Code::Cancelled,
                    "Waiting for children to get online timed out",
                ));
            }
        }
    }
}

/// TODO
pub async fn list_nexuses(rpc: SharedRpcHandle) -> Result<Vec<Nexus>, Status> {
    rpc.lock()
        .await
        .nexus
        .list_nexus(ListNexusOptions {
            name: None,
            uuid: None,
        })
        .await
        .map(|r| r.into_inner().nexus_list)
}

/// TODO
pub async fn find_nexus_by_uuid(
    rpc: SharedRpcHandle,
    uuid: &str,
) -> Result<Nexus, Status> {
    list_nexuses(rpc)
        .await?
        .into_iter()
        .find(|n| n.uuid == uuid)
        .ok_or_else(|| {
            Status::new(Code::NotFound, format!("Nexus '{uuid}' not found"))
        })
}

/// TODO
pub async fn test_write_to_nexus(
    nex: &NexusBuilder,
    offset: DataSize,
    count: usize,
    buf_size: DataSize,
) -> std::io::Result<()> {
    test_write_to_nvmf(&nex.nvmf_location(), offset, count, buf_size).await
}

/// TODO
pub async fn test_fio_to_nexus(
    nex: &NexusBuilder,
    fio: &Fio,
) -> std::io::Result<()> {
    test_fio_to_nvmf(&nex.nvmf_location(), fio).await
}
