use composer::ComposeTest;

use std::{
    net::{SocketAddr, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use tonic::transport::Channel;
pub use tonic::Status;

pub use mayastor_api::v1::*;

type HandleLock<T> = tokio::sync::Mutex<T>;
type HandleLockGuard<'a, T> = tokio::sync::MutexGuard<'a, T>;

/// Shared (Send, Sync) RPC handle.
#[derive(Clone)]
pub struct SharedRpcHandle {
    handle: Arc<HandleLock<RpcHandle>>,
    name: String,
    endpoint: SocketAddr,
}

impl SharedRpcHandle {
    pub async fn lock(&self) -> HandleLockGuard<RpcHandle> {
        self.handle.lock().await
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

impl PartialEq for SharedRpcHandle {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.endpoint == other.endpoint
    }
}

/// RPC handle.
#[derive(Clone)]
pub struct RpcHandle {
    pub name: String,
    pub endpoint: SocketAddr,
    pub bdev: bdev::BdevRpcClient<Channel>,
    pub json: json::JsonRpcClient<Channel>,
    pub pool: pool::PoolRpcClient<Channel>,
    pub replica: replica::ReplicaRpcClient<Channel>,
    pub host: host::HostRpcClient<Channel>,
    pub nexus: nexus::NexusRpcClient<Channel>,
    pub snapshot: snapshot::SnapshotRpcClient<Channel>,
}

impl RpcHandle {
    /// connect to the containers and construct a handle
    pub(super) async fn connect(
        name: String,
        endpoint: SocketAddr,
    ) -> Result<Self, String> {
        let mut attempts = 40;
        loop {
            if TcpStream::connect_timeout(&endpoint, Duration::from_millis(100))
                .is_ok()
            {
                break;
            } else {
                thread::sleep(Duration::from_millis(101));
            }
            attempts -= 1;
            if attempts == 0 {
                return Err(format!("Failed to connect to {name}/{endpoint}"));
            }
        }

        let bdev = bdev::BdevRpcClient::connect(format!("http://{endpoint}"))
            .await
            .unwrap();

        let json = json::JsonRpcClient::connect(format!("http://{endpoint}"))
            .await
            .unwrap();

        let pool = pool::PoolRpcClient::connect(format!("http://{endpoint}"))
            .await
            .unwrap();

        let replica =
            replica::ReplicaRpcClient::connect(format!("http://{endpoint}"))
                .await
                .unwrap();

        let host = host::HostRpcClient::connect(format!("http://{endpoint}"))
            .await
            .unwrap();

        let nexus =
            nexus::NexusRpcClient::connect(format!("http://{endpoint}"))
                .await
                .unwrap();
        let snapshot =
            snapshot::SnapshotRpcClient::connect(format!("http://{endpoint}"))
                .await
                .unwrap();

        Ok(Self {
            name,
            endpoint,
            bdev,
            json,
            pool,
            replica,
            host,
            nexus,
            snapshot,
        })
    }
}

pub struct GrpcConnect<'a> {
    ct: &'a ComposeTest,
}

impl<'a> GrpcConnect<'a> {
    /// create new gRPC connect object
    pub fn new(comp: &'a ComposeTest) -> Self {
        Self {
            ct: comp,
        }
    }

    /// return grpc handles to the containers
    pub async fn grpc_handles(&self) -> Result<Vec<RpcHandle>, String> {
        let mut handles = Vec::new();
        for v in self.ct.containers() {
            handles.push(
                RpcHandle::connect(
                    v.0.clone(),
                    format!("{}:10124", v.1 .1)
                        .parse::<std::net::SocketAddr>()
                        .unwrap(),
                )
                .await?,
            );
        }

        Ok(handles)
    }

    /// return grpc handle to the container
    pub async fn grpc_handle(&self, name: &str) -> Result<RpcHandle, String> {
        match self.ct.containers().iter().find(|&c| c.0 == name) {
            Some(container) => Ok(RpcHandle::connect(
                container.0.clone(),
                format!("{}:10124", container.1 .1)
                    .parse::<std::net::SocketAddr>()
                    .unwrap(),
            )
            .await?),
            None => Err(format!("Container {name} not found!")),
        }
    }

    pub async fn grpc_handle_shared(
        &self,
        name: &str,
    ) -> Result<SharedRpcHandle, String> {
        self.grpc_handle(name).await.map(|rpc| {
            let name = rpc.name.clone();
            let endpoint = rpc.endpoint;
            SharedRpcHandle {
                handle: Arc::new(HandleLock::new(rpc)),
                name,
                endpoint,
            }
        })
    }
}
