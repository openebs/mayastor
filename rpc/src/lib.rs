pub mod mayastor {
    use std::str::FromStr;

    #[derive(Debug)]
    pub enum Error {
        ParseError,
    }

    impl From<()> for Null {
        fn from(_: ()) -> Self {
            Self {}
        }
    }

    impl FromStr for NvmeAnaState {
        type Err = Error;
        fn from_str(state: &str) -> Result<Self, Self::Err> {
            match state {
                "optimized" => Ok(Self::NvmeAnaOptimizedState),
                "non_optimized" => Ok(Self::NvmeAnaNonOptimizedState),
                "inaccessible" => Ok(Self::NvmeAnaInaccessibleState),
                _ => Err(Error::ParseError),
            }
        }
    }

    include!(concat!(env!("OUT_DIR"), "/mayastor.rs"));

    /// module to access v1 version of grpc APIs
    pub mod v1 {

        // dont export the raw pb generated code
        mod pb {
            include!(concat!(env!("OUT_DIR"), "/mayastor.v1.rs"));
        }

        /// v1 version of bdev grpc API
        pub mod bdev {
            pub use super::pb::{
                bdev_rpc_server::{BdevRpc, BdevRpcServer},
                nullable_string::Kind,
                BdevRequest,
                BdevResponse,
                BdevShareRequest,
                BdevShareResponse,
                BdevUnshareRequest,
                ListBdevResponse,
                NullableString,
            };
        }

        /// v1 version of json-rpc grpc API
        pub mod json {
            pub use super::pb::{
                json_rpc_server::{JsonRpc, JsonRpcServer},
                JsonRpcRequest,
                JsonRpcResponse,
            };
        }

        pub mod pool {
            pub use super::pb::{
                pool_rpc_server::{PoolRpc, PoolRpcServer},
                CreatePoolRequest,
                DestroyPoolRequest,
                ExportPoolRequest,
                ImportPoolRequest,
                ListPoolOptions,
                ListPoolsResponse,
                Pool,
                PoolState,
                PoolType,
            };
        }

        pub mod replica {
            pub use super::pb::{
                replica_rpc_server::{ReplicaRpc, ReplicaRpcServer},
                CreateReplicaRequest,
                DestroyReplicaRequest,
                ListReplicaOptions,
                ListReplicasResponse,
                Replica,
                ShareReplicaRequest,
                UnshareReplicaRequest,
            };
        }

        pub mod host {
            pub use super::pb::{
                block_device::{Filesystem, Partition},
                host_rpc_server::{HostRpc, HostRpcServer},
                BlockDevice,
                GetMayastorResourceUsageResponse,
                ListBlockDevicesRequest,
                ListBlockDevicesResponse,
                ListNvmeControllersResponse,
                MayastorFeatures,
                MayastorInfoResponse,
                NvmeController,
                NvmeControllerIoStats,
                NvmeControllerState,
                ResourceUsage,
                StatNvmeControllerRequest,
                StatNvmeControllerResponse,
            };
        }
    }
}
