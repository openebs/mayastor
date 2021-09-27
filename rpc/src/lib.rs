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

    pub mod v1 {

        // dont export the raw pb generated code
        mod pb {
            /// covert from Null {} message for the unit type
            impl From<()> for Null {
                fn from(_: ()) -> Self {
                    Self {}
                }
            }

            include!(concat!(env!("OUT_DIR"), "/mayastor.v1.rs"));
        }

        pub use pb::{
            bdev_rpc_server::{BdevRpc, BdevRpcServer},
            json_rpc_server::{JsonRpc, JsonRpcServer},
            nullable_string::Kind,
            NullableString,
            BdevRequest,
            BdevResponse,
            Bdevs,
            JsonRpcRequest,
            JsonRpcResponse,
            Null,
            ShareRequest,
            ShareResponse,
            UnshareRequest,
        };
    }
}
