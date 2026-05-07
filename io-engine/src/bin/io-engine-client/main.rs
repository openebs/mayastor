use snafu::{Backtrace, Snafu};
use tonic::transport::Channel;

use io_engine_api::v0::{
    bdev_rpc_client::BdevRpcClient, json_rpc_client::JsonRpcClient, mayastor_client::MayastorClient,
};
pub(crate) mod context;
mod v0;
mod v1;

type MayaClient = MayastorClient<Channel>;
type BdevClient = BdevRpcClient<Channel>;
type JsonClient = JsonRpcClient<Channel>;

#[derive(Debug, Snafu)]
#[snafu(context(suffix(false)))]
pub enum ClientError {
    #[snafu(display("gRPC status: {}", source))]
    GrpcStatus {
        source: tonic::Status,
        backtrace: Option<Backtrace>,
    },
    #[snafu(display("Context building error: {}", source))]
    ContextCreate {
        source: context::Error,
        backtrace: Option<Backtrace>,
    },
    #[snafu(display("Missing value for {}", field))]
    MissingValue { field: String },
}

type Result<T, E = ClientError> = std::result::Result<T, E>;

#[tokio::main(worker_threads = 2)]
async fn main() {
    env_logger::init();
    let result = match std::env::var("API_VERSION").unwrap_or_default().as_str() {
        "v0" => v0::main_().await,
        "v1" => v1::main_().await,
        "" => v1::main_().await,
        version => {
            panic!("Invalid Api version set: {}", version)
        }
    };
    if let Err(error) = result {
        eprintln!("{error}");
        use snafu::ErrorCompat;
        if let Some(bt) = ErrorCompat::backtrace(&error) {
            eprintln!("{bt:#?}");
        }
        std::process::exit(1);
    }
}
