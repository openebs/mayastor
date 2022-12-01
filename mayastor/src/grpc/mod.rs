use std::{
    error::Error,
    fmt::{Debug, Display},
    time::Duration,
};

use futures::{channel::oneshot::Receiver, Future};
pub use server::MayastorGrpcServer;
use tonic::{Request, Response, Status};

use crate::{
    core::{CoreError, Mthread, Reactor},
    nexus_uri::NexusBdevError,
};

impl From<NexusBdevError> for tonic::Status {
    fn from(e: NexusBdevError) -> Self {
        match e {
            NexusBdevError::UrlParseError {
                ..
            } => Status::invalid_argument(e.to_string()),
            NexusBdevError::UriSchemeUnsupported {
                ..
            } => Status::invalid_argument(e.to_string()),
            NexusBdevError::UriInvalid {
                ..
            } => Status::invalid_argument(e.to_string()),
            e => Status::internal(e.to_string()),
        }
    }
}

impl From<CoreError> for tonic::Status {
    fn from(e: CoreError) -> Self {
        Status::internal(e.to_string())
    }
}
mod bdev_grpc;
mod controller_grpc;
mod json_grpc;
mod mayastor_grpc;
mod nexus_grpc;
mod server;
pub mod v1 {
    pub mod bdev;
    pub mod host;
    pub mod json;
    pub mod nexus;
    pub mod pool;
    pub mod replica;
}

/// Default timeout for gRPC calls, in seconds. Should be enforced in case
/// no timeout is explicitly provided by the client upon gRPC method invocation.
pub const DEFAULT_GRPC_TIMEOUT_SEC: u64 = 15;

#[derive(Debug)]
pub(crate) struct GrpcClientContext {
    /// Method arguments.
    pub args: String,
    /// Method id.
    pub id: String,
    /// Method timeout.
    pub timeout: Duration,
}

impl GrpcClientContext {
    #[track_caller]
    pub fn new<T>(req: &Request<T>, fid: &str) -> Self
    where
        T: Debug,
    {
        Self {
            timeout: get_request_timeout(req),
            args: format!("{:?}", req.get_ref()),
            id: fid.to_string(),
        }
    }
}

#[async_trait::async_trait]
/// trait to lock serialize gRPC request outstanding
pub(crate) trait Serializer<F, T> {
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status>;
}

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;

/// call the given future within the context of the reactor on the first core
/// on the init thread, while the future is waiting to be completed the reactor
/// is continuously polled so that forward progress can be made
pub fn rpc_call<G, I, L, A>(future: G) -> Result<Response<A>, tonic::Status>
where
    G: Future<Output = Result<I, L>> + 'static,
    I: 'static,
    L: Into<Status> + Error + 'static,
    A: 'static + From<I>,
{
    Reactor::block_on(future)
        .unwrap()
        .map(|r| Response::new(A::from(r)))
        .map_err(|e| e.into())
}

pub fn rpc_submit<F, R, E>(
    future: F,
) -> Result<Receiver<Result<R, E>>, tonic::Status>
where
    E: Send + Debug + Display + 'static,
    F: Future<Output = Result<R, E>> + 'static,
    R: Send + Debug + 'static,
{
    Mthread::get_init()
        .spawn_local(future)
        .map_err(|_| Status::resource_exhausted("ENOMEM"))
}

macro_rules! default_ip {
    () => {
        "0.0.0.0"
    };
}
macro_rules! default_port {
    () => {
        10124
    };
}

/// Default server port
pub fn default_port() -> u16 {
    default_port!()
}

/// Default endpoint - ip:port
pub fn default_endpoint_str() -> &'static str {
    concat!(default_ip!(), ":", default_port!())
}

/// Default endpoint - ip:port
pub fn default_endpoint() -> std::net::SocketAddr {
    default_endpoint_str()
        .parse()
        .expect("Expected a valid endpoint")
}

/// If endpoint is missing a port number then add the default one.
pub fn endpoint(endpoint: String) -> std::net::SocketAddr {
    (if endpoint.contains(':') {
        endpoint
    } else {
        format!("{}:{}", endpoint, default_port())
    })
    .parse()
    .expect("Invalid gRPC endpoint")
}

const SECONDS_IN_HOUR: u64 = 60 * 60;
const SECONDS_IN_MINUTE: u64 = 60;

/// Get gRPC timeout from the request and parse it into a Duration instance.
/// In case there is no timeout explicitly provided by the gRPC client or
/// the timeout is malformed, the default timeout is applied.
pub fn get_request_timeout<T>(req: &Request<T>) -> Duration {
    match req.metadata().get("grpc-timeout") {
        Some(v) => {
            match v.to_str() {
                // Valid string representation of the timeout exists, parse it.
                Ok(timeout) => {
                    // At least one digit for the value + 1 character for unit.
                    if timeout.len() >= 2 {
                        let (t_value, t_unit) =
                            timeout.split_at(timeout.len() - 1);
                        if let Ok(tv) = t_value.parse() {
                            return match t_unit {
                                // Hours
                                "H" => {
                                    Duration::from_secs(tv * SECONDS_IN_HOUR)
                                }
                                // Minutes
                                "M" => {
                                    Duration::from_secs(tv * SECONDS_IN_MINUTE)
                                }
                                // Seconds
                                "S" => Duration::from_secs(tv),
                                // Milliseconds
                                "m" => Duration::from_millis(tv),
                                // Microseconds
                                "u" => Duration::from_micros(tv),
                                // Nanoseconds
                                "n" => Duration::from_nanos(tv),
                                _ => {
                                    error!(
                                        timeout,
                                        "Unsupported time unit in gRPC timeout, applying default gRPC timeout"
                                    );
                                    Duration::from_secs(
                                        DEFAULT_GRPC_TIMEOUT_SEC,
                                    )
                                }
                            };
                        }
                    }
                    Duration::from_secs(DEFAULT_GRPC_TIMEOUT_SEC)
                }
                // Timeout value contains non-ASCII characters and can't
                // be parsed, apply the default timeout.
                Err(_) => {
                    error!("Malformed gRPC timeout provided, applying default gRPC timeout");
                    Duration::from_secs(DEFAULT_GRPC_TIMEOUT_SEC)
                }
            }
        }
        // No I/O timeout provided by gRPC client, use the default one.
        None => Duration::from_secs(DEFAULT_GRPC_TIMEOUT_SEC),
    }
}
