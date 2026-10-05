#![warn(missing_docs)]

use crate::core::{MayastorBugFixes, MayastorEnvironment, MayastorFeatures};
use futures::{select, FutureExt, StreamExt};
use http::Uri;
use io_engine_api::v1::registration::{
    registration_client, ApiVersion as ApiVersionGrpc, DeregisterRequest, RegisterRequest,
};
use once_cell::sync::OnceCell;
use std::{env, str::FromStr, time::Duration};
use version_info::raw_version_string;

/// Mayastor sends registration messages in this interval (kind of heart-beat)
const HB_INTERVAL_SEC: Duration = Duration::from_secs(5);
/// How long we wait to send a registration message before timing out
const HB_TIMEOUT_SEC: Duration = Duration::from_secs(5);
/// The http2 keep alive interval.
const HTTP_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// The http2 keep alive TIMEOUT.
const HTTP_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Copy, Clone, Debug, PartialEq)]
/// ApiVersion to be supported
pub enum ApiVersion {
    /// V0 Version of api
    V0,
    /// V1 version of api
    V1,
}

impl FromStr for ApiVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v0" => Ok(Self::V0),
            "v1" => Ok(Self::V1),
            _ => Err(format!("The version : {s} entered is not supported")),
        }
    }
}

#[derive(Clone)]
struct Configuration {
    /// Id of the node that mayastor is running on
    node: String,
    /// NVMe initiator hostnqn used by mayastor.
    node_nqn: Option<String>,
    /// gRPC endpoint of the server provided by mayastor
    grpc_endpoint: String,
    /// heartbeat interval (how often the register message is sent)
    hb_interval_sec: Duration,
    /// how long we wait to send a registration message before timing out
    hb_timeout_sec: Duration,
    /// ApiVersion to be supported by the instance
    api_versions: Vec<ApiVersion>,
    /// Uuid that is randomly generated on process start.
    /// It's used to identify process restarts.
    instance_uuid: uuid::Uuid,
}

/// Registration component for registering dataplane to controlplane
#[derive(Clone)]
pub struct Registration {
    /// Configuration of the registration
    config: Configuration,
    /// Registration client
    client: registration_client::RegistrationClient<tonic::transport::Channel>,
    /// Receive channel for messages and termination
    rcv_chan: async_channel::Receiver<()>,
    /// Termination channel
    fini_chan: async_channel::Sender<()>,
}

static GRPC_REGISTRATION: OnceCell<Registration> = OnceCell::new();
impl Registration {
    /// Initialise the global registration instance
    pub fn init(
        node: &str,
        node_nqn: &Option<String>,
        grpc_endpoint: &str,
        registration_addr: Uri,
        api_versions: Vec<ApiVersion>,
        grpc_tls: Option<crate::grpc::tls::GrpcServerTls>,
    ) -> Result<(), String> {
        GRPC_REGISTRATION.get_or_try_init(|| {
            Registration::new(
                node,
                node_nqn,
                grpc_endpoint,
                registration_addr,
                api_versions,
                grpc_tls,
            )
        })?;
        Ok(())
    }

    /// Create a new registration instance
    pub fn new(
        node: &str,
        node_nqn: &Option<String>,
        grpc_endpoint: &str,
        registration_addr: Uri,
        api_versions: Vec<ApiVersion>,
        grpc_tls: Option<crate::grpc::tls::GrpcServerTls>,
    ) -> Result<Self, String> {
        let (msg_sender, msg_receiver) = async_channel::unbounded::<()>();
        let config = Configuration {
            api_versions,
            node: node.to_owned(),
            node_nqn: node_nqn.to_owned(),
            grpc_endpoint: grpc_endpoint.to_owned(),
            hb_interval_sec: match env::var("MAYASTOR_HB_INTERVAL_SEC").map(|v| v.parse::<u64>()) {
                Ok(Ok(num)) => Duration::from_secs(num),
                _ => HB_INTERVAL_SEC,
            },
            hb_timeout_sec: match env::var("MAYASTOR_HB_TIMEOUT_SEC").map(|v| v.parse::<u64>()) {
                Ok(Ok(num)) => Duration::from_secs(num),
                _ => HB_TIMEOUT_SEC,
            },
            instance_uuid: uuid::Uuid::new_v4(),
        };
        // When connecting over TLS the handshake is performed by a custom
        // connector, so the endpoint must carry an `http` scheme to stop tonic
        // from applying (and rejecting) its own TLS logic.
        let registration_addr = if grpc_tls.is_some() {
            force_http_scheme(registration_addr)?
        } else {
            registration_addr
        };
        let endpoint = tonic::transport::Endpoint::from(registration_addr)
            .connect_timeout(config.hb_timeout_sec)
            .timeout(config.hb_timeout_sec)
            .http2_keep_alive_interval(HTTP_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(HTTP_KEEP_ALIVE_TIMEOUT);
        let channel = crate::grpc::tls::registration_channel(&endpoint, grpc_tls.as_ref())?;
        Ok(Self {
            config,
            client: registration_client::RegistrationClient::new(channel),
            rcv_chan: msg_receiver,
            fini_chan: msg_sender,
        })
    }

    /// Get the instance uuid.
    pub fn instance_uuid(&self) -> &uuid::Uuid {
        &self.config.instance_uuid
    }

    /// Get the global registration instance
    pub(crate) fn get() -> Option<&'static Registration> {
        GRPC_REGISTRATION.get()
    }

    /// Terminate the channel to deregister.
    pub fn fini(&self) {
        self.fini_chan.close();
    }

    /// Register a new node over rpc
    pub async fn register(&mut self) -> Result<(), tonic::Status> {
        let api_versions = self.config.api_versions.iter();
        let api_versions = api_versions.map(|v| ApiVersionGrpc::from(*v) as i32);
        let register = RegisterRequest {
            id: self.config.node.to_string(),
            grpc_endpoint: self.config.grpc_endpoint.clone(),
            instance_uuid: Some(self.config.instance_uuid.to_string()),
            api_version: api_versions.collect(),
            hostnqn: self.config.node_nqn.clone(),
            features: Some(MayastorFeatures::get().into()),
            bugfixes: Some(MayastorBugFixes::get().into()),
            version: Some(raw_version_string()),
            nvmf_target: Some(MayastorEnvironment::nvmf_target_info().into()),
            transport_caps: Some(MayastorEnvironment::transport_caps().into()),
        };
        self.client
            .register(tonic::Request::new(register))
            .await
            .map(|_| ())
    }

    /// Deregister a node over rpc
    pub async fn deregister(&mut self) -> Result<(), tonic::Status> {
        match self
            .client
            .deregister(tonic::Request::new(DeregisterRequest {
                id: self.config.node.to_string(),
            }))
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Deregistered '{:?}' and grpc server {}",
                    self.config.node,
                    self.config.grpc_endpoint
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// runner responsible for registering and
    /// de-registering the mayastor instance on shutdown
    pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(registration) = GRPC_REGISTRATION.get() {
            registration.clone().run_loop().await;
        }
        Ok(())
    }

    /// Connect to the server and start emitting periodic register
    /// requests.
    pub async fn run_loop(&mut self) {
        let mut show_error: bool = true;
        info!(
            "Registering '{:?}' with grpc server {} ...",
            self.config.node, self.config.grpc_endpoint
        );
        let mut rcv_chan = Box::pin(self.rcv_chan.clone());
        loop {
            match self.register().await {
                Ok(_) => {
                    if !show_error {
                        info!(
                            "Re-registered '{:?}' with grpc server {} ...",
                            self.config.node, self.config.grpc_endpoint
                        );
                    }
                    show_error = true;
                }
                Err(err) => {
                    if show_error {
                        error!("Registration failed: {:?}", err);
                        show_error = false;
                    }
                }
            };
            select! {
                _ = tokio::time::sleep(self.config.hb_interval_sec).fuse() => continue,
                msg = rcv_chan.next().fuse() => {
                    match msg {
                        Some(_) => info!("Messages have not been implemented yet"),
                        _ => {
                            info!("Terminating the registration handler");
                            break;
                        }
                    }
                }
            };
        }
        if let Err(err) = self.deregister().await {
            error!("Deregistration failed: {:?}", err);
        };
    }
}

impl From<ApiVersion> for io_engine_api::v1::registration::ApiVersion {
    fn from(api_version: ApiVersion) -> Self {
        match api_version {
            ApiVersion::V0 => Self::V0,
            ApiVersion::V1 => Self::V1,
        }
    }
}

/// Rewrite the endpoint's scheme to `http`.
///
/// When TLS is enabled the handshake is driven by a custom connector, so the
/// endpoint itself must carry an `http` scheme to prevent tonic from applying
/// (and rejecting) its own TLS logic.
fn force_http_scheme(uri: Uri) -> Result<Uri, String> {
    let mut parts = uri.into_parts();
    parts.scheme = Some(http::uri::Scheme::HTTP);
    // `Uri::from_parts` requires a path-and-query once a scheme/authority is
    // set; default to the root path when the original endpoint had none.
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));
    }
    Uri::from_parts(parts).map_err(|error| format!("invalid http registration endpoint: {error}"))
}
