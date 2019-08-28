//! Implementation of gRPC methods from CSI Identity gRPC service.

use super::csi::*;
use futures::{future, Future};
use std::{boxed::Box, collections::HashMap};
use tower_grpc::{Request, Response, Status};

use jsonrpc::{self, error::Error as JsRpcError};

const PLUGIN_NAME: &str = "io.openebs.csi-mayastor";
// TODO: can we generate version with commit SHA dynamically?
const PLUGIN_VERSION: &str = "0.1";

#[derive(Clone, Debug)]
pub struct Identity {
    pub socket: String,
}

impl server::Identity for Identity {
    type GetPluginInfoFuture =
        future::FutureResult<Response<GetPluginInfoResponse>, Status>;
    type GetPluginCapabilitiesFuture =
        future::FutureResult<Response<GetPluginCapabilitiesResponse>, Status>;
    type ProbeFuture = Box<
        dyn future::Future<Item = Response<ProbeResponse>, Error = Status>
            + Send,
    >;

    fn get_plugin_info(
        &mut self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Self::GetPluginInfoFuture {
        debug!("GetPluginInfo request ({}:{})", PLUGIN_NAME, PLUGIN_VERSION);

        future::ok(Response::new(GetPluginInfoResponse {
            name: PLUGIN_NAME.to_owned(),
            vendor_version: PLUGIN_VERSION.to_owned(),
            manifest: HashMap::new(),
        }))
    }

    fn get_plugin_capabilities(
        &mut self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Self::GetPluginCapabilitiesFuture {
        let caps = vec![
            plugin_capability::service::Type::ControllerService,
            plugin_capability::service::Type::VolumeAccessibilityConstraints,
        ];
        debug!("GetPluginCapabilities request: {:?}", caps);

        future::ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: caps
                .into_iter()
                .map(|c| PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(
                        plugin_capability::Service {
                            r#type: c as i32,
                        },
                    )),
                })
                .collect(),
        }))
    }

    fn probe(&mut self, _request: Request<ProbeRequest>) -> Self::ProbeFuture {
        let f = jsonrpc::call::<(), bool>(
            &self.socket,
            "wait_subsystem_init",
            None,
        )
        .then(move |result| match result {
            Ok(val) => {
                debug!("Probe request: ready={}", val);
                future::ok(Response::new(ProbeResponse {
                    ready: Some(val),
                }))
            }
            Err(err) => match err {
                JsRpcError::ConnectError {
                    ..
                } => {
                    warn!("Probe request: mayastor not running");
                    future::ok(Response::new(ProbeResponse {
                        ready: Some(false),
                    }))
                }
                _ => {
                    error!("Probe request: {}", err);
                    future::err(err.into_status())
                }
            },
        });
        Box::new(f)
    }
}
