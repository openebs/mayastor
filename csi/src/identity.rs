//! Implementation of gRPC methods from CSI Identity gRPC service.

use std::{boxed::Box, collections::HashMap};

use tonic::{Code, Request, Response, Status};

use jsonrpc::{self};

use super::csi::*;

const PLUGIN_NAME: &str = "io.openebs.csi-mayastor";
// TODO: can we generate version with commit SHA dynamically?
const PLUGIN_VERSION: &str = "0.1";

#[derive(Clone, Debug)]
pub struct Identity {
    pub socket: String,
}

impl Identity {}
#[tonic::async_trait]
impl identity_server::Identity for Identity {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        debug!("GetPluginInfo request ({}:{})", PLUGIN_NAME, PLUGIN_VERSION);

        Ok(Response::new(GetPluginInfoResponse {
            name: PLUGIN_NAME.to_owned(),
            vendor_version: PLUGIN_VERSION.to_owned(),
            manifest: HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let caps = vec![
            plugin_capability::service::Type::ControllerService,
            plugin_capability::service::Type::VolumeAccessibilityConstraints,
        ];
        debug!("GetPluginCapabilities request: {:?}", caps);

        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: caps
                .into_iter()
                .map(|c| PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(
                        plugin_capability::Service { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        let f = jsonrpc::call::<(), bool>(
            &self.socket,
            "wait_subsystem_init",
            None,
        )
        .await;

        if let Ok(f) = f {
            Ok(Response::new(ProbeResponse { ready: Some(f) }))
        } else {
            Err(Status::new(Code::Unavailable, "MayaStor is is not running"))
        }
    }
}
