//! The mayastor node plugin gRPC service
//! This provides access to functionality that needs to be executed on the same
//! node as a Mayastor CSI node plugin, but it is not possible to do so within
//! the CSI framework. This service must be deployed on all nodes the
//! Mayastor CSI node plugin is deployed.
use crate::freezefs;
use freezefs::{freeze_volume, unfreeze_volume, FreezeFsError};
use mayastor_node_plugin::*;
use tonic::{transport::Server, Code, Request, Response, Status};

pub mod mayastor_node_plugin {
    tonic::include_proto!("mayastornodeplugin");
}

#[derive(Debug, Default)]
pub struct MayastorNodePluginSvc {}

impl From<FreezeFsError> for Status {
    fn from(err: FreezeFsError) -> Self {
        match err {
            FreezeFsError::VolumeNotFound {
                ..
            } => Status::new(Code::NotFound, err.to_string()),
            FreezeFsError::FsfreezeFailed {
                ..
            } => Status::new(Code::Internal, err.to_string()),
            FreezeFsError::InvalidVolumeId {
                ..
            } => Status::new(Code::InvalidArgument, err.to_string()),
            FreezeFsError::InternalFailure {
                ..
            } => Status::new(Code::Internal, err.to_string()),
            FreezeFsError::IOError {
                ..
            } => Status::new(Code::Unknown, err.to_string()),
        }
    }
}

#[tonic::async_trait]
impl mayastor_node_plugin_server::MayastorNodePlugin for MayastorNodePluginSvc {
    async fn freeze_fs(
        &self,
        request: Request<FreezeFsRequest>,
    ) -> Result<Response<FreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("freeze_fs({})", volume_id);
        freeze_volume(&volume_id).await?;
        Ok(Response::new(mayastor_node_plugin::FreezeFsReply {}))
    }

    async fn unfreeze_fs(
        &self,
        request: Request<UnfreezeFsRequest>,
    ) -> Result<Response<UnfreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("unfreeze_fs({})", volume_id);
        unfreeze_volume(&volume_id).await?;
        Ok(Response::new(mayastor_node_plugin::UnfreezeFsReply {}))
    }
}

pub struct MayastorNodePluginGrpcServer {}

impl MayastorNodePluginGrpcServer {
    pub async fn run(endpoint: std::net::SocketAddr) -> Result<(), ()> {
        info!(
            "Mayastor node plugin gRPC server configured at address {:?}",
            endpoint
        );
        if let Err(e) = Server::builder()
            .add_service(
                mayastor_node_plugin_server::MayastorNodePluginServer::new(
                    MayastorNodePluginSvc {},
                ),
            )
            .serve(endpoint)
            .await
        {
            error!("gRPC server failed with error: {}", e);
            return Err(());
        }
        Ok(())
    }
}
