//! The mayastor node plugin gRPC service
//! This provides access to functionality that needs to be executed on the same
//! node as a Mayastor CSI node plugin, but it is not possible to do so within
//! the CSI framework. This service must be deployed on all nodes the
//! Mayastor CSI node plugin is deployed.
use crate::nodeplugin_svc;
use mayastor_node_plugin::{
    mayastor_node_plugin_server::{
        MayastorNodePlugin,
        MayastorNodePluginServer,
    },
    FindVolumeReply,
    FindVolumeRequest,
    FreezeFsReply,
    FreezeFsRequest,
    UnfreezeFsReply,
    UnfreezeFsRequest,
    VolumeType,
};

use nodeplugin_svc::{
    find_volume,
    freeze_volume,
    unfreeze_volume,
    ServiceError,
    TypeOfMount,
};
use tonic::{transport::Server, Code, Request, Response, Status};

pub mod mayastor_node_plugin {
    tonic::include_proto!("mayastornodeplugin");
}

#[derive(Debug, Default)]
pub struct MayastorNodePluginSvc {}

impl From<ServiceError> for Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::VolumeNotFound {
                ..
            } => Status::new(Code::NotFound, err.to_string()),
            ServiceError::FsfreezeFailed {
                ..
            } => Status::new(Code::Internal, err.to_string()),
            ServiceError::InvalidVolumeId {
                ..
            } => Status::new(Code::InvalidArgument, err.to_string()),
            ServiceError::InternalFailure {
                ..
            } => Status::new(Code::Internal, err.to_string()),
            ServiceError::IOError {
                ..
            } => Status::new(Code::Unknown, err.to_string()),
            ServiceError::InconsistentMountFs {
                ..
            } => Status::new(Code::Unknown, err.to_string()),
            ServiceError::BlockDeviceMount {
                ..
            } => Status::new(Code::FailedPrecondition, err.to_string()),
        }
    }
}

#[tonic::async_trait]
impl MayastorNodePlugin for MayastorNodePluginSvc {
    async fn freeze_fs(
        &self,
        request: Request<FreezeFsRequest>,
    ) -> Result<Response<FreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("freeze_fs({})", volume_id);
        freeze_volume(&volume_id).await?;
        Ok(Response::new(FreezeFsReply {}))
    }

    async fn unfreeze_fs(
        &self,
        request: Request<UnfreezeFsRequest>,
    ) -> Result<Response<UnfreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("unfreeze_fs({})", volume_id);
        unfreeze_volume(&volume_id).await?;
        Ok(Response::new(UnfreezeFsReply {}))
    }

    async fn find_volume(
        &self,
        request: Request<FindVolumeRequest>,
    ) -> Result<Response<FindVolumeReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("find_volume({})", volume_id);
        match find_volume(&volume_id).await? {
            TypeOfMount::FileSystem => Ok(Response::new(FindVolumeReply {
                volume_type: VolumeType::Filesystem as i32,
            })),
            TypeOfMount::RawBlock => Ok(Response::new(FindVolumeReply {
                volume_type: VolumeType::Rawblock as i32,
            })),
        }
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
            .add_service(MayastorNodePluginServer::new(
                MayastorNodePluginSvc {},
            ))
            .serve(endpoint)
            .await
        {
            error!("gRPC server failed with error: {}", e);
            return Err(());
        }
        Ok(())
    }
}
