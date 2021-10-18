use std::{
    boxed::Box,
    collections::HashMap,
    fs,
    io::ErrorKind,
    path::Path,
    time::Duration,
    vec::Vec,
};

use tonic::{Code, Request, Response, Status};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use uuid::Uuid;

use crate::{
    block_vol::publish_block_volume,
    csi::{
        volume_capability::{access_mode::Mode, AccessType},
        *,
    },
    dev::Device,
    filesystem_vol::{publish_fs_volume, stage_fs_volume, unstage_fs_volume},
    mount,
};

#[derive(Clone, Debug)]
pub struct Node {
    pub node_name: String,
    pub filesystems: Vec<String>,
}

const ATTACH_TIMEOUT_INTERVAL: Duration = Duration::from_millis(100);
const ATTACH_RETRIES: u32 = 100;

// Determine if given access mode in conjunction with ro mount flag makes
// sense or not. If access mode is not supported or the combination does
// not make sense, return error string.
//
// NOTE: Following is based on our limited understanding of access mode
// meaning. Access mode does not control if the mount is rw/ro (that is
// rather part of the mount flags). Access mode serves as advisory info
// for CO when attaching volumes to pods. It is out of scope of storage
// plugin running on particular node to check that access mode for particular
// publish or stage request makes sense.

/// Check that the access_mode from VolumeCapability is consistent with
/// the readonly status
fn check_access_mode(
    volume_capability: &Option<VolumeCapability>,
    readonly: bool,
) -> Result<(), String> {
    match volume_capability {
        Some(capability) => match &capability.access_mode {
            Some(access) => match Mode::from_i32(access.mode) {
                Some(mode) => match mode {
                    Mode::SingleNodeWriter | Mode::MultiNodeSingleWriter => {
                        Ok(())
                    }
                    Mode::SingleNodeReaderOnly | Mode::MultiNodeReaderOnly => {
                        if readonly {
                            return Ok(());
                        }
                        Err(format!("volume capability: invalid combination of access mode ({:?}) and mount flag (rw)", mode))
                    }
                    Mode::Unknown => Err(String::from(
                        "volume capability: unknown access mode",
                    )),
                    _ => Err(format!(
                        "volume capability: unsupported access mode: {:?}",
                        mode
                    )),
                },
                None => Err(format!(
                    "volume capability: invalid access mode: {}",
                    access.mode
                )),
            },
            None => Err(String::from("volume capability: missing access mode")),
        },
        None => Err(String::from("missing volume capability")),
    }
}

/// Retrieve the AccessType from VolumeCapability
fn get_access_type(
    volume_capability: &Option<VolumeCapability>,
) -> Result<&AccessType, String> {
    match volume_capability {
        Some(capability) => match &capability.access_type {
            Some(access) => Ok(access),
            None => Err(String::from("volume capability: missing access type")),
        },
        None => Err(String::from("missing volume capability")),
    }
}

/// Detach the nexus device from the system, either at volume unstage,
/// or after failed filesystem mount at volume stage.
async fn detach(uuid: &Uuid, errheader: String) -> Result<(), Status> {
    if let Some(device) = Device::lookup(uuid).await.map_err(|error| {
        failure!(
            Code::Internal,
            "{} error locating device: {}",
            &errheader,
            error
        )
    })? {
        let device_path = device.devname();
        debug!("Detaching device {}", device_path);
        if let Err(error) = device.detach().await {
            return Err(failure!(
                Code::Internal,
                "{} failed to detach device {}: {}",
                errheader,
                device_path,
                error
            ));
        }
    }
    Ok(())
}

impl Node {}
#[tonic::async_trait]
impl node_server::Node for Node {
    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        let node_id = format!("mayastor://{}", &self.node_name);
        let mut segments = HashMap::new();
        segments.insert(
            "kubernetes.io/hostname".to_owned(),
            self.node_name.clone(),
        );

        debug!("NodeGetInfo request: ID={}", node_id);

        Ok(Response::new(NodeGetInfoResponse {
            node_id,
            max_volumes_per_node: 0,
            accessible_topology: Some(Topology {
                segments,
            }),
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let caps = vec![node_service_capability::rpc::Type::StageUnstageVolume];

        debug!("NodeGetCapabilities request: {:?}", caps);

        // We don't support stage/unstage and expand volume rpcs
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: caps
                .into_iter()
                .map(|c| NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc {
                            r#type: c as i32,
                        },
                    )),
                })
                .collect(),
        }))
    }

    /// This RPC is called by the CO when a workload that wants to use the
    /// specified volume is placed (scheduled) on a node. The Plugin SHALL
    /// assume that this RPC will be executed on the node where the volume will
    /// be used. If the corresponding Controller Plugin has
    /// PUBLISH_UNPUBLISH_VOLUME controller capability, the CO MUST guarantee
    /// that this RPC is called after ControllerPublishVolume is called for the
    /// given volume on the given node and returns a success. This operation
    /// MUST be idempotent. If the volume corresponding to the volume_id has
    /// already been published at the specified target_path, and is compatible
    /// with the specified volume_capability and readonly flag, the Plugin MUST
    /// reply 0 OK. If this RPC failed, or the CO does not know if it failed or
    /// not, it MAY choose to call NodePublishVolume again, or choose to call
    /// NodeUnpublishVolume. This RPC MAY be called by the CO multiple times on
    /// the same node for the same volume with possibly different target_path
    /// and/or other arguments if the volume has MULTI_NODE capability (i.e.,
    /// access_mode is either MULTI_NODE_READER_ONLY, MULTI_NODE_SINGLE_WRITER
    /// or MULTI_NODE_MULTI_WRITER).
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_publish_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume: missing volume id"
            ));
        }

        if msg.target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: missing target path",
                &msg.volume_id
            ));
        }

        if let Err(error) =
            check_access_mode(&msg.volume_capability, msg.readonly)
        {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: {}",
                &msg.volume_id,
                error
            ));
        }

        // Note that the staging path is NOT optional,
        // as we advertise StageUnstageVolume.
        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        // The CO must ensure that the parent of target path exists,
        // make sure that it exists.
        let target_parent = Path::new(&msg.target_path).parent().unwrap();
        if !target_parent.exists() || !target_parent.is_dir() {
            return Err(Status::new(
                Code::Internal,
                format!(
                    "Failed to find parent dir for mountpoint {}, volume {}",
                    &msg.target_path, &msg.volume_id
                ),
            ));
        }

        match get_access_type(&msg.volume_capability).map_err(|error| {
            failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: {}",
                &msg.volume_id,
                error
            )
        })? {
            AccessType::Mount(mnt) => {
                publish_fs_volume(&msg, mnt, &self.filesystems)?;
            }
            AccessType::Block(_) => {
                publish_block_volume(&msg).await?;
            }
        }
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    /// This RPC is called by the CO when a workload using the specified
    /// volume is removed (unscheduled) from a node.
    /// If the corresponding Controller Plugin has PUBLISH_UNPUBLISH_VOLUME
    /// controller capability, the CO MUST guarantee that this RPC is called
    /// after ControllerPublishVolume is called for the given volume on the
    /// given node and returns a success.
    ///
    /// This operation MUST be idempotent.
    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_unpublish_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unpublish volume: missing volume id"
            ));
        }

        if msg.target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unpublish volume {}: missing target path",
                msg.volume_id
            ));
        }

        if mount::find_mount(None, Some(&msg.target_path)).is_some() {
            debug!("Unmounting {}", msg.target_path);
            if let Err(error) = mount::bind_unmount(&msg.target_path) {
                return Err(failure!(
                    Code::Internal,
                    "Failed to unpublish volume {}: failed to unmount {}: {}",
                    msg.volume_id,
                    msg.target_path,
                    error
                ));
            }
        }

        let mount_point = Path::new(&msg.target_path);
        match fs::metadata(mount_point) {
            Ok(metadata) => {
                if metadata.is_dir() {
                    // Mount Volume
                    debug!("Removing directory {}", msg.target_path);
                    if let Err(error) = fs::remove_dir(mount_point) {
                        return Err(failure!(
                            Code::Internal,
                            "Failed to unpublish volume {}: failed to remove directory {}: {}",
                            msg.volume_id,
                            msg.target_path,
                            error
                        ));
                    }
                } else if metadata.is_file() {
                    // Block Volume
                    debug!("Removing file {}", msg.target_path);
                    if let Err(error) = fs::remove_file(mount_point) {
                        return Err(failure!(
                            Code::Internal,
                            "Failed to unpublish volume {}: failed to remove file {}: {}",
                            msg.volume_id,
                            msg.target_path,
                            error
                        ));
                    }
                } else {
                    return Err(failure!(
                        Code::Internal,
                        "Failed to unpublish volume {}: target path {} has unexpected type: {:?}",
                        msg.volume_id,
                        msg.target_path,
                        metadata.file_type()
                    ));
                }
            }
            Err(error) => {
                if error.kind() != ErrorKind::NotFound {
                    return Err(failure!(
                        Code::Internal,
                        "Failed to unpublish volume {}: failed to stat {}: {}",
                        msg.volume_id,
                        msg.target_path,
                        error
                    ));
                }
            }
        }

        info!(
            "Volume {} unpublished from {}",
            msg.volume_id, msg.target_path
        );

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    /// Get volume stats method is currently not implemented,
    /// although it's simple to do.
    ///
    /// TODO: Just read the data about capacity/used space
    /// inodes/bytes from the system using the mountpoint.
    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let msg = request.into_inner();
        trace!("node_get_volume_stats {:?}", msg);

        /*
        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![VolumeUsage {
                total: 0 as i64,
                unit: volume_usage::Unit::Bytes as i32,
                available: 0,
                used: 0,
            }],
        }))
        */
        error!("Unimplemented {:?}", msg);
        Err(Status::new(Code::Unimplemented, "Method not implemented"))
    }

    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let msg = request.into_inner();
        error!("Unimplemented {:?}", msg);
        Err(Status::new(Code::Unimplemented, "Method not implemented"))
    }

    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let msg = request.into_inner();

        trace!("node_stage_volume {:?}", msg);

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume: missing volume id"
            ));
        }

        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        if let Err(error) = check_access_mode(
            &msg.volume_capability,
            // relax the check a bit by pretending all stage mounts are ro
            true,
        ) {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: {}",
                &msg.volume_id,
                error
            ));
        };

        let access_type = match get_access_type(&msg.volume_capability) {
            Ok(accesstype) => accesstype,
            Err(error) => {
                return Err(failure!(
                    Code::InvalidArgument,
                    "Failed to stage volume {}: {}",
                    &msg.volume_id,
                    error
                ));
            }
        };

        let uri = &msg.publish_context.get("uri").ok_or_else(|| {
            failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: URI attribute missing from publish context",
                &msg.volume_id
            )
        })?;

        let uuid = Uuid::parse_str(&msg.volume_id).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to stage volume {}: not a valid UUID: {}",
                &msg.volume_id,
                error
            )
        })?;

        // Note checking existence of staging_target_path, is delegated to
        // code handling those volume types where it is relevant.

        // All checks complete, now attach, if not attached already.
        debug!("Volume {} has URI {}", &msg.volume_id, uri);

        let mut device = Device::parse(uri).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to stage volume {}: error parsing URI {}: {}",
                &msg.volume_id,
                uri,
                error
            )
        })?;
        device
            .parse_parameters(&msg.publish_context)
            .await
            .map_err(|error| {
                failure!(
            Code::InvalidArgument,
            "Failed to parse storage class parameters for volume {}: {}",
            &msg.volume_id,
            error
        )
            })?;

        let device_path = match device.find().await.map_err(|error| {
            failure!(
            Code::Internal,
            "Failed to stage volume {}: error locating device for URI {}: {}",
            &msg.volume_id,
            uri,
            error
        )
        })? {
            Some(devpath) => devpath,
            None => {
                debug!("Attaching volume {}", &msg.volume_id);
                // device.attach is idempotent, so does not restart the attach
                // process
                if let Err(error) = device.attach().await {
                    return Err(failure!(
                        Code::Internal,
                        "Failed to stage volume {}: attach failed: {}",
                        &msg.volume_id,
                        error
                    ));
                }

                let devpath = Device::wait_for_device(
                    &*device,
                    ATTACH_TIMEOUT_INTERVAL,
                    ATTACH_RETRIES,
                )
                .await
                .map_err(|error| {
                    failure!(
                        Code::Unavailable,
                        "Failed to stage volume {}: {}",
                        &msg.volume_id,
                        error
                    )
                })?;

                device.fixup().await.map_err(|error| {
                    failure!(
                        Code::Internal,
                        "Could not set parameters on staged device {}: {}",
                        &msg.volume_id,
                        error
                    )
                })?;

                devpath
            }
        };

        // Attach successful, now stage mount if required.
        match access_type {
            AccessType::Mount(mnt) => {
                if let Err(fsmount_error) =
                    stage_fs_volume(&msg, device_path, mnt, &self.filesystems)
                        .await
                {
                    detach(
                        &uuid,
                        format!(
                            "Failed to stage volume {}: {};",
                            &msg.volume_id, fsmount_error
                        ),
                    )
                    .await?;
                    return Err(fsmount_error);
                }
            }
            AccessType::Block(_) => {
                // block volumes are not staged
            }
        }
        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let msg = request.into_inner();

        if msg.volume_id.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unstage volume: missing volume id"
            ));
        }

        if msg.staging_target_path.is_empty() {
            return Err(failure!(
                Code::InvalidArgument,
                "Failed to unstage volume {}: missing staging path",
                &msg.volume_id
            ));
        }

        debug!("Unstaging volume {}", &msg.volume_id);

        let uuid = Uuid::parse_str(&msg.volume_id).map_err(|error| {
            failure!(
                Code::Internal,
                "Failed to unstage volume {}: not a valid UUID: {}",
                &msg.volume_id,
                error
            )
        })?;

        // All checks complete, stage unmount if required.

        // unstage_fs_volume checks for mounted filesystems
        // at the staging directory and umounts if any are
        // found.
        unstage_fs_volume(&msg).await?;

        // unmounts (if any) are complete.
        // If the device is attached, detach the device.
        // Device::lookup will return None for nbd devices,
        // this is correct, as the attach for nbd is a no-op.
        detach(
            &uuid,
            format!("Failed to unstage volume {}:", &msg.volume_id),
        )
        .await?;
        info!("Volume {} unstaged", &msg.volume_id);
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }
}
