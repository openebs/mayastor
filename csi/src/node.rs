use std::{boxed::Box, fs, io::ErrorKind, path::PathBuf, vec::Vec};

use tonic::{Code, Request, Response, Status};

use glob::glob;

use rpc::mayastor::{ListNexusReply, Nexus};

use crate::{
    csi::{volume_capability::access_mode::Mode, *},
    format::probed_format,
    mount::{match_mount, mount_fs, mount_opts_compare, unmount_fs, Fs},
};

#[derive(Clone, Debug)]
pub struct Node {
    pub node_name: String,
    pub socket: String,
    pub addr: String,
    pub port: u16,
    pub filesystems: Vec<Fs>,
}

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
fn check_access_mode(
    volume_id: &str,
    access_mode: &Option<volume_capability::AccessMode>,
    readonly: bool,
) -> Result<(), String> {
    let rdonly_access_mode = match access_mode {
        Some(m) => match Mode::from_i32(m.mode).unwrap() {
            Mode::SingleNodeWriter => false,
            Mode::SingleNodeReaderOnly => true,
            Mode::MultiNodeReaderOnly => true,
            Mode::MultiNodeSingleWriter => false,
            _ => {
                return Err(format!(
                    "Unsupported publish volume mode {:?} for {}",
                    Mode::from_i32(m.mode),
                    volume_id
                ));
            }
        },
        None => return Err(format!("Missing publish mode for {}", volume_id)),
    };
    if !readonly && rdonly_access_mode {
        Err(format!(
            "Invalid combination of access mode and ro mount flag for {}",
            volume_id,
        ))
    } else {
        Ok(())
    }
}

/// Return a future which lists nexus's from mayastor and returns the one with
/// matching uuid or None.
async fn lookup_nexus(
    socket: &str,
    uuid: &str,
) -> Result<Option<Nexus>, Status> {
    let uuid = uuid.to_string();

    let list: ListNexusReply =
        jsonrpc::call::<(), ListNexusReply>(socket, "list_nexus", None)
            .await
            .unwrap();

    for nexus in list.nexus_list {
        if nexus.uuid == uuid {
            return Ok(Some(nexus));
        }
    }

    Ok(None)
}

impl Node {}
#[tonic::async_trait]
impl node_server::Node for Node {
    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        let node_id = format!(
            "mayastor://{}/{}:{}",
            &self.node_name, &self.addr, self.port,
        );
        let max_volumes_per_node =
            glob("/dev/nbd*").expect("Invalid glob pattern").count() as i64;

        debug!(
            "NodeGetInfo request: ID={}, max volumes={}",
            node_id, max_volumes_per_node
        );

        Ok(Response::new(NodeGetInfoResponse {
            node_id,
            max_volumes_per_node,
            accessible_topology: None,
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        let caps = vec![
            node_service_capability::rpc::Type::GetVolumeStats,
            node_service_capability::rpc::Type::StageUnstageVolume,
        ];

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

        trace!("{:?}", msg);

        let staging_path = &msg.staging_target_path;
        let target_path = &msg.target_path;
        let volume_id = &msg.volume_id;

        // According to the spec, the staging path is optional, but must be set
        // if the plugin advertises stage volume -- which we do so here we go.
        if staging_path == "" || target_path == "" {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("Invalid target or staging path for {}", volume_id),
            ));
        }

        // TODO: Support raw volumes
        let mnt = match msg.volume_capability.as_ref().unwrap().access_type {
            Some(volume_capability::AccessType::Mount(ref m)) => m,
            Some(volume_capability::AccessType::Block(_)) => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "Raw block not ratified yet",
                ));
            }
            None => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    format!("Missing access type for {}", volume_id),
                ));
            }
        };

        // apparently, it does not matter what the source (device) is
        // to me thats odd but thats how the spec says it today
        if match_mount(None, Some(staging_path), true).is_none() {
            return Err(Status::new(
                Code::InvalidArgument,
                format!(
                    "No mount {} for volume {} (hint: volume unstaged?)",
                    staging_path, volume_id
                ),
            ));
        }

        if let Err(reason) = check_access_mode(
            volume_id,
            &msg.volume_capability.as_ref().unwrap().access_mode,
            msg.readonly,
        ) {
            return Err(Status::new(Code::InvalidArgument, reason));
        }

        let filesystem = if mnt.fs_type.is_empty() {
            &self.filesystems[0]
        } else {
            match self.filesystems.iter().find(|ent| ent.name == mnt.fs_type) {
                Some(fs) => fs,
                None => {
                    return Err(Status::new(
                        Code::InvalidArgument,
                        format!("Filesystem {} is not supported", mnt.fs_type),
                    ));
                }
            }
        };
        let mut mnt_flags = mnt.mount_flags.clone();

        if msg.readonly {
            mnt_flags.push("ro".into());
        } else {
            mnt_flags.push("rw".into());
        }

        mnt_flags.extend(filesystem.defaults.clone());

        if let Some(mount) =
            match_mount(Some(staging_path), Some(target_path), true)
        {
            // we are already mounted check flags, if they match return OK
            let equal =
                mount_opts_compare(&mnt_flags, &mount.opts, msg.readonly);

            if equal {
                info!("Already mounted with compatible flags");
                return Ok(Response::new(NodePublishVolumeResponse {}));
            } else {
                // this is just to provide more context around the error
                return Err(Status::new(
                        Code::AlreadyExists,
                        "Failed to publish volume, already exists with incompatible flags".to_string()
                    ));
            }
        }

        // if we are here, it means that we mount it for the first time or -- we
        // are mounting the same staged volume again to a different target.
        if let Err(err) = fs::create_dir_all(PathBuf::from(target_path)) {
            return Err(Status::new(
                Code::Internal,
                format!(
                    "Failed to create mountpoint {} for volume {}: {}",
                    target_path, volume_id, err
                ),
            ));
        }
        if let Err(err) = mount_fs(
            &staging_path,
            &target_path,
            true,
            &filesystem.name,
            &mnt_flags,
        ) {
            Err(Status::new(
                Code::Internal,
                format!("Failed to publish volume {}: {}", volume_id, err),
            ))
        } else {
            info!("Published volume {}", volume_id);
            Ok(Response::new(NodePublishVolumeResponse {}))
        }
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

        trace!("{:?}", msg);

        let target_path = &msg.target_path;
        let volume_id = &msg.volume_id;

        // TODO: Support raw volumes
        match match_mount(None, Some(target_path), true) {
            Some(_) => {
                debug!("Unmount volume {} at {}...", volume_id, target_path);

                if let Err(err) = unmount_fs(target_path, true) {
                    return Err(Status::new(
                        Code::Internal,
                        format!(
                            "Failed to unpublish volume {}: {}",
                            volume_id, err
                        ),
                    ));
                }
                info!("Unpublished volume {} at {}", volume_id, target_path);
            }
            None => error!("Volume {} is not published", volume_id),
        }

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let msg = request.into_inner();
        trace!("{:?}", msg);
        let volume_id = msg.volume_id;

        let nexus = lookup_nexus(&self.socket, &volume_id).await?.unwrap();
        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![VolumeUsage {
                total: nexus.size as i64,
                unit: volume_usage::Unit::Bytes as i32,
                // TODO: set available and used when we
                // know how to
                // find out their values
                available: 0,
                used: 0,
            }],
        }))
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
        let volume_id = &msg.volume_id;
        let staging_path = &msg.staging_target_path;

        trace!("{:?}", msg);

        if staging_path == "" || volume_id == "" {
            return Err(Status::new(
                Code::InvalidArgument,
                "Invalid target path or volume id",
            ));
        }

        if msg.volume_capability.is_none() {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("No volume capabilities provided for {}", volume_id),
            ));
        }

        // TODO: support raw block volumes
        let mnt = match msg.volume_capability.as_ref().unwrap().access_type {
            Some(volume_capability::AccessType::Mount(ref m)) => m.clone(),
            Some(volume_capability::AccessType::Block(_)) => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "Raw block support is not supported",
                ))
            }
            None => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    format!("Missing access type for volume {}", volume_id),
                ))
            }
        };

        if let Err(reason) = check_access_mode(
            &volume_id,
            &msg.volume_capability.as_ref().unwrap().access_mode,
            // relax the check a bit by pretending all stage mounts are ro
            true,
        ) {
            return Err(Status::new(Code::InvalidArgument, reason));
        };

        let filesystem = if mnt.fs_type.is_empty() {
            self.filesystems[0].clone()
        } else {
            match self.filesystems.iter().find(|ent| ent.name == mnt.fs_type) {
                Some(fs) => fs.clone(),
                None => {
                    return Err(Status::new(
                        Code::InvalidArgument,
                        format!("Filesystem {} is not supported", mnt.fs_type),
                    ));
                }
            }
        };

        debug!("Staging volume {} to {}", volume_id, staging_path);

        if let Err(err) = fs::create_dir_all(PathBuf::from(&staging_path)) {
            if err.kind() != ErrorKind::AlreadyExists {
                return Err(Status::new(
                    Code::Internal,
                    format!(
                        "Failed to create mountpoint {} for volume {}: {}",
                        &staging_path, volume_id, err
                    ),
                ));
            }
        }

        let nexus = match lookup_nexus(&self.socket, &volume_id).await? {
            Some(nexus) => nexus,
            None => {
                return Err(Status::new(
                    Code::NotFound,
                    format!("Volume {} not found", volume_id),
                ))
            }
        };

        if &nexus.device_path == "" {
            return Err(Status::new(
                Code::InvalidArgument,
                format!("The volume {} has not been published", volume_id,),
            ));
        }

        if let Some(mount) =
            match_mount(Some(&nexus.device_path), Some(&staging_path), false)
        {
            if mount.source == nexus.device_path && &mount.dest == staging_path
            {
                // the device is already mounted we should
                // return OK
                return Ok(Response::new(NodeStageVolumeResponse {}));
            } else {
                // something else is there already
                return Err(Status::new(
                    Code::AlreadyExists,
                    format!("Mountpoint {} is already used", staging_path,),
                ));
            }
        }

        if let Err(e) =
            probed_format(&nexus.device_path, &filesystem.name).await
        {
            return Err(Status::new(Code::Internal, e));
        }

        match mount_fs(
            &nexus.device_path,
            &staging_path,
            false,
            &filesystem.name,
            &mnt.mount_flags,
        ) {
            Err(r) => Err(Status::new(Code::Internal, r)),
            Ok(_) => Ok(Response::new(NodeStageVolumeResponse {})),
        }
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let msg = request.into_inner();
        let volume_id = msg.volume_id.clone();
        let stage_path = msg.staging_target_path;

        debug!("Unstaging volume {} at {}", volume_id, stage_path);

        let nexus = match lookup_nexus(&self.socket, &volume_id).await? {
            Some(nexus) => nexus,
            None => {
                return Err(Status::new(
                    Code::NotFound,
                    format!("Volume {} not found", volume_id),
                ))
            }
        };

        if nexus.device_path != "" {
            if let Some(mount) =
                match_mount(Some(&nexus.device_path), Some(&stage_path), true)
            {
                if mount.source == nexus.device_path && stage_path == mount.dest
                {
                    // we have an exact match -> unmount
                    if let Err(reason) = unmount_fs(&stage_path, false) {
                        return Err(Status::new(Code::Internal, reason));
                    }
                }
            }
        }
        // if already unstaged or staging does not match target path -
        // must reply OK
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }
}
