//! Functions for CSI publish and unpublish block mode volumes.

use tonic::{Code, Status};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use crate::{
    csi::*,
    dev::Device,
    mount::{self},
};

pub async fn publish_block_volume(
    msg: &NodePublishVolumeRequest,
) -> Result<(), Status> {
    let target_path = &msg.target_path;
    let volume_id = &msg.volume_id;

    let uri = msg.publish_context.get("uri").ok_or_else(|| {
            failure!(
                Code::InvalidArgument,
                "Failed to stage volume {}: URI attribute missing from publish context",
                volume_id
            )
        })?;

    // Block volumes are not staged, instead
    // bind mount to the device path,
    // this can be done for mutliple target paths.
    let device = Device::parse(&uri).map_err(|error| {
        failure!(
            Code::Internal,
            "Failed to publish volume {}: error parsing URI {}: {}",
            volume_id,
            uri,
            error
        )
    })?;

    if let Some(device_path) = device.find().await.map_err(|error| {
        failure!(
            Code::Internal,
            "Failed to publish volume {}: error locating device for URI {}: {}",
            volume_id,
            uri,
            error
        )
    })? {
        let devt = unsafe { libc::makedev(259, 254) };

        let cstr_dst = std::ffi::CString::new(target_path.as_str()).unwrap();
        let res =
            unsafe { libc::mknod(cstr_dst.as_ptr(), libc::S_IFBLK, devt) };

        if res != 0 {
            return Err(failure!(
                Code::Internal,
                "Failed to publish volume {}: mknod at {} failed",
                volume_id,
                target_path
            ));
        }

        if let Err(error) = mount::blockdevice_mount(
            &device_path,
            target_path.as_str(),
            msg.readonly,
        ) {
            return Err(failure!(
                Code::Internal,
                "Failed to publish volume {}: {}",
                volume_id,
                error
            ));
        }
        Ok(())
    } else {
        Err(failure!(
            Code::Internal,
            "Failed to publish volume {}: unable to retrieve device path",
            volume_id
        ))
    }
}

pub fn unpublish_block_volume(
    msg: &NodeUnpublishVolumeRequest,
) -> Result<(), Status> {
    let target_path = &msg.target_path;
    let volume_id = &msg.volume_id;

    // block volumes are mounted on block special file, which is not
    // a regular file.
    if mount::find_mount(None, Some(target_path)).is_some() {
        match mount::blockdevice_unmount(&target_path) {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::new(
                    Code::Internal,
                    format!(
                        "Failed to unpublish volume {}: {}",
                        volume_id, err
                    ),
                ));
            }
        }
    }

    debug!("Removing block special file {}", target_path);

    if let Err(error) = std::fs::remove_file(target_path) {
        warn!("Failed to remove block file {}: {}", target_path, error);
    }

    info!("Volume {} unpublished from {}", volume_id, target_path);
    Ok(())
}
