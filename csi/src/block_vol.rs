//! Functions for CSI publish and unpublish block mode volumes.

use std::path::Path;

use tonic::{Code, Status};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use crate::{
    csi::*,
    dev::Device,
    findmnt,
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
    let device = Device::parse(uri).map_err(|error| {
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
        let path_target = Path::new(target_path);
        if path_target.exists()
            && !path_target.is_file()
            && !path_target.is_dir()
        {
            //target exists and is a special file

            // Idempotency, if we have done this already just return success.
            match findmnt::get_devicepath(target_path) {
                Ok(findmnt_dev) => {
                    if let Some(fm_devpath) = findmnt_dev {
                        if fm_devpath == device_path {
                            debug!(
                                "{}({}) is already mounted onto {}",
                                fm_devpath, device_path, target_path
                            );
                            return Ok(());
                        } else {
                            return Err(Status::new(
                                Code::Internal,
                                format!(
                                    "Failed to publish volume {}: found device {} mounted at {}, not {}",
                                    volume_id,
                                    fm_devpath,
                                    target_path,
                                    device_path)));
                        }
                    }
                }
                Err(err) => {
                    return Err(Status::new(
                        Code::Internal,
                        format!(
                            "Failed to publish volume {}: error whilst checking mount on {} : {}",
                            volume_id,
                            target_path,
                            err)));
                }
            }
        }

        if !path_target.exists() {
            std::fs::File::create(&target_path)?;
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
            "Failed to publish volume {}: unable to retrieve device path for {}",
            volume_id,
            uri
        ))
    }
}
