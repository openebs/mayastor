//! Functions for CSI publish and unpublish block mode volumes.

use serde_json::Value;
use std::process::Command;

use tonic::{Code, Status};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use crate::{
    csi::*,
    dev::Device,
    error::DeviceError,
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
        // Idempotency, if we have done this already just return success.
        match findmnt_device(target_path) {
            Ok(findmnt_dev) => {
                if let Some(fm_devpath) = findmnt_dev {
                    if equals_findmnt_device(&fm_devpath, &device_path) {
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

        std::fs::File::create(&target_path)?;

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

/// Keys of interest we expect to find in the JSON output generated
/// by findmnt.
const TARGET_KEY: &str = "target";
const SOURCE_KEY: &str = "source";

/// This function recurses over the de-serialised JSON returned by findmnt
/// and searches for a target (file or directory) and returns the associated
/// device if found.
/// The assumptions made on the structure are:
///  1. An object has keys named "target" and "source" for a mount point.
///  2. An object may contain nested arrays of objects.
///
/// The search is deliberately generic (and hence slower) in an attempt to
/// be more robust to future changes in findmnt.
fn find_findmnt_target_device(
    json_val: &serde_json::value::Value,
    mountpoint: &str,
) -> Result<Option<String>, DeviceError> {
    if let Some(json_array) = json_val.as_array() {
        for val in json_array {
            if let Some(found) = find_findmnt_target_device(&val, mountpoint)? {
                return Ok(Some(found));
            }
        }
    }
    if let Some(json_map) = json_val.as_object() {
        if let Some(target) = json_map.get(TARGET_KEY) {
            if let Some(source) = json_map.get(SOURCE_KEY) {
                if mountpoint == target {
                    if let Some(source_str) = source.as_str() {
                        return Ok(Some(source_str.to_string()));
                    } else {
                        return Err(DeviceError {
                            message: "findmnt empty source field".to_string(),
                        });
                    }
                }
            } else {
                return Err(DeviceError {
                    message: "findmnt missing source field".to_string(),
                });
            }
        }
        // If the object has arrays, then the assumption is that they are arrays
        // of objects.
        for (_, value) in json_map {
            if value.is_array() {
                if let Some(found) =
                    find_findmnt_target_device(value, mountpoint)?
                {
                    return Ok(Some(found));
                }
            }
        }
    }
    Ok(None)
}

/// findmnt command and arguments.
const FINDMNT: &str = "findmnt";
const FINDMNT_ARGS: [&str; 1] = ["-J"];

/// Use the Linux utility findmnt to find the name of the device mounted at a
/// directory or block special file, if any.
fn findmnt_device(mountpoint: &str) -> Result<Option<String>, DeviceError> {
    let output = Command::new(FINDMNT).args(&FINDMNT_ARGS).output()?;
    if output.status.success() {
        let json_str = String::from_utf8(output.stdout)?;
        let json: Value = serde_json::from_str(&json_str)?;
        if let Some(device) = find_findmnt_target_device(&json, mountpoint)? {
            return Ok(Some(device));
        }
    }
    Ok(None)
}

/// Unfortunately findmnt may return device names in a format different
/// to that returned by udev.
fn equals_findmnt_device(findmnt_device_path: &str, device_path: &str) -> bool {
    if device_path == findmnt_device_path {
        return true;
    } else {
        let v: Vec<&str> = device_path.split('/').collect();
        let l = v.len();
        assert_eq!(v[l - 2], "dev");
        let tmp = format!("dev[/{}]", v[l - 1]);
        if tmp == findmnt_device_path {
            return true;
        }
        let tmp = format!("udev[/{}]", v[l - 1]);
        if tmp == findmnt_device_path {
            return true;
        }
    }
    false
}
