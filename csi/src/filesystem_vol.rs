//! Functions for CSI stage, unstage, publish and unpublish filesystem volumes.

use std::{fs, io::ErrorKind, path::PathBuf};

use tonic::{Code, Status};

macro_rules! failure {
    (Code::$code:ident, $msg:literal) => {{ error!($msg); Status::new(Code::$code, $msg) }};
    (Code::$code:ident, $fmt:literal $(,$args:expr)+) => {{ let message = format!($fmt $(,$args)+); error!("{}", message); Status::new(Code::$code, message) }};
}

use crate::{
    csi::{volume_capability::MountVolume, *},
    format::prepare_device,
    mount::{self, subset, ReadOnly},
};

pub async fn stage_fs_volume(
    msg: &NodeStageVolumeRequest,
    device_path: String,
    mnt: &MountVolume,
    filesystems: &[String],
) -> Result<(), Status> {
    let volume_id = &msg.volume_id;
    let fs_staging_path = &msg.staging_target_path;

    // One final check for fs volumes, ignore for block volumes.
    if let Err(err) = fs::create_dir_all(PathBuf::from(&fs_staging_path)) {
        if err.kind() != ErrorKind::AlreadyExists {
            return Err(Status::new(
                Code::Internal,
                format!(
                    "Failed to create mountpoint {} for volume {}: {}",
                    &fs_staging_path, volume_id, err
                ),
            ));
        }
    }

    debug!("Staging volume {} to {}", volume_id, fs_staging_path);

    let fstype = if mnt.fs_type.is_empty() {
        String::from(&filesystems[0])
    } else {
        match filesystems.iter().find(|&entry| entry == &mnt.fs_type) {
            Some(fstype) => String::from(fstype),
            None => {
                return Err(failure!(
                        Code::InvalidArgument,
                        "Failed to stage volume {}: unsupported filesystem type: {}",
                        volume_id,
                        mnt.fs_type
                    ));
            }
        }
    };

    if mount::find_mount(Some(&device_path), Some(fs_staging_path)).is_some() {
        debug!(
            "Device {} is already mounted onto {}",
            device_path, fs_staging_path
        );
        info!(
            "Volume {} is already staged to {}",
            volume_id, fs_staging_path
        );
        return Ok(());
    }

    // abort if device is mounted somewhere else
    if mount::find_mount(Some(&device_path), None).is_some() {
        return Err(failure!(
            Code::AlreadyExists,
            "Failed to stage volume {}: device {} is already mounted elsewhere",
            volume_id,
            device_path
        ));
    }

    // abort if some another device is mounted on staging_path
    if mount::find_mount(None, Some(fs_staging_path)).is_some() {
        return Err(failure!(
                    Code::AlreadyExists,
                    "Failed to stage volume {}: another device is already mounted onto {}",
                    volume_id,
                    fs_staging_path
                ));
    }

    if let Err(error) = prepare_device(&device_path, &fstype).await {
        return Err(failure!(
            Code::Internal,
            "Failed to stage volume {}: error preparing device {}: {}",
            volume_id,
            device_path,
            error
        ));
    }

    debug!("Mounting device {} onto {}", device_path, fs_staging_path);

    if let Err(error) = mount::filesystem_mount(
        &device_path,
        fs_staging_path,
        &fstype,
        &mnt.mount_flags,
    ) {
        return Err(failure!(
            Code::Internal,
            "Failed to stage volume {}: failed to mount device {} onto {}: {}",
            volume_id,
            device_path,
            fs_staging_path,
            error
        ));
    }

    info!("Volume {} staged to {}", volume_id, fs_staging_path);

    Ok(())
}

/// Unstage a filesystem volume
pub async fn unstage_fs_volume(
    msg: &NodeUnstageVolumeRequest,
) -> Result<(), Status> {
    let volume_id = &msg.volume_id;
    let fs_staging_path = &msg.staging_target_path;

    if let Some(mount) = mount::find_mount(None, Some(fs_staging_path)) {
        debug!(
            "Unstaging filesystem volume {}, unmounting device {:?} from {}",
            volume_id, mount.source, fs_staging_path
        );
        if let Err(error) = mount::filesystem_unmount(fs_staging_path) {
            return Err(failure!(
                    Code::Internal,
                    "Failed to unstage volume {}: failed to unmount device {:?} from {}: {}",
                    volume_id,
                    mount.source,
                    fs_staging_path,
                    error
                ));
        }
    }

    Ok(())
}

/// Publish a filesystem volume
pub fn publish_fs_volume(
    msg: &NodePublishVolumeRequest,
    mnt: &MountVolume,
    filesystems: &[String],
) -> Result<(), Status> {
    let target_path = &msg.target_path;
    let volume_id = &msg.volume_id;
    let fs_staging_path = &msg.staging_target_path;

    debug!(
        "Publishing volume {} from {} to {}",
        volume_id, fs_staging_path, target_path
    );

    let staged =
        mount::find_mount(None, Some(fs_staging_path)).ok_or_else(|| {
            failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: no mount for staging path {}",
                volume_id,
                fs_staging_path
            )
        })?;

    // TODO: Should also check that the staged "device"
    // corresponds to the the volume uuid

    if !mnt.fs_type.is_empty() && mnt.fs_type != staged.fstype {
        return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: filesystem type ({}) does not match staged volume ({})",
                volume_id,
                mnt.fs_type,
                staged.fstype
            ));
    }

    if !filesystems.iter().any(|entry| entry == &staged.fstype) {
        return Err(failure!(
            Code::InvalidArgument,
            "Failed to publish volume {}: unsupported filesystem type: {}",
            volume_id,
            staged.fstype
        ));
    }

    let readonly = staged.options.readonly();

    if readonly && !msg.readonly {
        return Err(failure!(
                Code::InvalidArgument,
                "Failed to publish volume {}: volume is staged as \"ro\" but publish requires \"rw\"",
                volume_id
            ));
    }

    if let Some(mount) = mount::find_mount(None, Some(target_path)) {
        if mount.source != staged.source {
            return Err(failure!(
                Code::AlreadyExists,
                "Failed to publish volume {}: directory {} is already in use",
                volume_id,
                target_path
            ));
        }

        if !subset(&mnt.mount_flags, &mount.options)
            || msg.readonly != mount.options.readonly()
        {
            return Err(failure!(
                    Code::AlreadyExists,
                    "Failed to publish volume {}: directory {} is already mounted but with incompatible flags",
                    volume_id,
                    target_path
                ));
        }

        info!(
            "Volume {} is already published to {}",
            volume_id, target_path
        );

        return Ok(());
    }

    debug!("Creating directory {}", target_path);

    if let Err(error) = fs::create_dir_all(PathBuf::from(target_path)) {
        if error.kind() != ErrorKind::AlreadyExists {
            return Err(failure!(
                    Code::Internal,
                    "Failed to publish volume {}: failed to create directory {}: {}",
                    volume_id,
                    target_path,
                    error
                ));
        }
    }

    debug!("Mounting {} to {}", fs_staging_path, target_path);

    if let Err(error) = mount::bind_mount(fs_staging_path, target_path, false) {
        return Err(failure!(
            Code::Internal,
            "Failed to publish volume {}: failed to mount {} to {}: {}",
            volume_id,
            fs_staging_path,
            target_path,
            error
        ));
    }

    if msg.readonly && !readonly {
        let mut options = mnt.mount_flags.clone();
        options.push(String::from("ro"));

        debug!("Remounting {} as readonly", target_path);

        if let Err(error) = mount::bind_remount(target_path, &options) {
            let message = format!(
                    "Failed to publish volume {}: failed to mount {} to {} as readonly: {}",
                    volume_id,
                    fs_staging_path,
                    target_path,
                    error
                );

            error!("Failed to remount {}: {}", target_path, error);

            debug!("Unmounting {}", target_path);

            if let Err(error) = mount::bind_unmount(target_path) {
                error!("Failed to unmount {}: {}", target_path, error);
            }

            return Err(Status::new(Code::Internal, message));
        }
    }

    info!("Volume {} published to {}", volume_id, target_path);

    Ok(())
}
