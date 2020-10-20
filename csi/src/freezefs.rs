//! The files system freeze support using linux utility fsfreeze
use crate::{
    dev::{Device, DeviceError},
    mount,
};
use snafu::{ResultExt, Snafu};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum FreezeFsError {
    #[snafu(display("Cannot find volume: volume ID: {}", volid))]
    VolumeNotFound { volid: String },
    #[snafu(display("Invalid volume ID: {}, {}", volid, source))]
    InvalidVolumeId {
        source: uuid::parser::ParseError,
        volid: String,
    },
    #[snafu(display("fsfreeze failed: volume ID: {}, {}", volid, error))]
    FsfreezeFailed { volid: String, error: String },
    #[snafu(display("Internal failure: volume ID:{}, {}", volid, source))]
    InternalFailure { source: DeviceError, volid: String },
    #[snafu(display("IO error: volume ID: {}, {}", volid, source))]
    IOError {
        source: std::io::Error,
        volid: String,
    },
}

const FSFREEZE: &str = "fsfreeze";

async fn fsfreeze(
    volume_id: &str,
    freeze_op: &str,
) -> Result<(), FreezeFsError> {
    let uuid = Uuid::parse_str(volume_id).context(InvalidVolumeId {
        volid: volume_id.to_string(),
    })?;

    if let Some(device) =
        Device::lookup(&uuid).await.context(InternalFailure {
            volid: volume_id.to_string(),
        })?
    {
        let device_path = device.devname();
        if let Some(mnt) = mount::find_mount(Some(&device_path), None) {
            let args = [freeze_op, &mnt.dest];
            let output =
                Command::new(FSFREEZE).args(&args).output().await.context(
                    IOError {
                        volid: volume_id.to_string(),
                    },
                )?;
            if output.status.success() {
                return Ok(());
            } else {
                return Err(FreezeFsError::FsfreezeFailed {
                    volid: volume_id.to_string(),
                    error: String::from_utf8(output.stderr).unwrap(),
                });
            }
        }
    }
    Err(FreezeFsError::VolumeNotFound {
        volid: volume_id.to_string(),
    })
}
pub async fn freeze_volume(volume_id: &str) -> Result<(), FreezeFsError> {
    fsfreeze(volume_id, "--freeze").await
}

pub async fn unfreeze_volume(volume_id: &str) -> Result<(), FreezeFsError> {
    fsfreeze(volume_id, "--unfreeze").await
}
