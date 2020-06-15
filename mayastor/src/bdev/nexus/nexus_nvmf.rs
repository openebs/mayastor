//! Utility functions and wrappers for working with NVMEoF devices in SPDK.

use std::fmt;

use snafu::Snafu;

use crate::{
    core::Bdev,
    subsys::NvmfSubsystem,
    target::nvmf::{share, unshare},
};

#[derive(Debug, Snafu)]
pub enum NexusNvmfError {
    #[snafu(display("Bdev not found {}", dev))]
    BdevNotFound { dev: String },
    #[snafu(display(
        "Failed to create nvmf target for bdev uuid {}, error {}",
        dev,
        err
    ))]
    CreateTargetFailed { dev: String, err: String },
}

/// Nvmf target representation.
pub struct NexusNvmfTarget {
    uuid: String,
}

impl NexusNvmfTarget {
    pub async fn create(my_uuid: &str) -> Result<Self, NexusNvmfError> {
        info!("Creating nvmf nexus target: {}", my_uuid);
        let bdev = match Bdev::lookup_by_name(&my_uuid) {
            None => {
                return Err(NexusNvmfError::BdevNotFound {
                    dev: my_uuid.to_string(),
                });
            }
            Some(bd) => bd,
        };

        match share(&my_uuid, &bdev).await {
            Ok(_) => Ok(Self {
                uuid: my_uuid.to_string(),
            }),
            Err(e) => Err(NexusNvmfError::CreateTargetFailed {
                dev: my_uuid.to_string(),
                err: e.to_string(),
            }),
        }
    }
    pub async fn destroy(self) {
        info!("Destroying nvmf nexus target");
        match unshare(&self.uuid).await {
            Ok(()) => (),
            Err(e) => {
                error!("Failed to destroy nvmf frontend target, error {}", e)
            }
        }
    }

    pub fn as_uri(&self) -> String {
        NvmfSubsystem::nqn_lookup(&self.uuid)
            .unwrap()
            .uri_endpoints()
            .unwrap()
            .pop()
            .unwrap()
    }
}

impl fmt::Debug for NexusNvmfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.as_uri(), self.uuid)
    }
}
