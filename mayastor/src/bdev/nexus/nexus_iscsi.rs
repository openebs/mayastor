//! Utility functions and wrappers for working with iSCSI devices in SPDK.

use std::fmt;

use snafu::Snafu;

use crate::{
    core::Bdev,
    target::{
        iscsi::{create_uri, share, target_name, unshare},
        Side,
    },
};

#[derive(Debug, Snafu)]
pub enum NexusIscsiError {
    #[snafu(display("Bdev not found {}", dev))]
    BdevNotFound { dev: String },
    #[snafu(display(
        "Failed to create iscsi target for bdev uuid {}, error {}",
        dev,
        err
    ))]
    CreateTargetFailed { dev: String, err: String },
}

/// Iscsi target representation.
pub struct NexusIscsiTarget {
    bdev_name: String, /* logically we might store a spdk_iscsi_tgt_node here but ATM the bdev name is all we actually need */
}

impl NexusIscsiTarget {
    /// Allocate iscsi device for the bdev and start it.
    /// When the function returns the iscsi target is ready for IO.
    pub fn create(bdev_name: &str) -> Result<Self, NexusIscsiError> {
        let bdev = match Bdev::lookup_by_name(bdev_name) {
            None => {
                return Err(NexusIscsiError::BdevNotFound {
                    dev: bdev_name.to_string(),
                })
            }
            Some(bd) => bd,
        };

        match share(bdev_name, &bdev, Side::Nexus) {
            Ok(_) => Ok(Self {
                bdev_name: bdev_name.to_string(),
            }),
            Err(e) => Err(NexusIscsiError::CreateTargetFailed {
                dev: bdev_name.to_string(),
                err: e.to_string(),
            }),
        }
    }

    pub async fn destroy(self) {
        info!("Destroying iscsi frontend target");
        match unshare(&self.bdev_name).await {
            Ok(()) => (),
            Err(e) => {
                error!("Failed to destroy iscsi frontend target, error {}", e)
            }
        }
    }

    pub fn as_uri(&self) -> String {
        create_uri(Side::Nexus, &target_name(&self.bdev_name))
    }
}

impl fmt::Debug for NexusIscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.as_uri(), self.bdev_name)
    }
}

impl fmt::Display for NexusIscsiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_uri())
    }
}
