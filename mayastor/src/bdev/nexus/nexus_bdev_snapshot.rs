//! Implements snapshot operations on a nexus.

use rpc::mayastor::CreateSnapshotReply;

use crate::{
    bdev::nexus::nexus_bdev::{Error, Nexus},
    core::BdevHandle,
    lvs::Lvol,
};

impl Nexus {
    /// Create a snapshot on all children
    pub async fn create_snapshot(&self) -> Result<CreateSnapshotReply, Error> {
        if let Ok(h) = BdevHandle::open_with_bdev(&self.bdev, true) {
            match h.create_snapshot().await {
                Ok(t) => Ok(CreateSnapshotReply {
                    name: Lvol::format_snapshot_name(&self.bdev.name(), t),
                }),
                Err(_e) => Err(Error::FailedCreateSnapshot),
            }
        } else {
            Err(Error::FailedGetHandle)
        }
    }
}
