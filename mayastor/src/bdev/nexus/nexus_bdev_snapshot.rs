//! Implements snapshot operations on a nexus.

use rpc::mayastor::CreateSnapshotReply;

use super::{Error, Nexus};

use crate::{core::BdevHandle, lvs::Lvol};

impl<'n> Nexus<'n> {
    /// Create a snapshot on all children
    pub async fn create_snapshot(&self) -> Result<CreateSnapshotReply, Error> {
        if let Ok(h) = BdevHandle::open_with_bdev(&self.bdev(), false) {
            match h.create_snapshot().await {
                Ok(t) => Ok(CreateSnapshotReply {
                    name: Lvol::format_snapshot_name(self.bdev().name(), t),
                }),
                Err(e) => Err(Error::FailedCreateSnapshot {
                    name: self.bdev().name().to_string(),
                    source: e,
                }),
            }
        } else {
            Err(Error::FailedGetHandle)
        }
    }
}
