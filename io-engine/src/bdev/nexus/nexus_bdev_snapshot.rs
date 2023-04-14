//! Implements snapshot operations on a nexus.

use mayastor_api::v0::CreateSnapshotReply;

use super::{Error, Nexus};
use crate::{core::SnapshotParams, lvs::Lvol};

impl<'n> Nexus<'n> {
    /// Create a snapshot on all children
    pub async fn create_snapshot(
        &self,
        snapshot: SnapshotParams,
    ) -> Result<CreateSnapshotReply, Error> {
        if let Ok(h) = unsafe { self.open_bdev_handle(false) } {
            match h.create_snapshot(snapshot).await {
                Ok(t) => Ok(CreateSnapshotReply {
                    name: Lvol::format_snapshot_name(&self.bdev_name(), t),
                }),
                Err(e) => Err(Error::FailedCreateSnapshot {
                    name: self.bdev_name(),
                    source: e,
                }),
            }
        } else {
            Err(Error::FailedGetHandle)
        }
    }
}
