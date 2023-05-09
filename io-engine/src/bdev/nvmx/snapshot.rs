use serde::{Deserialize, Serialize};

use crate::core::SnapshotParams;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NvmeSnapshotMessageV1 {
    params: SnapshotParams,
}

impl NvmeSnapshotMessageV1 {
    /// Create a V1 snapshot creation message.
    pub fn new(params: SnapshotParams) -> Self {
        Self {
            params,
        }
    }

    /// Get snapshot params payload.
    pub fn params(&self) -> &SnapshotParams {
        &self.params
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NvmeSnapshotMessage {
    V1(NvmeSnapshotMessageV1),
}
