/// Snapshot-related logic of LVS store.
use crate::{
    core::{
        logical_volume::LogicalVolume,
        CloneDescriptor,
        CloneParams,
        Snapshot,
        SnapshotDescriptor,
        SnapshotParams,
        UntypedBdev,
    },
    lvs::{lvs_lvol::LvsLvol, Lvol, LvolSpaceUsage, Lvs},
};
use async_trait::async_trait;
use std::{convert::TryFrom, iter::Iterator};

pub struct LvolSnapshot {
    lvol: Lvol,
    props: SnapshotParams,
}

/// Generic lvol functionality for snapshot.
impl LogicalVolume for LvolSnapshot {
    /// Returns the name of the Logical Volume
    fn name(&self) -> String {
        self.lvol.name()
    }

    /// Returns the UUID of the Logical Volume
    fn uuid(&self) -> String {
        self.lvol.uuid()
    }

    /// Returns the pool name of the Logical Volume
    fn pool_name(&self) -> String {
        self.lvol.pool_name()
    }

    /// Returns the pool uuid of the Logical Volume
    fn pool_uuid(&self) -> String {
        self.lvol.pool_uuid()
    }

    /// Returns a boolean indicating if the Logical Volume is thin provisioned
    fn is_thin(&self) -> bool {
        self.lvol.is_thin()
    }

    /// Returns a boolean indicating if the Logical Volume is read-only
    fn is_read_only(&self) -> bool {
        self.lvol.is_read_only()
    }

    /// Return the size of the Logical Volume in bytes
    fn size(&self) -> u64 {
        self.lvol.size()
    }

    /// Returns Lvol disk space usage
    fn usage(&self) -> LvolSpaceUsage {
        self.lvol.usage()
    }
}

impl LvolSnapshot {
    /// Construct Snapshot object from lvol that represents snapshot.
    fn from_lvol(lvol: Lvol) -> Option<Self> {
        if !lvol.is_snapshot() {
            None
        } else {
            let props = lvol.get_snapshot_param();

            Some(LvolSnapshot {
                lvol,
                props,
            })
        }
    }
}

impl Lvs {
    /// List all snapshots available on all lvol stores.
    pub(crate) fn snapshots() -> impl Iterator<Item = LvolSnapshot> {
        let mut snapshots: Vec<LvolSnapshot> = Vec::new();

        if let Some(bdev) = UntypedBdev::bdev_first() {
            snapshots = bdev
                .into_iter()
                .filter(|b| b.driver() == "lvol")
                .filter_map(|b| {
                    let lvol = Lvol::try_from(b)
                        .expect("Can't create Lvol from device");
                    LvolSnapshot::from_lvol(lvol)
                })
                .collect::<Vec<LvolSnapshot>>();
        }

        snapshots.into_iter()
    }
}

impl SnapshotDescriptor for LvolSnapshot {
    fn txn_id(&self) -> Option<String> {
        self.props.txn_id()
    }

    /// Get Entity Id of the Snapshot.
    fn entity_id(&self) -> Option<String> {
        self.props.entity_id()
    }

    /// Get Parent Id of the Snapshot.
    fn parent_id(&self) -> Option<String> {
        self.props.parent_id()
    }

    /// Get Snapshot Name.
    fn name(&self) -> Option<String> {
        self.props.name()
    }
}

#[async_trait(?Send)]
impl Snapshot for LvolSnapshot {
    async fn clone(_params: CloneParams) -> Result<CloneDescriptor, String> {
        unimplemented!(
            "clone() method for LvolSnapshot is not yet implemented"
        );
    }
}
