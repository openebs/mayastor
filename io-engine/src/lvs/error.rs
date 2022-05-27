use nix::errno::Errno;
use snafu::Snafu;

use crate::{core::CoreError, lvs::PropName, nexus_uri::NexusBdevError};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("failed to import pool {}", name))]
    Import { source: Errno, name: String },

    #[snafu(display("errno: {} failed to create pool {}", source, name))]
    PoolCreate { source: Errno, name: String },

    #[snafu(display("failed to export pool {}", name))]
    Export { source: Errno, name: String },

    #[snafu(display("failed to destroy pool {}", name))]
    Destroy {
        source: NexusBdevError,
        name: String,
    },

    InvalidBdev {
        source: NexusBdevError,
        name: String,
    },

    #[snafu(display("errno {}: {}", source, msg))]
    Invalid { source: Errno, msg: String },

    #[snafu(display("lvol exists {}", name))]
    RepExists { source: Errno, name: String },

    #[snafu(display("errno: {} failed to create lvol {}", source, name))]
    RepCreate { source: Errno, name: String },

    #[snafu(display("failed to destroy lvol {}", name))]
    RepDestroy { source: Errno, name: String },

    #[snafu(display("bdev {} is not a lvol", name))]
    NotALvol { source: Errno, name: String },

    #[snafu(display("failed to share lvol {}", name))]
    LvolShare { source: CoreError, name: String },

    #[snafu(display("failed to unshare lvol {}", name))]
    LvolUnShare { source: CoreError, name: String },

    #[snafu(display(
        "failed to get property {} ({}) from {}",
        prop,
        source,
        name
    ))]
    GetProperty {
        source: Errno,
        prop: PropName,
        name: String,
    },
    #[snafu(display("failed to set property {} on {}", prop, name))]
    SetProperty {
        source: Errno,
        prop: PropName,
        name: String,
    },
    #[snafu(display("failed to sync properties {}", name))]
    SyncProperty { source: Errno, name: String },
    #[snafu(display("invalid property value: {}", name))]
    Property { source: Errno, name: String },
    #[snafu(display("invalid replica share protocol value: {}", value))]
    ReplicaShareProtocol { value: i32 },
}
