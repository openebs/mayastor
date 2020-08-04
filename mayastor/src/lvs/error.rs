use crate::{core::CoreError, nexus_uri::NexusBdevError};
use nix::errno::Errno;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("failed to import pool {}", msg))]
    Import { err: Errno, msg: String },
    #[snafu(display("failed to create pool {}", msg))]
    Create { err: Errno, msg: String },
    #[snafu(display("failed to export pool {}", msg))]
    Export { err: Errno, msg: String },
    #[snafu(display("failed to destroy pool {}", msg))]
    Destroy { source: NexusBdevError, msg: String },
    #[snafu(display("invalid bdev specified {}", msg))]
    InvalidBdev { source: NexusBdevError, msg: String },
    #[snafu(display(
        "Invalid number of disks specified: should be 1, got {}",
        num
    ))]
    BadNumDisks { num: usize },
    #[snafu(display("lvol exists {}", msg))]
    RepExists { err: Errno, msg: String },
    #[snafu(display("failed to create lvol {}", msg))]
    RepCreate { source: Errno, msg: String },
    #[snafu(display("failed to create lvol {}", msg))]
    RepDestroy { source: Errno, msg: String },
    #[snafu(display("bdev is not a lvol"))]
    NotALvol { source: Errno, msg: String },
    #[snafu(display("failed to share lvol {}", msg))]
    LvolShare { source: CoreError, msg: String },
    #[snafu(display("failed to share lvol {}", msg))]
    LvolUnShare { source: CoreError, msg: String },
    #[snafu(display("{}", msg))]
    Property { source: Errno, msg: String },
}
