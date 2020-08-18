use crate::{core::CoreError, nexus_uri::NexusBdevError};
use nix::errno::Errno;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("errno {}: {}", err.to_string(), msg))]
    Import { err: Errno, msg: String },
    #[snafu(display("failed to create pool {}", msg))]
    Create { err: Errno, msg: String },
    #[snafu(display("errno {}: {}", err.to_string(), msg))]
    Export { err: Errno, msg: String },
    #[snafu(display("source {}: {}", source.to_string(), msg))]
    Destroy { source: NexusBdevError, msg: String },
    #[snafu(display("source {}: {}", source.to_string(), msg))]
    InvalidBdev { source: NexusBdevError, msg: String },
    #[snafu(display("errno {}: {}", source.to_string(), msg))]
    Invalid { source: Errno, msg: String },
    #[snafu(display("lvol exists {}", msg))]
    RepExists { err: Errno, msg: String },
    #[snafu(display("failed to create lvol {}", msg))]
    RepCreate { source: Errno, msg: String },
    #[snafu(display("failed to create lvol {}", msg))]
    RepDestroy { source: Errno, msg: String },
    #[snafu(display("bdev is not a lvol"))]
    NotALvol { source: Errno, msg: String },
    #[snafu(display("source: {} {}", source.to_string(), msg))]
    LvolShare { source: CoreError, msg: String },
    #[snafu(display("failed to share lvol {}", msg))]
    LvolUnShare { source: CoreError, msg: String },
    #[snafu(display("errno {}: {}", source.to_string(), msg))]
    Property { source: Errno, msg: String },
}
