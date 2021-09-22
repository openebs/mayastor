use core::result;
use snafu::Snafu;

/// Errors for SPDK wrappers.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum SpdkError {
    #[snafu(display("Bdev module '{}' does not exist", name))]
    BdevModuleNotFound { name: String },

    #[snafu(display("Bdev '{}' is already claimed by another module", name))]
    BdevAlreadyClaimed { name: String },

    #[snafu(display(
        "Bdev '{}' is not claimed by this module '{}'",
        name,
        mod_name
    ))]
    BdevNotClaimed { name: String, mod_name: String },

    #[snafu(display("Serde JSON serialization failed: {}", source))]
    SerdeFailed { source: serde_json::Error },

    #[snafu(display("SPDK JSON write failed: error code {}", code))]
    JsonWriteFailed { code: i32 },

    #[snafu(display("Failed to create Uring for Bdev '{}'", name))]
    UringCreateFailed { name: String },
}

/// TODO
pub type Result<T> = result::Result<T, SpdkError>;
