use std::{num::ParseIntError, str::ParseBoolError};

use futures::channel::oneshot::Canceled;
use nix::errno::Errno;
use snafu::Snafu;
use url::ParseError;

use crate::bdev::Uri;

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
pub enum NexusBdevError {
    // Generic URL parse errors
    #[snafu(display("Error parsing URI \"{}\"", uri))]
    UrlParseError { source: ParseError, uri: String },
    #[snafu(display("Unsupported URI scheme: {}", scheme))]
    UriSchemeUnsupported { scheme: String },
    // Scheme specific URI format errors
    #[snafu(display("Invalid URI \"{}\": {}", uri, message))]
    UriInvalid { uri: String, message: String },
    #[snafu(display(
        "Invalid URI \"{}\": could not parse {} parameter value",
        uri,
        parameter
    ))]
    BoolParamParseError {
        source: ParseBoolError,
        uri: String,
        parameter: String,
    },
    #[snafu(display(
        "Invalid URI \"{}\": could not parse {} parameter value",
        uri,
        parameter
    ))]
    IntParamParseError {
        source: ParseIntError,
        uri: String,
        parameter: String,
    },
    // Bdev create/destroy errors
    #[snafu(display("bdev {} already exists", name))]
    BdevExists { name: String },
    #[snafu(display("bdev {} not found", name))]
    BdevNotFound { name: String },
    #[snafu(display("Invalid parameters for bdev create {}", name))]
    InvalidParams { source: Errno, name: String },
    #[snafu(display("Failed to create bdev {}", name))]
    CreateBdev { source: Errno, name: String },
    #[snafu(display("Failed to destroy bdev {}", name))]
    DestroyBdev { source: Errno, name: String },
    #[snafu(display("Command canceled for bdev {}", name))]
    CancelBdev { source: Canceled, name: String },
}

/// Parse URI and create bdev described in the URI.
/// Return the bdev name (which can be different from URI).
pub async fn bdev_create(uri: &str) -> Result<String, NexusBdevError> {
    Uri::parse(uri)?.create().await
}

/// Parse URI and destroy bdev described in the URI.
pub async fn bdev_destroy(uri: &str) -> Result<(), NexusBdevError> {
    Uri::parse(uri)?.destroy().await
}

pub fn bdev_get_name(uri: &str) -> Result<String, NexusBdevError> {
    Ok(Uri::parse(uri)?.get_name())
}
