use futures::channel::oneshot::Canceled;
use nix::errno::Errno;
use snafu::Snafu;
use std::{convert::TryFrom, num::ParseIntError, str::ParseBoolError};
use url::ParseError;

use crate::{
    bdev::uri,
    core::{Bdev, Share},
};

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum BdevError {
    // Generic URL parse errors.
    #[snafu(display("Error parsing URI '{}'", uri))]
    UriParseFailed { source: ParseError, uri: String },
    // No matching URI error.
    #[snafu(display(
        "No matching URI found for BDEV '{}' in aliases {:?}",
        name,
        aliases
    ))]
    BdevNoMatchingUri { name: String, aliases: Vec<String> },
    // Unsupported URI scheme.
    #[snafu(display("Unsupported URI scheme: '{}'", scheme))]
    UriSchemeUnsupported { scheme: String },
    // Scheme-specific URI format errors.
    #[snafu(display("Invalid URI '{}': {}", uri, message))]
    InvalidUri { uri: String, message: String },
    // Bad value of a boolean parameter.
    #[snafu(display(
        "Invalid URI '{}': could not parse value of parameter '{}': '{}' is given, \
            a boolean expected",
        uri,
        parameter,
        value
    ))]
    BoolParamParseFailed {
        source: ParseBoolError,
        uri: String,
        parameter: String,
        value: String,
    },
    // Bad value of an integer parameter.
    #[snafu(display(
        "Invalid URI '{}': could not parse value of parameter '{}': '{}' is given, \
            an integer expected",
        uri,
        parameter,
        value
    ))]
    IntParamParseFailed {
        source: ParseIntError,
        uri: String,
        parameter: String,
        value: String,
    },
    // Bad value of a UUID parameter.
    #[snafu(display(
        "Invalid URI '{}': could not parse value of UUID parameter",
        uri
    ))]
    UuidParamParseFailed { source: uuid::Error, uri: String },
    // BDEV name already exists.
    #[snafu(display(
        "Failed to create a BDEV: name '{}' already exists",
        name
    ))]
    BdevExists { name: String },
    // Creating a BDEV with a different UUID.
    #[snafu(display(
        "Failed to create a BDEV: '{}' already exists with a different UUID: '{}'",
        name,
        uuid
    ))]
    BdevWrongUuid { name: String, uuid: String },
    // BDEV is not found.
    #[snafu(display("BDEV '{}' could not be found", name))]
    BdevNotFound { name: String },
    // Invalid creation parameters.
    #[snafu(display("Failed to create a BDEV '{}'", name))]
    CreateBdevInvalidParams { source: Errno, name: String },
    // Generic creation failure.
    #[snafu(display("Failed to create a BDEV '{}'", name))]
    CreateBdevFailed { source: Errno, name: String },
    // Generic destruction failure.
    #[snafu(display("Failed to destroy a BDEV '{}'", name))]
    DestroyBdevFailed { source: Errno, name: String },
    // Generic resize failure.
    #[snafu(display("Failed to resize a BDEV '{}'", name))]
    ResizeBdevFailed { source: Errno, name: String },
    #[snafu(display("Failed to create BDEV '{name}': {error}"))]
    CreateBdevFailedStr { name: String, error: String },
    #[snafu(display("Failed to destroy BDEV '{name}': {error}"))]
    DestroyBdevFailedStr { name: String, error: String },
    // Command canceled.
    #[snafu(display("Command canceled for a BDEV '{}'", name))]
    BdevCommandCanceled { source: Canceled, name: String },
    #[snafu(display("Failed to wipe the BDEV"))]
    WipeFailed {},
}

/// Parse URI and create bdev described in the URI.
/// Return the bdev name (which can be different from URI).
pub async fn bdev_create(uri: &str) -> Result<String, BdevError> {
    info!(?uri, "create");
    uri::parse(uri)?.create().await
}

/// Parse URI and destroy bdev described in the URI.
pub async fn bdev_destroy(uri: &str) -> Result<(), BdevError> {
    info!(?uri, "destroy");
    uri::parse(uri)?.destroy().await
}

/// TODO
pub fn bdev_get_name(uri: &str) -> Result<String, BdevError> {
    Ok(uri::parse(uri)?.get_name())
}

/// TODO
pub fn bdev_uri_eq<T>(bdev: &Bdev<T>, uri: &url::Url) -> bool
where
    T: spdk_rs::BdevOps,
{
    match uri::parse(uri.as_ref()) {
        Ok(device) if device.get_name() == bdev.name() => {
            bdev.driver()
                == match uri.scheme() {
                    "nvmf" | "nvmf+tcp" | "nvmf+rdma+tcp" | "pcie" => "nvme",
                    scheme => scheme,
                }
        }
        _ => false,
    }
}

/// TODO
pub fn bdev_url_eq<T>(bdev: &Bdev<T>, uri: &url::Url) -> bool
where
    T: spdk_rs::BdevOps,
{
    match uri::parse(uri.as_ref()) {
        Ok(device) if device.get_name() == bdev.name() => {
            bdev.driver()
                == match uri.scheme() {
                    "nvmf" | "nvmf+tcp" | "nvmf+rdma+tcp" | "pcie" => "nvme",
                    scheme => scheme,
                }
        }
        _ => false,
    }
}

impl<T> TryFrom<Bdev<T>> for url::Url
where
    T: spdk_rs::BdevOps,
{
    type Error = BdevError;

    fn try_from(bdev: Bdev<T>) -> Result<Self, Self::Error> {
        bdev.bdev_uri().ok_or(BdevError::BdevNoMatchingUri {
            name: bdev.name().to_string(),
            aliases: bdev.aliases(),
        })
    }
}
