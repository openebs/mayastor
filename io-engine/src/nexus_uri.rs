use std::{convert::TryFrom, num::ParseIntError, str::ParseBoolError};

use crate::{bdev::uri, core::Bdev};
use futures::channel::oneshot::Canceled;
use nix::errno::Errno;
use snafu::Snafu;

use url::ParseError;

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
pub enum NexusBdevError {
    // Generic URL parse errors.
    #[snafu(display("Error parsing URI '{}'", uri))]
    UrlParseError { source: ParseError, uri: String },

    // No matching URI error.
    #[snafu(display(
        "No matching URI found for BDEV '{}' in aliases {:?}",
        name,
        aliases
    ))]
    BdevNoUri { name: String, aliases: Vec<String> },

    // Unsupported URI scheme.
    #[snafu(display("Unsupported URI scheme: '{}'", scheme))]
    UriSchemeUnsupported { scheme: String },

    // Scheme-specific URI format errors.
    #[snafu(display("Invalid URI '{}': {}", uri, message))]
    UriInvalid { uri: String, message: String },

    // Bad value of a boolean parameter.
    #[snafu(display(
        "Invalid URI '{}': could not parse value of parameter '{}': '{}' is given, \
            a boolean expected",
        uri,
        parameter,
        value
    ))]
    BoolParamParseError {
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
    IntParamParseError {
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
    UuidParamParseError { source: uuid::Error, uri: String },

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
    #[snafu(display(
        "Failed to create a BDEV '{}': invalid parameters",
        name
    ))]
    CreateBdevInvalidParams { source: Errno, name: String },

    // Generic creation failure.
    #[snafu(display("Failed to create a BDEV '{}'", name))]
    CreateBdev { source: Errno, name: String },

    // Generic destruction failure.
    #[snafu(display("Failed to destroy a BDEV '{}'", name))]
    DestroyBdev { source: Errno, name: String },

    // Command canceled.
    #[snafu(display("Command canceled for a BDEV '{}'", name))]
    CancelBdev { source: Canceled, name: String },
}

/// Parse URI and create bdev described in the URI.
/// Return the bdev name (which can be different from URI).
pub async fn bdev_create(uri: &str) -> Result<String, NexusBdevError> {
    info!(?uri, "create");
    uri::parse(uri)?.create().await
}

/// Parse URI and destroy bdev described in the URI.
pub async fn bdev_destroy(uri: &str) -> Result<(), NexusBdevError> {
    info!(?uri, "destroy");
    uri::parse(uri)?.destroy().await
}

/// TODO
pub fn bdev_get_name(uri: &str) -> Result<String, NexusBdevError> {
    Ok(uri::parse(uri)?.get_name())
}

/// TODO
pub fn bdev_uri_eq<T>(bdev: &Bdev<T>, uri: &url::Url) -> bool
where
    T: spdk_rs::BdevOps,
{
    match uri::parse(&uri.to_string()) {
        Ok(device) if device.get_name() == bdev.name() => {
            bdev.driver()
                == match uri.scheme() {
                    "nvmf" | "pcie" => "nvme",
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
    match uri::parse(&uri.to_string()) {
        Ok(device) if device.get_name() == bdev.name() => {
            bdev.driver()
                == match uri.scheme() {
                    "nvmf" | "pcie" => "nvme",
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
    type Error = NexusBdevError;

    fn try_from(bdev: Bdev<T>) -> Result<Self, Self::Error> {
        for alias in bdev.aliases().iter() {
            if let Ok(mut uri) = url::Url::parse(alias) {
                if bdev_uri_eq(&bdev, &uri) {
                    if !uri.query_pairs().any(|e| e.0 == "uuid") {
                        uri.query_pairs_mut()
                            .append_pair("uuid", &bdev.uuid_as_string());
                    }
                    return Ok(uri);
                }
            }
        }

        Err(NexusBdevError::BdevNoUri {
            name: bdev.name().to_string(),
            aliases: bdev.aliases(),
        })
    }
}
