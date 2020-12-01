use std::{convert::TryFrom, num::ParseIntError, str::ParseBoolError};

use crate::{bdev::Uri, core::Bdev};
use futures::channel::oneshot::Canceled;
use nix::errno::Errno;
use snafu::Snafu;
use tracing::instrument;
use url::ParseError;

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub(crate)")]
pub enum NexusBdevError {
    // Generic URL parse errors
    #[snafu(display("Error parsing URI \"{}\"", uri))]
    UrlParseError { source: ParseError, uri: String },
    #[snafu(display(
        "No matching URI found for bdev {} in aliases {:?}",
        name,
        aliases
    ))]
    BdevNoUri { name: String, aliases: Vec<String> },
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
    #[snafu(display(
        "Invalid URI \"{}\": could not parse uuid parameter value",
        uri,
    ))]
    UuidParamParseError {
        source: uuid::parser::ParseError,
        uri: String,
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
#[instrument]
pub async fn bdev_create(uri: &str) -> Result<String, NexusBdevError> {
    Uri::parse(uri)?.create().await
}

/// Parse URI and destroy bdev described in the URI.
#[instrument]
pub async fn bdev_destroy(uri: &str) -> Result<(), NexusBdevError> {
    Uri::parse(uri)?.destroy().await
}

pub fn bdev_get_name(uri: &str) -> Result<String, NexusBdevError> {
    Ok(Uri::parse(uri)?.get_name())
}

impl std::cmp::PartialEq<url::Url> for &Bdev {
    fn eq(&self, uri: &url::Url) -> bool {
        match Uri::parse(&uri.to_string()) {
            Ok(device) if device.get_name() == self.name() => {
                self.driver()
                    == match uri.scheme() {
                        "nvmf" | "pcie" => "nvme",
                        scheme => scheme,
                    }
            }
            _ => false,
        }
    }
}

impl std::cmp::PartialEq<url::Url> for Bdev {
    fn eq(&self, uri: &url::Url) -> bool {
        match Uri::parse(&uri.to_string()) {
            Ok(device) if device.get_name() == self.name() => {
                self.driver()
                    == match uri.scheme() {
                        "nvmf" | "pcie" => "nvme",
                        scheme => scheme,
                    }
            }
            _ => false,
        }
    }
}

impl TryFrom<Bdev> for url::Url {
    type Error = NexusBdevError;

    fn try_from(bdev: Bdev) -> Result<Self, Self::Error> {
        for alias in bdev.aliases().iter() {
            if let Ok(mut uri) = url::Url::parse(alias) {
                if bdev == uri {
                    if uri.query_pairs().find(|e| e.0 == "uuid").is_none() {
                        uri.query_pairs_mut()
                            .append_pair("uuid", &bdev.uuid_as_string());
                    }
                    return Ok(uri);
                }
            }
        }

        Err(NexusBdevError::BdevNoUri {
            name: bdev.name(),
            aliases: bdev.aliases(),
        })
    }
}
