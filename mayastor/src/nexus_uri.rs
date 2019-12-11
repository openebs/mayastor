use crate::{
    aio_dev,
    iscsi_dev,
    jsonrpc::{Code, RpcErrorCode},
    nvmf_dev,
};
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};
use std::convert::TryFrom;
use url::{ParseError, Url};

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum BdevError {
    // URI parse errors
    #[snafu(display("Invalid URI \"{}\"", uri))]
    UriInvalid { source: ParseError, uri: String },
    #[snafu(display("Unsupported URI scheme \"{}\"", scheme))]
    UriSchemeUnsupported { scheme: String },
    #[snafu(display("Failed to parse aio URI \"{}\"", uri))]
    ParseAioUri {
        source: aio_dev::ParseError,
        uri: String,
    },
    #[snafu(display("Failed to parse iscsi URI \"{}\"", uri))]
    ParseIscsiUri {
        source: iscsi_dev::ParseError,
        uri: String,
    },
    #[snafu(display("Failed to parse nvmf URI \"{}\"", uri))]
    ParseNvmfUri {
        source: nvmf_dev::ParseError,
        uri: String,
    },
    // bdev create/destroy errors
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
}

impl RpcErrorCode for BdevError {
    fn rpc_error_code(&self) -> Code {
        match self {
            BdevError::UriInvalid {
                ..
            } => Code::InvalidParams,
            BdevError::UriSchemeUnsupported {
                ..
            } => Code::InvalidParams,
            BdevError::ParseAioUri {
                ..
            } => Code::InvalidParams,
            BdevError::ParseIscsiUri {
                ..
            } => Code::InvalidParams,
            BdevError::ParseNvmfUri {
                ..
            } => Code::InvalidParams,
            BdevError::BdevExists {
                ..
            } => Code::AlreadyExists,
            BdevError::BdevNotFound {
                ..
            } => Code::NotFound,
            BdevError::InvalidParams {
                ..
            } => Code::InvalidParams,
            _ => Code::InternalError,
        }
    }
}

/// enum type of URL to args we currently support
#[derive(Debug)]
pub enum BdevType {
    /// you should not be using this other then testing
    Aio(aio_dev::AioBdev),
    /// backend iSCSI target most stable
    Iscsi(iscsi_dev::IscsiBdev),
    /// backend NVMF target pretty unstable as of Linux 5.2
    Nvmf(nvmf_dev::NvmfBdev),
    /// bdev type is arbitrary bdev found in spdk (used for local replicas)
    Bdev(String),
}

/// Converts an array of Strings into the appropriate args type
/// to construct the children from which we create the nexus.
pub fn nexus_uri_parse_vec(
    uris: &[String],
) -> Result<Vec<BdevType>, BdevError> {
    let mut results = Vec::new();
    for target in uris {
        results.push(nexus_parse_uri(target)?);
    }

    Ok(results)
}

/// Parse the given URI into a ChildBdev
pub fn nexus_parse_uri(uri: &str) -> Result<BdevType, BdevError> {
    let parsed_uri = Url::parse(uri).context(UriInvalid {
        uri: uri.to_owned(),
    })?;
    let bdev_type = match parsed_uri.scheme() {
        "aio" => BdevType::Aio(
            aio_dev::AioBdev::try_from(&parsed_uri).context(ParseAioUri {
                uri,
            })?,
        ),
        "iscsi" => BdevType::Iscsi(
            iscsi_dev::IscsiBdev::try_from(&parsed_uri).context(
                ParseIscsiUri {
                    uri,
                },
            )?,
        ),
        "nvmf" => {
            BdevType::Nvmf(nvmf_dev::NvmfBdev::try_from(&parsed_uri).context(
                ParseNvmfUri {
                    uri,
                },
            )?)
        }
        // strip the first slash in uri path
        "bdev" => BdevType::Bdev(parsed_uri.path()[1 ..].to_string()),
        scheme => {
            return Err(BdevError::UriSchemeUnsupported {
                scheme: scheme.to_owned(),
            })
        }
    };
    Ok(bdev_type)
}

/// Parse URI and destroy bdev described in the URI.
pub async fn bdev_destroy(uri: &str, bdev_name: &str) -> Result<(), BdevError> {
    match nexus_parse_uri(uri)? {
        BdevType::Aio(args) => args.destroy(bdev_name).await,
        BdevType::Iscsi(args) => args.destroy(bdev_name).await,
        BdevType::Nvmf(args) => args.destroy(bdev_name),
        BdevType::Bdev(_) => Ok(()),
    }
}

/// Parse URI and create bdev described in the URI.
/// Return the bdev name (can be different from URI).
pub async fn bdev_create(uri: &str) -> Result<String, BdevError> {
    match nexus_parse_uri(uri)? {
        BdevType::Aio(args) => args.create().await,
        BdevType::Iscsi(args) => args.create().await,
        BdevType::Nvmf(args) => args.create().await,
        BdevType::Bdev(name) => Ok(name),
    }
}
