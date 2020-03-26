use std::convert::TryFrom;

use nix::errno::Errno;
use snafu::{ResultExt, Snafu};
use url::{ParseError, Url};

use crate::{
    bdev::{
        AioBdev, AioParseError, IscsiBdev, IscsiParseError, NvmeCtlAttachReq,
        NvmfParseError, UringBdev, UringParseError,
    },
    jsonrpc::{Code, RpcErrorCode},
};

// parse URI and bdev create/destroy errors common for all types of bdevs
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum BdevCreateDestroy {
    // URI parse errors
    #[snafu(display("Invalid URI \"{}\"", uri))]
    UriInvalid { source: ParseError, uri: String },
    #[snafu(display("Unsupported URI scheme \"{}\"", scheme))]
    UriSchemeUnsupported { scheme: String },
    #[snafu(display("Failed to parse aio URI \"{}\"", uri))]
    ParseAioUri { source: AioParseError, uri: String },
    #[snafu(display("Failed to parse iscsi URI \"{}\"", uri))]
    ParseIscsiUri {
        source: IscsiParseError,
        uri: String,
    },
    #[snafu(display("Failed to parse nvmf URI \"{}\"", uri))]
    ParseNvmfUri { source: NvmfParseError, uri: String },
    #[snafu(display("Failed to parse uring URI \"{}\"", uri))]
    ParseUringUri {
        source: UringParseError,
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

impl RpcErrorCode for BdevCreateDestroy {
    fn rpc_error_code(&self) -> Code {
        match self {
            BdevCreateDestroy::UriInvalid { .. } => Code::InvalidParams,
            BdevCreateDestroy::UriSchemeUnsupported { .. } => {
                Code::InvalidParams
            }
            BdevCreateDestroy::ParseAioUri { .. } => Code::InvalidParams,
            BdevCreateDestroy::ParseIscsiUri { .. } => Code::InvalidParams,
            BdevCreateDestroy::ParseNvmfUri { .. } => Code::InvalidParams,
            BdevCreateDestroy::ParseUringUri { .. } => Code::InvalidParams,
            BdevCreateDestroy::BdevExists { .. } => Code::AlreadyExists,
            BdevCreateDestroy::BdevNotFound { .. } => Code::NotFound,
            BdevCreateDestroy::InvalidParams { .. } => Code::InvalidParams,
            _ => Code::InternalError,
        }
    }
}

/// enum type of URL to args we currently support
#[derive(Debug)]
pub enum BdevType {
    /// you should not be using this other than for testing
    Aio(AioBdev),
    /// backend iSCSI target most stable
    Iscsi(IscsiBdev),
    /// backend NVMF target pretty unstable as of Linux 5.2
    Nvmf(NvmeCtlAttachReq),
    /// also for testing, requires Linux 5.1
    Uring(UringBdev),
    /// bdev type is arbitrary bdev found in spdk (used for local replicas)
    Bdev(String),
}

/// Converts an array of Strings into the appropriate args type
/// to construct the children from which we create the nexus.
pub fn nexus_uri_parse_vec(
    uris: &[String],
) -> Result<Vec<BdevType>, BdevCreateDestroy> {
    let mut results = Vec::new();
    for target in uris {
        results.push(nexus_parse_uri(target)?);
    }

    Ok(results)
}

/// Parse the given URI into a ChildBdev
fn nexus_parse_uri(uri: &str) -> Result<BdevType, BdevCreateDestroy> {
    let parsed_uri = Url::parse(uri).context(UriInvalid {
        uri: uri.to_owned(),
    })?;
    let bdev_type = match parsed_uri.scheme() {
        "aio" => BdevType::Aio(
            AioBdev::try_from(&parsed_uri).context(ParseAioUri { uri })?,
        ),
        "iscsi" => BdevType::Iscsi(
            IscsiBdev::try_from(&parsed_uri).context(ParseIscsiUri { uri })?,
        ),
        "nvmf" => BdevType::Nvmf(
            NvmeCtlAttachReq::try_from(&parsed_uri)
                .context(ParseNvmfUri { uri })?,
        ),
        "uring" => BdevType::Uring(
            UringBdev::try_from(&parsed_uri).context(ParseUringUri { uri })?,
        ),
        // strip the first slash in uri path
        "bdev" => BdevType::Bdev(parsed_uri.path()[1..].to_string()),
        scheme => {
            return Err(BdevCreateDestroy::UriSchemeUnsupported {
                scheme: scheme.to_owned(),
            })
        }
    };
    Ok(bdev_type)
}

/// Parse URI and destroy bdev described in the URI.
pub async fn bdev_destroy(uri: &str) -> Result<(), BdevCreateDestroy> {
    match nexus_parse_uri(uri)? {
        BdevType::Aio(args) => args.destroy().await,
        BdevType::Iscsi(args) => args.destroy().await,
        BdevType::Nvmf(args) => args.destroy(),
        BdevType::Uring(args) => args.destroy().await,
        BdevType::Bdev(_) => Ok(()),
    }
}

/// Parse URI and create bdev described in the URI.
/// Return the bdev name (can be different from URI).
pub async fn bdev_create(uri: &str) -> Result<String, BdevCreateDestroy> {
    match nexus_parse_uri(uri)? {
        BdevType::Aio(args) => args.create().await,
        BdevType::Iscsi(args) => args.create().await,
        BdevType::Nvmf(args) => args.create().await,
        BdevType::Uring(args) => args.create().await,
        BdevType::Bdev(name) => Ok(name),
    }
}
