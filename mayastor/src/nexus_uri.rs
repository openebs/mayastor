use crate::{aio_dev::AioBdev, iscsi_dev::IscsiBdev, nvme_dev::NvmfBdev};
use std::convert::TryFrom;
use url::Url;

#[derive(Debug)]
pub enum UriError {
    /// the provided scheme is invalid
    InvalidScheme,
    /// the schema is valid but we do not support it
    Unsupported,
    /// the path segment of the uri is invalid for the schema
    InvalidPathSegment,
}
/// enum type of URL to args we currently support
#[derive(Debug)]
pub enum BdevType {
    /// you should not be using this other then testing
    Aio(AioBdev),
    /// backend iSCSI target most stable
    Iscsi(IscsiBdev),
    /// backend NVMF target pretty unstable as of Linux 5.2
    Nvmf(NvmfBdev),
    /// bdev type is arbitrary bdev found in spdk (used for local replicas)
    Bdev(String),
}

/// Converts an array of Strings into the appropriate args type
/// to construct the children from which we create the nexus.
pub fn nexus_uri_parse_vec(uris: &[String]) -> Result<Vec<BdevType>, UriError> {
    let mut results = Vec::new();
    for target in uris {
        results.push(nexus_parse_uri(target)?);
    }

    Ok(results)
}
/// Parse the given URI into a ChildBdev
pub fn nexus_parse_uri(uri: &str) -> Result<BdevType, UriError> {
    if let Ok(uri) = Url::parse(uri) {
        let bdev_type = match uri.scheme() {
            "aio" => BdevType::Aio(AioBdev::try_from(&uri)?),
            "iscsi" => BdevType::Iscsi(IscsiBdev::try_from(&uri)?),
            "nvmf" => BdevType::Nvmf(NvmfBdev::try_from(&uri)?),
            // strip the first slash in uri path
            "bdev" => BdevType::Bdev(uri.path()[1..].to_string()),
            _ => {
                warn!("Unknown URL scheme {}", uri.to_string());
                return Err(UriError::Unsupported);
            }
        };

        Ok(bdev_type)
    } else {
        Err(UriError::InvalidScheme)
    }
}
