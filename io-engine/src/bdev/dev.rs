//! Implementation of parse() - the main
//! dispatch function for parsing device URIs.
//! This should be the only place where we require any
//! knowledge of the URI schemes and the corresponding
//! bdev types that we can support.
//!
//! Adding support for a new device type requires the following:
//!  - Providing an implementation for the bdev::CreateDestroy trait.
//!  - Providing an implementation for the bdev::GetName trait.
//!  - Providing an implementation for the TryFrom<&Url> trait.
//!  - Arranging for the try_from() method to be called by the parse() function
//!    below for a URI with the appropriate scheme.
//!
//! See mod.rs for the appropriate trait definition(s), and refer
//! to the files in the dev directory for sample implementations.
//!
//! Creating a bdev for any supported device type is now as simple as:
//! ```ignore
//!     let uri = "aio:///tmp/disk1.img?blk_size=512";
//!     bdev::uri::parse(&uri)?.create().await?;
//! ```

use std::collections::HashMap;

use super::nvmx;
use crate::{
    bdev::SpdkBlockDevice,
    bdev_api::BdevError,
    core::{BlockDevice, BlockDeviceDescriptor, CoreError},
};

use url::Url;

pub(crate) mod uri {
    use std::convert::TryFrom;

    use snafu::ResultExt;

    use crate::{
        bdev::{
            aio,
            loopback,
            malloc,
            null_bdev,
            nvme,
            nvmx,
            nx,
            uring,
            BdevCreateDestroy,
        },
        bdev_api::{self, BdevError},
    };

    pub fn parse(
        uri: &str,
    ) -> Result<Box<dyn BdevCreateDestroy<Error = BdevError>>, BdevError> {
        let url = url::Url::parse(uri).context(bdev_api::UriParseFailed {
            uri: uri.to_string(),
        })?;

        match url.scheme() {
            "aio" => Ok(Box::new(aio::Aio::try_from(&url)?)),
            "bdev" | "loopback" => {
                Ok(Box::new(loopback::Loopback::try_from(&url)?))
            }
            "malloc" => Ok(Box::new(malloc::Malloc::try_from(&url)?)),
            "null" => Ok(Box::new(null_bdev::Null::try_from(&url)?)),
            "nvmf" => Ok(Box::new(nvmx::NvmfDeviceTemplate::try_from(&url)?)),
            "pcie" => Ok(Box::new(nvme::NVMe::try_from(&url)?)),
            "uring" => Ok(Box::new(uring::Uring::try_from(&url)?)),
            "nexus" => Ok(Box::new(nx::Nexus::try_from(&url)?)),

            scheme => Err(BdevError::UriSchemeUnsupported {
                scheme: scheme.to_string(),
            }),
        }
    }
}

pub(crate) fn reject_unknown_parameters(
    url: &Url,
    parameters: HashMap<String, String>,
) -> Result<(), BdevError> {
    if !parameters.is_empty() {
        let invalid_parameters = parameters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(BdevError::InvalidUri {
            uri: url.to_string(),
            message: format!("unrecognized parameter(s): {invalid_parameters}"),
        })
    } else {
        Ok(())
    }
}

// Lookup up a block device via its symbolic name.
pub fn device_lookup(name: &str) -> Option<Box<dyn BlockDevice>> {
    // First try to lookup NVMF devices, then try to lookup SPDK native devices.
    nvmx::lookup_by_name(name).or_else(|| SpdkBlockDevice::lookup_by_name(name))
}

/// Lookup up device name by its uri.
pub fn device_name(uri: &str) -> Result<String, BdevError> {
    Ok(uri::parse(uri)?.get_name())
}

pub async fn device_create(uri: &str) -> Result<String, BdevError> {
    uri::parse(uri)?.create().await
}

pub async fn device_destroy(uri: &str) -> Result<(), BdevError> {
    uri::parse(uri)?.destroy().await
}

pub fn device_open(
    name: &str,
    read_write: bool,
) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
    // First try to open NVMF devices, then try to lookup SPDK native devices.
    nvmx::open_by_name(name, read_write)
        .or_else(|_| SpdkBlockDevice::open_by_name(name, read_write))
}
