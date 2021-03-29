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
//!     bdev::Uri::parse(&uri)?.create().await?;
//! ```

use std::{collections::HashMap, convert::TryFrom};

use snafu::ResultExt;
use url::Url;

use crate::{
    bdev::{BdevCreateDestroy, Uri},
    core::{BlockDevice, BlockDeviceDescriptor, CoreError},
    nexus_uri::{self, NexusBdevError},
};

mod aio;
mod device;
mod loopback;
mod malloc;
mod nvmx;
mod null;
mod iscsi;
mod nvme;
mod uring;

pub(crate) use device::SpdkBlockDevice;

impl Uri {
    pub fn parse(
        uri: &str,
    ) -> Result<
        Box<dyn BdevCreateDestroy<Error = NexusBdevError>>,
        NexusBdevError,
    > {
        let url = Url::parse(uri).context(nexus_uri::UrlParseError {
            uri: uri.to_string(),
        })?;

        match url.scheme() {
            // backend NVMF target - fairly unstable (as of Linux 5.2)
            "nvmf" => Ok(Box::new(nvmx::NvmfDeviceTemplate::try_from(&url)?)),
            "malloc" => Ok(Box::new(malloc::Malloc::try_from(&url)?)),
            "aio" => Ok(Box::new(aio::Aio::try_from(&url)?)),
            "bdev" => Ok(Box::new(loopback::Loopback::try_from(&url)?)),
            "null" => Ok(Box::new(null::Null::try_from(&url)?)),
            "loopback" => Ok(Box::new(loopback::Loopback::try_from(&url)?)),
            "iscsi" => Ok(Box::new(iscsi::Iscsi::try_from(&url)?)),
            "pcie" => Ok(Box::new(nvme::NVMe::try_from(&url)?)),

            // also for testing - requires Linux 5.1 or higher
            "uring" => Ok(Box::new(uring::Uring::try_from(&url)?)),

            scheme => Err(NexusBdevError::UriSchemeUnsupported {
                scheme: scheme.to_string(),
            }),
        }
    }
}

fn reject_unknown_parameters(
    url: &Url,
    parameters: HashMap<String, String>,
) -> Result<(), NexusBdevError> {
    if !parameters.is_empty() {
        let invalid_parameters = parameters
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(", ");
        Err(NexusBdevError::UriInvalid {
            uri: url.to_string(),
            message: format!(
                "unrecognized parameters: {}.",
                invalid_parameters
            ),
        })
    } else {
        Ok(())
    }
}

// Lookup up a block device via its symbolic name.
pub fn device_lookup(name: &str) -> Option<Box<dyn BlockDevice>> {
    debug!("Looking up device by name: {}", name);
    // First try to lookup NVMF devices, then try to lookup SPDK native devices.
    nvmx::lookup_by_name(name).or_else(|| SpdkBlockDevice::lookup_by_name(name))
}

#[instrument]
pub async fn device_create(uri: &str) -> Result<String, NexusBdevError> {
    Uri::parse(uri)?.create().await
}

#[instrument]
pub async fn device_destroy(uri: &str) -> Result<(), NexusBdevError> {
    Uri::parse(uri)?.destroy().await
}

#[instrument]
pub fn device_open(
    name: &str,
    read_write: bool,
) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
    // First try to open NVMF devices, then try to lookup SPDK native devices.
    nvmx::open_by_name(name, read_write)
        .or_else(|_| SpdkBlockDevice::open_by_name(name, read_write))
}
