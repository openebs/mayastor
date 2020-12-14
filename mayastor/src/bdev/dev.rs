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

use std::convert::TryFrom;

use snafu::ResultExt;
use url::Url;

use crate::{
    bdev::{BdevCreateDestroy, Uri},
    core::{BlockDevice, BlockDeviceDescriptor, CoreError},
    nexus_uri::{self, NexusBdevError},
};

mod nvmx;

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

            scheme => Err(NexusBdevError::UriSchemeUnsupported {
                scheme: scheme.to_string(),
            }),
        }
    }
}

// Lookup up a block device via its symbolic name.
pub fn device_lookup(name: &str) -> Option<Box<dyn BlockDevice>> {
    debug!("Looking up device by name: {}", name);

    // First, try to lookup NVMF devices bypassing SPDK device namespace,
    // and lookup bdev afterwards.
    nvmx::lookup_by_name(name)
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
    nvmx::open_by_name(name, read_write)
}
