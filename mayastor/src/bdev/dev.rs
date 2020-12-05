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
    nexus_uri::{self, NexusBdevError},
};

mod aio;
mod iscsi;
mod loopback;
mod malloc;
mod null;
mod nvme;
mod nvmf;
mod nvmx;
mod uring;

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
            // really should not be used other than for testing
            "aio" => Ok(Box::new(aio::Aio::try_from(&url)?)),
            "malloc" => Ok(Box::new(malloc::Malloc::try_from(&url)?)),
            "null" => Ok(Box::new(null::Null::try_from(&url)?)),

            // retain this for the time being for backwards compatibility
            "bdev" => Ok(Box::new(loopback::Loopback::try_from(&url)?)),
            // arbitrary bdev found in spdk (used for local replicas)
            "loopback" => Ok(Box::new(loopback::Loopback::try_from(&url)?)),
            // backend iSCSI target - most stable
            "iscsi" => Ok(Box::new(iscsi::Iscsi::try_from(&url)?)),

            // backend NVMF target - fairly unstable (as of Linux 5.2)
            "nvmf" => Ok(Box::new(nvmf::Nvmf::try_from(&url)?)),
            "pcie" => Ok(Box::new(nvme::NVMe::try_from(&url)?)),

            // also for testing - requires Linux 5.1 or higher
            "uring" => Ok(Box::new(uring::Uring::try_from(&url)?)),

            // new NVMF device for Nexus 2.0
            "nvmx" => Ok(Box::new(nvmx::NvmfDeviceTemplate::try_from(&url)?)),

            scheme => Err(NexusBdevError::UriSchemeUnsupported {
                scheme: scheme.to_string(),
            }),
        }
    }
}
