//!
//! nvmeadm deals with finding attached, and connecting remote NVMe devices
//! # Disconnecting all fabric connected devices
//!
//! To discover all subsystems on a remote host we can use a discovery builder
//!
//! # Discovery builder
//! ```rust
//! use nvmeadm::nvmf_discovery::DiscoveryBuilder;
//!
//! let mut disc = DiscoveryBuilder::default()
//!     .transport("tcp".to_string())
//!     .traddr("127.0.0.1".to_string())
//!     .trsvcid(4420)
//!     .build()
//!     .unwrap();
//! // connect to an nqn:
//! let result = disc.connect("mynqn");
//! ```

#[macro_use]
extern crate derive_builder;
extern crate glob;
#[macro_use]
extern crate nix;
#[macro_use]
extern crate ioctl_gen;
#[macro_use]
extern crate enum_primitive_derive;
use crate::nvme_page::NvmeAdminCmd;
use std::{fs, path::Path, str::FromStr};

pub mod error;
pub mod nvme_namespaces;
mod nvme_page;
pub mod nvmf_discovery;
pub mod nvmf_subsystem;

use error::{IoError, NvmeError};
use snafu::ResultExt;

/// the device entry in /dev for issuing ioctls to the kernels nvme driver
const NVME_FABRICS_PATH: &str = "/dev/nvme-fabrics";
/// ioctl for passing any NVMe command to the kernel
const NVME_ADMIN_CMD_IOCTL: u32 =
    iowr!(b'N', 0x41, std::mem::size_of::<NvmeAdminCmd>());

/// Read and parse value from a sysfs file
pub fn parse_value<T>(dir: &Path, file: &str) -> Result<T, NvmeError>
where
    T: FromStr,
    T::Err: ToString,
{
    let path = dir.join(file);
    let s = fs::read_to_string(&path).context(IoError {})?;
    let s = s.trim();

    match s.parse() {
        Ok(v) => Ok(v),
        Err(e) => Err(NvmeError::ValueParseError {
            path: path.as_path().to_str().unwrap().to_string(),
            contents: s.to_string(),
            error: e.to_string(),
        }),
    }
}
