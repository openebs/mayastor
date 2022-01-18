//!
//! Simple crate for doing device look ups.
pub use block_device::BlkDev;
mod block_device;
use snafu::Snafu;
pub mod mountinfo;
pub mod partition;

#[allow(non_camel_case_types)]
pub mod blkid;
#[derive(Debug, Snafu)]
pub enum DevInfoError {
    #[snafu(display("Device {} not found", path))]
    NotFound { path: String },
    #[snafu(display("Failed to parse value {}", value))]
    ParseError { value: String },
    #[snafu(display("Name qualifier invalid {} ", value))]
    NqnInvalid { value: String },
    #[snafu(display("Device not supported: {} ", value))]
    NotSupported { value: String },
    #[snafu(display("udev internal error {}", value))]
    Udev { value: String },
    #[snafu(display("I/O error: {}", source))]
    Io { source: std::io::Error },
    #[snafu(display("non-UTF8 string"))]
    InvalidStr,
}

#[test]
pub fn basic() {
    use std::convert::TryFrom;

    let path = "nvmf://fooo/nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756";
    let dev = BlkDev::try_from(path).unwrap();
    let path = dev.lookup();
    dbg!(&path);

    let path = "file:///dev/sda";
    let dev = BlkDev::try_from(path).unwrap();
    let path = dev.lookup();
    dbg!(&path);
}
