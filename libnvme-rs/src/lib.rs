#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[allow(clippy::all)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub use bindings::*;

pub mod error;
pub mod nvme_namespaces;
mod nvme_tree;
mod nvme_uri;

use error::{IoError, NvmeError};
use snafu::ResultExt;
use std::{fs, path::Path, str::FromStr};

pub use nvme_uri::NvmeTarget;

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
