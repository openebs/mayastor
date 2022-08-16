#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[allow(clippy::all)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub use bindings::*;

pub mod error;
pub mod nvme_device;
mod nvme_tree;
mod nvme_uri;

pub use nvme_device::NvmeDevice;
pub use nvme_uri::NvmeTarget;
