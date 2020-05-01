use std::ffi::CStr;

pub use aio_dev::{AioBdev, AioParseError};
pub use iscsi_dev::{IscsiBdev, IscsiParseError};
pub use nexus::{
    nexus_bdev::{nexus_create, nexus_lookup, Nexus, NexusStatus},
    nexus_label::{GPTHeader, GptEntry},
};
pub use nvmf_dev::{NvmeCtlAttachReq, NvmfParseError};
use spdk_sys::{spdk_conf_section, spdk_conf_section_get_nmval};
pub use uring_dev::{UringBdev, UringParseError};

/// Allocate C string and return pointer to it.
/// NOTE: you must explicitly free it, otherwise the memory is leaked!
macro_rules! c_str {
    ($lit:expr) => {
        std::ffi::CString::new($lit).unwrap().into_raw();
    };
}

mod aio_dev;
mod iscsi_dev;
pub(crate) mod nexus;
mod nvmf_dev;
mod uring_dev;
pub mod uring_util;

unsafe fn parse_config_param<T>(
    sp: *mut spdk_conf_section,
    dev_name: &str,
    dev_num: i32,
    position: i32,
) -> Result<T, String>
where
    T: std::str::FromStr,
{
    let dev_name_c = std::ffi::CString::new(dev_name).unwrap();
    let val =
        spdk_conf_section_get_nmval(sp, dev_name_c.as_ptr(), dev_num, position);
    if val.is_null() {
        return Err(format!(
            "Config value for {}{} at position {} not found",
            dev_name, dev_num, position
        ));
    }
    let val = CStr::from_ptr(val).to_str().unwrap().parse::<T>();
    match val {
        Err(_) => Err(format!(
            "Invalid config value for {}{} at position {}",
            dev_name, dev_num, position
        )),
        Ok(val) => Ok(val),
    }
}
