use std::ptr::null_mut;

use spdk_sys::{spdk_conf_find_section, spdk_conf_section_get_nval};

use crate::bdev::{nexus::nexus_instance_new, parse_config_param};

pub(crate) fn parse_ini_config_file() -> i32 {
    let section_name = std::ffi::CString::new("Nexus").unwrap();
    let sp =
        unsafe { spdk_conf_find_section(null_mut(), section_name.as_ptr()) };

    if sp.is_null() {
        return 0;
    }

    let mut devnum = 0;
    loop {
        let dev = unsafe {
            let dev_string = std::ffi::CString::new("Dev").unwrap();
            spdk_conf_section_get_nval(sp, dev_string.as_ptr(), devnum)
        };
        if dev.is_null() {
            break;
        }

        let name: String = unsafe {
            match parse_config_param(sp, "Dev", devnum, 0) {
                Ok(val) => val,
                Err(err) => {
                    error!("{}", err);
                    return libc::EINVAL;
                }
            }
        };

        // parse bdev block size
        let block_size: u32 = unsafe {
            match parse_config_param(sp, "Dev", devnum, 2) {
                Ok(val) => val,
                Err(err) => {
                    error!("{}", err);
                    return libc::EINVAL;
                }
            }
        };

        // parse bdev size
        let lu_size: u64 = unsafe {
            match parse_config_param::<u64>(sp, "Dev", devnum, 1) {
                Ok(val) => val * 1024 * 1024 / u64::from(block_size),
                Err(err) => {
                    error!("{}", err);
                    return libc::EINVAL;
                }
            }
        };
        let mut child_bdevs = Vec::new();
        for i in 3.. {
            unsafe {
                match parse_config_param::<String>(sp, "Dev", devnum, i) {
                    Ok(val) => child_bdevs.push(val),
                    Err(_) => break,
                }
            }
        }

        debug!(
            "Found Nexus device {}: block_count={}, block_size={} with nvmf targets {:?}",
            name, lu_size, block_size, &child_bdevs
        );

        nexus_instance_new(name, lu_size, child_bdevs);
        devnum += 1;
    }
    0
}
