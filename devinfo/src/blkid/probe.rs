use crate::blkid::{
    blkid_do_probe,
    blkid_do_safeprobe,
    blkid_free_probe,
    blkid_new_probe,
    blkid_new_probe_from_filename,
    blkid_probe,
    blkid_probe_has_value,
    blkid_probe_lookup_value,
};
use core::slice;
use std::{
    ffi::{CStr, CString},
    path::Path,
};

use crate::blkid::to_result;
pub struct Probe(blkid_probe);
use crate::DevInfoError;

impl Probe {
    pub fn new() -> Result<Probe, DevInfoError> {
        unsafe { Ok(Probe(to_result(blkid_new_probe())?)) }
    }

    pub fn new_from_filename<P: AsRef<Path>>(
        path: P,
    ) -> Result<Probe, DevInfoError> {
        let path =
            CString::new(path.as_ref().as_os_str().to_string_lossy().as_ref())
                .expect("provided path contained null bytes");

        unsafe {
            Ok(Probe(to_result(blkid_new_probe_from_filename(
                path.as_ptr(),
            ))?))
        }
    }

    pub fn do_probe(&self) -> Result<bool, DevInfoError> {
        unsafe { to_result(blkid_do_probe(self.0)).map(|v| v == 1) }
    }

    pub fn do_safe_probe(&self) -> Result<i32, DevInfoError> {
        unsafe { to_result(blkid_do_safeprobe(self.0)) }
    }

    pub fn has_value(self, name: &str) -> bool {
        let name = CString::new(name).unwrap();
        let ret = unsafe { blkid_probe_has_value(self.0, name.as_ptr()) };
        ret == 1
    }

    /// Fetch a value by name.
    pub fn lookup_value(self, name: &str) -> Result<String, DevInfoError> {
        let name = CString::new(name).unwrap();
        let mut data_ptr = std::ptr::null();
        let mut len = 0;
        unsafe {
            to_result::<i32>(blkid_probe_lookup_value(
                self.0,
                name.as_ptr(),
                &mut data_ptr,
                &mut len,
            ))?;

            let str = CStr::from_bytes_with_nul(slice::from_raw_parts(
                data_ptr.cast(),
                len as usize,
            ))
            .map_err(|_e| DevInfoError::InvalidStr {})?
            .to_str()
            .map_err(|_e| DevInfoError::InvalidStr {})?
            .to_string();
            Ok(str)
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        if self.0.is_null() {
            // No cleanup needed
            return;
        }
        unsafe {
            blkid_free_probe(self.0);
        }
    }
}
