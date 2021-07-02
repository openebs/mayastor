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

    /// Fetch a value by name.
    pub fn lookup_value(self, name: &str) -> Result<String, DevInfoError> {
        let name = CString::new(name).unwrap();
        let data_ptr = std::ptr::null_mut();
        let mut len = 0;
        unsafe {
            to_result::<i32>(blkid_probe_lookup_value(
                self.0,
                name.as_ptr(),
                data_ptr,
                &mut len,
            ))?;
            Ok(CStr::from_ptr(data_ptr.cast())
                .to_string_lossy()
                .to_string())
        }
    }

    /// Returns `true` if the value exists.
    pub fn has_value(&self, name: &str) -> Result<bool, DevInfoError> {
        let name =
            CString::new(name).expect("provided path contained null bytes");

        unsafe {
            to_result(blkid_probe_has_value(self.0, name.as_ptr()))
                .map(|v| v == 1)
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
