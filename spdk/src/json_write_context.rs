use std::{
    ffi::{c_void, CString},
    ptr::NonNull,
};

use serde::Serialize;

use crate::{SpdkResult, SpdkError};

use spdk_sys::{
    spdk_json_write_array_end,
    spdk_json_write_ctx,
    spdk_json_write_named_array_begin,
    spdk_json_write_val_raw,
};

/// Wrapper for SPDK JSON write context.
pub struct JsonWriteContext {
    inner: NonNull<spdk_json_write_ctx>,
}

impl JsonWriteContext {
    /// Writes a serializable value.
    pub fn write<T>(&self, val: &T) -> SpdkResult<()>
    where
        T: ?Sized + Serialize,
    {
        match serde_json::to_string(val) {
            Ok(s) => self.write_string(&s),
            Err(err) => Err(SpdkError::SerdeFailed {
                source: err,
            }),
        }
    }

    /// Writes a `String`.
    pub fn write_string(&self, s: &str) -> SpdkResult<()> {
        let t = CString::new(s).unwrap();
        self.write_raw(t.as_ptr() as *const _, t.as_bytes().len() as usize)
    }

    /// Append bytes directly to the output stream without validation.
    pub(crate) fn write_raw(
        &self,
        data: *const c_void,
        len: usize,
    ) -> SpdkResult<()> {
        let err =
            unsafe { spdk_json_write_val_raw(self.as_ptr(), data, len as u64) };
        if err == 0 {
            Ok(())
        } else {
            Err(SpdkError::JsonWriteFailed {
                code: err,
            })
        }
    }

    /// TODO
    pub fn write_named_array_begin(&self, name: &str) {
        let cname = CString::new(name).unwrap();
        unsafe {
            // TODO: error processing
            spdk_json_write_named_array_begin(self.as_ptr(), cname.as_ptr());
        };
    }

    /// TODO
    pub fn write_array_end(&self) {
        unsafe {
            // TODO: error processing
            spdk_json_write_array_end(self.as_ptr());
        }
    }

    /// TODO
    pub(crate) fn from_ptr(ptr: *mut spdk_json_write_ctx) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
        }
    }

    /// TODO
    fn as_ptr(&self) -> *mut spdk_json_write_ctx {
        self.inner.as_ptr()
    }
}
