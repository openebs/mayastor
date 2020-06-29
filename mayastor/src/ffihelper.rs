use std::{
    error::Error,
    ffi::{CStr, CString},
    os::{
        raw,
        raw::{c_char, c_void},
    },
    ptr::NonNull,
};

use futures::channel::oneshot;
use nix::errno::Errno;

pub(crate) trait AsStr {
    fn as_str(&self) -> &str;
}

impl AsStr for *const c_char {
    fn as_str(&self) -> &str {
        unsafe {
            CStr::from_ptr(*self).to_str().unwrap_or_else(|_| {
                warn!("invalid UTF8 data");
                Default::default()
            })
        }
    }
}

impl AsStr for [c_char] {
    fn as_str(&self) -> &str {
        unsafe {
            CStr::from_ptr(self.as_ptr()).to_str().unwrap_or_else(|_| {
                warn!("invalid UTF8 data");
                Default::default()
            })
        }
    }
}

pub(crate) trait IntoCString {
    fn into_cstring(self) -> CString;
}

impl IntoCString for String {
    fn into_cstring(self) -> CString {
        CString::new(self).unwrap()
    }
}

impl IntoCString for &str {
    fn into_cstring(self) -> CString {
        CString::new(self).unwrap()
    }
}

/// Result having Errno error.
pub type ErrnoResult<T, E = Errno> = Result<T, E>;

/// Construct callback argument for spdk async function.
/// The argument is a oneshot sender channel for result of the operation.
pub fn cb_arg<T>(sender: oneshot::Sender<T>) -> *mut c_void {
    Box::into_raw(Box::new(sender)) as *const _ as *mut c_void
}

/// A generic callback for spdk async functions expecting to be called with
/// single argument which is a sender channel to notify the other end about
/// the result.
pub extern "C" fn done_cb<T>(sender_ptr: *mut c_void, val: T)
where
    T: std::fmt::Debug,
{
    let sender =
        unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<T>) };

    // the receiver side might be gone, if this happens it either means that the
    // function has gone out of scope or that the future was cancelled. We can
    // not cancel futures as they are driven by reactor. We currently fail
    // hard if the receiver is gone but in reality the determination of it
    // being fatal depends largely on what the future was supposed to do.
    sender
        .send(val)
        .expect("done callback receiver side disappeared");
}

/// Callback for spdk async functions called with errno value.
/// Special case of the more general done_cb() above. The advantage being
/// that it converts the errno value to Result before it is sent, so the
/// receiver can use receiver.await.expect(...)? notation for processing
/// the result.
pub extern "C" fn done_errno_cb(sender_ptr: *mut c_void, errno: i32) {
    let sender = unsafe {
        Box::from_raw(sender_ptr as *mut oneshot::Sender<ErrnoResult<()>>)
    };

    sender
        .send(errno_result_from_i32((), errno))
        .expect("done callback receiver side disappeared");
}

/// Utility function for converting i32 errno value returned by SPDK to
/// a Result with Errno error holding the appropriate message for given
/// errno value. The idea is that callbacks should send this over the
/// channel and caller can then use just `.await.expect(...)?` expression
/// to process the result.
pub fn errno_result_from_i32<T>(val: T, errno: i32) -> ErrnoResult<T> {
    if errno == 0 {
        Ok(val)
    } else {
        Err(Errno::from_i32(errno.abs()))
    }
}

/// Helper routines to convert from FFI functions
pub(crate) trait FfiResult {
    type Ok;
    fn to_result<E: Error, F>(self, f: F) -> Result<Self::Ok, E>
    where
        F: FnOnce(Self) -> E,
        Self: Sized;
}

impl<T> FfiResult for *mut T {
    type Ok = NonNull<T>;

    #[inline]
    fn to_result<E: Error, F>(self, f: F) -> Result<Self::Ok, E>
    where
        F: FnOnce(Self) -> E,
    {
        NonNull::new(self).ok_or_else(|| f(self))
    }
}

impl<T> FfiResult for *const T {
    type Ok = *const T;

    #[inline]
    fn to_result<E: Error, F>(self, f: F) -> Result<Self::Ok, E>
    where
        F: FnOnce(Self) -> E,
    {
        if self.is_null() {
            Err(f(self))
        } else {
            Ok(self)
        }
    }
}

impl FfiResult for raw::c_int {
    type Ok = ();

    #[inline]
    fn to_result<E: Error + snafu::Error, F>(self, f: F) -> Result<Self::Ok, E>
    where
        F: FnOnce(Self) -> E,
    {
        if self == 0 {
            Ok(())
        } else {
            Err(f(self.abs()))
        }
    }
}

impl FfiResult for u32 {
    type Ok = ();

    #[inline]
    fn to_result<E: Error + snafu::Error, F>(self, f: F) -> Result<Self::Ok, E>
    where
        F: FnOnce(Self) -> E,
    {
        if self == 0 {
            Ok(())
        } else {
            Err(f(self))
        }
    }
}
