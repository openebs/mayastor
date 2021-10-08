use crate::{
    error::SpdkError::UringCreateFailed,
    ffihelper::IntoCString,
    Bdev,
    BdevOps,
    SpdkResult,
};
use std::os::raw::c_void;

impl<BdevData> Bdev<BdevData>
where
    BdevData: BdevOps,
{
    /// TODO
    pub fn create_uring_bdev(
        name: &str,
        filename: &str,
        block_len: u32,
    ) -> SpdkResult<Self> {
        let r = unsafe {
            crate::libspdk::create_uring_bdev(
                name.into_cstring().as_ptr(),
                filename.into_cstring().as_ptr(),
                block_len,
            )
        };

        if r.is_null() {
            Err(UringCreateFailed {
                name: String::from(name),
            })
        } else {
            Ok(Self::from_ptr(r))
        }
    }

    /// TODO
    pub unsafe fn delete_uring_bdev(
        &mut self,
        complete_cb: extern "C" fn(*mut c_void, i32),
        ctx: *mut c_void,
    ) {
        crate::libspdk::delete_uring_bdev(self.as_ptr(), Some(complete_cb), ctx)
    }
}
