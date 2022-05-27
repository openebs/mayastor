use std::{os::raw::c_void, ptr::NonNull};

use crate::ffihelper::IntoCString;
use spdk_rs::libspdk::{
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
    spdk_io_channel_iter_get_ctx,
    spdk_io_device_register,
    spdk_io_device_unregister,
};

/// TODO
#[derive(Debug)]
pub struct IoDevice(NonNull<c_void>);

// TODO: is `IoDevice` really a Sync/Send type?
unsafe impl Sync for IoDevice {}
unsafe impl Send for IoDevice {}

/// TODO
type IoDeviceCreateCb = unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32;

/// TODO
type IoDeviceDestroyCb = unsafe extern "C" fn(*mut c_void, *mut c_void);

/// Abstraction around SPDK I/O device, which hides low-level SPDK
/// API and provides high-level API for I/O channel traversal.
impl IoDevice {
    /// Create a new I/O device using target address as a unique
    /// I/O device identifier.
    pub fn new<C: Sized>(
        devptr: NonNull<c_void>,
        name: &str,
        create_cb: Option<IoDeviceCreateCb>,
        destroy_cb: Option<IoDeviceDestroyCb>,
    ) -> Self {
        let cname = name.into_cstring();
        unsafe {
            spdk_io_device_register(
                devptr.as_ptr(),
                create_cb,
                destroy_cb,
                std::mem::size_of::<C>() as u32,
                cname.as_ptr(),
            )
        }

        debug!("{} I/O device registered at {:p}", name, devptr.as_ptr());

        Self(devptr)
    }

    /// Iterate over all I/O channels associated with this I/O device.
    pub fn traverse_io_channels<T, I: 'static>(
        &self,
        channel_cb: impl FnMut(&mut I, &mut T) -> i32 + 'static,
        done_cb: impl FnMut(i32, T) + 'static,
        ctx_getter: impl Fn(*mut spdk_io_channel) -> &'static mut I + 'static,
        caller_ctx: T,
    ) {
        struct TraverseCtx<N, C: 'static> {
            channel_cb: Box<dyn FnMut(&mut C, &mut N) -> i32 + 'static>,
            done_cb: Box<dyn FnMut(i32, N) + 'static>,
            ctx_getter:
                Box<dyn Fn(*mut spdk_io_channel) -> &'static mut C + 'static>,
            ctx: N,
        }

        let traverse_ctx = Box::into_raw(Box::new(TraverseCtx {
            channel_cb: Box::new(channel_cb),
            done_cb: Box::new(done_cb),
            ctx_getter: Box::new(ctx_getter),
            ctx: caller_ctx,
        }));
        assert!(
            !traverse_ctx.is_null(),
            "Failed to allocate context for I/O channels iteration"
        );

        /// Low-level per-channel visitor to be invoked by SPDK I/O channel
        /// enumeration logic.
        extern "C" fn _visit_channel<V, P: 'static>(
            i: *mut spdk_io_channel_iter,
        ) {
            let traverse_ctx = unsafe {
                let p =
                    spdk_io_channel_iter_get_ctx(i) as *mut TraverseCtx<V, P>;
                &mut *p
            };
            let io_channel = unsafe {
                let ch = spdk_io_channel_iter_get_channel(i);
                (traverse_ctx.ctx_getter)(ch)
            };

            let rc =
                (traverse_ctx.channel_cb)(io_channel, &mut traverse_ctx.ctx);

            unsafe {
                spdk_for_each_channel_continue(i, rc);
            }
        }

        /// Low-level completion callback for SPDK I/O channel enumeration
        /// logic.
        extern "C" fn _visit_channel_done<V, P: 'static>(
            i: *mut spdk_io_channel_iter,
            status: i32,
        ) {
            // Reconstruct the context box to let all the resources be properly
            // dropped.
            let mut traverse_ctx = unsafe {
                Box::<TraverseCtx<V, P>>::from_raw(
                    spdk_io_channel_iter_get_ctx(i) as *mut TraverseCtx<V, P>,
                )
            };

            (traverse_ctx.done_cb)(status, traverse_ctx.ctx);
        }

        // Start I/O channel iteration via SPDK.
        unsafe {
            spdk_for_each_channel(
                self.0.as_ptr(),
                Some(_visit_channel::<T, I>),
                traverse_ctx as *mut c_void,
                Some(_visit_channel_done::<T, I>),
            );
        }
    }
}

impl Drop for IoDevice {
    fn drop(&mut self) {
        debug!("unregistering I/O device at {:p}", self.0.as_ptr());

        unsafe {
            spdk_io_device_unregister(self.0.as_ptr(), None);
        }
    }
}
