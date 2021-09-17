use std::{marker::PhantomData, os::raw::c_void};

use crate::{IoChannel, IoDevice};
use spdk_sys::{
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_ctx,
};

/// TODO
#[derive(Debug)]
pub enum ChannelTraverseStatus {
    Ok,
    Cancel,
}

impl From<i32> for ChannelTraverseStatus {
    fn from(i: i32) -> Self {
        match i {
            0 => Self::Ok,
            _ => Self::Cancel,
        }
    }
}

impl From<ChannelTraverseStatus> for i32 {
    fn from(s: ChannelTraverseStatus) -> Self {
        match s {
            ChannelTraverseStatus::Ok => 0,
            ChannelTraverseStatus::Cancel => 1,
        }
    }
}

/// TODO
struct TraverseCtx<'a, 'b, 'c, ChannelData, Ctx> {
    channel_cb: Box<
        dyn FnMut(&mut ChannelData, &mut Ctx) -> ChannelTraverseStatus + 'a,
    >,
    done_cb: Box<dyn FnMut(ChannelTraverseStatus, Ctx) + 'b>,
    ctx: Ctx,
    _cd: PhantomData<ChannelData>,
    _c: PhantomData<&'c ()>,
}

impl<'a, 'b, 'c, ChannelData, Ctx> TraverseCtx<'a, 'b, 'c, ChannelData, Ctx> {
    /// TODO
    fn new(
        channel_cb: impl FnMut(&mut ChannelData, &mut Ctx) -> ChannelTraverseStatus
            + 'a,
        done_cb: impl FnMut(ChannelTraverseStatus, Ctx) + 'b,
        caller_ctx: Ctx,
    ) -> Self {
        Self {
            channel_cb: Box::new(channel_cb),
            done_cb: Box::new(done_cb),
            ctx: caller_ctx,
            _cd: Default::default(),
            _c: Default::default(),
        }
    }

    /// TODO
    #[inline]
    fn from_iter(i: *mut spdk_io_channel_iter) -> &'c mut Self {
        unsafe { &mut *(spdk_io_channel_iter_get_ctx(i) as *mut Self) }
    }
}

/// TODO
pub trait IoDeviceChannelTraverse: IoDevice {
    /// Iterates over all I/O channels associated with this I/O device.
    /// TODO
    fn traverse_io_channels<'a, 'b, Ctx>(
        &self,
        channel_cb: impl FnMut(
                &mut <Self as IoDevice>::ChannelData,
                &mut Ctx,
            ) -> ChannelTraverseStatus
            + 'a,
        done_cb: impl FnMut(ChannelTraverseStatus, Ctx) + 'b,
        context: Ctx,
    ) {
        let ctx = Box::new(TraverseCtx::new(channel_cb, done_cb, context));

        // Start I/O channel iteration via SPDK.
        unsafe {
            spdk_for_each_channel(
                self.get_io_device_id(),
                Some(inner_traverse_channel::<Self::ChannelData, Ctx>),
                Box::into_raw(ctx) as *mut c_void,
                Some(inner_traverse_channel_done::<Self::ChannelData, Ctx>),
            );
        }
    }
}

/// Low-level per-channel visitor to be invoked by SPDK I/O channel
/// enumeration logic.
extern "C" fn inner_traverse_channel<ChannelData, Ctx>(
    i: *mut spdk_io_channel_iter,
) {
    let ctx = TraverseCtx::<ChannelData, Ctx>::from_iter(i);
    let mut chan = IoChannel::<ChannelData>::from_iter(i);

    let rc = (ctx.channel_cb)(chan.channel_data_mut(), &mut ctx.ctx);

    unsafe {
        spdk_for_each_channel_continue(i, rc.into());
    }
}

/// Low-level completion callback for SPDK I/O channel enumeration logic.
extern "C" fn inner_traverse_channel_done<ChannelData, Ctx>(
    i: *mut spdk_io_channel_iter,
    status: i32,
) {
    // Reconstruct the context box to let all the resources be properly
    // dropped.
    let ctx = TraverseCtx::<ChannelData, Ctx>::from_iter(i);
    let mut ctx = unsafe { Box::from_raw(ctx) };
    (ctx.done_cb)(status.into(), ctx.ctx);
}
