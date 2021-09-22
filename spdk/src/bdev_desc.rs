use std::{marker::PhantomData, ptr::NonNull};

use crate::{
    ffihelper::{errno_error, ErrnoResult, IntoCString},
    Bdev,
    BdevOps,
};
use spdk_sys::{
    spdk_bdev,
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_desc_get_bdev,
    spdk_bdev_event_type,
    spdk_bdev_open_ext,
    SPDK_BDEV_EVENT_MEDIA_MANAGEMENT,
    SPDK_BDEV_EVENT_REMOVE,
    SPDK_BDEV_EVENT_RESIZE,
};
use std::os::raw::c_void;

/// Wrapper for `spdk_bdev_desc`.
#[derive(Debug)]
pub struct BdevDesc<BdevData>
where
    BdevData: BdevOps,
{
    inner: NonNull<spdk_bdev_desc>,
    _data: PhantomData<BdevData>,
}

impl<BdevData> BdevDesc<BdevData>
where
    BdevData: BdevOps,
{
    /// TODO
    pub fn open(
        bdev_name: &str,
        rw: bool,
        // event_cb: impl FnMut(BdevEvent, Bdev<BdevData>) + 'static,
        event_cb: fn(BdevEvent, Bdev<BdevData>),
    ) -> ErrnoResult<Self> {
        let mut desc: *mut spdk_bdev_desc = std::ptr::null_mut();

        // let ctx = Box::new(BdevEventContext::<BdevData> {
        //     event_cb: Box::new(event_cb),
        // });

        let rc = unsafe {
            spdk_bdev_open_ext(
                bdev_name.into_cstring().as_ptr(),
                rw,
                Some(inner_bdev_event_cb::<BdevData>),
                // Box::into_raw(ctx) as *mut c_void,
                event_cb as *mut c_void,
                &mut desc,
            )
        };

        if rc != 0 {
            errno_error::<Self>(rc)
        } else {
            assert_eq!(desc.is_null(), false);
            Ok(Self::from_ptr(desc))
        }
    }

    /// TODO
    pub fn close(&mut self) {
        unsafe {
            // // Free the event callback context.
            // Box::from_raw(self.as_mut().callback.ctx);

            // Close the desc.
            spdk_bdev_close(self.as_ptr());
        }
    }

    /// Returns a Bdev associated with this descriptor.
    /// A descriptor cannot exist without a Bdev.
    /// TODO
    pub fn bdev(&self) -> Bdev<BdevData> {
        let b = unsafe { spdk_bdev_desc_get_bdev(self.as_ptr()) };
        Bdev::from_ptr(b)
    }

    /// Returns a pointer to the underlying `spdk_bdev_desc` structure.
    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev_desc {
        self.inner.as_ptr()
    }

    /// TODO
    pub fn legacy_as_ptr(&self) -> *mut spdk_bdev_desc {
        self.as_ptr()
    }

    // /// Returns a reference to the underlying `spdk_bdev_desc` structure.
    // pub(crate) fn as_ref(&self) -> &spdk_bdev_desc {
    //     unsafe { self.inner.as_ref() }
    // }

    // /// Returns a mutable reference to the underlying `spdk_bdev_desc` structure.
    // pub(crate) fn as_mut(&mut self) -> &mut spdk_bdev_desc {
    //     unsafe { self.inner.as_mut() }
    // }

    /// TODO
    pub(crate) fn from_ptr(ptr: *mut spdk_bdev_desc) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
            _data: Default::default(),
        }
    }

    /// TODO
    pub fn legacy_from_ptr(ptr: *mut spdk_bdev_desc) -> Self {
        Self::from_ptr(ptr)
    }
}

impl<BdevData> Clone for BdevDesc<BdevData>
where
    BdevData: BdevOps,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            _data: Default::default(),
        }
    }
}

/// TODO
pub enum BdevEvent {
    Remove,
    Resize,
    MediaManagement,
}

impl From<spdk_bdev_event_type> for BdevEvent {
    fn from(t: spdk_bdev_event_type) -> Self {
        match t {
            SPDK_BDEV_EVENT_REMOVE => BdevEvent::Remove,
            SPDK_BDEV_EVENT_RESIZE => BdevEvent::Resize,
            SPDK_BDEV_EVENT_MEDIA_MANAGEMENT => BdevEvent::MediaManagement,
            _ => panic!("Bad Bdev event type: {}", t),
        }
    }
}

impl From<BdevEvent> for spdk_bdev_event_type {
    fn from(t: BdevEvent) -> Self {
        match t {
            BdevEvent::Remove => SPDK_BDEV_EVENT_REMOVE,
            BdevEvent::Resize => SPDK_BDEV_EVENT_RESIZE,
            BdevEvent::MediaManagement => SPDK_BDEV_EVENT_MEDIA_MANAGEMENT,
        }
    }
}

// /// TODO
// struct BdevEventContext<BdevData>
// where
//     BdevData: BdevOps,
// {
//     event_cb: Box<dyn FnMut(BdevEvent, Bdev<BdevData>) + 'static>,
// }

/// TODO
unsafe extern "C" fn inner_bdev_event_cb<BdevData>(
    event: spdk_bdev_event_type,
    bdev: *mut spdk_bdev,
    ctx: *mut c_void,
) where
    BdevData: BdevOps,
{
    // let ctx = &mut *(ctx as *mut BdevEventContext<_>);
    let ctx = std::mem::transmute::<_, fn(BdevEvent, Bdev<BdevData>)>(ctx);
    (ctx)(event.into(), Bdev::<BdevData>::from_ptr(bdev));
}
