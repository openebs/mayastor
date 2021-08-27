use std::os::raw::c_void;

use crate::{cpu_cores::Cores, ffihelper::IntoCString};
use spdk_sys::{spdk_io_device_register, spdk_io_device_unregister};

/// Abstraction over SPDK concept of I/O device.
pub trait IoDevice: Sized {
    /// Type of per-core channel data owned by the I/O channel of this I/O
    /// device.
    type ChannelData: Sized;

    /// Called in order to create a new per-core I/O channel data
    /// instance.
    fn io_channel_create(&self) -> Self::ChannelData;

    /// Called in order to destroy the given per-core I/O channel data
    /// instance. The default implementation just drops it.
    fn io_channel_destroy(&self, _chan: Self::ChannelData) {}

    /// Consumes this `IoDevice` and registers it within SPDK.
    fn io_device_register(self: Box<Self>, name: &str) {
        dbgln!(IoDevice, self.dbg(); "io_device_register");

        // `spdk_io_device_register` copies the name argument internally,
        // so we don't have to keep track on it.
        unsafe {
            spdk_io_device_register(
                Box::into_raw(self) as *mut c_void,
                Some(inner_io_channel_create::<Self>),
                Some(inner_io_channel_destroy::<Self>),
                std::mem::size_of::<Self::ChannelData>() as u32,
                String::from(name).into_cstring().as_ptr(),
            );
        }
    }

    /// Consumes this `IoDevice`, unregisters it, and frees
    /// all associated resources.
    fn io_device_unregister(self: Box<Self>) {
        dbgln!(IoDevice, self.dbg(); "io_device_unregister [pending free]");

        unsafe {
            spdk_io_device_unregister(
                Box::into_raw(self) as *mut c_void,
                Some(inner_io_device_unregister_cb::<Self>),
            )
        };
    }

    /// Returns unique device identifier for this `IoDevice`.
    fn get_io_device_id(&self) -> *const c_void {
        self as *const Self as *const c_void
    }

    /// Makes a debug string for this device.
    fn dbg(&self) -> String {
        format!("IoDev[id '{:p}']", self.get_io_device_id(),)
    }
}

/// Called by SPDK in order to create a new channel data owned by an I/O
/// channel.
unsafe extern "C" fn inner_io_channel_create<D>(
    ctx: *mut c_void,
    buf: *mut c_void,
) -> i32
where
    D: IoDevice,
{
    let io_dev = &*(ctx as *mut D);

    dbgln!(IoDevice, io_dev.dbg(); "inner_io_channel_create: buf {:p}", buf);

    let io_chan = io_dev.io_channel_create();
    std::ptr::write(buf as *mut D::ChannelData, io_chan);

    0
}

/// Called by SPDK in order to destroy the channel data owned by an I/O channel.
unsafe extern "C" fn inner_io_channel_destroy<D>(
    ctx: *mut c_void,
    buf: *mut c_void,
) where
    D: IoDevice,
{
    let io_dev = &*(ctx as *mut D);

    dbgln!(IoDevice, io_dev.dbg(); "inner_io_channel_destroy: buf {:p}", buf);

    let io_chan = std::ptr::read(buf as *mut D::ChannelData);
    io_dev.io_channel_destroy(io_chan);
}

/// I/O device unregister callback.
unsafe extern "C" fn inner_io_device_unregister_cb<D>(ctx: *mut c_void)
where
    D: IoDevice,
{
    let io_dev = &*(ctx as *mut D);
    dbgln!(IoDevice, io_dev.dbg(); "io_device_unregister: final free");
    Box::from_raw(ctx as *mut D);
}
