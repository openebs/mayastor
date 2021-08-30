use std::{
    os::raw::{c_char, c_void},
    ptr,
};

use crate::{cpu_cores::Cores, ffihelper::IntoCString};

use spdk_sys::{spdk_io_device_register, spdk_io_device_unregister};

/// Abstraction over SPDK concept of I/O device.
pub trait IoDevice: Sized {
    /// Type of per-core channel data owned by the I/O channel of this I/O
    /// device.
    type ChannelData;

    /// Called during device unregisration process to allow the client code
    /// to do a clean up.
    /// The default implementation does nothing.
    fn unregister_callback(&self) {
        dbgln!(IoDevice, self.dbg(); "io_device_unregister: final callback");
    }

    /// Called to create a new per-core I/O channel data instance.
    fn io_channel_create(&self) -> Self::ChannelData;

    /// Called to destroy the given per-core I/O channel data instance.
    /// The default implementation just drops it.
    fn io_channel_destroy(&self, _chan: Self::ChannelData) {}

    /// Registers this I/O device within SPDK.
    ///
    /// # Parameters
    ///
    /// * `name` - Optional I/O device name used only for debug purposes.
    ///
    /// TODO: check for register errors (spdk_io_device_register is void).
    /// TODO: check double registeration errors
    fn io_device_register(&self, name: Option<&str>) {
        dbgln!(IoDevice, self.dbg(); "io_device_register");

        // `spdk_io_device_register` copies the name argument internally,
        // so we don't have to keep track on it.
        let name = if let Some(s) = name {
            String::from(s).into_cstring().as_ptr()
        } else {
            std::ptr::null_mut::<c_char>()
        };

        unsafe {
            spdk_io_device_register(
                self.get_io_device_id(),
                Some(inner_io_channel_create::<Self>),
                Some(inner_io_channel_destroy::<Self>),
                std::mem::size_of::<Self::ChannelData>() as u32,
                name,
            );
        }
    }

    /// Unregisters this I/O device from SPDK.
    fn io_device_unregister(&self) {
        dbgln!(IoDevice, self.dbg(); "io_device_unregister [pending free]");

        unsafe {
            spdk_io_device_unregister(
                self.get_io_device_id(),
                Some(inner_io_device_unregister_cb::<Self>),
            )
        };
    }

    /// Returns a unique device identifier for this `IoDevice`.
    fn get_io_device_id(&self) -> *mut c_void {
        self as *const Self as *mut c_void
    }

    /// Makes a debug string for this device.
    fn dbg(&self) -> String {
        format!("id '{:p}'", self.get_io_device_id(),)
    }
}

/// Returns a reference to I/O device for the given I/O device identifier.
///
/// # Parameters
///
/// TODO
fn from_io_device_id<'a, Dev>(ctx: *mut c_void) -> &'a Dev
where
    Dev: IoDevice,
{
    // TODO: NULL check.
    unsafe { &*(ctx as *const Dev) }
}

/// Called by SPDK in order to create a new channel data owned by an I/O
/// channel.
///
/// # Parameters
///
/// TODO
unsafe extern "C" fn inner_io_channel_create<Dev>(
    ctx: *mut c_void,
    buf: *mut c_void,
) -> i32
where
    Dev: IoDevice,
{
    let io_dev = from_io_device_id::<Dev>(ctx);

    dbgln!(IoDevice, io_dev.dbg(); "inner_io_channel_create: buf {:p}", buf);

    let io_chan = io_dev.io_channel_create();
    ptr::write(buf as *mut Dev::ChannelData, io_chan);

    0
}

/// Called by SPDK in order to destroy the channel data owned by an I/O channel.
///
/// # Parameters
///
/// TODO
unsafe extern "C" fn inner_io_channel_destroy<Dev>(
    ctx: *mut c_void,
    buf: *mut c_void,
) where
    Dev: IoDevice,
{
    let io_dev = from_io_device_id::<Dev>(ctx);

    dbgln!(IoDevice, io_dev.dbg(); "inner_io_channel_destroy: buf {:p}", buf);

    let io_chan = ptr::read(buf as *mut Dev::ChannelData);
    io_dev.io_channel_destroy(io_chan);
}

/// I/O device unregister callback.
///
/// # Parameters
///
/// TODO
unsafe extern "C" fn inner_io_device_unregister_cb<Dev>(ctx: *mut c_void)
where
    Dev: IoDevice,
{
    from_io_device_id::<Dev>(ctx).unregister_callback();
}
