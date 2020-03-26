use std::ffi::c_void;

use once_cell::sync::Lazy;

use spdk_sys::{
    spdk_bdev_fn_table, spdk_bdev_io, spdk_bdev_io_type, spdk_get_io_channel,
    spdk_io_channel,
};

use crate::bdev::nexus::{
    instances,
    nexus_bdev::Nexus,
    nexus_channel::NexusChannel,
    nexus_io::{io_type, Bio},
};

static NEXUS_FN_TBL: Lazy<NexusFnTable> = Lazy::new(NexusFnTable::new);

pub struct NexusFnTable {
    pub(crate) f_tbl: spdk_bdev_fn_table,
}

unsafe impl Sync for NexusFnTable {}
unsafe impl Send for NexusFnTable {}

/// The FN table are function pointers called by SPDK when work is send
/// our way. The functions are static, and shared between all instances.

impl NexusFnTable {
    fn new() -> Self {
        let mut f_tbl = spdk_bdev_fn_table::default();
        f_tbl.io_type_supported = Some(Self::io_supported);
        f_tbl.submit_request = Some(Self::io_submit);
        f_tbl.get_io_channel = Some(Self::io_channel);
        f_tbl.destruct = Some(Self::destruct);

        NexusFnTable { f_tbl }
    }

    /// get a reference to this static function table to pass on to every
    /// instance
    pub fn table() -> &'static spdk_bdev_fn_table {
        &NEXUS_FN_TBL.f_tbl
    }

    /// check all the children for the specified IO type and return if it
    /// supported
    extern "C" fn io_supported(
        ctx: *mut c_void,
        io_type: spdk_bdev_io_type,
    ) -> bool {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        match io_type {
            // we always assume the device supports read/write commands
            io_type::READ | io_type::WRITE => true,
            io_type::FLUSH | io_type::RESET | io_type::UNMAP => {
                let supported = nexus.io_is_supported(io_type);
                if !supported {
                    trace!(
                        "IO type {:?} not supported for {}",
                        io_type,
                        nexus.bdev.name()
                    );
                }
                supported
            }
            _ => {
                trace!(
                    "IO type {} not supported for {}",
                    io_type,
                    nexus.bdev.name()
                );
                false
            }
        }
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    extern "C" fn io_submit(
        channel: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
    ) {
        if let Some(io_type) = Bio::io_type(io) {
            let mut nio = Bio(io);
            let mut ch = NexusChannel::inner_from_channel(channel);
            let nexus = nio.nexus_as_ref();

            match io_type {
                io_type::READ => {
                    //trace!("{}: Dispatching READ {:p}", nexus.name(), io);
                    nexus.readv(io, &mut ch)
                }
                io_type::WRITE => {
                    //trace!("{}: Dispatching WRITE {:p}", nexus.name(), io);
                    nexus.writev(io, &ch)
                }
                io_type::UNMAP => {
                    if nexus.io_is_supported(io_type) {
                        nexus.unmap(io, &ch)
                    } else {
                        nio.fail();
                    }
                }
                _ => panic!("{} Received unsupported IO!", nexus.name),
            };
        } else {
            // something is very wrong ...
            error!("Received unknown IO type {}", unsafe { (*io).type_ });
        }
    }

    /// called per core to create IO channels per Nexus instance
    extern "C" fn io_channel(ctx: *mut c_void) -> *mut spdk_io_channel {
        let n = unsafe { Nexus::from_raw(ctx) };
        trace!("{}: Get IO channel", n.bdev.name());
        unsafe { spdk_get_io_channel(ctx) }
    }

    /// called when the nexus instance is unregistered
    extern "C" fn destruct(ctx: *mut c_void) -> i32 {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        nexus.destruct();
        let instances = instances();
        // removing the nexus from the list should cause a drop
        instances.retain(|x| x.name != nexus.name);

        0
    }
}
