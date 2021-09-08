use std::{
    ffi::{c_void, CString},
    os::raw::c_char,
};

use once_cell::sync::Lazy;

use spdk_sys::{
    spdk_bdev_fn_table,
    spdk_bdev_io,
    spdk_bdev_io_type,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_json_write_array_end,
    spdk_json_write_ctx,
    spdk_json_write_named_array_begin,
    spdk_json_write_val_raw,
};

use crate::{
    bdev::nexus::{
        NexusInstances,
        nexus_bdev::Nexus,
        nexus_io::{nexus_submit_io, NexusBio},
    },
    core::IoType,
};

static NEXUS_FN_TBL: Lazy<NexusFnTable> = Lazy::new(NexusFnTable::new);

pub struct NexusFnTable {
    pub(crate) f_tbl: spdk_bdev_fn_table,
}

unsafe impl Sync for NexusFnTable {}

unsafe impl Send for NexusFnTable {}

/// The FN table are function pointers called by SPDK when work is sent
/// our way. The functions are static, and shared between all instances.

impl NexusFnTable {
    fn new() -> Self {
        let f_tbl = spdk_bdev_fn_table {
            io_type_supported: Some(Self::io_supported),
            submit_request: Some(Self::io_submit),
            get_io_channel: Some(Self::io_channel),
            destruct: Some(Self::destruct),
            dump_info_json: Some(Self::dump_info_json),
            write_config_json: None,
            get_spin_time: None,
            get_module_ctx: None,
        };

        NexusFnTable {
            f_tbl,
        }
    }

    /// get a reference to this static function table to pass on to every
    /// instance
    pub fn table() -> &'static spdk_bdev_fn_table {
        &NEXUS_FN_TBL.f_tbl
    }

    /// check all the children for the specified IO type and return if it is
    /// supported
    extern "C" fn io_supported(
        ctx: *mut c_void,
        io_type: spdk_bdev_io_type,
    ) -> bool {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        let _io_type = IoType::from(io_type);
        match _io_type {
            // we always assume the device supports read/write commands
            // allow NVMe Admin as it is needed for local replicas
            IoType::Read | IoType::Write | IoType::NvmeAdmin => true,
            IoType::Flush
            | IoType::Reset
            | IoType::Unmap
            | IoType::WriteZeros => {
                let supported = nexus.io_is_supported(_io_type);
                if !supported {
                    trace!(
                        "IO type {:?} not supported for {}",
                        _io_type,
                        nexus.bdev().name()
                    );
                }
                supported
            }
            _ => {
                debug!(
                    "un matched IO type {:#?} not supported for {}",
                    _io_type,
                    nexus.bdev().name()
                );
                false
            }
        }
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    /// This function is not called when the IO is re-submitted (see below).
    #[no_mangle]
    pub extern "C" fn io_submit(
        channel: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
    ) {
        let bio = unsafe { NexusBio::nexus_bio_setup(channel, io) };
        nexus_submit_io(bio);
    }

    /// called per core to create IO channels per Nexus instance
    extern "C" fn io_channel(ctx: *mut c_void) -> *mut spdk_io_channel {
        let n = unsafe { Nexus::from_raw(ctx) };
        trace!("{}: Get IO channel", n.bdev().name());
        unsafe { spdk_get_io_channel(ctx) }
    }

    /// called when the nexus instance is unregistered
    extern "C" fn destruct(ctx: *mut c_void) -> i32 {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        let nexus_name = nexus.name.clone();
        nexus.destruct();

        // removing the nexus from the list should cause a drop
        NexusInstances::as_mut().remove_by_name(&nexus_name);

        0
    }

    /// device specific information which is returned
    /// by the get_bdevs RPC call.
    extern "C" fn dump_info_json(
        ctx: *mut c_void,
        w: *mut spdk_json_write_ctx,
    ) -> i32 {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        unsafe {
            spdk_json_write_named_array_begin(
                w,
                "children\0".as_ptr() as *const c_char,
            );
        };

        let data =
            CString::new(serde_json::to_string(&nexus.children).unwrap())
                .unwrap();

        unsafe {
            spdk_json_write_val_raw(
                w,
                data.as_ptr() as *const _,
                data.as_bytes().len() as u64,
            );

            spdk_json_write_array_end(w);
        }
        0
    }
}
