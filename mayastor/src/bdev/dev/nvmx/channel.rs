/* I/O channel for NVMe controller, one per core. */

use std::{cmp::max, mem::size_of, os::raw::c_void, ptr::NonNull};

use crate::{bdev::dev::nvmx::NVME_CONTROLLERS, subsys::NvmeBdevOpts};

use spdk_sys::{
    spdk_nvme_ctrlr_alloc_io_qpair,
    spdk_nvme_ctrlr_connect_io_qpair,
    spdk_nvme_ctrlr_get_default_io_qpair_opts,
    spdk_nvme_io_qpair_opts,
    spdk_nvme_poll_group,
    spdk_nvme_poll_group_add,
    spdk_nvme_poll_group_create,
    spdk_nvme_qpair,
    spdk_poller,
    spdk_poller_register_named,
};

#[repr(C)]
pub struct NvmeIoChannel {
    inner: *mut NvmeIoChannelInner,
}

impl NvmeIoChannel {
    #[inline]
    fn from_raw<'a>(p: *mut c_void) -> &'a mut NvmeIoChannel {
        unsafe { &mut *(p as *mut NvmeIoChannel) }
    }

    #[inline]
    fn inner_mut(&mut self) -> &mut NvmeIoChannelInner {
        unsafe { &mut *self.inner }
    }
}

struct NvmeIoChannelInner {
    qpair: NonNull<spdk_nvme_qpair>,
    poll_group: NonNull<spdk_nvme_poll_group>,
    poller: NonNull<spdk_poller>,
}

pub struct NvmeControllerIoChannel {}

extern "C" fn nvme_poll(ctx: *mut c_void) -> i32 {
    let inner = NvmeIoChannel::from_raw(ctx).inner_mut();

    // TODO: only for passing git-commit hooks. Pollers will be used later.
    if inner.poller.as_ptr().is_null() {
        error!("poller is null");
    }

    if inner.poll_group.as_ptr().is_null() {
        error!("poll_group is null");
    }

    if inner.qpair.as_ptr().is_null() {
        error!("qpair is null");
    }

    1
}

impl NvmeControllerIoChannel {
    pub extern "C" fn create(device: *mut c_void, ctx: *mut c_void) -> i32 {
        let id = device as u64;

        debug!("Creating IO channel for controller ID 0x{:X}", id);
        let controllers = NVME_CONTROLLERS.read().unwrap();
        let controller = match controllers.get(&id.to_string()) {
            None => {
                error!("No NVMe controller found for ID 0x{:X}", id);
                return 1;
            }
            Some(c) => c.lock().unwrap(),
        };

        let nvme_channel = NvmeIoChannel::from_raw(ctx);

        // Create qpair for target controller.
        let mut opts = spdk_nvme_io_qpair_opts::default();
        let default_opts = NvmeBdevOpts::default();

        unsafe {
            spdk_nvme_ctrlr_get_default_io_qpair_opts(
                controller.spdk_handle(),
                &mut opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            )
        }

        //opts.__bindgen_anon_1.delay_cmd_submit =
        // default_opts.delay_cmd_submit;
        opts.io_queue_requests =
            max(opts.io_queue_requests, default_opts.io_queue_requests);
        opts.create_only = true;

        let qpair: *mut spdk_nvme_qpair = unsafe {
            spdk_nvme_ctrlr_alloc_io_qpair(
                controller.spdk_handle(),
                &opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            )
        };

        if qpair.is_null() {
            error!("{} Failed to allocate qpair", controller.get_name());
            return 1;
        }

        debug!("{} Qpair successfully allocated", controller.get_name());

        // Create poll group.
        let poll_group: *mut spdk_nvme_poll_group =
            unsafe { spdk_nvme_poll_group_create(ctx) };
        if poll_group.is_null() {
            error!(
                "{} Failed to create a poll group for the qpair",
                controller.get_name()
            );
            return 1;
        }

        // Create poller.
        let poller = unsafe {
            spdk_poller_register_named(
                Some(nvme_poll),
                ctx as *mut c_void,
                default_opts.nvme_ioq_poll_period_us,
                "nvme_poll\0" as *const _ as *mut _,
            )
        };

        let inner = Box::new(NvmeIoChannelInner {
            qpair: NonNull::new(qpair).unwrap(),
            poll_group: NonNull::new(poll_group).unwrap(),
            poller: NonNull::new(poller).unwrap(),
        });

        nvme_channel.inner = Box::into_raw(inner);

        let mut rc = unsafe { spdk_nvme_poll_group_add(poll_group, qpair) };
        if rc != 0 {
            error!(
                "{} failed to add qpair to poll group",
                controller.get_name()
            );
            return 1;
        }

        // Connect qpair.
        rc = unsafe {
            spdk_nvme_ctrlr_connect_io_qpair(controller.spdk_handle(), qpair)
        };

        if rc != 0 {
            error!(
                "{} failed to connect qpair (errno={})",
                controller.get_name(),
                rc
            );
            return 1;
        }

        info!("{} qpair successfully connected", controller.get_name());
        0
    }

    pub extern "C" fn destroy(device: *mut c_void, _ctx: *mut c_void) {
        debug!(
            "Destroying IO channel for controller ID 0x{:X}",
            device as u64
        );
        // let controller = unsafe {NvmeController::from_raw(device)};
        // debug!("{} Destroying IO channels", controller.get_name());
    }
}
