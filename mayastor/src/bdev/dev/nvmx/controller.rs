//!
//!
//! This file contains the main structures for a NVMe controller
use std::{
    convert::From,
    os::raw::c_void,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use spdk_sys::{
    spdk_io_device_register,
    spdk_io_device_unregister,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_get_ns,
    spdk_nvme_ctrlr_process_admin_completions,
    spdk_nvme_detach,
    spdk_poller,
    spdk_poller_register_named,
    spdk_poller_unregister,
};

use crate::bdev::dev::nvmx::{
    channel::{NvmeControllerIoChannel, NvmeIoChannel},
    nvme_bdev_running_config,
    nvme_controller_lookup,
    uri::NvmeControllerContext,
    NvmeNamespace,
    NVME_CONTROLLERS,
};

#[derive(Debug, PartialEq)]
pub enum NvmeControllerState {
    Initializing,
    Running,
}

#[derive(Debug)]
pub(crate) struct NvmeControllerInner {
    pub(crate) namespaces: Vec<Arc<NvmeNamespace>>,
    pub(crate) ctrlr: NonNull<spdk_nvme_ctrlr>,
    pub(crate) adminq_poller: NonNull<spdk_poller>,
}

impl NvmeControllerInner {
    fn new(ctrlr: NonNull<spdk_nvme_ctrlr>) -> Self {
        let adminq_poller = NonNull::new(unsafe {
            spdk_poller_register_named(
                Some(nvme_poll_adminq),
                ctrlr.as_ptr().cast(),
                nvme_bdev_running_config().nvme_adminq_poll_period_us,
                "nvme_poll_adminq\0" as *const _ as *mut _,
            )
        })
        .expect("failed to create poller");

        Self {
            ctrlr,
            adminq_poller,
            namespaces: Vec::new(),
        }
    }
}

/*
 * NVME controller implementation.
 */
#[derive(Debug)]
pub struct NvmeController {
    name: String,
    id: u64,
    prchk_flags: u32,
    pub(crate) state: NvmeControllerState,
    inner: Option<NvmeControllerInner>,
}

unsafe impl Send for NvmeController {}
unsafe impl Sync for NvmeController {}

impl NvmeController {
    /// Creates a new NVMe controller with the given name.
    pub fn new(name: &str, prchk_flags: u32) -> Option<Self> {
        let l = NvmeController {
            name: String::from(name),
            id: 0,
            prchk_flags,
            state: NvmeControllerState::Initializing,
            inner: None,
        };

        debug!("{}: new NVMe controller created", l.get_name());
        Some(l)
    }

    /// returns the name of the current controller
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// returns the protection flags the controller is created with
    pub fn flags(&self) -> u32 {
        self.prchk_flags
    }

    /// returns the ID of the controller
    pub fn id(&self) -> u64 {
        assert_ne!(self.id, 0, "Controller ID is not yet initialized");
        self.id
    }

    fn set_id(&mut self, id: u64) -> u64 {
        assert_ne!(id, 0, "Controller ID can't be zero");
        self.id = id;
        debug!("{} ID set to 0x{:X}", self.name, self.id);
        id
    }

    // As of now, only 1 namespace per controller is supported.
    pub fn namespace(&self) -> Option<Arc<NvmeNamespace>> {
        let inner = self
            .inner
            .as_ref()
            .expect("(BUG) no inner NVMe controller defined yet");

        if let Some(ns) = inner.namespaces.get(0) {
            Some(Arc::clone(ns))
        } else {
            debug!("no namespaces associated with the current controller");
            None
        }
    }

    /// register the controller as an io device
    fn register_io_device(&self) {
        unsafe {
            spdk_io_device_register(
                self.id() as *mut c_void,
                Some(NvmeControllerIoChannel::create),
                Some(NvmeControllerIoChannel::destroy),
                std::mem::size_of::<NvmeIoChannel>() as u32,
                self.get_name().as_ptr() as *const i8,
            )
        }

        debug!(
            "{}: I/O device registered at 0x{:X}",
            self.get_name(),
            self.id()
        );
    }

    /// we should try to avoid this
    pub fn ctrlr_as_ptr(&self) -> *mut spdk_nvme_ctrlr {
        self.inner.as_ref().map_or(std::ptr::null_mut(), |c| {
            let ptr = c.ctrlr.as_ptr();
            debug!("SPDK handle {:p}", ptr);
            ptr
        })
    }

    /// populate name spaces, at current we only populate the first namespace
    fn populate_namespaces(&mut self) {
        let ns = unsafe { spdk_nvme_ctrlr_get_ns(self.ctrlr_as_ptr(), 1) };

        if ns.is_null() {
            warn!(
                "{} no namespaces reported by the NVMe controller",
                self.get_name()
            );
        }

        self.inner.as_mut().unwrap().namespaces =
            vec![Arc::new(NvmeNamespace::from_ptr(ns))]
    }
}

impl Drop for NvmeController {
    fn drop(&mut self) {
        let inner = self.inner.take().expect("nvme inner already gone");
        unsafe { spdk_poller_unregister(&mut inner.adminq_poller.as_ptr()) }

        debug!(
            "{}: unregistering I/O device at 0x{:X}",
            self.get_name(),
            self.id()
        );
        unsafe {
            spdk_io_device_unregister(self.id() as *mut c_void, None);
        }
        let rc = unsafe { spdk_nvme_detach(inner.ctrlr.as_ptr()) };

        assert_eq!(rc, 0, "Failed to detach NVMe controller");
        debug!("{}: NVMe controller successfully detached", self.name);
    }
}

/// return number of completions processed (maybe 0) or negated on error. -ENXIO
//  in the special case that the qpair is failed at the transport layer.
pub extern "C" fn nvme_poll_adminq(ctx: *mut c_void) -> i32 {
    //println!("adminq poll");

    let rc = unsafe {
        spdk_nvme_ctrlr_process_admin_completions(ctx as *mut spdk_nvme_ctrlr)
    };

    if rc == 0 {
        0
    } else {
        1
    }
}

pub(crate) fn connected_attached_cb(
    ctx: &mut NvmeControllerContext,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
) {
    ctx.unregister_poller();
    // we use the ctrlr address as the controller id in the global table
    let cid = ctrlr.as_ptr() as u64;

    // get a reference to our controller we created when we kicked of the async
    // attaching process
    let controller =
        nvme_controller_lookup(&ctx.name()).expect("no controller in the list");

    // clone it now such that we can lock the original, and insert it later.
    let ctl = Arc::clone(&controller);

    let mut controller = controller.lock().unwrap();

    controller.set_id(cid);
    controller.inner = Some(NvmeControllerInner::new(ctrlr));
    controller.register_io_device();

    debug!(
        "{}: I/O device registered at 0x{:X}",
        controller.get_name(),
        controller.id()
    );

    controller.populate_namespaces();
    controller.state = NvmeControllerState::Running;

    nvme_controller_insert(cid, ctl);
    // Wake up the waiter and complete controller registration.
    ctx.sender()
        .send(Ok(()))
        .expect("done callback receiver side disappeared");
}

fn nvme_controller_insert(cid: u64, ctl: Arc<Mutex<NvmeController>>) {
    let mut controllers = NVME_CONTROLLERS.write().unwrap();
    controllers.insert(cid.to_string(), ctl);
}
