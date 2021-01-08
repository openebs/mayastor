//!
//!
//! This file contains the main structures for a NVMe controller
use std::{convert::From, os::raw::c_void, ptr::NonNull, sync::Arc};

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
        let inner = self.inner.take().expect("NVMe inner already gone");
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
    let controller = NVME_CONTROLLERS
        .lookup_by_name(&ctx.name())
        .expect("no controller in the list");

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

    NVME_CONTROLLERS.insert_controller(cid.to_string(), ctl);
    // Wake up the waiter and complete controller registration.
    ctx.sender()
        .send(Ok(()))
        .expect("done callback receiver side disappeared");
}

pub(crate) mod options {
    use std::mem::size_of;

    use spdk_sys::{
        spdk_nvme_ctrlr_get_default_ctrlr_opts,
        spdk_nvme_ctrlr_opts,
    };

    /// structure that holds the default NVMe controller options. This is
    /// different from ['NvmeBdevOpts'] as it exposes more control over
    /// variables.

    pub struct NvmeControllerOpts(spdk_nvme_ctrlr_opts);
    impl NvmeControllerOpts {
        pub fn as_ptr(&self) -> *const spdk_nvme_ctrlr_opts {
            &self.0
        }
    }

    impl Default for NvmeControllerOpts {
        fn default() -> Self {
            let mut default = spdk_nvme_ctrlr_opts::default();
            unsafe {
                spdk_nvme_ctrlr_get_default_ctrlr_opts(
                    &mut default,
                    size_of::<spdk_nvme_ctrlr_opts>() as u64,
                );
            }

            Self(default)
        }
    }

    #[derive(Debug, Default)]
    pub struct Builder {
        admin_timeout_ms: Option<u32>,
        disable_error_logging: Option<bool>,
        fabrics_connect_timeout_us: Option<u64>,
        transport_retry_count: Option<u8>,
        keep_alive_timeout_ms: Option<u32>,
    }

    #[allow(dead_code)]
    impl Builder {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_admin_timeout_ms(mut self, timeout: u32) -> Self {
            self.admin_timeout_ms = Some(timeout);
            self
        }
        pub fn with_fabrics_connect_timeout_us(mut self, timeout: u64) -> Self {
            self.fabrics_connect_timeout_us = Some(timeout);
            self
        }

        pub fn with_transport_retry_count(mut self, count: u8) -> Self {
            self.transport_retry_count = Some(count);
            self
        }

        pub fn with_keep_alive_timeout_ms(mut self, timeout: u32) -> Self {
            self.keep_alive_timeout_ms = Some(timeout);
            self
        }

        pub fn disable_error_logging(mut self, disable: bool) -> Self {
            self.disable_error_logging = Some(disable);
            self
        }

        /// Builder to override default values
        pub fn build(self) -> NvmeControllerOpts {
            let mut opts = NvmeControllerOpts::default();

            if let Some(timeout_ms) = self.admin_timeout_ms {
                opts.0.admin_timeout_ms = timeout_ms;
            }
            if let Some(timeout_us) = self.fabrics_connect_timeout_us {
                opts.0.fabrics_connect_timeout_us = timeout_us;
            }

            if let Some(retries) = self.transport_retry_count {
                opts.0.transport_retry_count = retries;
            }

            if let Some(timeout_ms) = self.keep_alive_timeout_ms {
                opts.0.keep_alive_timeout_ms = timeout_ms;
            }

            opts
        }
    }
    #[cfg(test)]
    mod test {
        use crate::bdev::dev::nvmx::controller::options;

        #[test]
        fn nvme_default_controller_options() {
            let opts = options::Builder::new()
                .with_admin_timeout_ms(1)
                .with_fabrics_connect_timeout_us(1)
                .with_transport_retry_count(1)
                .build();

            assert_eq!(opts.0.admin_timeout_ms, 1);
            assert_eq!(opts.0.fabrics_connect_timeout_us, 1);
            assert_eq!(opts.0.transport_retry_count, 1);
        }
    }
}

pub(crate) mod transport {
    use libc::c_void;
    use spdk_sys::spdk_nvme_transport_id;
    use std::{ffi::CStr, fmt::Debug, ptr::copy_nonoverlapping};

    pub struct NvmeTransportId(spdk_nvme_transport_id);

    impl Debug for NvmeTransportId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(
                f,
                "Transport ID: {}: {}: {}: {}:",
                self.trtype(),
                self.traddr(),
                self.subnqn(),
                self.svcid()
            )
        }
    }

    impl NvmeTransportId {
        pub fn trtype(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.trstring[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn traddr(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.traddr[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn subnqn(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.subnqn[0])
                    .to_string_lossy()
                    .to_string()
            }
        }
        pub fn svcid(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.trsvcid[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn as_ptr(&self) -> *const spdk_nvme_transport_id {
            &self.0
        }
    }

    #[derive(Debug)]
    enum TransportId {
        TCP = 0x3,
    }

    impl Default for TransportId {
        fn default() -> Self {
            Self::TCP
        }
    }

    impl From<TransportId> for String {
        fn from(t: TransportId) -> Self {
            match t {
                TransportId::TCP => String::from("tcp"),
            }
        }
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    pub(crate) enum AdressFamily {
        NvmfAdrfamIpv4 = 0x1,
        NvmfAdrfamIpv6 = 0x2,
        NvmfAdrfamIb = 0x3,
        NvmfAdrfamFc = 0x4,
        NvmfAdrfamLoop = 0xfe,
    }

    impl Default for AdressFamily {
        fn default() -> Self {
            Self::NvmfAdrfamIpv4
        }
    }

    #[derive(Default, Debug)]
    pub struct Builder {
        trid: TransportId,
        adrfam: AdressFamily,
        svcid: String,
        traddr: String,
        subnqn: String,
    }

    impl Builder {
        pub fn new() -> Self {
            Self {
                ..Default::default()
            }
        }

        /// the address to connect to
        pub fn with_traddr(mut self, traddr: &str) -> Self {
            self.traddr = traddr.to_string();
            self
        }
        /// svcid (port) to connect to

        pub fn with_svcid(mut self, svcid: &str) -> Self {
            self.svcid = svcid.to_string();
            self
        }

        /// target nqn
        pub fn with_subnqn(mut self, subnqn: &str) -> Self {
            self.subnqn = subnqn.to_string();
            self
        }

        /// builder for transportID currently defaults to TCP IPv4
        pub fn build(self) -> NvmeTransportId {
            let trtype = String::from(TransportId::TCP);
            let mut trid = spdk_nvme_transport_id::default();

            trid.adrfam = AdressFamily::NvmfAdrfamIpv4 as u32;
            trid.trtype = TransportId::TCP as u32;

            unsafe {
                copy_nonoverlapping(
                    trtype.as_ptr().cast(),
                    &mut trid.trstring[0] as *const _ as *mut c_void,
                    trtype.len(),
                );

                copy_nonoverlapping(
                    self.traddr.as_ptr().cast(),
                    &mut trid.traddr[0] as *const _ as *mut c_void,
                    self.traddr.len(),
                );
                copy_nonoverlapping(
                    self.svcid.as_ptr() as *const c_void,
                    &mut trid.trsvcid[0] as *const _ as *mut c_void,
                    self.svcid.len(),
                );
                copy_nonoverlapping(
                    self.subnqn.as_ptr() as *const c_void,
                    &mut trid.subnqn[0] as *const _ as *mut c_void,
                    self.subnqn.len(),
                );
            };

            NvmeTransportId(trid)
        }
    }

    #[cfg(test)]
    mod test {
        use crate::bdev::dev::nvmx::controller::transport;

        #[test]
        fn test_transport_id() {
            let transport = transport::Builder::new()
                .with_subnqn("nqn.2021-01-01:test.nqn")
                .with_svcid("4420")
                .with_traddr("127.0.0.1")
                .build();

            assert_eq!(transport.traddr(), "127.0.0.1");
            assert_eq!(transport.subnqn(), "nqn.2021-01-01:test.nqn");
            assert_eq!(transport.svcid(), "4420");
        }
    }
}
