use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    mem::size_of,
    os::raw::c_void,
    ptr::{copy_nonoverlapping, NonNull},
    sync::{Arc, Mutex},
};
use tracing::instrument;
use url::Url;

use spdk_sys::{
    self,
    spdk_io_device_register,
    spdk_io_device_unregister,
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_get_default_ctrlr_opts,
    spdk_nvme_ctrlr_get_ns,
    spdk_nvme_ctrlr_opts,
    spdk_nvme_ctrlr_process_admin_completions,
    spdk_nvme_detach,
    spdk_nvme_probe_ctx,
    spdk_nvme_probe_poll_async,
    spdk_nvme_transport_id,
    spdk_poller,
    spdk_poller_register_named,
    spdk_poller_unregister,
};

use crate::{
    bdev::{
        dev::nvmx::{
            channel::{NvmeControllerIoChannel, NvmeIoChannel},
            NvmeNamespace,
            NVME_CONTROLLERS,
        },
        util::uri,
        CreateDestroy,
        GetName,
    },
    ffihelper::ErrnoResult,
    nexus_uri::{
        NexusBdevError,
        {self},
    },
    subsys::NvmeBdevOpts,
};

const DEFAULT_NVMF_PORT: u16 = 8420;

#[derive(Debug)]
pub struct NvmfDeviceTemplate {
    /// name of the nvme controller and base name of the bdev
    /// that should be created for each namespace found
    name: String,
    /// alias which can be used to open the bdev
    alias: String,
    /// the remote target host (address)
    host: String,
    /// the transport service id (ie. port)
    port: u16,
    /// the nqn of the subsystem we want to connect to
    subnqn: String,
    /// Enable protection information checking (reftag, guard)
    prchk_flags: u32,
    /// uuid of the spdk bdev
    uuid: Option<uuid::Uuid>,
}

impl TryFrom<&Url> for NvmfDeviceTemplate {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let host =
            url.host_str().ok_or_else(|| NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("missing host"),
            })?;

        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segment"),
            });
        }

        if segments.len() > 1 {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("too many path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let mut prchk_flags: u32 = 0;

        if let Some(value) = parameters.remove("reftag") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("reftag"),
                },
            )? {
                prchk_flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
            }
        }

        if let Some(value) = parameters.remove("guard") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("guard"),
                },
            )? {
                prchk_flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
            }
        }

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: url.to_string(),
            },
        )?;

        if let Some(keys) = uri::keys(parameters) {
            warn!("ignored parameters: {}", keys);
        }

        Ok(NvmfDeviceTemplate {
            name: url[url::Position::BeforeHost .. url::Position::AfterPath]
                .to_string(),
            alias: url.to_string(),
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NVMF_PORT),
            subnqn: segments[0].to_string(),
            prchk_flags,
            uuid,
        })
    }
}

impl GetName for NvmfDeviceTemplate {
    fn get_name(&self) -> String {
        format!("{}n1", self.name)
    }
}

#[derive(Debug, PartialEq)]
pub enum NvmeControllerState {
    Initializing,
    Running,
}
pub struct NvmeControllerInner {
    namespaces: Vec<Arc<NvmeNamespace>>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    adminq_poller: NonNull<spdk_poller>,
}
/*
 * NVME controller implementation.
 */
pub struct NvmeController {
    name: String,
    id: u64,
    prchk_flags: u32,
    state: NvmeControllerState,
    inner: Option<NvmeControllerInner>,
}

unsafe impl Send for NvmeController {}
unsafe impl Sync for NvmeController {}

impl Drop for NvmeController {
    fn drop(&mut self) {
        debug!("{}: dropping controller object", self.get_name());
    }
}

impl NvmeController {
    fn new(name: &str, prchk_flags: u32) -> Self {
        let l = NvmeController {
            name: String::from(name),
            id: 0,
            prchk_flags,
            state: NvmeControllerState::Initializing,
            inner: None,
        };

        debug!("{}: New controller created", l.get_name());
        l
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_flags(&self) -> u32 {
        self.prchk_flags
    }

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
        if self.inner.is_none() {
            None
        } else {
            let inner = self.inner.as_ref().unwrap();
            if let Some(ns) = inner.namespaces.get(0) {
                Some(Arc::clone(ns))
            } else {
                None
            }
        }
    }

    pub fn spdk_handle(&self) -> *mut spdk_nvme_ctrlr {
        if self.inner.is_none() {
            error!("{} No SPDK handle configured !", self.name);
            std::ptr::null_mut()
        } else {
            debug!(
                "{} SPDK handle: {:p}",
                self.name,
                self.inner.as_ref().unwrap().ctrlr.as_ptr()
            );
            self.inner.as_ref().unwrap().ctrlr.as_ptr()
        }
    }
}

extern "C" fn nvme_async_poll(ctx: *mut c_void) -> i32 {
    let _rc =
        unsafe { spdk_nvme_probe_poll_async(ctx as *mut spdk_nvme_probe_ctx) };
    1
}

extern "C" fn nvme_poll_adminq(ctx: *mut c_void) -> i32 {
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

// Callback to be called once NVME controller successfully created.
extern "C" fn connect_attach_cb(
    _cb_ctx: *mut c_void,
    _trid: *const spdk_nvme_transport_id,
    ctrlr: *mut spdk_nvme_ctrlr,
    _opts: *const spdk_nvme_ctrlr_opts,
) {
    let context =
        unsafe { &mut *(_cb_ctx as *const _ as *mut NvmeControllerContext) };

    let mut poller = context.poller.take().unwrap();
    unsafe {
        spdk_poller_unregister(&mut poller);
    }

    let cid = ctrlr as u64;

    // Save SPDK controller reference for further use.
    let controllers = NVME_CONTROLLERS.write().unwrap();
    // Create a clone of controller to insert later for ID lookup.
    let rc = controllers.get(&context.name).unwrap();
    let clone = Arc::clone(rc);
    let mut controller = rc.lock().unwrap();

    controller.set_id(cid);
    // Register I/O device for the controller.
    unsafe {
        spdk_io_device_register(
            controller.id() as *mut c_void,
            Some(NvmeControllerIoChannel::create),
            Some(NvmeControllerIoChannel::destroy),
            std::mem::size_of::<NvmeIoChannel>() as u32,
            controller.get_name().as_ptr() as *const i8,
        )
    }
    debug!(
        "{}: I/O device registered at 0x{:X}",
        controller.get_name(),
        controller.id()
    );

    // Configure poller for controller's admin queue.
    let default_opts = NvmeBdevOpts::default();

    let adminq_poller = unsafe {
        spdk_poller_register_named(
            Some(nvme_poll_adminq),
            ctrlr as *mut c_void,
            default_opts.nvme_adminq_poll_period_us,
            "nvme_poll_adminq\0" as *const _ as *mut _,
        )
    };

    if adminq_poller.is_null() {
        error!(
            "{}: failed to create admin queue poller",
            controller.get_name()
        );
    }

    let mut namespaces: Vec<Arc<NvmeNamespace>> = Vec::new();
    // Initialize namespaces (currently only 1).
    let ns = unsafe { spdk_nvme_ctrlr_get_ns(ctrlr, 1) };

    if ns.is_null() {
        warn!(
            "{} no namespaces reported by the NVMe controller",
            controller.get_name()
        );
    } else {
        namespaces.push(Arc::new(NvmeNamespace::from_ptr(ns)));
    }

    let inner_state = NvmeControllerInner {
        ctrlr: NonNull::new(ctrlr).unwrap(),
        adminq_poller: NonNull::new(adminq_poller).unwrap(),
        namespaces,
    };

    controller.inner.replace(inner_state);
    controller.state = NvmeControllerState::Running;

    // Release the guard early to let the waiter access the controller instance
    // safely.
    drop(controller);
    drop(controllers);

    // Add 'controller id -> controller' mapping once the controller is fully
    // setup.
    let mut controllers = NVME_CONTROLLERS.write().unwrap();
    controllers.insert(cid.to_string(), clone);

    // Wake up the waiter and complete controller registration.
    let sender = context.sender.take().unwrap();
    sender
        .send(Ok(()))
        .expect("done callback receiver side disappeared");
}

#[async_trait(?Send)]
impl CreateDestroy for NvmfDeviceTemplate {
    type Error = NexusBdevError;

    #[instrument(err)]
    async fn create(&self) -> Result<String, Self::Error> {
        let cname = self.get_name();

        // Check against existing controller with the transport ID.
        let mut controllers = NVME_CONTROLLERS.write().unwrap();
        if controllers.contains_key(&cname) {
            return Err(NexusBdevError::BdevExists {
                name: cname,
            });
        }

        // Insert a new controller instance (uninitialized) as a guard, and
        // release the lock to keep the write path as short, as
        // possible.
        let rc =
            Arc::new(Mutex::new(NvmeController::new(&cname, self.prchk_flags)));
        controllers.insert(cname.clone(), rc);
        drop(controllers);

        let mut context = NvmeControllerContext::new(self);

        // Initiate connection with remote NVMe target.
        let probe_ctx: *mut spdk_nvme_probe_ctx = unsafe {
            spdk_nvme_connect_async(
                &context.trid,
                &context.opts,
                Some(connect_attach_cb),
            )
        };

        if probe_ctx.is_null() {
            // Remove controller record before returning error.
            let mut controllers = NVME_CONTROLLERS.write().unwrap();
            controllers.remove(&cname);

            return Err(NexusBdevError::CreateBdev {
                name: cname,
                source: Errno::ENODEV,
            });
        }

        // Register poller to check for connection status.
        let poller = unsafe {
            spdk_poller_register_named(
                Some(nvme_async_poll),
                probe_ctx as *mut c_void,
                1000, // TODO: fix.
                "nvme_async_poll\0" as *const _ as *mut _,
            )
        };

        context.poller = Some(poller);

        context
            .receiver
            .await
            .context(nexus_uri::CancelBdev {
                name: self.name.clone(),
            })?
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        // Check that controller is fully initialized.
        let controllers = NVME_CONTROLLERS.read().unwrap();
        let controller = controllers.get(&cname).unwrap().lock().unwrap();
        assert_eq!(
            controller.state,
            NvmeControllerState::Running,
            "NVMe controller is not fully initialized"
        );

        info!("{} NVMe controller successfully initialized", cname);
        Ok(cname)
    }

    // nvme_bdev_ctrlr_create
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        let cname = self.get_name();

        let mut controllers = NVME_CONTROLLERS.write().unwrap();
        if !controllers.contains_key(&cname) {
            return Err(NexusBdevError::BdevNotFound {
                name: cname,
            });
        }

        // Remove 'controller name -> controller' mapping.
        let e = controllers.remove(&cname).unwrap();
        let mut controller = e.lock().unwrap();
        debug!("{}: removing NVMe controller", cname);

        if let Some(inner) = controller.inner.take() {
            debug!("{} unregistering adminq poller", controller.get_name());
            unsafe {
                spdk_poller_unregister(&mut inner.adminq_poller.as_ptr());
            }

            debug!(
                "{}: unregistering I/O device at 0x{:X}",
                controller.get_name(),
                controller.id()
            );
            unsafe {
                spdk_io_device_unregister(controller.id() as *mut c_void, None);
            }

            // Detach SPDK controller.
            let rc = unsafe { spdk_nvme_detach(inner.ctrlr.as_ptr()) };

            assert_eq!(rc, 0, "Failed to detach NVMe controller");
            debug!("{}: NVMe controller successfully detached", cname);
        }

        // Remove 'controller id->controller' mappig.
        controllers.remove(&controller.id().to_string());

        Ok(())
    }
}

// Context for an NVMe controller being created.
struct NvmeControllerContext {
    opts: spdk_nvme_ctrlr_opts,
    name: String,
    trid: spdk_nvme_transport_id,
    sender: Option<oneshot::Sender<Result<(), Errno>>>,
    receiver: oneshot::Receiver<Result<(), Errno>>,
    poller: Option<*mut spdk_poller>,
}

impl NvmeControllerContext {
    pub fn new(template: &NvmfDeviceTemplate) -> NvmeControllerContext {
        let port = template.port.to_string();
        let protocol = "tcp";

        let default_opts = NvmeBdevOpts::default();
        let mut trid = spdk_nvme_transport_id::default();
        let mut opts = spdk_nvme_ctrlr_opts::default();

        // Initialize options for NVMe controller to defaults.
        unsafe {
            spdk_nvme_ctrlr_get_default_ctrlr_opts(
                &mut opts,
                size_of::<spdk_nvme_ctrlr_opts>() as u64,
            );
        }

        opts.transport_retry_count = default_opts.retry_count as u8;

        unsafe {
            copy_nonoverlapping(
                protocol.as_ptr() as *const c_void,
                &mut trid.trstring[0] as *const _ as *mut c_void,
                protocol.len(),
            );
            copy_nonoverlapping(
                template.host.as_ptr() as *const c_void,
                &mut trid.traddr[0] as *const _ as *mut c_void,
                template.host.len(),
            );
            copy_nonoverlapping(
                port.as_ptr() as *const c_void,
                &mut trid.trsvcid[0] as *const _ as *mut c_void,
                port.len(),
            );
            copy_nonoverlapping(
                template.subnqn.as_ptr() as *const c_void,
                &mut trid.subnqn[0] as *const _ as *mut c_void,
                template.subnqn.len(),
            );
        }

        trid.trtype = spdk_sys::SPDK_NVME_TRANSPORT_TCP;
        trid.adrfam = spdk_sys::SPDK_NVMF_ADRFAM_IPV4;

        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();

        NvmeControllerContext {
            opts,
            trid,
            name: template.get_name(),
            sender: Some(sender),
            receiver,
            poller: None,
        }
    }
}
