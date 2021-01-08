//!
//!
//! This file handles the conversion from URI to NVMe controller creation(s).
//! It's not very clean, but also the least important for now.

use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    mem::size_of,
    os::raw::c_void,
    ptr::{copy_nonoverlapping, NonNull},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;
use tracing::instrument;
use url::Url;

use spdk_sys::{
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_get_default_ctrlr_opts,
    spdk_nvme_ctrlr_opts,
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
            controller,
            nvme_controller_lookup,
            NvmeControllerState,
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
use futures::channel::oneshot::Sender;

use super::nvme_controller_remove;

const DEFAULT_NVMF_PORT: u16 = 8420;
// Callback to be called once NVMe controller is successfully created.
extern "C" fn connect_attach_cb(
    _cb_ctx: *mut c_void,
    _trid: *const spdk_nvme_transport_id,
    ctrlr: *mut spdk_nvme_ctrlr,
    _opts: *const spdk_nvme_ctrlr_opts,
) {
    let context =
        unsafe { &mut *(_cb_ctx as *const _ as *mut NvmeControllerContext) };
    controller::connected_attached_cb(context, NonNull::new(ctrlr).unwrap());
}
/// returns -EAGAIN if there is more work to be done here!
extern "C" fn nvme_async_poll(ctx: *mut c_void) -> i32 {
    unsafe { spdk_nvme_probe_poll_async(ctx as *mut spdk_nvme_probe_ctx) }
}

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

// Context for an NVMe controller being created.
pub(crate) struct NvmeControllerContext {
    opts: spdk_nvme_ctrlr_opts,
    name: String,
    trid: spdk_nvme_transport_id,
    sender: Option<oneshot::Sender<Result<(), Errno>>>,
    receiver: oneshot::Receiver<Result<(), Errno>>,
    poller: Option<NonNull<spdk_poller>>,
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

    /// unregister the poller used during connect/attach
    pub(crate) fn unregister_poller(&mut self) {
        let poller = self.poller.take().expect("No poller registered");
        unsafe {
            spdk_poller_unregister(&mut poller.as_ptr());
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn sender(&mut self) -> Sender<Result<(), Errno>> {
        self.sender.take().expect("no sender available")
    }
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
        let rc = Arc::new(Mutex::new(
            controller::NvmeController::new(&cname, self.prchk_flags)
                .expect("failed to create new NVMe controller instance"),
        ));
        controllers.insert(cname.clone(), rc);
        drop(controllers);

        let mut context = NvmeControllerContext::new(self);

        // Initiate connection with remote NVMe target.
        let probe_ctx = NonNull::new(unsafe {
            spdk_nvme_connect_async(
                &context.trid,
                &context.opts,
                Some(connect_attach_cb),
            )
        });

        if probe_ctx.is_none() {
            // Remove controller record before returning error.
            let mut controllers = NVME_CONTROLLERS.write().unwrap();
            controllers.remove(&cname);

            return Err(NexusBdevError::CreateBdev {
                name: cname,
                source: Errno::ENODEV,
            });
        }

        // Register poller to check for connection status.
        let poller = NonNull::new(unsafe {
            spdk_poller_register_named(
                Some(nvme_async_poll),
                probe_ctx.unwrap().as_ptr().cast(),
                1000, // TODO: fix.
                "nvme_async_poll\0" as *const _ as *mut _,
            )
        })
        .expect("failed to create attach poller");

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

        let controller =
            nvme_controller_lookup(&cname).expect("no controller in the list");

        let controller = controller.lock().expect("failed to lock controller");

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
        let name = nvme_controller_remove(self.get_name()).map_err(|_| {
            NexusBdevError::BdevNotFound {
                name: self.get_name(),
            }
        })?;
        debug!("{}: removed from controller list", name);
        Ok(())
    }
}
