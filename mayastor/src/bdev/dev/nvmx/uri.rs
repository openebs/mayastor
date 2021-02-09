//!
//!
//! This file handles the conversion from URI to NVMe controller creation(s).
//! It's not very clean, but also the least important for now.

use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    ffi::c_void,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use controller::options::NvmeControllerOpts;
use futures::channel::{oneshot, oneshot::Sender};
use nix::errno::Errno;
use poller::Poller;
use snafu::ResultExt;
use spdk_sys::{
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_opts,
    spdk_nvme_probe_poll_async,
    spdk_nvme_transport_id,
};
use tracing::instrument;
use url::Url;

use crate::{
    bdev::{
        dev::nvmx::{controller, NvmeControllerState, NVME_CONTROLLERS},
        util::uri,
        CreateDestroy,
        GetName,
    },
    core::poller,
    ffihelper::{cb_arg, done_cb, ErrnoResult},
    nexus_uri::{
        NexusBdevError,
        {self},
    },
    subsys::NvmeBdevOpts,
};

use super::controller::transport::NvmeTransportId;
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
pub(crate) struct NvmeControllerContext<'probe> {
    opts: NvmeControllerOpts,
    name: String,
    trid: NvmeTransportId,
    sender: Option<oneshot::Sender<Result<(), Errno>>>,
    receiver: oneshot::Receiver<Result<(), Errno>>,
    poller: Option<Poller<'probe>>,
}

impl<'probe> NvmeControllerContext<'probe> {
    pub fn new(template: &NvmfDeviceTemplate) -> NvmeControllerContext {
        let trid = controller::transport::Builder::new()
            .with_subnqn(&template.subnqn)
            .with_svcid(&template.port.to_string())
            .with_traddr(&template.host)
            .build();

        let device_defaults = NvmeBdevOpts::default();
        let opts = controller::options::Builder::new()
            .with_keep_alive_timeout_ms(device_defaults.keep_alive_timeout_ms)
            .with_transport_retry_count(device_defaults.retry_count as u8)
            .build();

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
        self.poller.take().expect("No poller registered");
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
        if NVME_CONTROLLERS.lookup_by_name(&cname).is_some() {
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

        NVME_CONTROLLERS.insert_controller(cname.clone(), rc);

        let mut context = NvmeControllerContext::new(self);

        // Initiate connection with remote NVMe target.
        let probe_ctx = NonNull::new(unsafe {
            spdk_nvme_connect_async(
                context.trid.as_ptr(),
                context.opts.as_ptr(),
                Some(connect_attach_cb),
            )
        });

        if probe_ctx.is_none() {
            // Remove controller record before returning error.
            NVME_CONTROLLERS.remove_by_name(&cname).unwrap();
            return Err(NexusBdevError::CreateBdev {
                name: cname,
                source: Errno::ENODEV,
            });
        }

        let poller = poller::Builder::new()
            .with_name("nvme_async_probe_poller")
            .with_interval(1000)
            .with_poll_fn(move || unsafe {
                spdk_nvme_probe_poll_async(probe_ctx.unwrap().as_ptr())
            })
            .build();

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

        let controller = NVME_CONTROLLERS
            .lookup_by_name(&cname)
            .expect("no controller in the list");

        let controller = controller.lock().expect("failed to lock controller");

        assert_eq!(
            controller.get_state(),
            NvmeControllerState::Running,
            "NVMe controller is not fully initialized"
        );

        info!("{} NVMe controller successfully initialized", cname);
        Ok(cname)
    }

    // nvme_bdev_ctrlr_create
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        // 1. Initiate controller shutdown, which shuts down all I/O resources
        // of the controller.
        let controller = NVME_CONTROLLERS
            .lookup_by_name(self.get_name())
            .ok_or(NexusBdevError::BdevNotFound {
                name: self.get_name(),
            })?;

        let (s, r) = oneshot::channel::<bool>();
        {
            let mut controller = controller.lock().expect("lock poisoned");

            fn _shutdown_callback(success: bool, ctx: *mut c_void) {
                done_cb(ctx, success);
            }

            controller.shutdown(_shutdown_callback, cb_arg(s)).map_err(
                |_| NexusBdevError::DestroyBdev {
                    source: Errno::EAGAIN,
                    name: self.get_name(),
                },
            )?;
        }

        if !r.await.expect("Failed awaiting at shutdown()") {
            error!("{} failed to shutdown controller", self.get_name());
            return Err(NexusBdevError::DestroyBdev {
                source: Errno::EAGAIN,
                name: self.get_name(),
            });
        }

        // 2. Remove controller from the list so that a new controller with the
        // same name can be inserted. Note that there may exist other
        // references to the controller before removal, but since all
        // controller's resources have been invalidated, that exposes no
        // risk, as no operations will be possible on such controllers.
        let name =
            NVME_CONTROLLERS
                .remove_by_name(self.get_name())
                .map_err(|_| NexusBdevError::BdevNotFound {
                    name: self.get_name(),
                })?;
        debug!("{}: removed from controller list", name);
        Ok(())
    }
}
