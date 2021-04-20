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
use futures::channel::{oneshot, oneshot::Sender};
use nix::errno::Errno;
use snafu::ResultExt;
use tracing::instrument;
use url::Url;

use controller::options::NvmeControllerOpts;
use poller::Poller;
use spdk_sys::{
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_opts,
    spdk_nvme_probe_poll_async,
    spdk_nvme_transport_id,
};

use crate::{
    bdev::{
        nvmx::{
            controller,
            controller_inner::SpdkNvmeController,
            NvmeControllerState,
            NVME_CONTROLLERS,
        },
        util::uri,
        CreateDestroy,
        GetName,
    },
    core::poller,
    ffihelper::ErrnoResult,
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
    controller::connected_attached_cb(
        context,
        SpdkNvmeController::from_ptr(ctrlr)
            .expect("probe callback with NULL ptr"),
    );
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

        // setting the HOSTNQN allows tracking who is connected to what. These
        // makes debugging connections easier in certain cases. If no
        // HOSTNQN is provided.

        let device_defaults = NvmeBdevOpts::default();
        let mut opts = controller::options::Builder::new()
            .with_keep_alive_timeout_ms(device_defaults.keep_alive_timeout_ms)
            .with_transport_retry_count(device_defaults.retry_count as u8);

        if let Ok(host_nqn) = std::env::var("HOSTNQN") {
            opts = opts.with_hostnqn(host_nqn);
        }

        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
        let opts = opts.build();

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
            .with_interval(1000) // poll every 1 second
            .with_poll_fn(move || unsafe {
                spdk_nvme_probe_poll_async(probe_ctx.unwrap().as_ptr())
            })
            .build();

        context.poller = Some(poller);

        let attach_status = context.receiver.await.unwrap();

        match attach_status {
            Err(e) => {
                // Remove controller from the list in case of attach failures.
                controller::destroy_device(self.get_name())
                    .await
                    // Propagate initial error once controller has been
                    // deinitialized.
                    .and_then(|_| {
                        Err(NexusBdevError::CreateBdev {
                            source: e,
                            name: self.name.clone(),
                        })
                    })
            }
            Ok(_) => {
                let controller = NVME_CONTROLLERS
                    .lookup_by_name(&cname)
                    .expect("no controller in the list");

                let controller =
                    controller.lock().expect("failed to lock controller");

                // Successfully attached controllers must be in Running state.
                assert_eq!(
                    controller.get_state(),
                    NvmeControllerState::Running,
                    "NVMe controller is not fully initialized"
                );

                info!("{} NVMe controller successfully initialized", cname);
                Ok(cname)
            }
        }
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        controller::destroy_device(self.get_name()).await
    }
}
