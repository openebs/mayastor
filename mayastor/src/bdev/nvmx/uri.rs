//!
//!
//! This file handles the conversion from URI to NVMe controller creation(s).
//! It's not very clean, but also the least important for now.

use async_trait::async_trait;
use futures::channel::{oneshot, oneshot::Sender};
use libc;
use nix::errno::Errno;
use parking_lot::Mutex;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    ffi::c_void,
    ptr::NonNull,
    sync::Arc,
};
use url::Url;
use uuid::Uuid;

use controller::options::NvmeControllerOpts;
use poller::Poller;
use spdk_rs::libspdk::{
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_opts,
    spdk_nvme_probe_ctx,
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
    nexus_uri::{self, NexusBdevError},
    subsys::Config,
};

use super::controller::transport::NvmeTransportId;

const DEFAULT_NVMF_PORT: u16 = 8420;
// Callback to be called once NVMe controller attach sequence completes.
extern "C" fn connect_attach_cb(
    _cb_ctx: *mut c_void,
    _trid: *const spdk_nvme_transport_id,
    ctrlr: *mut spdk_nvme_ctrlr,
    _opts: *const spdk_nvme_ctrlr_opts,
) {
    let context =
        unsafe { &mut *(_cb_ctx as *const _ as *mut NvmeControllerContext) };

    // Normally, the attach handler is called by the poller after
    // the controller is connected. In such a case 'spdk_nvme_probe_poll_async'
    // returns zero. However, in case of attach errors zero is also returned.
    // In order to notify the polling function about successfull attach,
    // we set up the flag.
    assert!(!context.attached);
    context.attached = true;

    // Unregister poller immediately after controller attach completes.
    context.unregister_poller();

    // Check whether controller attach failed.
    if ctrlr.is_null() {
        context
            .sender()
            .send(Err(Errno::ENXIO))
            .expect("done callback receiver side disappeared");
    } else {
        // Instantiate the controller in case attach succeeded.
        controller::connected_attached_cb(
            context,
            SpdkNvmeController::from_ptr(ctrlr)
                .expect("probe callback with NULL ptr"),
        );
    }
}

#[derive(Debug)]
#[allow(dead_code)]
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
                    value: value.to_string(),
                },
            )? {
                prchk_flags |=
                    spdk_rs::libspdk::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
            }
        }

        if let Some(value) = parameters.remove("guard") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("guard"),
                    value: value.to_string(),
                },
            )? {
                prchk_flags |= spdk_rs::libspdk::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
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
    attached: bool,
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

        let mut opts = controller::options::Builder::new()
            .with_keep_alive_timeout_ms(
                Config::get().nvme_bdev_opts.keep_alive_timeout_ms,
            )
            .with_transport_retry_count(
                Config::get().nvme_bdev_opts.transport_retry_count as u8,
            );

        if let Ok(ext_host_id) = std::env::var("MAYASTOR_NVMF_HOSTID") {
            if let Ok(uuid) = Uuid::parse_str(&ext_host_id) {
                opts = opts.with_ext_host_id(*uuid.as_bytes());
                if std::env::var("HOSTNQN").is_err() {
                    opts = opts.with_hostnqn(format!(
                        "nqn.2019-05.io.openebs:uuid:{}",
                        uuid
                    ));
                }
            }
        }

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
            attached: false,
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

    async fn create(&self) -> Result<String, Self::Error> {
        info!("::create() {}", self.get_name());
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
        let probe_ctx = match NonNull::new(unsafe {
            spdk_nvme_connect_async(
                context.trid.as_ptr(),
                context.opts.as_ptr(),
                Some(connect_attach_cb),
            )
        }) {
            Some(ctx) => ctx,
            None => {
                // Remove controller record before returning error.
                NVME_CONTROLLERS.remove_by_name(&cname).unwrap();
                return Err(NexusBdevError::CreateBdev {
                    name: cname,
                    source: Errno::ENODEV,
                });
            }
        };

        struct AttachCtx {
            probe_ctx: NonNull<spdk_nvme_probe_ctx>,
            /// NvmeControllerContext required for handling of attach failures.
            cb_ctx: *const spdk_nvme_ctrlr_opts,
            name: String,
        }

        let attach_cb_ctx = AttachCtx {
            probe_ctx,
            cb_ctx: context.opts.as_ptr(),
            name: self.get_name(),
        };

        let poller = poller::Builder::new()
            .with_name("nvme_async_probe_poller")
            .with_interval(1000) // poll every 1 second
            .with_poll_fn(move || unsafe {
                let context =
                    &mut *(attach_cb_ctx.cb_ctx as *mut NvmeControllerContext);

                let r = spdk_nvme_probe_poll_async(
                    attach_cb_ctx.probe_ctx.as_ptr(),
                );

                if r != -libc::EAGAIN {
                    // Double check against successful attach, as we expect
                    // the attach handler to be called by the poller.
                    if !context.attached {
                        error!(
                            "{} controller attach failed",
                            attach_cb_ctx.name
                        );

                        connect_attach_cb(
                            attach_cb_ctx.cb_ctx as *mut c_void,
                            std::ptr::null(),
                            std::ptr::null_mut(),
                            std::ptr::null(),
                        );
                    }
                }

                r
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

                let controller = controller.lock();

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
