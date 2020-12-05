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
    sync::RwLock,
};
use tracing::instrument;
use url::Url;

use spdk_sys::{
    self,
    spdk_nvme_connect_async,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_get_default_ctrlr_opts,
    spdk_nvme_ctrlr_get_ns,
    spdk_nvme_ctrlr_opts,
    spdk_nvme_ns,
    spdk_nvme_ns_get_extended_sector_size,
    spdk_nvme_ns_get_md_size,
    spdk_nvme_ns_get_num_sectors,
    spdk_nvme_ns_get_size,
    spdk_nvme_ns_get_uuid,
    spdk_nvme_ns_supports_compare,
    spdk_nvme_probe_ctx,
    spdk_nvme_probe_poll_async,
    spdk_nvme_transport_id,
    spdk_poller,
    spdk_poller_register_named,
    spdk_poller_unregister,
};

use crate::{
    bdev::{nexus::nexus_io::IoType, util::uri, CreateDestroy, GetName},
    core::{
        uuid::Uuid,
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceStats,
        CoreError,
    },
    ffihelper::ErrnoResult,
    nexus_uri::{
        NexusBdevError,
        {self},
    },
    subsys::NvmeBdevOpts,
};

lazy_static! {
    static ref NVME_CONTROLLERS: RwLock<HashMap<String, NvmeController>> =
        RwLock::new(HashMap::<String, NvmeController>::new());
}

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

extern "C" fn nvme_async_poll(ctx: *mut c_void) -> i32 {
    let _rc =
        unsafe { spdk_nvme_probe_poll_async(ctx as *mut spdk_nvme_probe_ctx) };
    1
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

    // Save SPDK controller reference for further use.
    let mut controllers = NVME_CONTROLLERS.write().unwrap();
    let mut nvme_controller = controllers.get_mut(&context.name).unwrap();
    nvme_controller.spdk_nvme_ctrlr = ctrlr;
    drop(controllers);

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
        controllers.insert(cname.clone(), NvmeController::new(&cname));
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
            return Err(NexusBdevError::CreateBdev {
                name: "XXX".to_string(),
                source: Errno::ENODEV,
            });
        }

        // Register poller to check for connection status.
        let poller = unsafe {
            spdk_poller_register_named(
                Some(nvme_async_poll),
                probe_ctx as *mut c_void,
                1000,
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

        // Finally, configure controller's innter state.
        let inner_state = NvmeControllerInner {};

        let mut controllers = NVME_CONTROLLERS.write().unwrap();
        let nvme_controller = controllers.get_mut(&cname).unwrap();
        nvme_controller.inner.replace(inner_state);
        nvme_controller.state = NvmeControllerState::Runing;

        drop(controllers);
        test_ctrl_open(&cname);

        Ok(cname)
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        let cname = self.get_name();

        let mut controllers = NVME_CONTROLLERS.write().unwrap();
        if !controllers.contains_key(&cname) {
            return Err(NexusBdevError::BdevNotFound {
                name: cname,
            });
        }

        controllers.remove(&cname).unwrap();
        Ok(())
    }
}

impl GetName for NvmfDeviceTemplate {
    fn get_name(&self) -> String {
        format!("{}n1", self.name)
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

enum NvmeControllerState {
    Initializing,
    Runing,
}

struct NvmeControllerInner {}

/*
 * Descriptor for an opened NVMe device that represents a namespace for
 * an NVMe controller.
 */
struct NvmeDeviceDescriptor {
    ns: NonNull<spdk_nvme_ns>,
    name: String,
}

impl NvmeDeviceDescriptor {
    fn create(
        controller: &NvmeController,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let ns: *mut spdk_nvme_ns =
            unsafe { spdk_nvme_ctrlr_get_ns(controller.as_ptr(), 1) };

        if ns.is_null() {
            Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            })
        } else {
            Ok(Box::new(NvmeDeviceDescriptor {
                ns: NonNull::new(ns).unwrap(),
                name: controller.get_name(),
            }))
        }
    }
}

impl BlockDeviceDescriptor for NvmeDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        println!("descriptor created for device {:?}", self.name);
        Box::new(NvmeBlockDevice::from_ns(&self.name, self.ns.as_ptr()))
    }
}

/*
 * NVME controller implementation.
 */
pub(super) struct NvmeController {
    name: String,
    state: NvmeControllerState,
    spdk_nvme_ctrlr: *mut spdk_nvme_ctrlr,
    inner: Option<NvmeControllerInner>,
}

unsafe impl Send for NvmeController {}
unsafe impl Sync for NvmeController {}

impl NvmeController {
    fn new(name: &str) -> Self {
        NvmeController {
            name: String::from(name),
            state: NvmeControllerState::Initializing,
            inner: None,
            spdk_nvme_ctrlr: std::ptr::null_mut(),
        }
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn as_ptr(&self) -> *mut spdk_nvme_ctrlr {
        self.spdk_nvme_ctrlr as *mut spdk_nvme_ctrlr
    }
}

struct NvmeBlockDevice {
    ns: NonNull<spdk_nvme_ns>,
    name: String,
}

impl NvmeBlockDevice {
    pub fn open_by_name(
        name: &str,
        _read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let controllers = NVME_CONTROLLERS.read().unwrap();
        if !controllers.contains_key(name) {
            return Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            });
        }
        let controller = controllers.get(name).unwrap();
        let descr = NvmeDeviceDescriptor::create(controller)?;
        Ok(descr)
    }

    fn from_ns(name: &str, ns: *mut spdk_nvme_ns) -> NvmeBlockDevice {
        NvmeBlockDevice {
            ns: NonNull::new(ns)
                .expect("nullptr dereference while accessing NVMe namespace"),
            name: String::from(name),
        }
    }
}

impl BlockDevice for NvmeBlockDevice {
    fn size_in_bytes(&self) -> u64 {
        unsafe { spdk_nvme_ns_get_size(self.ns.as_ptr()) }
    }

    fn block_len(&self) -> u32 {
        unsafe { spdk_nvme_ns_get_extended_sector_size(self.ns.as_ptr()) }
    }

    fn num_blocks(&self) -> u64 {
        unsafe { spdk_nvme_ns_get_num_sectors(self.ns.as_ptr()) }
    }

    fn uuid(&self) -> String {
        let u = Uuid(unsafe { spdk_nvme_ns_get_uuid(self.ns.as_ptr()) });
        uuid::Uuid::from_bytes(u.as_bytes())
            .to_hyphenated()
            .to_string()
    }

    fn product_name(&self) -> String {
        "NVMe disk".to_string()
    }

    fn driver_name(&self) -> String {
        String::from("nvme")
    }

    fn device_name(&self) -> String {
        self.name.clone()
    }

    fn alignment(&self) -> u64 {
        1
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        let spdk_ns = self.ns.as_ptr();

        // bdev_nvme_io_type_supported
        match io_type {
            IoType::Read
            | IoType::Write
            | IoType::Reset
            | IoType::Flush
            | IoType::NvmeAdmin
            | IoType::NvmeIO
            | IoType::Abort => true,
            IoType::Compare => unsafe {
                spdk_nvme_ns_supports_compare(spdk_ns)
            },
            IoType::NvmeIOMD => {
                let t = unsafe { spdk_nvme_ns_get_md_size(spdk_ns) };
                t > 0
            }
            IoType::Unmap => false,
            IoType::WriteZeros => false,
            IoType::CompareAndWrite => false,
            _ => false,
        }
    }

    fn io_stats(&self) -> Result<BlockDeviceStats, NexusBdevError> {
        Ok(Default::default())
    }

    fn claimed_by(&self) -> Option<String> {
        None
    }
}

pub fn test_ctrl_open(cname: &str) {
    match NvmeBlockDevice::open_by_name(cname, false) {
        Err(e) => println!("** FAILED TO OPEN DEVICE: {:?}", e),
        Ok(descr) => {
            println!("** DEVICE OPENED !");

            let bdev = descr.get_device();
            println!("= size_in bytes: {}", bdev.size_in_bytes());
            println!("= block_len: {}", bdev.block_len());
            println!("= num_blocks: {}", bdev.num_blocks());
            println!("= uuid: {:?}", bdev.uuid());
            println!("= product_name: {:?}", bdev.product_name());
            println!("= driver_name: {:?}", bdev.driver_name());
            println!("= device_name: {:?}", bdev.device_name());
            println!("= alignment: {:?}", bdev.alignment());
            println!(
                "= io_type_supported: {:?}",
                bdev.io_type_supported(IoType::Unmap)
            );
        }
    }
}
