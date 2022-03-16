//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    fmt::{Display, Formatter},
    marker::PhantomPinned,
    os::raw::c_void,
    pin::Pin,
};

use futures::channel::oneshot;
use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use tonic::{Code, Status};
use uuid::Uuid;

use super::{
    nexus_lookup_name_uuid,
    nexus_submit_request,
    ChildError,
    ChildState,
    DrEvent,
    NbdDisk,
    NbdError,
    NexusChannel,
    NexusChild,
    NexusModule,
    PersistOp,
};

use crate::{
    bdev::{
        device_destroy,
        nexus::{nexus_persistence::PersistentNexusInfo, NexusIoSubsystem},
    },
    core::{
        Bdev,
        BdevHandle,
        Command,
        CoreError,
        DeviceEventSink,
        IoType,
        Protocol,
        Reactor,
        Share,
        MWQ,
    },
    nexus_uri::NexusBdevError,
    rebuild::RebuildError,
    subsys::{NvmfError, NvmfSubsystem},
};

use spdk_rs::{
    BdevIo,
    BdevOps,
    ChannelTraverseStatus,
    IoChannel,
    IoDevice,
    IoDeviceChannelTraverse,
    JsonWriteContext,
};

pub static NVME_MIN_CNTLID: u16 = 1;
pub static NVME_MAX_CNTLID: u16 = 0xffef;

/// Obtain the full error chain
pub trait VerboseError {
    fn verbose(&self) -> String;
}

impl<T> VerboseError for T
where
    T: std::error::Error,
{
    /// loops through the error chain and formats into a single string
    /// containing all the lower level errors
    fn verbose(&self) -> String {
        let mut msg = format!("{}", self);
        let mut opt_source = self.source();
        while let Some(source) = opt_source {
            msg = format!("{}: {}", msg, source);
            opt_source = source.source();
        }
        msg
    }
}

/// Common errors for nexus basic operations and child operations
/// which are part of nexus object.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Nexus {} does not exist", name))]
    NexusNotFound { name: String },
    #[snafu(display("Nexus {} exists and is initialising", name))]
    NexusInitialising { name: String },
    #[snafu(display("Invalid nexus uuid \"{}\"", uuid))]
    InvalidUuid { uuid: String },
    #[snafu(display(
        "Nexus uuid \"{}\" already exists for nexus \"{}\"",
        uuid,
        nexus
    ))]
    UuidExists { uuid: String, nexus: String },
    #[snafu(display("Nexus with name \"{}\" already exists", name))]
    NameExists { name: String },
    #[snafu(display("Invalid encryption key"))]
    InvalidKey {},
    #[snafu(display("Failed to create crypto bdev for nexus {}", name))]
    CreateCryptoBdev { source: Errno, name: String },
    #[snafu(display("Failed to destroy crypto bdev for nexus {}", name))]
    DestroyCryptoBdev { source: Errno, name: String },
    #[snafu(display(
        "The nexus {} has been already shared with a different protocol",
        name
    ))]
    AlreadyShared { name: String },
    #[snafu(display("The nexus {} has not been shared", name))]
    NotShared { name: String },
    #[snafu(display("The nexus {} has not been shared over NVMf", name))]
    NotSharedNvmf { name: String },
    #[snafu(display("Failed to share nexus over NBD {}", name))]
    ShareNbdNexus { source: NbdError, name: String },
    #[snafu(display("Failed to share nvmf nexus {}", name))]
    ShareNvmfNexus { source: CoreError, name: String },
    #[snafu(display("Failed to unshare nexus {}", name))]
    UnshareNexus { source: CoreError, name: String },
    #[snafu(display(
        "Failed to register IO device nexus {}: {}",
        name,
        source
    ))]
    RegisterNexus { source: Errno, name: String },
    #[snafu(display("Failed to create child of nexus {}: {}", name, source))]
    CreateChild {
        source: NexusBdevError,
        name: String,
    },
    #[snafu(display("Deferring open because nexus {} is incomplete", name))]
    NexusIncomplete { name: String },
    #[snafu(display(
        "Child {} of nexus {} is too small: size = {} x {}",
        child,
        name,
        num_blocks,
        block_size
    ))]
    ChildTooSmall {
        child: String,
        name: String,
        num_blocks: u64,
        block_size: u64,
    },
    #[snafu(display("Children of nexus {} have mixed block sizes", name))]
    MixedBlockSizes { name: String },
    #[snafu(display(
        "Child {} of nexus {} has incompatible size or block size",
        child,
        name
    ))]
    ChildGeometry { child: String, name: String },
    #[snafu(display("Child {} of nexus {} cannot be found", child, name))]
    ChildMissing { child: String, name: String },
    #[snafu(display("Child {} of nexus {} has no error store", child, name))]
    ChildMissingErrStore { child: String, name: String },
    #[snafu(display(
        "Failed to acquire write exclusive reservation on child {} of nexus {}",
        child,
        name
    ))]
    ChildWriteExclusiveResvFailed {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to open child {} of nexus {}", child, name))]
    OpenChild {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to close child {} of nexus {}", child, name))]
    CloseChild {
        source: NexusBdevError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Cannot delete the last child {} of nexus {}",
        child,
        name
    ))]
    DestroyLastChild { child: String, name: String },
    #[snafu(display(
        "Cannot remove the last child {} of nexus {} from the IO path",
        child,
        name
    ))]
    DestroyLastHealthyChild { child: String, name: String },
    #[snafu(display(
        "Cannot remove the last healthy child {} of nexus {} from the IO path",
        child,
        name
    ))]
    RemoveLastChild { child: String, name: String },
    #[snafu(display(
        "Cannot fault the last healthy child {} of nexus {}",
        child,
        name
    ))]
    FaultingLastHealthyChild { child: String, name: String },
    #[snafu(display("Failed to destroy child {} of nexus {}", child, name))]
    DestroyChild {
        source: NexusBdevError,
        child: String,
        name: String,
    },
    #[snafu(display("Child {} of nexus {} not found", child, name))]
    ChildNotFound { child: String, name: String },
    #[snafu(display("Child {} of nexus {} already exists", child, name))]
    ChildAlreadyExists { child: String, name: String },
    #[snafu(display("Failed to pause child {} of nexus {}", child, name))]
    PauseChild { child: String, name: String },
    #[snafu(display("Suitable rebuild source for nexus {} not found", name))]
    NoRebuildSource { name: String },
    #[snafu(display(
        "Failed to create rebuild job for child {} of nexus {}",
        child,
        name,
    ))]
    CreateRebuild {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Rebuild job not found for child {} of nexus {}",
        child,
        name,
    ))]
    RebuildJobNotFound {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Failed to remove rebuild job {} of nexus {}",
        child,
        name,
    ))]
    RemoveRebuildJob {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Failed to execute rebuild operation on job {} of nexus {}",
        job,
        name,
    ))]
    RebuildOperation {
        job: String,
        name: String,
        source: RebuildError,
    },
    #[snafu(display("Invalid ShareProtocol value {}", sp_value))]
    InvalidShareProtocol { sp_value: i32 },
    #[snafu(display("Invalid NvmeAnaState value {}", ana_value))]
    InvalidNvmeAnaState { ana_value: i32 },
    #[snafu(display("Invalid arguments for nexus {}: {}", name, args))]
    InvalidArguments { name: String, args: String },
    #[snafu(display("Failed to create nexus {}", name))]
    NexusCreate { name: String },
    #[snafu(display("Failed to destroy nexus {}", name))]
    NexusDestroy { name: String },
    #[snafu(display(
        "Child {} of nexus {} is not degraded but {}",
        child,
        name,
        state
    ))]
    ChildNotDegraded {
        child: String,
        name: String,
        state: String,
    },
    #[snafu(display("Failed to get BdevHandle for snapshot operation"))]
    FailedGetHandle,
    #[snafu(display("Failed to create snapshot on nexus {}", name))]
    FailedCreateSnapshot { name: String, source: CoreError },
    #[snafu(display("NVMf subsystem error: {}", e))]
    SubsysNvmf { e: String },
    #[snafu(display("failed to pause {} current state {:?}", name, state))]
    Pause {
        state: NexusPauseState,
        name: String,
    },
}

impl From<NvmfError> for Error {
    fn from(error: NvmfError) -> Self {
        Error::SubsysNvmf {
            e: error.to_string(),
        }
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::NexusNotFound {
                ..
            } => Status::not_found(e.to_string()),
            Error::InvalidUuid {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidKey {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::AlreadyShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::NotShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::NotSharedNvmf {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::CreateChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::MixedBlockSizes {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildGeometry {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::OpenChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::DestroyLastChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildNotFound {
                ..
            } => Status::not_found(e.to_string()),
            e => Status::new(Code::Internal, e.to_string()),
        }
    }
}

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

#[derive(Debug)]
pub enum NexusTarget {
    NbdDisk(NbdDisk),
    NexusNvmfTarget,
}
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NexusPauseState {
    Unpaused,
    Pausing,
    Paused,
    Unpausing,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NvmeAnaState {
    InvalidState, // invalid, do not use
    OptimizedState,
    NonOptimizedState,
    InaccessibleState,
    PersistentLossState, // not yet supported
    ChangeState,         // not yet supported
}

impl NvmeAnaState {
    pub fn from_i32(value: i32) -> Result<NvmeAnaState, Error> {
        match value {
            0 => Ok(NvmeAnaState::InvalidState),
            1 => Ok(NvmeAnaState::OptimizedState),
            2 => Ok(NvmeAnaState::NonOptimizedState),
            3 => Ok(NvmeAnaState::InaccessibleState),
            4 => Ok(NvmeAnaState::PersistentLossState),
            15 => Ok(NvmeAnaState::ChangeState),
            _ => Err(Error::InvalidNvmeAnaState {
                ana_value: value,
            }),
        }
    }
}

/// NVMe-specific parameters for the Nexus
#[derive(Debug)]
pub struct NexusNvmeParams {
    /// minimum NVMe controller ID for sharing over NVMf
    pub(crate) min_cntlid: u16,
    /// maximum NVMe controller ID
    pub(crate) max_cntlid: u16,
    /// NVMe reservation key for children
    pub(crate) resv_key: u64,
    /// NVMe preempt key for children, 0 to not preempt
    pub(crate) preempt_key: Option<std::num::NonZeroU64>,
}

impl Default for NexusNvmeParams {
    fn default() -> Self {
        NexusNvmeParams {
            min_cntlid: NVME_MIN_CNTLID,
            max_cntlid: NVME_MAX_CNTLID,
            resv_key: 0x1234_5678,
            preempt_key: None,
        }
    }
}

impl NexusNvmeParams {
    pub fn set_min_cntlid(&mut self, min_cntlid: u16) {
        self.min_cntlid = min_cntlid;
    }
    pub fn set_max_cntlid(&mut self, max_cntlid: u16) {
        self.max_cntlid = max_cntlid;
    }
    pub fn set_resv_key(&mut self, resv_key: u64) {
        self.resv_key = resv_key;
    }
    pub fn set_preempt_key(
        &mut self,
        preempt_key: Option<std::num::NonZeroU64>,
    ) {
        self.preempt_key = preempt_key;
    }
}

/// The main nexus structure
#[derive(Debug)]
pub struct Nexus<'n> {
    /// Name of the Nexus instance
    pub(crate) name: String,
    /// The requested size of the Nexus in bytes. Children are allowed to
    /// be larger. The actual Nexus size will be calculated based on the
    /// capabilities of the underlying child devices.
    pub(crate) req_size: u64,
    /// number of children part of this nexus
    pub(crate) child_count: u32,
    /// vector of children
    pub children: Vec<NexusChild<'n>>,
    /// NVMe parameters
    pub(crate) nvme_params: NexusNvmeParams,
    /// uuid of the nexus (might not be the same as the nexus bdev!)
    nexus_uuid: Uuid,
    /// Bdev wrapper instance.
    bdev: Option<Bdev<Nexus<'n>>>,
    /// represents the current state of the Nexus
    pub state: parking_lot::Mutex<NexusState>,
    /// The offset in blocks where the data partition starts.
    pub(crate) data_ent_offset: u64,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
    /// enum containing the protocol-specific target used to publish the nexus
    pub nexus_target: Option<NexusTarget>,
    /// Indicates if the Nexus has an I/O device.
    has_io_device: bool,
    /// Information associated with the persisted NexusInfo structure.
    pub nexus_info: futures::lock::Mutex<PersistentNexusInfo>,
    /// Nexus I/O subsystem.
    io_subsystem: Option<NexusIoSubsystem<'n>>,
    /// TODO
    event_sink: Option<DeviceEventSink>,
    /// Prevent auto-Unpin.
    _pin: PhantomPinned,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusStatus {
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
    /// Online
    Online,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// closed
    Closed,
    /// open
    Open,
    /// reconfiguring internal IO channels
    Reconfiguring,
}

impl ToString for NexusState {
    fn to_string(&self) -> String {
        match *self {
            NexusState::Init => "init",
            NexusState::Closed => "closed",
            NexusState::Open => "open",
            NexusState::Reconfiguring => "reconfiguring",
        }
        .parse()
        .unwrap()
    }
}

impl ToString for NexusStatus {
    fn to_string(&self) -> String {
        match *self {
            NexusStatus::Degraded => "degraded",
            NexusStatus::Online => "online",
            NexusStatus::Faulted => "faulted",
        }
        .parse()
        .unwrap()
    }
}

/// TODO
struct UpdateFailFastCtx {
    sender: oneshot::Sender<bool>,
    nexus: String,
    child: Option<String>,
}

/// TODO
fn update_failfast_cb(
    channel: &mut NexusChannel,
    ctx: &mut UpdateFailFastCtx,
) -> ChannelTraverseStatus {
    let channel = channel.inner_mut();
    ctx.child.as_ref().map(|child| channel.remove_child(child));
    debug!(?ctx.nexus, ?ctx.child, "removed from channel");
    ChannelTraverseStatus::Ok
}

/// TODO
fn update_failfast_done(
    _status: ChannelTraverseStatus,
    ctx: UpdateFailFastCtx,
) {
    ctx.sender.send(true).expect("Receiver disappeared");
}

impl<'n> Nexus<'n> {
    /// create a new nexus instance with optionally directly attaching
    /// children to it.
    fn new(
        name: &str,
        size: u64,
        bdev_uuid: Option<&str>,
        nexus_uuid: Option<uuid::Uuid>,
        nvme_params: NexusNvmeParams,
        child_bdevs: Option<&[String]>,
        nexus_info_key: Option<String>,
    ) -> spdk_rs::Bdev<Nexus<'n>> {
        let n = Nexus {
            name: name.to_string(),
            child_count: 0,
            children: Vec::new(),
            state: parking_lot::Mutex::new(NexusState::Init),
            bdev: None,
            data_ent_offset: 0,
            share_handle: None,
            req_size: size,
            nexus_target: None,
            nvme_params,
            has_io_device: false,
            nexus_info: futures::lock::Mutex::new(PersistentNexusInfo::new(
                nexus_info_key,
            )),
            io_subsystem: None,
            nexus_uuid: Default::default(),
            event_sink: None,
            _pin: Default::default(),
        };

        let mut bdev = NexusModule::current()
            .bdev_builder()
            .with_name(name)
            .with_product_name(NEXUS_PRODUCT_ID)
            .with_uuid(Self::make_uuid(name, bdev_uuid))
            .with_block_length(0)
            .with_block_count(0)
            .with_required_alignment(9)
            .with_data(n)
            .build();

        unsafe {
            let n = bdev.data_mut().get_unchecked_mut();
            n.bdev = Some(Bdev::new(bdev.clone()));

            n.event_sink = Some(DeviceEventSink::new(bdev.data_mut()));

            // Set the nexus UUID to be the specified nexus UUID, otherwise
            // inherit the bdev UUID.
            n.nexus_uuid = nexus_uuid.unwrap_or_else(|| n.bdev().uuid());

            // Set I/O subsystem.
            n.io_subsystem = Some(NexusIoSubsystem::new(
                name.to_string(),
                n.bdev.as_mut().unwrap(),
            ));
        }

        // register children
        if let Some(child_bdevs) = child_bdevs {
            bdev.data_mut().register_children(child_bdevs);
        }

        bdev
    }

    /// TODO
    pub(crate) fn get_event_sink(&self) -> DeviceEventSink {
        self.event_sink
            .clone()
            .expect("Nexus device event sink not ready")
    }

    /// Makes the UUID of the underlying Bdev of this nexus.
    /// Generates a new UUID if specified uuid is None (or invalid).
    fn make_uuid(name: &str, uuid: Option<&str>) -> spdk_rs::Uuid {
        match uuid {
            Some(s) => match uuid::Uuid::parse_str(s) {
                Ok(u) => {
                    info!("UUID set to {} for nexus {}", u, name);
                    return u.into();
                }
                Err(error) => {
                    warn!(
                        "nexus {}: invalid UUID specified {}: {}",
                        name, s, error
                    );
                }
            },
            None => {
                info!("no UUID specified for nexus {}", name);
            }
        }

        let u = spdk_rs::Uuid::generate();
        info!("using generated UUID {} for nexus {}", u, name);
        u
    }

    /// Returns the Nexus uuid.
    pub(crate) fn uuid(&self) -> Uuid {
        self.nexus_uuid
    }

    /// Sets the state of the Nexus.
    fn set_state(self: Pin<&mut Self>, state: NexusState) -> NexusState {
        debug!(
            "{} Transitioned state from {:?} to {:?}",
            self.name, self.state, state
        );
        *self.state.lock() = state;
        state
    }

    /// Returns name of the underlying Bdev.
    pub(crate) fn bdev_name(&self) -> String {
        unsafe { self.bdev().name().to_string() }
    }

    /// Returns the actual size of the Nexus instance, in bytes.
    pub fn size_in_bytes(&self) -> u64 {
        unsafe { self.bdev().size_in_bytes() }
    }

    /// Returns Nexus's block size in bytes.
    pub fn block_len(&self) -> u64 {
        unsafe { self.bdev().block_len() as u64 }
    }

    /// Returns the actual size of the Nexus instance, in blocks.
    pub fn num_blocks(&self) -> u64 {
        unsafe { self.bdev().num_blocks() }
    }

    /// Returns the required alignment of the Nexus.
    pub fn alignment(&self) -> u64 {
        unsafe { self.bdev().alignment() }
    }

    /// Returns the required alignment of the Nexus.
    pub fn required_alignment(&self) -> u8 {
        unsafe { self.bdev().required_alignment() }
    }

    /// Reconfigures the child event handler.
    pub(crate) async fn reconfigure(&self, event: DrEvent) {
        info!(
            "{}: Dynamic reconfiguration event: {:?} started",
            self.name, event
        );

        let (sender, recv) = oneshot::channel::<ChannelTraverseStatus>();

        self.traverse_io_channels(
            |chan, _sender| -> ChannelTraverseStatus {
                chan.inner_mut().refresh();
                ChannelTraverseStatus::Ok
            },
            |status, sender| {
                info!("{}: Reconfigure completed", self.name);
                sender.send(status).expect("reconfigure channel gone");
            },
            sender,
        );

        let result = recv.await.expect("reconfigure sender already dropped");

        info!(
            "{}: Dynamic reconfiguration event: {:?} completed {:?}",
            self.name, event, result
        );
    }

    /// Opens the Nexus instance for IO.
    /// Once this function is called, the device is visible and can
    /// be used for IO.
    async fn register_instance(
        bdev: &mut spdk_rs::Bdev<Nexus<'_>>,
    ) -> Result<(), Error> {
        let mut nex = bdev.data_mut();
        assert_eq!(*nex.state.lock(), NexusState::Init);

        debug!("Opening nexus {}", nex.name);

        nex.as_mut().try_open_children().await?;

        // Register the bdev with SPDK and set the callbacks for io channel
        // creation.
        nex.register_io_device(Some(&nex.name));

        debug!(
            "{}: IO device registered at {:p}",
            nex.name, &*nex as *const Nexus
        );

        match bdev.register_bdev() {
            Ok(_) => {
                // Persist the fact that the nexus is now successfully open.
                // We have to do this before setting the nexus to open so that
                // nexus list does not return this nexus until it is persisted.
                nex.persist(PersistOp::Create).await;
                nex.as_mut().set_state(NexusState::Open);
                unsafe { nex.get_unchecked_mut().has_io_device = true };
                Ok(())
            }
            Err(err) => {
                unsafe {
                    for child in
                        nex.as_mut().get_unchecked_mut().children.iter_mut()
                    {
                        if let Err(e) = child.close().await {
                            error!(
                                "{}: child {} failed to close with error {}",
                                bdev.data_mut().name,
                                child.get_name(),
                                e.verbose()
                            );
                        }
                    }
                }
                nex.as_mut().set_state(NexusState::Closed);
                Err(err).context(RegisterNexus {
                    name: nex.name.clone(),
                })
            }
        }
    }

    /// Destroy the Nexus.
    pub async fn destroy(mut self: Pin<&mut Self>) -> Result<(), Error> {
        info!("Destroying nexus {}", self.name);

        self.as_mut().destroy_shares().await;

        // wait for all rebuild jobs to be cancelled before proceeding with the
        // destruction of the nexus
        for child in self.children.iter() {
            self.cancel_child_rebuild_jobs(child.get_name()).await;
        }

        unsafe {
            for child in self.as_mut().get_unchecked_mut().children.iter_mut() {
                info!("Destroying child bdev {}", child.get_name());
                if let Err(e) = child.close().await {
                    // TODO: should an error be returned here?
                    error!(
                        "Failed to close child {} with error {}",
                        child.get_name(),
                        e.verbose()
                    );
                }
            }
        }

        // Persist the fact that the nexus destruction has completed.
        self.persist(PersistOp::Shutdown).await;

        unsafe {
            let name = self.name.clone();
            match self.bdev_mut().unregister_bdev_async().await {
                Ok(_) => Ok(()),
                Err(_) => Err(Error::NexusDestroy {
                    name,
                }),
            }
        }
    }

    /// Returns a mutable reference to Nexus I/O.
    fn io_subsystem_mut(self: Pin<&mut Self>) -> &mut NexusIoSubsystem<'n> {
        unsafe { self.get_unchecked_mut().io_subsystem.as_mut().unwrap() }
    }

    /// Resumes I/O to the Bdev.
    /// Note: in order to handle concurrent resumes properly, this function must
    /// be called only from the master core.
    pub async fn resume(self: Pin<&mut Self>) -> Result<(), Error> {
        self.io_subsystem_mut().resume().await
    }

    /// Suspend any incoming IO to the bdev pausing the controller allows us to
    /// handle internal events and which is a protocol feature.
    /// In case concurrent pause requests take place, the other callers
    /// will wait till the nexus is resumed and will continue execution
    /// with the nexus paused once they are awakened via resume().
    /// Note: in order to handle concurrent pauses properly, this function must
    /// be called only from the master core.
    pub async fn pause(self: Pin<&mut Self>) -> Result<(), Error> {
        self.io_subsystem_mut().suspend().await
    }

    // Abort all active I/O for target child and set I/O fail-fast flag
    // for the child.
    #[allow(dead_code)]
    async fn update_failfast(
        &self,
        increment: bool,
        child: Option<String>,
    ) -> Result<(), Error> {
        let (sender, r) = oneshot::channel::<bool>();

        let ctx = UpdateFailFastCtx {
            sender,
            nexus: self.name.clone(),
            child,
        };

        assert!(self.has_io_device);

        self.traverse_io_channels(
            update_failfast_cb,
            update_failfast_done,
            ctx,
        );

        info!("{}: Updating fail-fast, increment={}", self.name, increment);
        r.await.expect("update failfast sender already dropped");
        info!("{}: Failfast updated", self.name);
        Ok(())
    }

    async fn child_retire_for_each_channel(
        &self,
        child: Option<String>,
    ) -> Result<(), Error> {
        let (sender, r) = oneshot::channel::<bool>();

        let ctx = UpdateFailFastCtx {
            sender,
            nexus: self.name.clone(),
            child,
        };

        // if let Some(io_device) = self.io_device.as_ref() {
        if self.has_io_device {
            self.traverse_io_channels(
                update_failfast_cb,
                update_failfast_done,
                ctx,
            );

            debug!(?self, "all channels retired");
            r.await.expect("update failfast sender already dropped");
        }

        Ok(())
    }

    pub async fn child_retire(
        mut self: Pin<&mut Self>,
        name: String,
    ) -> Result<(), Error> {
        self.child_retire_for_each_channel(Some(name.clone()))
            .await?;
        debug!(?self, "PAUSE");
        self.as_mut().pause().await?;
        debug!(?self, "UNPAUSE");
        if let Some(child) = self.lookup_child(&name) {
            let uri = child.name.clone();
            // schedule the deletion of the child eventhough etcd has not been
            // updated yet we do not need to wait for that to
            // complete anyway.
            MWQ.enqueue(Command::RemoveDevice(self.name.clone(), name.clone()));

            // Do not persist child state in case it's the last healthy child of
            // the nexus: let Control Plane reconstruct the nexus
            // using this device as the replica with the most recent
            // user data.
            self.persist(PersistOp::UpdateCond(
                (uri.clone(), child.state(), &|nexus_info| {
                    // Determine the amount of healthy replicas in the persistent state and
                    // check against the last healthy replica remaining.
                    let num_healthy = nexus_info.children.iter().fold(0, |n, c| {
                        if c.healthy {
                            n + 1
                        } else {
                            n
                        }
                    });

                    match num_healthy {
                        0 => {
                            warn!(
                                "nexus {}: no healthy replicas persent in persistent store when retiring replica {}:
                                not persisting the replica state",
                                &name, &uri,
                            );
                            false
                        }
                        1 => {
                            warn!(
                                "nexus {}: retiring the last healthy replica {}, not persisting the replica state",
                                &name, &uri,
                            );
                            false
                        },
                        _ => true,
                    }
                })))
                .await;
        }
        self.resume().await
    }

    #[allow(dead_code)]
    pub async fn set_failfast(&self) -> Result<(), Error> {
        self.update_failfast(true, None).await
    }

    #[allow(dead_code)]
    pub async fn clear_failfast(&self) -> Result<(), Error> {
        self.update_failfast(false, None).await
    }

    /// get ANA state of the NVMe subsystem
    pub async fn get_ana_state(&self) -> Result<NvmeAnaState, Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                let ana_state = subsystem.get_ana_state().await? as i32;
                return NvmeAnaState::from_i32(ana_state);
            }
        }

        Err(Error::NotSharedNvmf {
            name: self.name.clone(),
        })
    }

    /// set ANA state of the NVMe subsystem
    pub async fn set_ana_state(
        &self,
        ana_state: NvmeAnaState,
    ) -> Result<(), Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                subsystem.pause().await?;
                let res = subsystem.set_ana_state(ana_state as u32).await;
                subsystem.resume().await?;
                return Ok(res?);
            }
        }

        Err(Error::NotSharedNvmf {
            name: self.name.clone(),
        })
    }

    /// determine if any of the children do not support the requested
    /// io type. Break the loop on first occurrence.
    /// TODO: optionally add this check during nexus creation
    pub fn io_is_supported(&self, io_type: IoType) -> bool {
        !self
            .children
            .iter()
            .filter_map(|e| e.get_device().ok())
            .any(|b| !b.io_type_supported(io_type))
    }

    /// IO completion for local replica
    pub fn io_completion_local(_success: bool, _parent_io: *mut c_void) {
        unimplemented!();
    }

    /// Status of the nexus
    /// Online
    /// All children must also be online
    ///
    /// Degraded
    /// At least one child must be online
    ///
    /// Faulted
    /// No child is online so the nexus is faulted
    /// This may be made more configurable in the future
    pub fn status(&self) -> NexusStatus {
        match *self.state.lock() {
            NexusState::Init => NexusStatus::Degraded,
            NexusState::Closed => NexusStatus::Faulted,
            NexusState::Open | NexusState::Reconfiguring => {
                if self
                    .children
                    .iter()
                    // All children are online, so the Nexus is also online
                    .all(|c| c.state() == ChildState::Open)
                {
                    NexusStatus::Online
                } else if self
                    .children
                    .iter()
                    // at least one child online, so the Nexus is also online
                    .any(|c| c.state() == ChildState::Open)
                {
                    NexusStatus::Degraded
                } else {
                    // nexus has no children or at least no child is online
                    NexusStatus::Faulted
                }
            }
        }
    }
}

// Unsafe part of Nexus.
impl<'n> Nexus<'n> {
    /// Returns a reference to Nexus's Bdev.
    pub(super) unsafe fn bdev(&self) -> &Bdev<Nexus<'n>> {
        self.bdev
            .as_ref()
            .expect("Nexus Bdev object is not initialized")
    }

    /// Returns a mutable reference to Nexus's Bdev.
    pub(super) unsafe fn bdev_mut(
        self: Pin<&mut Self>,
    ) -> &mut Bdev<Nexus<'n>> {
        self.get_unchecked_mut().bdev.as_mut().unwrap()
    }

    /// Returns a pinned Bdev reference to allow calling methods that require a
    /// Pin<&mut>, e.g. methods of Share trait.
    pub(super) fn pin_bdev_mut(self: Pin<&mut Self>) -> Pin<&mut Bdev<Self>> {
        unsafe { Pin::new_unchecked(self.bdev_mut()) }
    }

    /// Sets the required alignment of the Nexus.
    pub(crate) unsafe fn set_required_alignment(
        self: Pin<&mut Self>,
        new_val: u8,
    ) {
        (*self.bdev_mut().unsafe_inner_mut_ptr()).required_alignment = new_val;
    }

    /// Sets the block size of the underlying device.
    pub(crate) unsafe fn set_block_len(self: Pin<&mut Self>, blk_size: u32) {
        self.bdev_mut().set_block_len(blk_size)
    }

    /// Sets number of blocks for this device.
    pub(crate) unsafe fn set_num_blocks(self: Pin<&mut Self>, count: u64) {
        self.bdev_mut().set_num_blocks(count)
    }

    /// TODO
    pub(crate) unsafe fn set_data_ent_offset(self: Pin<&mut Self>, val: u64) {
        self.get_unchecked_mut().data_ent_offset = val;
    }

    /// Open Bdev handle for the Nexus.
    pub(crate) unsafe fn open_bdev_handle(
        &self,
        read_write: bool,
    ) -> Result<BdevHandle, CoreError> {
        BdevHandle::open_with_bdev(self.bdev(), read_write)
    }
}

impl Drop for Nexus<'_> {
    fn drop(&mut self) {
        info!("Dropping Nexus instance: {}", self.name);
    }
}

impl Display for Nexus<'_> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        let _ = writeln!(
            f,
            "{}: state: {:?} blk_cnt: {}, blk_size: {}",
            self.name,
            self.state,
            self.num_blocks(),
            self.block_len(),
        );

        self.children
            .iter()
            .map(|c| write!(f, "\t{}", c))
            .for_each(drop);
        Ok(())
    }
}

impl IoDevice for Nexus<'_> {
    type ChannelData = NexusChannel;

    fn io_channel_create(self: Pin<&mut Self>) -> NexusChannel {
        debug!("{}: Creating IO channels", self.bdev_name());
        NexusChannel::new(self)
    }

    fn io_channel_destroy(self: Pin<&mut Self>, chan: NexusChannel) {
        debug!("{} Destroying IO channels", self.bdev_name());
        chan.clear(); // TODO: use chan drop.
    }
}

impl IoDeviceChannelTraverse for Nexus<'_> {}

unsafe fn unsafe_static_ptr(nexus: &Nexus) -> *mut Nexus<'static> {
    let r = ::std::mem::transmute::<_, &'static Nexus>(nexus);
    r as *const Nexus as *mut Nexus
}

impl<'n> BdevOps for Nexus<'n> {
    type ChannelData = NexusChannel;
    type BdevData = Nexus<'n>;
    type IoDev = Nexus<'n>;

    /// TODO
    fn destruct(mut self: Pin<&mut Self>) {
        // A closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if *self.state.lock() == NexusState::Closed {
            trace!("{}: already closed", self.name);
            return;
        }

        trace!("{}: closing, from state: {:?} ", self.name, self.state);

        let self_ptr = unsafe { unsafe_static_ptr(&*self) };

        Reactor::block_on(async move {
            let self_ref = unsafe { &mut *self_ptr };

            for child in self_ref.children.iter_mut() {
                if child.state() == ChildState::Open {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            self_ref.name,
                            child.get_name(),
                            e.verbose()
                        );
                    }
                }
            }

            self_ref.children.clear();
            self_ref.child_count = 0;
        });

        self.as_mut().unregister_io_device();

        unsafe {
            self.as_mut().get_unchecked_mut().has_io_device = false;
        }

        trace!("{}: closed", self.name);
        self.set_state(NexusState::Closed);
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    /// This function is not called when the IO is re-submitted (see below).
    fn submit_request(
        &self,
        chan: IoChannel<NexusChannel>,
        bio: BdevIo<Nexus<'n>>,
    ) {
        nexus_submit_request(chan, bio);
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        match io_type {
            // we always assume the device supports read/write commands
            // allow NVMe Admin as it is needed for local replicas
            IoType::Read | IoType::Write | IoType::NvmeAdmin => true,
            IoType::Flush
            | IoType::Reset
            | IoType::Unmap
            | IoType::WriteZeros => {
                let supported = self.io_is_supported(io_type);
                if !supported {
                    trace!(
                        "IO type {:?} not supported for {}",
                        io_type,
                        self.bdev_name()
                    );
                }
                supported
            }
            _ => {
                debug!(
                    "un matched IO type {:#?} not supported for {}",
                    io_type,
                    self.bdev_name()
                );
                false
            }
        }
    }

    /// Called per core to create IO channels per Nexus instance.
    fn get_io_device(&self) -> &Self::IoDev {
        trace!("{}: Get IO channel", self.bdev_name());
        self
    }

    /// Device specific information which is returned by the get_bdevs RPC call.
    fn dump_info_json(&self, w: JsonWriteContext) {
        w.write_named_array_begin("children");
        if let Err(err) = w.write(&self.children) {
            error!("Failed to dump into JSON: {}", err);
        }
        w.write_array_end();
    }
}

/// Create a new nexus and bring it online.
/// If we fail to create any of the children, then we fail the whole operation.
/// On failure, we must cleanup by destroying any children that were
/// successfully created. Also, once the nexus is created, there still might
/// be a configuration mismatch that would prevent us from going online.
/// Currently, we can only determine this once we are already online,
/// and so we check the errors twice for now.
pub async fn nexus_create(
    name: &str,
    size: u64,
    uuid: Option<&str>,
    children: &[String],
) -> Result<(), Error> {
    nexus_create_internal(
        name,
        size,
        uuid,
        None,
        NexusNvmeParams::default(),
        children,
        None,
    )
    .await
}

/// As create_nexus with additional parameters:
/// min_cntlid, max_cntldi: NVMe controller ID range when sharing over NVMf
/// resv_key: NVMe reservation key for children
pub async fn nexus_create_v2(
    name: &str,
    size: u64,
    uuid: &str,
    nvme_params: NexusNvmeParams,
    children: &[String],
    nexus_info_key: Option<String>,
) -> Result<(), Error> {
    if nvme_params.min_cntlid < NVME_MIN_CNTLID
        || nvme_params.min_cntlid > nvme_params.max_cntlid
        || nvme_params.max_cntlid > NVME_MAX_CNTLID
    {
        let args = format!(
            "invalid NVMe controller ID range [{:x}h, {:x}h]",
            nvme_params.min_cntlid, nvme_params.max_cntlid
        );
        error!("failed to create nexus {}: {}", name, args);
        return Err(Error::InvalidArguments {
            name: name.to_owned(),
            args,
        });
    }
    if nvme_params.resv_key == 0 {
        let args = "invalid NVMe reservation key";
        error!("failed to create nexus {}: {}", name, args);
        return Err(Error::InvalidArguments {
            name: name.to_owned(),
            args: args.to_string(),
        });
    }

    match uuid::Uuid::parse_str(name) {
        Ok(name_uuid) => {
            let bdev_uuid = name_uuid.to_string();
            let nexus_uuid = uuid::Uuid::parse_str(uuid).map_err(|_| {
                Error::InvalidUuid {
                    uuid: uuid.to_string(),
                }
            })?;
            nexus_create_internal(
                name,
                size,
                Some(bdev_uuid.as_str()),
                Some(nexus_uuid),
                nvme_params,
                children,
                nexus_info_key,
            )
            .await
        }
        Err(_) => {
            nexus_create_internal(
                name,
                size,
                Some(uuid),
                None,
                nvme_params,
                children,
                nexus_info_key,
            )
            .await
        }
    }
}

async fn nexus_create_internal(
    name: &str,
    size: u64,
    bdev_uuid: Option<&str>,
    nexus_uuid: Option<Uuid>,
    nvme_params: NexusNvmeParams,
    children: &[String],
    nexus_info_key: Option<String>,
) -> Result<(), Error> {
    if let Some(nexus) = nexus_lookup_name_uuid(name, nexus_uuid) {
        // FIXME: Instead of error, we return Ok without checking
        // that the children match, which seems wrong.
        if *nexus.state.lock() == NexusState::Init {
            return Err(Error::NexusInitialising {
                name: name.to_owned(),
            });
        }
        if nexus.name != name
            || (nexus_uuid.is_some() && Some(nexus.nexus_uuid) != nexus_uuid)
        {
            return Err(Error::UuidExists {
                uuid: nexus.nexus_uuid.to_string(),
                nexus: name.to_string(),
            });
        }
        return Ok(());
    }

    // Create a new Nexus object, and immediately add it to the global list.
    // This is necessary to ensure proper cleanup, as the code responsible for
    // closing a child assumes that the nexus to which it belongs will appear
    // in the global list of nexus instances. We must also ensure that the
    // nexus instance gets removed from the global list if an error occurs.
    let mut nexus_bdev = Nexus::new(
        name,
        size,
        bdev_uuid,
        nexus_uuid,
        nvme_params,
        None,
        nexus_info_key,
    );

    for child in children {
        if let Err(error) =
            nexus_bdev.data_mut().create_and_register(child).await
        {
            error!(
                "failed to create nexus {}: failed to create child {}: {}",
                name, child, error
            );
            nexus_bdev.data_mut().close_children().await;

            return Err(Error::CreateChild {
                source: error,
                name: String::from(name),
            });
        }
    }

    // let ni = nexus_bdev.data_mut();
    match Nexus::register_instance(&mut nexus_bdev).await {
        Err(Error::NexusIncomplete {
            ..
        }) => {
            // We still have code that waits for children to come online,
            // although this currently only works for config files.
            // We need to explicitly clean up child devices
            // if we get this error.
            error!(
                "failed to open nexus {}: not all children are available",
                name
            );
            let ni = nexus_bdev.data();
            for child in ni.children.iter() {
                // TODO: children may already be destroyed
                // TODO: mutability violation
                let _ = device_destroy(&child.name).await;
            }
            Err(Error::NexusCreate {
                name: String::from(name),
            })
        }

        Err(error) => {
            error!("failed to open nexus {}: {}", name, error);
            nexus_bdev.data_mut().close_children().await;
            Err(error)
        }

        Ok(_) => Ok(()),
    }
}
