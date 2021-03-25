//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    env,
    fmt::{Display, Formatter},
    os::raw::c_void,
    ptr::NonNull,
};

use futures::{channel::oneshot, future::join_all};
use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use tonic::{Code, Status};

use rpc::mayastor::NvmeAnaState;
use spdk_sys::{
    spdk_bdev,
    spdk_bdev_register,
    spdk_bdev_unregister,
    spdk_io_device_register,
    spdk_io_device_unregister,
};

use crate::{
    bdev::{
        nexus,
        nexus::{
            instances,
            nexus_channel::{DrEvent, NexusChannel, ReconfigureCtx},
            nexus_child::{ChildError, ChildState, NexusChild},
            nexus_label::LabelError,
            nexus_nbd::{NbdDisk, NbdError},
        },
    },
    core::{Bdev, CoreError, IoType, Protocol, Reactor, Share},
    ffihelper::errno_result_from_i32,
    nexus_uri::{bdev_destroy, NexusBdevError},
    rebuild::RebuildError,
    subsys::{NvmfError, NvmfSubsystem},
};

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
    #[snafu(display("Invalid nexus uuid \"{}\"", uuid))]
    InvalidUuid { uuid: String },
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
    #[snafu(display("Failed to share iscsi nexus {}", name))]
    ShareIscsiNexus { source: CoreError, name: String },
    #[snafu(display("Failed to share nvmf nexus {}", name))]
    ShareNvmfNexus { source: CoreError, name: String },
    #[snafu(display("Failed to unshare nexus {}", name))]
    UnshareNexus { source: CoreError, name: String },
    #[snafu(display(
        "Failed to read child label of nexus {}: {}",
        name,
        source
    ))]
    ReadLabel { source: LabelError, name: String },
    #[snafu(display(
        "Failed to write child label of nexus {}: {}",
        name,
        source
    ))]
    WriteLabel { source: LabelError, name: String },
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
    #[snafu(display("Suitable rebuild source for nexus {} not found", name))]
    NoRebuildSource { name: String },
    #[snafu(display(
        "Failed to create rebuild job for child {} of nexus {}",
        child,
        name,
    ))]
    CreateRebuildError {
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
    RebuildOperationError {
        job: String,
        name: String,
        source: RebuildError,
    },
    #[snafu(display("Invalid ShareProtocol value {}", sp_value))]
    InvalidShareProtocol { sp_value: i32 },
    #[snafu(display("Invalid NvmeAnaState value {}", ana_value))]
    InvalidNvmeAnaState { ana_value: i32 },
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
    SubsysNvmfError { e: String },
}

impl From<NvmfError> for Error {
    fn from(error: NvmfError) -> Self {
        Error::SubsysNvmfError {
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
    NexusIscsiTarget,
    NexusNvmfTarget,
}

/// The main nexus structure
#[derive(Debug)]
pub struct Nexus {
    /// Name of the Nexus instance
    pub(crate) name: String,
    /// the requested size of the nexus, children are allowed to be larger
    pub(crate) size: u64,
    /// number of children part of this nexus
    pub(crate) child_count: u32,
    /// vector of children
    pub children: Vec<NexusChild>,
    /// inner bdev
    pub(crate) bdev: Bdev,
    /// raw pointer to bdev (to destruct it later using Box::from_raw())
    bdev_raw: *mut spdk_bdev,
    /// represents the current state of the Nexus
    pub(super) state: std::sync::Mutex<NexusState>,
    /// the offset in num blocks where the data partition starts
    pub data_ent_offset: u64,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
    /// enum containing the protocol-specific target used to publish the nexus
    pub nexus_target: Option<NexusTarget>,
}

unsafe impl core::marker::Sync for Nexus {}
unsafe impl core::marker::Send for Nexus {}

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
}

impl ToString for NexusState {
    fn to_string(&self) -> String {
        match *self {
            NexusState::Init => "init",
            NexusState::Closed => "closed",
            NexusState::Open => "open",
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

impl Drop for Nexus {
    fn drop(&mut self) {
        unsafe {
            let b: Box<spdk_bdev> = Box::from_raw(self.bdev_raw);
            let _ = std::ffi::CString::from_raw(b.name);
            let _ = std::ffi::CString::from_raw(b.product_name);
        }
    }
}

impl Nexus {
    /// create a new nexus instance with optionally directly attaching
    /// children to it.
    pub fn new(
        name: &str,
        size: u64,
        uuid: Option<&str>,
        child_bdevs: Option<&[String]>,
    ) -> Box<Self> {
        let mut b = Box::new(spdk_bdev::default());

        b.name = c_str!(name);
        b.product_name = c_str!(NEXUS_PRODUCT_ID);
        b.fn_table = nexus::fn_table().unwrap();
        b.module = nexus::module().unwrap();
        b.blocklen = 0;
        b.blockcnt = 0;
        b.required_alignment = 9;

        let mut n = Box::new(Nexus {
            name: name.to_string(),
            child_count: 0,
            children: Vec::new(),
            bdev: Bdev::from(&*b as *const _ as *mut spdk_bdev),
            state: std::sync::Mutex::new(NexusState::Init),
            bdev_raw: Box::into_raw(b),
            data_ent_offset: 0,
            share_handle: None,
            size,
            nexus_target: None,
        });

        n.bdev.set_uuid(uuid.map(String::from));

        if let Some(child_bdevs) = child_bdevs {
            n.register_children(child_bdevs);
        }

        // store a reference to the Self in the bdev structure.
        unsafe {
            (*n.bdev.as_ptr()).ctxt = n.as_ref() as *const _ as *mut c_void;
        }
        n
    }

    /// set the state of the nexus
    pub(crate) fn set_state(&mut self, state: NexusState) -> NexusState {
        debug!(
            "{} Transitioned state from {:?} to {:?}",
            self.name, self.state, state
        );
        *self.state.lock().unwrap() = state;
        state
    }
    /// returns the size in bytes of the nexus instance
    pub fn size(&self) -> u64 {
        u64::from(self.bdev.block_len()) * self.bdev.num_blocks()
    }

    /// reconfigure the child event handler
    pub(crate) async fn reconfigure(&self, event: DrEvent) {
        let (s, r) = oneshot::channel::<i32>();

        info!(
            "{}: Dynamic reconfiguration event: {:?} started",
            self.name, event
        );

        let ctx = Box::new(ReconfigureCtx::new(
            s,
            NonNull::new(self.as_ptr()).unwrap(),
        ));

        NexusChannel::reconfigure(self.as_ptr(), ctx, &event);

        let result = r.await.expect("reconfigure sender already dropped");

        info!(
            "{}: Dynamic reconfiguration event: {:?} completed {:?}",
            self.name, event, result
        );
    }

    /// Opens the Nexus instance for IO
    pub async fn open(&mut self) -> Result<(), Error> {
        debug!("Opening nexus {}", self.name);

        self.try_open_children().await?;
        self.sync_labels().await?;
        self.register().await
    }

    pub async fn sync_labels(&mut self) -> Result<(), Error> {
        if env::var("NEXUS_DONT_READ_LABELS").is_ok() {
            // This is to allow for the specific case where the underlying
            // child devices are NULL bdevs, which may be written to
            // but cannot be read from. Just write out new labels,
            // and don't attempt to read them back afterwards.
            warn!("NOT reading disk labels on request");
            return self.create_child_labels().await.context(WriteLabel {
                name: self.name.clone(),
            });
        }

        // update child labels as necessary
        if let Err(error) = self.update_child_labels().await {
            warn!("error updating child labels: {}", error);
        }

        // check if we can read the labels back
        self.validate_child_labels().await.context(ReadLabel {
            name: self.name.clone(),
        })?;

        Ok(())
    }

    /// close the nexus and any children that are open
    pub(crate) fn destruct(&mut self) -> NexusState {
        // a closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if *self.state.lock().unwrap() == NexusState::Closed {
            trace!("{}: already closed", self.name);
            return NexusState::Closed;
        }

        trace!("{}: closing, from state: {:?} ", self.name, self.state);

        let nexus_name = self.name.clone();
        Reactor::block_on(async move {
            let nexus = nexus_lookup(&nexus_name).expect("Nexus not found");
            for child in &mut nexus.children {
                if child.state() == ChildState::Open {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            nexus.name,
                            child.get_name(),
                            e.verbose()
                        );
                    }
                }
            }
        });

        unsafe {
            spdk_io_device_unregister(self.as_ptr(), None);
        }

        trace!("{}: closed", self.name);
        self.set_state(NexusState::Closed)
    }

    /// Destroy the nexus
    pub async fn destroy(&mut self) -> Result<(), Error> {
        info!("Destroying nexus {}", self.name);
        // used to synchronize the destroy call
        extern "C" fn nexus_destroy_cb(arg: *mut c_void, rc: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<bool>) };

            if rc == 0 {
                let _ = s.send(true);
            } else {
                error!("failed to destroy nexus {}", rc);
                let _ = s.send(false);
            }
        }

        let _ = self.unshare_nexus().await;
        assert_eq!(self.share_handle, None);

        // no-op when not shared and will be removed once the old share bits are
        // gone
        self.bdev.unshare().await.unwrap();

        // wait for all rebuild jobs to be cancelled before proceeding with the
        // destruction of the nexus
        for child in self.children.iter() {
            self.cancel_child_rebuild_jobs(child.get_name()).await;
        }

        for child in self.children.iter_mut() {
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

        let (s, r) = oneshot::channel::<bool>();

        unsafe {
            // This will trigger a callback to destruct() in the fn_table.
            spdk_bdev_unregister(
                self.bdev.as_ptr(),
                Some(nexus_destroy_cb),
                Box::into_raw(Box::new(s)) as *mut _,
            );
        }

        if r.await.unwrap() {
            // Update the child states to remove them from the config file.
            NexusChild::save_state_change();
            Ok(())
        } else {
            Err(Error::NexusDestroy {
                name: self.name.clone(),
            })
        }
    }

    /// resume IO to the bdev
    pub(crate) async fn resume(&self) -> Result<(), Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                subsystem.resume().await.unwrap();
            }
        }

        Ok(())
    }

    /// suspend any incoming IO to the bdev pausing the controller allows us to
    /// handle internal events and which is a protocol feature.
    pub(crate) async fn pause(&self) -> Result<(), Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                subsystem.pause().await.unwrap();
            }
        }

        Ok(())
    }

    /// get ANA state of the NVMe subsystem
    pub async fn get_ana_state(&self) -> Result<NvmeAnaState, Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                let ana_state = subsystem.get_ana_state().await? as i32;
                return NvmeAnaState::from_i32(ana_state).ok_or({
                    Error::InvalidNvmeAnaState {
                        ana_value: ana_state,
                    }
                });
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

    /// register the bdev with SPDK and set the callbacks for io channel
    /// creation. Once this function is called, the device is visible and can
    /// be used for IO.
    pub(crate) async fn register(&mut self) -> Result<(), Error> {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);

        unsafe {
            spdk_io_device_register(
                self.as_ptr(),
                Some(NexusChannel::create),
                Some(NexusChannel::destroy),
                std::mem::size_of::<NexusChannel>() as u32,
                (*self.bdev.as_ptr()).name,
            );
        }

        debug!("{}: IO device registered at {:p}", self.name, self.as_ptr());

        let errno = unsafe { spdk_bdev_register(self.bdev.as_ptr()) };

        match errno_result_from_i32((), errno) {
            Ok(_) => {
                self.set_state(NexusState::Open);
                Ok(())
            }
            Err(err) => {
                unsafe {
                    spdk_io_device_unregister(self.as_ptr(), None);
                }
                for child in &mut self.children {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            self.name,
                            child.get_name(),
                            e.verbose()
                        );
                    }
                }
                self.set_state(NexusState::Closed);
                Err(err).context(RegisterNexus {
                    name: self.name.clone(),
                })
            }
        }
    }

    /// takes self and converts into a raw pointer
    pub(crate) fn as_ptr(&self) -> *mut c_void {
        self as *const _ as *mut _
    }

    /// takes a raw pointer and casts it to Self
    pub(crate) unsafe fn from_raw<'a>(n: *mut c_void) -> &'a mut Self {
        &mut *(n as *mut Nexus)
    }

    /// determine if any of the children do not support the requested
    /// io type. Break the loop on first occurrence.
    /// TODO: optionally add this check during nexus creation
    pub fn io_is_supported(&self, io_type: IoType) -> bool {
        self.children
            .iter()
            .filter_map(|e| e.get_device().ok())
            .any(|b| b.io_type_supported(io_type))
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
        match *self.state.lock().unwrap() {
            NexusState::Init => NexusStatus::Degraded,
            NexusState::Closed => NexusStatus::Faulted,
            NexusState::Open => {
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
    // global variable defined in the nexus module
    let nexus_list = instances();

    if nexus_list.iter().any(|n| n.name == name) {
        // FIXME: Instead of error, we return Ok without checking
        // that the children match, which seems wrong.
        return Ok(());
    }

    // Create a new Nexus object, and immediately add it to the global list.
    // This is necessary to ensure proper cleanup, as the code responsible for
    // closing a child assumes that the nexus to which it belongs will appear
    // in the global list of nexus instances. We must also ensure that the
    // nexus instance gets removed from the global list if an error occurs.
    nexus_list.push(Nexus::new(name, size, uuid, None));

    // Obtain a reference to the newly created Nexus object.
    let ni =
        nexus_list
            .iter_mut()
            .find(|n| n.name == name)
            .ok_or_else(|| Error::NexusNotFound {
                name: String::from(name),
            })?;

    for child in children {
        if let Err(error) = ni.create_and_register(child).await {
            error!(
                "failed to create nexus {}: failed to create child {}: {}",
                name, child, error
            );
            ni.close_children().await;
            nexus_list.retain(|n| n.name != name);
            return Err(Error::CreateChild {
                source: error,
                name: String::from(name),
            });
        }
    }

    match ni.open().await {
        Err(Error::NexusIncomplete {
            ..
        }) => {
            // We still have code that waits for children to come online,
            // although this currently only works for config files.
            // We need to explicitly clean up child bdevs if we get this error.
            error!("failed to open nexus {}: missing children", name);
            destroy_child_bdevs(name, children).await;
            nexus_list.retain(|n| n.name != name);
            Err(Error::NexusCreate {
                name: String::from(name),
            })
        }

        Err(error) => {
            error!("failed to open nexus {}: {}", name, error);
            ni.close_children().await;
            nexus_list.retain(|n| n.name != name);
            Err(error)
        }

        Ok(_) => Ok(()),
    }
}

/// Destroy list of child bdevs
async fn destroy_child_bdevs(name: &str, list: &[String]) {
    let futures = list.iter().map(String::as_str).map(bdev_destroy);
    let results = join_all(futures).await;
    if results.iter().any(|c| c.is_err()) {
        error!("{}: Failed to destroy child bdevs", name);
    }
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    instances()
        .iter_mut()
        .find(|n| n.name == name)
        .map(AsMut::as_mut)
}

impl Display for Nexus {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        let _ = writeln!(
            f,
            "{}: state: {:?} blk_cnt: {}, blk_size: {}",
            self.name,
            self.state,
            self.bdev.num_blocks(),
            self.bdev.block_len(),
        );

        self.children
            .iter()
            .map(|c| write!(f, "\t{}", c))
            .for_each(drop);
        Ok(())
    }
}
