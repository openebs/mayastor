//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
    marker::PhantomPinned,
    os::raw::c_void,
    pin::Pin,
};

use futures::channel::oneshot;
use serde::Serialize;
use snafu::ResultExt;
use uuid::Uuid;

use super::{
    nexus_err,
    nexus_injection::Injections,
    nexus_lookup_name_uuid,
    ChildState,
    DrEvent,
    Error,
    NbdDisk,
    NexusBio,
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
        CoreError,
        DeviceEventSink,
        IoType,
        Protocol,
        Reactor,
        Share,
        VerboseError,
    },
    subsys::NvmfSubsystem,
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

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

/// TODO
#[derive(Debug)]
pub enum NexusTarget {
    NbdDisk(NbdDisk),
    NexusNvmfTarget,
}

/// Sensitive nexus operations that might require extra checks against
/// current nexus state in order to be performed.
#[derive(Debug)]
pub enum NexusOperation {
    ReplicaAdd,
    ReplicaRemove,
    ReplicaOnline,
}

/// TODO
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

/// NVMe reservation types.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NvmeReservation {
    Reserved = 0,
    WriteExclusive = 1,
    ExclusiveAccess = 2,
    WriteExclusiveRegsOnly = 3,
    ExclusiveAccessRegsOnly = 4,
    WriteExclusiveAllRegs = 5,
    ExclusiveAccessAllRegs = 6,
}
impl TryFrom<u8> for NvmeReservation {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Reserved,
            1 => Self::WriteExclusive,
            2 => Self::ExclusiveAccess,
            3 => Self::WriteExclusiveRegsOnly,
            4 => Self::ExclusiveAccessRegsOnly,
            5 => Self::WriteExclusiveAllRegs,
            6 => Self::ExclusiveAccessAllRegs,
            reservation => {
                return Err(Error::InvalidReservation {
                    reservation,
                })
            }
        })
    }
}

/// Nexus NVMe preemption policy.
#[derive(Debug, Copy, Clone)]
pub enum NexusNvmePreemption {
    /// A "manual" preemption where we explicitly specify the reservation key,
    /// type and preempt key.
    ArgKey,
    /// An "automatic" preemption where we can preempt whatever is current
    /// holder. Useful when we just want to boot the existing holder out.
    Holder,
}

/// NVMe-specific parameters for the Nexus.
#[derive(Debug)]
pub struct NexusNvmeParams {
    /// The minimum NVMe controller ID for sharing over NVMf.
    pub(crate) min_cntlid: u16,
    /// The maximum NVMe controller ID.
    pub(crate) max_cntlid: u16,
    /// NVMe reservation key for children.
    pub(crate) resv_key: u64,
    /// NVMe preempt key for children, None to not preempt.
    pub(crate) preempt_key: Option<std::num::NonZeroU64>,
    /// NVMe reservation type.
    pub(crate) resv_type: NvmeReservation,
    /// NVMe Preempting policy.
    pub(crate) preempt_policy: NexusNvmePreemption,
}

impl Default for NexusNvmeParams {
    fn default() -> Self {
        NexusNvmeParams {
            min_cntlid: NVME_MIN_CNTLID,
            max_cntlid: NVME_MAX_CNTLID,
            resv_key: 0x1234_5678,
            preempt_key: None,
            resv_type: NvmeReservation::WriteExclusiveAllRegs,
            preempt_policy: NexusNvmePreemption::ArgKey,
        }
    }
}

impl NexusNvmeParams {
    /// Set the minimum controller id.
    pub fn set_min_cntlid(&mut self, min_cntlid: u16) {
        self.min_cntlid = min_cntlid;
    }
    /// Set the maximum controller id.
    pub fn set_max_cntlid(&mut self, max_cntlid: u16) {
        self.max_cntlid = max_cntlid;
    }
    /// Set the reservation key.
    pub fn set_resv_key(&mut self, resv_key: u64) {
        self.resv_key = resv_key;
    }
    /// Set the preemption key.
    pub fn set_preempt_key(
        &mut self,
        preempt_key: Option<std::num::NonZeroU64>,
    ) {
        self.preempt_key = preempt_key;
    }
    /// Set the reservation type.
    pub fn set_resv_type(&mut self, resv_type: NvmeReservation) {
        self.resv_type = resv_type;
    }
    /// Set the preemption policy.
    pub fn set_preempt_policy(&mut self, preempt_policy: NexusNvmePreemption) {
        self.preempt_policy = preempt_policy;
    }
}

/// The main nexus structure
pub struct Nexus<'n> {
    /// Name of the Nexus instance
    pub(crate) name: String,
    /// The requested size of the Nexus in bytes. Children are allowed to
    /// be larger. The actual Nexus size will be calculated based on the
    /// capabilities of the underlying child devices.
    req_size: u64,
    /// Vector of nexus children.
    children: Vec<NexusChild<'n>>,
    /// NVMe parameters
    pub(crate) nvme_params: NexusNvmeParams,
    /// uuid of the nexus (might not be the same as the nexus bdev!)
    nexus_uuid: Uuid,
    /// Bdev wrapper instance.
    bdev: Option<Bdev<Nexus<'n>>>,
    /// represents the current state of the Nexus
    pub(crate) state: parking_lot::Mutex<NexusState>,
    /// The offset in blocks where the data partition starts.
    pub(crate) data_ent_offset: u64,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
    /// enum containing the protocol-specific target used to publish the nexus
    pub(super) nexus_target: Option<NexusTarget>,
    /// Indicates if the Nexus has an I/O device.
    pub(super) has_io_device: bool,
    /// Information associated with the persisted NexusInfo structure.
    pub(super) nexus_info: futures::lock::Mutex<PersistentNexusInfo>,
    /// Nexus I/O subsystem.
    io_subsystem: Option<NexusIoSubsystem<'n>>,
    /// TODO
    event_sink: Option<DeviceEventSink>,
    /// TODO
    #[allow(dead_code)]
    pub(super) injections: Injections,
    /// Prevent auto-Unpin.
    _pin: PhantomPinned,
}

impl<'n> Debug for Nexus<'n> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        write!(f, "Nexus '{}' [{}]", self.name, s)
    }
}

/// Nexus status enumeration.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusStatus {
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
    /// Online
    Online,
    /// Shutdown in progress
    ShuttingDown,
    /// Shutdown
    Shutdown,
}

impl Display for NexusStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NexusStatus::Degraded => "degraded",
                NexusStatus::Online => "online",
                NexusStatus::Faulted => "faulted",
                NexusStatus::ShuttingDown => "shutting_down",
                NexusStatus::Shutdown => "shutdown",
            }
        )
    }
}

/// Nexus state enumeration.
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
    /// Shutdown in progress
    ShuttingDown,
    /// nexus has been shutdown
    Shutdown,
}

impl Display for NexusState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NexusState::Init => "init",
                NexusState::Closed => "closed",
                NexusState::Open => "open",
                NexusState::Reconfiguring => "reconfiguring",
                NexusState::ShuttingDown => "shutting_down",
                NexusState::Shutdown => "shutdown",
            }
        )
    }
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
        nexus_info_key: Option<String>,
    ) -> spdk_rs::Bdev<Nexus<'n>> {
        let n = Nexus {
            name: name.to_string(),
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
            injections: Injections::new(),
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

        info!(
            "{:?}: creating new nexus bdev with UUID '{}'",
            bdev.data(),
            bdev.data().uuid()
        );

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
                    debug!("Nexus '{}': UUID set to '{}'", name, u);
                    return u.into();
                }
                Err(error) => {
                    warn!("Nexus '{}': invalid UUID '{}': {}", name, s, error);
                }
            },
            None => {
                warn!("Nexus '{}': no UUID specified", name);
            }
        }

        let u = spdk_rs::Uuid::generate();
        debug!("Nexus '{}': using generated UUID '{}'", name, u);
        u
    }

    /// Returns nexus name.
    pub(crate) fn nexus_name(&self) -> &str {
        &self.name
    }

    /// Returns the Nexus uuid.
    pub(crate) fn uuid(&self) -> Uuid {
        self.nexus_uuid
    }

    /// Sets the state of the Nexus.
    fn set_state(self: Pin<&mut Self>, state: NexusState) -> NexusState {
        debug!("{:?}: changing state to '{}'", self, state);
        *self.state.lock() = state;
        state
    }

    /// Returns name of the underlying Bdev.
    pub(crate) fn bdev_name(&self) -> String {
        unsafe { self.bdev().name().to_string() }
    }

    /// TODO
    pub fn req_size(&self) -> u64 {
        self.req_size
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

    /// TODO
    pub fn children(&self) -> &Vec<NexusChild<'n>> {
        &self.children
    }

    /// TODO
    pub(super) unsafe fn child_add_unsafe(
        self: Pin<&mut Self>,
        child: NexusChild<'n>,
    ) {
        self.unpin_mut().children.push(child)
    }

    /// TODO
    pub(super) unsafe fn child_remove_at_unsafe(
        self: Pin<&mut Self>,
        idx: usize,
    ) {
        debug!(
            "{:?}: removing child at index: {}: '{}'",
            self,
            idx,
            self.children[idx].uri()
        );
        self.unpin_mut().children.remove(idx);
    }

    /// TODO
    pub fn child_at(&self, idx: usize) -> &NexusChild<'n> {
        self.children.get(idx).expect("Bad child index")
    }

    /// TODO
    pub(super) unsafe fn child_at_mut(
        self: Pin<&mut Self>,
        idx: usize,
    ) -> &mut NexusChild<'n> {
        self.unpin_mut()
            .children
            .get_mut(idx)
            .expect("Bad child index")
    }

    /// TODO
    pub fn children_iter(&self) -> std::slice::Iter<NexusChild<'n>> {
        self.children.iter()
    }

    /// TODO
    pub(super) unsafe fn children_iter_mut(
        self: Pin<&mut Self>,
    ) -> std::slice::IterMut<NexusChild<'n>> {
        self.unpin_mut().children.iter_mut()
    }

    /// TODO
    pub fn child_count(&self) -> usize {
        self.children.len()
    }

    /// Check whether nexus can perform target operation.
    pub(crate) fn check_nexus_operation(
        &self,
        _op: NexusOperation,
    ) -> Result<(), Error> {
        match *self.state.lock() {
            // When nexus under shutdown or is shutdown, no further nexus
            // operations allowed.
            NexusState::ShuttingDown | NexusState::Shutdown => {
                Err(Error::OperationNotAllowed {
                    reason: "Nexus is shutdown".to_string(),
                })
            }
            _ => Ok(()),
        }
    }

    /// Reconfigures the child event handler.
    pub(crate) async fn reconfigure(&self, event: DrEvent) {
        info!(
            "{:?}: dynamic reconfiguration event: {} started...",
            self, event
        );

        let (sender, recv) = oneshot::channel::<ChannelTraverseStatus>();

        self.traverse_io_channels(
            |chan, _sender| -> ChannelTraverseStatus {
                chan.reconnect_all();
                ChannelTraverseStatus::Ok
            },
            |status, sender| {
                debug!("{:?}: reconfigure completed", self);
                sender.send(status).expect("reconfigure channel gone");
            },
            sender,
        );

        let result = recv.await.expect("reconfigure sender already dropped");

        info!(
            "{:?}: dynamic reconfiguration event: {} completed: {:?}",
            self, event, result
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

        info!("{:?}: registering nexus bdev...", nex);

        nex.as_mut().try_open_children().await?;

        // Register the bdev with SPDK and set the callbacks for io channel
        // creation.
        nex.register_io_device(Some(&nex.name));

        info!("{:?}: IO device registered", nex);

        match bdev.register_bdev() {
            Ok(_) => {
                // Persist the fact that the nexus is now successfully open.
                // We have to do this before setting the nexus to open so that
                // nexus list does not return this nexus until it is persisted.
                nex.persist(PersistOp::Create).await;
                nex.as_mut().set_state(NexusState::Open);
                unsafe { nex.as_mut().unpin_mut().has_io_device = true };
                info!("{:?}: nexus bdev registered successfully", nex);
                Ok(())
            }
            Err(err) => {
                error!(
                    "{:?}: nexus bdev registration failed: {}",
                    nex,
                    err.verbose()
                );
                unsafe {
                    for child in nex.as_mut().children_iter_mut() {
                        if let Err(e) = child.close().await {
                            error!(
                                "{:?}: child failed to close: {}",
                                child,
                                e.verbose()
                            );
                        }
                    }
                }
                nex.as_mut().set_state(NexusState::Closed);
                Err(err).context(nexus_err::RegisterNexus {
                    name: nex.name.clone(),
                })
            }
        }
    }

    /// Destroy the Nexus.
    pub async fn destroy(mut self: Pin<&mut Self>) -> Result<(), Error> {
        info!("{:?}: destroying nexus...", self);

        self.as_mut().destroy_shares().await;

        // wait for all rebuild jobs to be cancelled before proceeding with the
        // destruction of the nexus
        let child_uris = self.children_uris();
        for child in child_uris {
            self.as_mut().cancel_rebuild_jobs(&child).await;
        }

        info!("{:?}: closing {} children...", self, self.children.len());
        unsafe {
            for child in self.as_mut().children_iter_mut() {
                if let Err(e) = child.close().await {
                    // TODO: should an error be returned here?
                    error!(
                        "{:?}: child failed to close: {}",
                        child,
                        e.verbose()
                    );
                }
            }
        }
        info!("{:?}: children closed", self);

        // Persist the fact that the nexus destruction has completed.
        self.persist(PersistOp::Shutdown).await;

        unsafe {
            let name = self.name.clone();

            // After calling unregister_bdev_async(), Nexus is gone.
            match self.as_mut().bdev_mut().unregister_bdev_async().await {
                Ok(_) => {
                    info!("Nexus '{}': nexus destroyed ok", name);
                    Ok(())
                }
                Err(err) => {
                    error!(
                        "Nexus '{}': failed to destroy: {}",
                        name,
                        err.verbose()
                    );
                    Err(Error::NexusDestroy {
                        name,
                    })
                }
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

    pub async fn shutdown(mut self: Pin<&mut Self>) -> Result<(), Error> {
        let prev_state = {
            let mut s = self.state.lock();

            match *s {
                // If nexus is already shutdown, operation is idempotent.
                NexusState::Shutdown => {
                    info!(
                        nexus=%self.name,
                        "Nexus is already shutdown, skipping shutdown operation"
                    );
                    return Ok(());
                }
                // In case of active shutdown operation, bail out.
                NexusState::ShuttingDown => {
                    return Err(Error::OperationNotAllowed {
                        reason: "Shutdown operation is already in progress"
                            .to_string(),
                    });
                }
                // Save current state and mark nexus as being under shutdown.
                t => {
                    *s = NexusState::ShuttingDown;
                    t
                }
            }
        };

        // Step 1: pause subsystem.
        // In case of error, restore previous nexus state.
        info!(
            nexus=%self.name,
            "Shutting down nexus"
        );
        self.as_mut().pause().await.map_err(|error| {
            error!(
                %error,
                nexus=%self.name,
                "Failed to pause I/O subsystem, shutdown failed"
            );

            // Restore previous nexus state.
            *self.state.lock() = prev_state;
            error
        })?;

        info!(
            nexus=%self.name,
            "I/O subsystem paused"
        );

        // Step 2: cancel all active rebuild jobs.
        let child_uris = self.children_uris();
        for child in child_uris {
            self.as_mut().cancel_rebuild_jobs(&child).await;
        }

        // Step 3: Close all nexus children.
        self.as_mut().close_children().await;

        // Step 4: Mark nexus as being properly shutdown in ETCd.
        self.persist(PersistOp::Shutdown).await;

        // Finally, mark nexus as being fully shutdown.
        *self.state.lock() = NexusState::Shutdown;

        info!(
            nexus=%self.name,
            "Nexus successfully shut down"
        );
        Ok(())
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
            NexusState::ShuttingDown => NexusStatus::ShuttingDown,
            NexusState::Shutdown => NexusStatus::Shutdown,
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
    /// Returns a mutable reference to the Nexus with the lifetime as the Nexus
    /// itself.
    #[inline(always)]
    unsafe fn unpin_mut(self: Pin<&mut Self>) -> &'n mut Nexus<'n> {
        &mut *(self.get_unchecked_mut() as *mut _)
    }

    /// Returns a pinned mutable reference of the same lifetime as the Nexus
    /// itself.
    #[inline(always)]
    pub(super) unsafe fn pinned_mut(
        self: Pin<&mut Self>,
    ) -> Pin<&'n mut Nexus<'n>> {
        Pin::new_unchecked(self.unpin_mut())
    }

    /// Returns a reference to Nexus's Bdev.
    #[inline(always)]
    pub(super) unsafe fn bdev(&self) -> &Bdev<Nexus<'n>> {
        self.bdev
            .as_ref()
            .expect("Nexus Bdev object is not initialized")
    }

    /// Returns a mutable reference to Nexus's Bdev.
    #[inline(always)]
    pub(super) unsafe fn bdev_mut(
        self: Pin<&mut Self>,
    ) -> &mut Bdev<Nexus<'n>> {
        self.get_unchecked_mut().bdev.as_mut().unwrap()
    }

    /// Returns a pinned Bdev reference to allow calling methods that require a
    /// Pin<&mut>, e.g. methods of Share trait.
    #[inline(always)]
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
    ) -> Result<BdevHandle<Self>, CoreError> {
        BdevHandle::open_with_bdev(self.bdev(), read_write)
    }
}

impl Drop for Nexus<'_> {
    fn drop(&mut self) {
        info!("{:?}: dropping nexus bdev", self);
    }
}

impl<'n> IoDevice for Nexus<'n> {
    type ChannelData = NexusChannel<'n>;

    fn io_channel_create(self: Pin<&mut Self>) -> NexusChannel<'n> {
        NexusChannel::new(self)
    }

    fn io_channel_destroy(self: Pin<&mut Self>, chan: NexusChannel<'n>) {
        chan.destroy();
    }
}

impl IoDeviceChannelTraverse for Nexus<'_> {}

unsafe fn unsafe_static_ptr(nexus: &Nexus) -> *mut Nexus<'static> {
    let r = ::std::mem::transmute::<_, &'static Nexus>(nexus);
    r as *const Nexus as *mut Nexus
}

impl<'n> BdevOps for Nexus<'n> {
    type ChannelData = NexusChannel<'n>;
    type BdevData = Nexus<'n>;
    type IoDev = Nexus<'n>;

    /// TODO
    fn destruct(mut self: Pin<&mut Self>) {
        info!("{:?}: unregistering nexus bdev...", self);

        // A closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if *self.state.lock() == NexusState::Closed {
            info!("{:?}: nexus already closed", self);
            return;
        }

        let self_ptr = unsafe { unsafe_static_ptr(&*self) };

        Reactor::block_on(async move {
            let self_ref = unsafe { &mut *self_ptr };

            let n = self_ref
                .children
                .iter()
                .filter(|c| c.state() == ChildState::Open)
                .count();

            if n > 0 {
                warn!(
                    "{:?}: {} open children remain(s), closing...",
                    self_ref, n
                );

                for child in self_ref.children.iter_mut() {
                    if child.state() == ChildState::Open {
                        if let Err(e) = child.close().await {
                            error!(
                                "{:?}: child failed to close: {}",
                                child,
                                e.verbose()
                            );
                        }
                    }
                }
            }

            self_ref.children.clear();
        });

        self.as_mut().unregister_io_device();
        unsafe {
            self.as_mut().get_unchecked_mut().has_io_device = false;
        }

        self.as_mut().set_state(NexusState::Closed);

        info!("{:?}: nexus bdev unregistered", self);
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    /// This function is not called when the IO is re-submitted (see below).
    fn submit_request(
        &self,
        chan: IoChannel<NexusChannel<'n>>,
        bio: BdevIo<Nexus<'n>>,
    ) {
        let io = NexusBio::new(chan, bio);
        io.submit_request();
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
                    info!(
                        "{:?}: I/O type '{:?}' not supported by at least \
                        one of child devices",
                        self, io_type
                    );
                }
                supported
            }
            _ => {
                warn!(
                    "{:?}: I/O type '{:?}' support not implemented",
                    self, io_type
                );
                false
            }
        }
    }

    /// Called per core to create IO channels per Nexus instance.
    fn get_io_device(&self) -> &Self::IoDev {
        trace!("{:?}: getting IO channel", self);
        self
    }

    /// Device specific information which is returned by the get_bdevs RPC call.
    fn dump_info_json(&self, w: JsonWriteContext) {
        w.write_named_array_begin("children");
        if let Err(err) = w.write(&self.children) {
            error!("Failed to dump into JSON: {}", err.to_string());
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
    info!("Creating new nexus '{}'...", name);

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
        nexus_info_key,
    );

    for uri in children {
        if let Err(error) = nexus_bdev.data_mut().new_child(uri).await {
            error!(
                "{:?}: failed to add child '{}': {}",
                nexus_bdev.data(),
                uri,
                error.verbose()
            );
            nexus_bdev.data_mut().close_children().await;

            error!(
                "{:?}: nexus creation failed: failed to create child '{}'",
                nexus_bdev.data(),
                uri
            );

            return Err(Error::CreateChild {
                source: error,
                name: name.to_owned(),
            });
        }
    }

    match Nexus::register_instance(&mut nexus_bdev).await {
        Err(Error::NexusIncomplete {
            ..
        }) => {
            // We still have code that waits for children to come online,
            // although this currently only works for config files.
            // We need to explicitly clean up child devices
            // if we get this error.
            error!("{:?}: not all children are available", nexus_bdev.data());
            for child in nexus_bdev.data().children_iter() {
                // TODO: children may already be destroyed
                // TODO: mutability violation
                let _ = device_destroy(child.uri()).await;
            }
            Err(Error::NexusCreate {
                name: String::from(name),
            })
        }

        Err(error) => {
            error!(
                "{:?}: failed to open nexus: {}",
                nexus_bdev.data(),
                error.verbose()
            );
            nexus_bdev.data_mut().close_children().await;
            Err(error)
        }
        Ok(_) => {
            info!("{:?}: nexus created ok", nexus_bdev.data());
            Ok(())
        }
    }
}
