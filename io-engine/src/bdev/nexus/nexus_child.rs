use std::{
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
};

use crossbeam::atomic::AtomicCell;
use futures::{channel::mpsc, SinkExt, StreamExt};
use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use url::Url;

use super::{nexus_lookup_mut, DrEvent};

use crate::{
    bdev::{device_create, device_destroy, device_lookup},
    bdev_api::BdevError,
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        CoreError,
        DeviceEventSink,
        Reactor,
        Reactors,
        VerboseError,
    },
    persistent_store::PersistentStore,
    rebuild::RebuildJob,
};

use spdk_rs::{
    libspdk::{
        spdk_nvme_registered_ctrlr_extended_data,
        spdk_nvme_reservation_status_extended_data,
    },
    nvme_reservation_acquire_action,
    nvme_reservation_register_action,
    nvme_reservation_register_cptpl,
    nvme_reservation_type,
    DmaError,
};

#[derive(Debug, Snafu)]
#[snafu(context(suffix(false)))]
pub enum ChildError {
    #[snafu(display("Child is permanently faulted"))]
    PermanentlyFaulted {},
    #[snafu(display("Child is faulted, it cannot be reopened"))]
    ChildFaulted {},
    #[snafu(display("Child is being destroyed"))]
    ChildBeingDestroyed {},
    #[snafu(display(
        "Child is smaller than parent {} vs {}",
        child_size,
        parent_size
    ))]
    ChildTooSmall { child_size: u64, parent_size: u64 },
    #[snafu(display("Open child"))]
    OpenChild { source: CoreError },
    #[snafu(display("Claim child"))]
    ClaimChild { source: Errno },
    #[snafu(display("Child is inaccessible"))]
    ChildInaccessible {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Failed to create a BlockDeviceHandle for child"))]
    HandleCreate { source: CoreError },
    #[snafu(display("Failed to open a BlockDeviceHandle for child"))]
    HandleOpen { source: CoreError },
    #[snafu(display("Failed to allocate DmaBuffer for child"))]
    HandleDmaMalloc { source: DmaError },
    #[snafu(display("Failed to register key for child: {}", source))]
    ResvRegisterKey { source: CoreError },
    #[snafu(display("Failed to acquire reservation for child: {}", source))]
    ResvAcquire { source: CoreError },
    #[snafu(display(
        "Failed to get reservation report for child: {}",
        source
    ))]
    ResvReport { source: CoreError },
    #[snafu(display("Failed to get NVMe host ID: {}", source))]
    NvmeHostId { source: CoreError },
    #[snafu(display("Failed to create a BlockDevice for child {}", child))]
    ChildBdevCreate { child: String, source: BdevError },
}

/// TODO
#[derive(Debug, Serialize, PartialEq, Deserialize, Eq, Copy, Clone)]
pub enum Reason {
    /// No particular reason for the child to be in this state.
    /// This is typically the init state.
    Unknown,
    /// Out of sync: child device is ok, but needs to be rebuilt.
    OutOfSync,
    /// Thin-provisioned child failed a write operate because
    /// the underlying logical volume failed to allocate space.
    /// This a recoverable state in case when addtional space
    /// can be freed from the logical volume store.
    NoSpace,
    /// The underlying device timed out.
    /// This a recoverable state in case the device can be expected
    /// to come back online.
    TimedOut,
    /// Cannot open device.
    CantOpen,
    /// The child failed to rebuild successfully.
    RebuildFailed,
    /// The child has been faulted due to I/O error(s).
    IoError,
    /// The child has been explicitly faulted due to an RPC call.
    ByClient,
    /// Admin command failure.
    AdminCommandFailed,
}

impl Display for Reason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::OutOfSync => write!(f, "out of sync"),
            Self::NoSpace => write!(f, "no space"),
            Self::TimedOut => write!(f, "timed out"),
            Self::CantOpen => write!(f, "cannot open"),
            Self::RebuildFailed => write!(f, "rebuild failed"),
            Self::IoError => write!(f, "io error"),
            Self::ByClient => write!(f, "by client"),
            Self::AdminCommandFailed => write!(f, "admin command failed"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChildState {
    /// Child has not been opened, but we are in the process of opening it.
    Init,
    /// Cannot add this block device to the parent as
    /// it iss incompatible property-wise.
    ConfigInvalid,
    /// The child is open for R/W.
    Open,
    /// The child device is being destroyed.
    Destroying,
    /// The child has been closed by the nexus.
    Closed,
    /// The child is faulted.
    Faulted(Reason),
}

impl Display for ChildState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Faulted(r) => write!(f, "faulted ({})", r),
            Self::Init => write!(f, "init"),
            Self::ConfigInvalid => write!(f, "config invalid"),
            Self::Open => write!(f, "open"),
            Self::Destroying => write!(f, "destroying"),
            Self::Closed => write!(f, "closed"),
        }
    }
}

#[derive(Serialize)]
pub struct NexusChild<'c> {
    /// name of the parent this child belongs too
    parent: String,
    /// current state of the child
    #[serde(skip_serializing)]
    pub state: AtomicCell<ChildState>,
    /// previous state of the child
    #[serde(skip_serializing)]
    prev_state: AtomicCell<ChildState>,
    /// TODO
    #[serde(skip_serializing)]
    remove_channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
    /// Name of the child is the URI used to create it.
    /// Name of the underlying block device can differ from it.
    ///
    /// TODO: we don't rename this field due to possible issues with
    /// TODO: child serialized state.
    name: String,
    /// Underlying block device.
    #[serde(skip_serializing)]
    device: Option<Box<dyn BlockDevice>>,
    /// TODO
    #[serde(skip_serializing)]
    device_descriptor: Option<Box<dyn BlockDeviceDescriptor>>,
    /// TODO
    #[serde(skip_serializing)]
    rebuild_job: Option<RebuildJob<'c>>,
    /// TODO
    #[serde(skip_serializing)]
    _c: PhantomData<&'c ()>,
}

impl Debug for NexusChild<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Child '{} @ {}' [{}]",
            self.name,
            self.parent,
            self.state(),
        )
    }
}

impl<'c> NexusChild<'c> {
    /// TODO
    pub(crate) fn set_state(&self, state: ChildState) {
        debug!("{:?}: changing state to '{}'", self, state);
        let prev_state = self.state.swap(state);
        self.prev_state.store(prev_state);
    }

    /// Open the child in RW mode and claim the device to be ours. If the child
    /// is already opened by someone else (i.e one of the targets) it will
    /// error out.
    ///
    /// only devices in the closed or Init state can be opened.
    ///
    /// A child can only be opened if:
    ///  - it's not faulted
    ///  - it's not already opened
    ///  - it's not being destroyed
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
        opened_state: ChildState,
    ) -> Result<String, ChildError> {
        info!("{:?}: opening child device...", self);

        // verify the state of the child before we open it
        match self.state() {
            ChildState::Faulted(_) => {
                error!("{:?}: cannot open: state is {}", self, self.state());
                return Err(ChildError::ChildFaulted {});
            }
            ChildState::Open => {
                // The child (should) already be open.
                assert!(self.device.is_some());
                assert!(self.device_descriptor.is_some());
                warn!("{:?}: already opened", self);
                return Ok(self.name.clone());
            }
            ChildState::Destroying => {
                error!(
                    "{:?}: cannot open: block device is being destroyed",
                    self
                );
                return Err(ChildError::ChildBeingDestroyed {});
            }
            _ => {}
        }

        let dev = self.device.as_ref().unwrap();

        let child_size = dev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{:?}: child is too small, parent size: {} child size: {}",
                self, parent_size, child_size
            );

            self.set_state(ChildState::ConfigInvalid);
            return Err(ChildError::ChildTooSmall {
                parent_size,
                child_size,
            });
        }

        let desc = dev.open(true).map_err(|source| {
            self.set_state(ChildState::Faulted(Reason::CantOpen));
            ChildError::OpenChild {
                source,
            }
        })?;
        self.device_descriptor = Some(desc);

        self.set_state(opened_state);

        info!("{:?}: opened successfully", self);
        Ok(self.name.clone())
    }

    /// Check if we're healthy.
    pub(crate) fn is_healthy(&self) -> bool {
        self.state() == ChildState::Open
    }

    /// Register an NVMe reservation, specifying a new key
    async fn resv_register(
        &self,
        hdl: &dyn BlockDeviceHandle,
        new_key: u64,
    ) -> Result<(), CoreError> {
        hdl.nvme_resv_register(
            0,
            new_key,
            nvme_reservation_register_action::REGISTER_KEY,
            nvme_reservation_register_cptpl::NO_CHANGES,
        )
        .await?;
        info!("{:?}: registered key {:0x}h", self, new_key);
        Ok(())
    }

    /// Acquire an NVMe reservation
    async fn resv_acquire(
        &self,
        hdl: &dyn BlockDeviceHandle,
        current_key: u64,
        preempt_key: u64,
        acquire_action: u8,
        resv_type: u8,
    ) -> Result<(), ChildError> {
        if let Err(e) = hdl
            .nvme_resv_acquire(
                current_key,
                preempt_key,
                acquire_action,
                resv_type,
            )
            .await
        {
            return Err(ChildError::ResvAcquire {
                source: e,
            });
        }

        info!(
            "{:?}: acquired reservation type {:x}h, action {:x}h, \
            current key {:0x}h, preempt key {:0x}h",
            self, resv_type, acquire_action, current_key, preempt_key,
        );
        Ok(())
    }

    /// Get NVMe reservation report
    /// Returns: (key, host id) of write exclusive reservation holder
    async fn resv_report(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<Option<(u64, [u8; 16])>, ChildError> {
        let mut buffer = hdl.dma_malloc(4096).context(HandleDmaMalloc {})?;
        if let Err(e) = hdl.nvme_resv_report(1, &mut buffer).await {
            return Err(ChildError::ResvReport {
                source: e,
            });
        }

        trace!("{:?}: received reservation report", self);

        let (stext, sl) = buffer.as_slice().split_at(std::mem::size_of::<
            spdk_nvme_reservation_status_extended_data,
        >());
        let (pre, resv_status_ext, post) = unsafe {
            stext.align_to::<spdk_nvme_reservation_status_extended_data>()
        };

        assert!(pre.is_empty());
        assert!(post.is_empty());

        let regctl = resv_status_ext[0].data.regctl;

        trace!(
            "reservation status: rtype {}, regctl {}, ptpls {}",
            resv_status_ext[0].data.rtype,
            regctl,
            resv_status_ext[0].data.ptpls,
        );

        let (pre, reg_ctrlr_ext, _post) = unsafe {
            sl.align_to::<spdk_nvme_registered_ctrlr_extended_data>()
        };

        if !pre.is_empty() {
            return Ok(None);
        }

        let mut numctrlr: usize = regctl.into();
        if numctrlr > reg_ctrlr_ext.len() {
            numctrlr = reg_ctrlr_ext.len();
            warn!(
                "Expecting data for {} controllers, received {}",
                regctl, numctrlr
            );
        }

        for (i, c) in reg_ctrlr_ext.iter().enumerate().take(numctrlr) {
            let cntlid = c.cntlid;
            let rkey = c.rkey;
            trace!(
                "ctrlr {}: cntlid {:0x}h, status {}, hostid {:0x?}, \
                rkey {:0x}h",
                i,
                cntlid,
                c.rcsts.status(),
                c.hostid,
                rkey,
            );
            if resv_status_ext[0].data.rtype == 1 && c.rcsts.status() == 1 {
                return Ok(Some((rkey, c.hostid)));
            }
        }
        Ok(None)
    }

    /// Register an NVMe reservation on the child then acquire a write
    /// exclusive reservation, preempting an existing reservation, if another
    /// host has it.
    /// Ignores bdevs without NVMe reservation support.
    pub(crate) async fn acquire_write_exclusive(
        &self,
        key: u64,
        preempt_key: Option<std::num::NonZeroU64>,
    ) -> Result<(), ChildError> {
        if std::env::var("NEXUS_NVMF_RESV_ENABLE").is_err() {
            return Ok(());
        }

        let hdl = self.get_io_handle().context(HandleOpen {})?;
        if let Err(e) = self.resv_register(&*hdl, key).await {
            match e {
                CoreError::NotSupported {
                    ..
                } => return Ok(()),
                _ => {
                    return Err(ChildError::ResvRegisterKey {
                        source: e,
                    })
                }
            }
        }

        if let Err(e) = self
            .resv_acquire(
                &*hdl,
                key,
                match preempt_key {
                    None => 0,
                    Some(k) => k.get(),
                },
                match preempt_key {
                    None => nvme_reservation_acquire_action::ACQUIRE,
                    Some(_) => nvme_reservation_acquire_action::PREEMPT,
                },
                nvme_reservation_type::WRITE_EXCLUSIVE_ALL_REGS,
            )
            .await
        {
            warn!(
                "{:?}: failed to acquire write exclusive: {}",
                self,
                e.verbose()
            );
        }

        if let Some((pkey, hostid)) = self.resv_report(&*hdl).await? {
            let my_hostid = match hdl.host_id().await {
                Ok(h) => h,
                Err(e) => {
                    return Err(ChildError::NvmeHostId {
                        source: e,
                    });
                }
            };
            if my_hostid != hostid {
                info!(
                    "{:?}: write exclusive reservation held by {:0x?}",
                    self, hostid
                );
                self.resv_acquire(
                    &*hdl,
                    key,
                    pkey,
                    nvme_reservation_acquire_action::PREEMPT,
                    nvme_reservation_type::WRITE_EXCLUSIVE_ALL_REGS,
                )
                .await?;
                if let Some((_, hostid)) = self.resv_report(&*hdl).await? {
                    if my_hostid != hostid {
                        info!(
                            "{:?}: write exclusive reservation held by {:0x?}",
                            self, hostid
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Fault the child with a specific reason.
    /// We do not close the child if it is out-of-sync because it will
    /// subsequently be rebuilt.
    pub(crate) async fn fault(&mut self, reason: Reason) {
        match reason {
            Reason::OutOfSync => {
                self.set_state(ChildState::Faulted(reason));
            }
            _ => {
                if let Err(e) = self.close().await {
                    error!("{:?}: failed to close: {}", self, e.verbose());
                }
                self.set_state(ChildState::Faulted(reason));
            }
        }
    }

    /// Set the child as temporarily offline
    pub(crate) async fn offline(&mut self) {
        if let Err(e) = self.close().await {
            error!("{:?}: failed to close: {}", self, e.verbose());
        }
    }

    /// Get URI of this Nexus child.
    pub(crate) fn uri(&self) -> &str {
        &self.name
    }

    /// Get name of the nexus this child belongs to.
    pub fn nexus_name(&self) -> &str {
        &self.parent
    }

    /// Online a previously offlined child.
    /// The child is set out-of-sync so that it will be rebuilt.
    /// TODO: channels need to be updated when block devices are opened.
    pub(crate) async fn online(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        info!("{:?}: bringing child online", self);

        // Only online a child if it was previously set offline.
        if !self.can_online() {
            warn!(
                "{:?}: child is permanently faulted and cannot \
                    be brought online",
                self
            );
            return Err(ChildError::PermanentlyFaulted {});
        }

        // Re-create the block device as it will have been previously
        // destroyed.
        let name =
            device_create(&self.name).await.context(ChildBdevCreate {
                child: self.name.clone(),
            })?;

        self.device = device_lookup(&name);
        if self.device.is_none() {
            error!(
                "{:?}: failed to find device after successful creation",
                self,
            );
            return Err(ChildError::ChildInaccessible {});
        }

        self.set_state(ChildState::Closed);
        self.open(parent_size, ChildState::Faulted(Reason::OutOfSync))
    }

    /// Determines if the child can be onlined.
    /// Check for a "Closed" state as that is what offlining a child
    /// will set it to.
    fn can_online(&self) -> bool {
        matches!(
            self.state.load(),
            ChildState::Faulted(Reason::NoSpace) | ChildState::Closed
        )
    }

    /// Extract a UUID from a URI.
    pub(crate) fn uuid(uri: &str) -> Option<String> {
        let url = Url::parse(uri).expect("Failed to parse URI");
        for pair in url.query_pairs() {
            if pair.0 == "uuid" {
                return Some(pair.1.to_string());
            }
        }
        None
    }

    /// returns the state of the child
    pub fn state(&self) -> ChildState {
        self.state.load()
    }

    pub(crate) fn rebuilding(&self) -> bool {
        self.rebuild_job.is_some()
            && self.state() == ChildState::Faulted(Reason::OutOfSync)
    }

    /// Closes the nexus child.
    pub(crate) async fn close(&mut self) -> Result<(), BdevError> {
        info!("{:?}: closing child...", self);
        if self.device.is_none() {
            warn!("{:?}: no block device: appears to be already closed", self);
            return Ok(());
        }

        // TODO: Check device claiming scheme.
        if self.device_descriptor.is_some() {
            self.device_descriptor.as_ref().unwrap().unclaim();
        }

        // Destruction raises a device removal event.
        let destroyed = self.destroy_device().await;

        // Only wait for block device removal if the child has been initialised.
        // An uninitialized child won't have an underlying devices.
        // Also check previous state as remove event may not have occurred.
        if self.state.load() != ChildState::Init
            && self.prev_state.load() != ChildState::Init
        {
            self.remove_channel.1.next().await;
        }

        info!("{:?}: child closed successfully", self);
        destroyed
    }

    /// Called in response to a device removal event.
    /// All the necessary teardown should be performed here before the
    /// underlying device is removed.
    ///
    /// Note: The descriptor *must* be dropped for the unplug to complete.
    pub(crate) fn unplug(&mut self) {
        info!("{:?}: unplugging child...", self);

        let mut state = self.state();

        // Only drop the device and the device descriptor if the child is being
        // destroyed. For a hot remove event, keep the device and descriptor.
        let mut was_destroying = false;
        if state == ChildState::Destroying {
            debug!("{:?}: dropping block device", self);

            // Block device is being removed, so ensure we don't use it again.
            self.device = None;
            was_destroying = true;
            state = self.prev_state.load();
        }

        match state {
            ChildState::Open | ChildState::Faulted(Reason::OutOfSync) => {
                // Change the state of the child to ensure it is taken out of
                // the I/O path when the nexus is reconfigured.
                self.set_state(ChildState::Closed);
            }
            // leave the state into whatever we found it as
            _ => {
                if was_destroying {
                    // Restore the previous state
                    info!("{:?}: reverting to previous state: {}", self, state);
                    self.set_state(state);
                }
            }
        }

        // Remove the child from the I/O path. If we had an IO error the block
        // device, the channels were already reconfigured so we don't
        // have to do that twice.
        // TODO: Revisit nexus reconfiguration once Nexus has switched to
        // BlockDevice-based children and is able to listen to
        // device-related events directly.
        if state != ChildState::Faulted(Reason::IoError) {
            let nexus_name = self.parent.clone();
            Reactor::block_on(async move {
                match nexus_lookup_mut(&nexus_name) {
                    Some(n) => n.reconfigure(DrEvent::ChildUnplug).await,
                    None => error!("Nexus '{}' not found", nexus_name),
                }
            });
        }

        if was_destroying {
            // Dropping the last descriptor results in the device being removed.
            // This must be performed in this function.
            self.device_descriptor.take();
        }

        self.unplug_complete();
        info!("{:?}: child successfully unplugged", self);
    }

    /// Signal that the child unplug is complete.
    fn unplug_complete(&self) {
        let mut sender = self.remove_channel.0.clone();
        let name = self.name.clone();
        Reactors::current().send_future(async move {
            if let Err(e) = sender.send(()).await {
                error!(
                    "Failed to send unplug complete for child '{}': {}",
                    name, e
                );
            }
        });
    }

    /// create a new nexus child
    pub fn new(
        name: String,
        parent: String,
        device: Option<Box<dyn BlockDevice>>,
    ) -> Self {
        // TODO: Remove check for persistent store
        if PersistentStore::enabled() && Self::uuid(&name).is_none() {
            panic!("Child name does not contain a UUID.");
        }

        NexusChild {
            name,
            device,
            parent,
            device_descriptor: None,
            state: AtomicCell::new(ChildState::Init),
            prev_state: AtomicCell::new(ChildState::Init),
            remove_channel: mpsc::channel(0),
            rebuild_job: None,
            _c: Default::default(),
        }
    }

    /// Destroys the child's block device.
    pub(super) async fn destroy_device(&self) -> Result<(), BdevError> {
        if self.device.is_some() {
            self.set_state(ChildState::Destroying);
            info!("{:?}: destroying block device...", self);
            device_destroy(&self.name).await?;
            info!("{:?}: block device destroyed ok", self);
        } else {
            warn!("{:?}: no block device, ignoring device destroy call", self);
        }

        Ok(())
    }

    /// Returns reference to child's block device.
    pub fn get_device(&self) -> Result<&dyn BlockDevice, ChildError> {
        if let Some(ref device) = self.device {
            Ok(&**device)
        } else {
            Err(ChildError::ChildInaccessible {})
        }
    }

    /// TODO
    pub(super) fn set_rebuild_job(&mut self, job: RebuildJob<'c>) {
        assert!(self.rebuild_job.is_none());
        self.rebuild_job = Some(job);
    }

    /// TODO
    pub(super) fn remove_rebuild_job(&mut self) -> Option<RebuildJob<'c>> {
        self.rebuild_job.take()
    }

    /// Return the rebuild job which is rebuilding this child, if rebuilding.
    pub fn rebuild_job(&self) -> Option<&RebuildJob<'c>> {
        self.rebuild_job.as_ref()
    }

    /// Return the rebuild job which is rebuilding this child, if rebuilding.
    pub fn rebuild_job_mut(&mut self) -> Option<&mut RebuildJob<'c>> {
        self.rebuild_job.as_mut()
    }

    /// Return the rebuild progress on this child, if rebuilding.
    pub fn get_rebuild_progress(&self) -> i32 {
        self.rebuild_job
            .as_ref()
            .map(|j| j.stats().progress as i32)
            .unwrap_or_else(|| -1)
    }

    /// Determine if a child is local to the nexus (i.e. on the same node).
    pub fn is_local(&self) -> Option<bool> {
        match &self.device {
            Some(dev) => {
                // A local child is not exported over nvme.
                let local = dev.driver_name() != "nvme";
                Some(local)
            }
            None => None,
        }
    }

    /// Get I/O handle for the block device associated with this Nexus child.
    pub fn get_io_handle(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        if let Some(desc) = self.device_descriptor.as_ref() {
            desc.get_io_handle()
        } else {
            error!("{:?}: child does not have valid descriptor", self);
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// TODO
    pub fn get_device_name(&self) -> Option<String> {
        self.device.as_ref().map(|d| d.device_name())
    }

    /// TODO
    pub fn match_device_name(&self, bdev_name: &str) -> bool {
        match self.get_device_name() {
            Some(n) => n == bdev_name,
            None => false,
        }
    }

    /// TODO
    pub(crate) fn set_event_listener(&mut self, listener: DeviceEventSink) {
        let dev = self
            .get_device()
            .expect("No block device associated with a Nexus child");

        match dev.add_event_listener(listener) {
            Err(err) => {
                error!(
                    ?err,
                    "{:?}: failed to add event for device '{}'",
                    self,
                    dev.device_name()
                )
            }
            _ => {
                debug!(
                    "{:?}: added event listener for device '{}'",
                    self,
                    dev.device_name()
                );
            }
        }
    }
}
