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

use super::{nexus_iter_mut, nexus_lookup_mut, DrEvent, VerboseError};

use crate::{
    bdev::{device_create, device_destroy, device_lookup},
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        CoreError,
        DeviceEventSink,
        Reactor,
        Reactors,
    },
    nexus_uri::NexusBdevError,
    persistent_store::PersistentStore,
    rebuild::{ClientOperations, RebuildJob},
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
pub enum ChildError {
    #[snafu(display("Child is not offline"))]
    ChildNotOffline {},
    #[snafu(display("Child is not closed"))]
    ChildNotClosed {},
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
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
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
    ChildBdevCreate {
        child: String,
        source: NexusBdevError,
    },
}

/// TODO
#[derive(Debug, Serialize, PartialEq, Deserialize, Eq, Copy, Clone)]
pub enum Reason {
    /// no particular reason for the child to be in this state
    /// this is typically the init state
    Unknown,
    /// out of sync - needs to be rebuilt
    OutOfSync,
    /// cannot open
    CantOpen,
    /// the child failed to rebuild successfully
    RebuildFailed,
    /// the child has been faulted due to I/O error(s)
    IoError,
    /// the child has been explicitly faulted due to a rpc call
    Rpc,
}

impl Display for Reason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::OutOfSync => {
                write!(f, "The child is out of sync and requires a rebuild")
            }
            Self::CantOpen => {
                write!(f, "The child block device could not be opened")
            }
            Self::RebuildFailed => {
                write!(f, "The child failed to rebuild successfully")
            }
            Self::IoError => write!(f, "The child had too many I/O errors"),
            Self::Rpc => write!(f, "The child is faulted due to a rpc call"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum ChildState {
    /// child has not been opened, but we are in the process of opening it
    Init,
    /// cannot add this block device to the parent as its incompatible property
    /// wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// the child is being destroyed
    Destroying,
    /// the child has been closed by the nexus
    Closed,
    /// the child is faulted
    Faulted(Reason),
}

impl Display for ChildState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Faulted(r) => write!(f, "Faulted with reason {}", r),
            Self::Init => write!(f, "Init"),
            Self::ConfigInvalid => write!(f, "Config parameters are invalid"),
            Self::Open => write!(f, "Child is open"),
            Self::Destroying => write!(f, "Child is being destroyed"),
            Self::Closed => write!(f, "Closed"),
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
    /// Note that block device name can differ from it!
    pub name: String,
    /// Underlying block device.
    #[serde(skip_serializing)]
    device: Option<Box<dyn BlockDevice>>,
    /// TODO
    #[serde(skip_serializing)]
    device_descriptor: Option<Box<dyn BlockDeviceDescriptor>>,
    /// TODO
    _c: PhantomData<&'c ()>,
}

impl Debug for NexusChild<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parent = {}, name = {}", self.parent, self.name)
    }
}

impl Display for NexusChild<'_> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match &self.device {
            Some(_dev) => writeln!(f, "{}: {:?}", self.name, self.state(),),
            None => writeln!(f, "{}: state {:?}", self.name, self.state()),
        }
    }
}

impl<'c> NexusChild<'c> {
    pub(crate) fn set_state(&self, state: ChildState) {
        let prev_state = self.state.swap(state);
        self.prev_state.store(prev_state);
        trace!(
            "{}: child {}: state change from {} to {}",
            self.parent,
            self.name,
            prev_state.to_string(),
            state.to_string(),
        );
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
    ) -> Result<String, ChildError> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        // verify the state of the child before we open it
        match self.state() {
            ChildState::Faulted(reason) => {
                error!(
                    "{}: can not open child {} reason {}",
                    self.parent, self.name, reason
                );
                return Err(ChildError::ChildFaulted {});
            }
            ChildState::Open => {
                // the child (should) already be open
                assert!(self.device.is_some());
                assert!(self.device_descriptor.is_some());
                info!("called open on an already opened child");
                return Ok(self.name.clone());
            }
            ChildState::Destroying => {
                error!(
                    "{}: cannot open child {} being destroyed",
                    self.parent, self.name
                );
                return Err(ChildError::ChildBeingDestroyed {});
            }
            _ => {}
        }

        let dev = self.device.as_ref().unwrap();

        let child_size = dev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{}: child {} too small, parent size: {} child size: {}",
                self.parent, self.name, parent_size, child_size
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

        self.set_state(ChildState::Open);

        debug!("{}: child {} opened successfully", self.parent, self.name);
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
        info!(
            "{}: registered key {:0x}h on child {}",
            self.parent, new_key, self.name
        );
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
            "{}: acquired reservation type {:x}h, action {:x}h, current key {:0x}h, preempt key {:0x}h on child {}",
            self.parent, resv_type, acquire_action, current_key, preempt_key, self.name
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
        trace!(
            "{}: received reservation report for child {}",
            self.parent,
            self.name
        );
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
                "ctrlr {}: cntlid {:0x}h, status {}, hostid {:0x?}, rkey {:0x}h",
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
            warn!("{}", e);
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
                info!("Write exclusive reservation held by {:0x?}", hostid);
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
                            "Write exclusive reservation held by {:0x?}",
                            hostid
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
                    error!(
                        "{}: child {} failed to close with error {}",
                        self.parent,
                        self.name,
                        e.verbose()
                    );
                }
                self.set_state(ChildState::Faulted(reason));
            }
        }
    }

    /// Set the child as temporarily offline
    pub(crate) async fn offline(&mut self) {
        if let Err(e) = self.close().await {
            error!(
                "{}: child {} failed to close with error {}",
                self.parent,
                self.name,
                e.verbose()
            );
        }
    }

    /// Get full name of this Nexus child.
    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    /// Get name of the nexus this child belongs to.
    pub fn get_nexus_name(&self) -> &str {
        &self.parent
    }

    /// Online a previously offlined child.
    /// The child is set out-of-sync so that it will be rebuilt.
    /// TODO: channels need to be updated when block devices are opened.
    pub(crate) async fn online(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        // Only online a child if it was previously set offline. Check for a
        // "Closed" state as that is what offlining a child will set it to.
        match self.state.load() {
            ChildState::Closed => {
                // Re-create the block device as it will have been previously
                // destroyed.
                let name = device_create(&self.name).await.context(
                    ChildBdevCreate {
                        child: self.name.clone(),
                    },
                )?;

                self.device = device_lookup(&name);
                if self.device.is_none() {
                    warn!(
                        "{}: failed to lookup device after successful creation",
                        self.name,
                    );
                }
            }
            _ => return Err(ChildError::ChildNotClosed {}),
        }

        let result = self.open(parent_size);
        self.set_state(ChildState::Faulted(Reason::OutOfSync));
        result
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
        match RebuildJob::lookup(&self.name) {
            Ok(_) => self.state() == ChildState::Faulted(Reason::OutOfSync),
            Err(_) => false,
        }
    }

    /// Close the nexus child.
    pub(crate) async fn close(&mut self) -> Result<(), NexusBdevError> {
        info!("{}: closing nexus child", self.name);
        if self.device.is_none() {
            info!("{}: nexus child already closed", self.name);
            return Ok(());
        }

        // TODO: Check device claiming scheme.
        if self.device_descriptor.is_some() {
            self.device_descriptor.as_ref().unwrap().unclaim();
        }

        // Destruction raises a device removal event.
        let destroyed = self.destroy().await;

        // Only wait for block device removal if the child has been initialised.
        // An uninitialized child won't have an underlying devices.
        // Also check previous state as remove event may not have occurred.
        if self.state.load() != ChildState::Init
            && self.prev_state.load() != ChildState::Init
        {
            self.remove_channel.1.next().await;
        }

        info!("{}: nexus child closed", self.name);
        destroyed
    }

    /// Called in response to a device removal event.
    /// All the necessary teardown should be performed here before the
    /// underlying device is removed.
    ///
    /// Note: The descriptor *must* be dropped for the remove to complete.
    pub(crate) fn remove(&mut self) {
        info!("{}: removing child", self.name);

        let mut state = self.state();

        let mut destroying = false;
        // Only remove the device if the child is being destroyed instead of
        // a hot remove event.
        if state == ChildState::Destroying {
            // Block device is being removed, so ensure we don't use it again.
            self.device = None;
            destroying = true;

            state = self.prev_state.load();
        }
        match state {
            ChildState::Open | ChildState::Faulted(Reason::OutOfSync) => {
                // Change the state of the child to ensure it is taken out of
                // the I/O path when the nexus is reconfigured.
                self.set_state(ChildState::Closed)
            }
            // leave the state into whatever we found it as
            _ => {
                if destroying {
                    // Restore the previous state
                    info!(
                        "Restoring previous child state {}",
                        state.to_string()
                    );
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
                    Some(n) => n.reconfigure(DrEvent::ChildRemove).await,
                    None => error!("Nexus {} not found", nexus_name),
                }
            });
        }

        if destroying {
            // Dropping the last descriptor results in the device being removed.
            // This must be performed in this function.
            self.device_descriptor.take();
        }

        self.remove_complete();
        info!("Child {} removed", self.name);
    }

    /// Signal that the child removal is complete.
    fn remove_complete(&self) {
        let mut sender = self.remove_channel.0.clone();
        let name = self.name.clone();
        Reactors::current().send_future(async move {
            if let Err(e) = sender.send(()).await {
                error!(
                    "Failed to send remove complete for child {}, error {}",
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
            _c: Default::default(),
        }
    }

    /// destroy the child device
    pub async fn destroy(&self) -> Result<(), NexusBdevError> {
        if self.device.is_some() {
            self.set_state(ChildState::Destroying);
            info!("{}: destroying underlying block device", self.name);
            device_destroy(&self.name).await?;
            info!("{}: underlying block device destroyed", self.name);
        } else {
            warn!("{}: no underlying block device", self.name);
        }

        Ok(())
    }

    /// Return reference to child's block device.
    pub fn get_device(&self) -> Result<&dyn BlockDevice, ChildError> {
        if let Some(ref device) = self.device {
            Ok(&**device)
        } else {
            Err(ChildError::ChildInaccessible {})
        }
    }

    /// Return the rebuild job which is rebuilding this child, if rebuilding.
    fn get_rebuild_job(&self) -> Option<&mut RebuildJob> {
        let job = RebuildJob::lookup(&self.name).ok()?;
        assert_eq!(job.nexus, self.parent);
        Some(job)
    }

    /// Return the rebuild progress on this child, if rebuilding.
    pub fn get_rebuild_progress(&self) -> i32 {
        self.get_rebuild_job()
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
            error!("{}: nexus child does not have valid descriptor", self.name);
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// TODO
    pub fn match_device_name(&self, bdev_name: &str) -> bool {
        match &self.device {
            Some(d) => d.device_name() == bdev_name,
            None => false,
        }
    }

    /// TODO
    pub(crate) fn set_event_listener(&mut self, listener: DeviceEventSink) {
        let dev = self
            .get_device()
            .expect("No block device associated with a Nexus child");

        let name = listener.get_listener_name();
        match dev.add_event_listener(listener) {
            Err(err) => {
                error!(
                    ?err,
                    "{}: failed to register event listener for child {}",
                    name,
                    self.get_name(),
                )
            }
            _ => {
                info!(
                    "{}: listening to child events: {}",
                    name,
                    self.get_name()
                );
            }
        }
    }
}

/// Looks up a child based on the underlying block device name.
pub fn lookup_nexus_child(bdev_name: &str) -> Option<&mut NexusChild> {
    for nexus in nexus_iter_mut() {
        if let Some(c) = nexus.lookup_child_mut(bdev_name) {
            return Some(c);
        }
    }
    None
}
