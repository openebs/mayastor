use std::fmt::{Debug, Display, Formatter};

use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};

use crate::{
    bdev::{
        device_create,
        device_destroy,
        device_lookup,
        nexus::{
            instances,
            nexus_channel::DrEvent,
            nexus_child::ChildState::Faulted,
            nexus_child_status_config::ChildStatusConfig,
        },
        nexus_lookup,
        VerboseError,
    },
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        CoreError,
        Reactor,
        Reactors,
    },
    nexus_uri::NexusBdevError,
    rebuild::{ClientOperations, RebuildJob},
};
use crossbeam::atomic::AtomicCell;
use futures::{channel::mpsc, SinkExt, StreamExt};

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
    #[snafu(display("Failed to create a BlockDevice for child {}", child))]
    ChildBdevCreate {
        child: String,
        source: NexusBdevError,
    },
}

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
pub struct NexusChild {
    /// name of the parent this child belongs too
    parent: String,

    /// current state of the child
    #[serde(skip_serializing)]
    pub state: AtomicCell<ChildState>,
    /// previous state of the child
    #[serde(skip_serializing)]
    pub prev_state: AtomicCell<ChildState>,
    #[serde(skip_serializing)]
    remove_channel: (mpsc::Sender<()>, mpsc::Receiver<()>),

    /// Name of the child is the URI used to create it.
    /// Note that block device name can differ from it!
    name: String,
    #[serde(skip_serializing)]
    /// Underlaying block device.
    device: Option<Box<dyn BlockDevice>>,
    #[serde(skip_serializing)]
    device_descriptor: Option<Box<dyn BlockDeviceDescriptor>>,
}

impl Debug for NexusChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parent = {}, name = {}", self.parent, self.name)
    }
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match &self.device {
            Some(dev) => writeln!(
                f,
                "{}: {:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state(),
                dev.num_blocks(),
                dev.block_len(),
            ),
            None => writeln!(f, "{}: state {:?}", self.name, self.state()),
        }
    }
}

impl NexusChild {
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
                assert_eq!(self.device.is_some(), true);
                assert_eq!(self.device_descriptor.is_some(), true);
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
            self.set_state(Faulted(Reason::CantOpen));
            ChildError::OpenChild {
                source,
            }
        })?;
        self.device_descriptor = Some(desc);

        self.set_state(ChildState::Open);

        debug!("{}: child {} opened successfully", self.parent, self.name);
        Ok(self.name.clone())
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
        NexusChild::save_state_change();
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
        NexusChild::save_state_change();
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
        NexusChild::save_state_change();
        result
    }

    /// Save the state of the children to the config file
    pub(crate) fn save_state_change() {
        if ChildStatusConfig::save().is_err() {
            error!("Failed to save child status information");
        }
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
    /// underlaying device is removed.
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
            ChildState::Open | Faulted(Reason::OutOfSync) => {
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
                match nexus_lookup(&nexus_name) {
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
        NexusChild {
            name,
            device,
            parent,
            device_descriptor: None,
            state: AtomicCell::new(ChildState::Init),
            prev_state: AtomicCell::new(ChildState::Init),
            remove_channel: mpsc::channel(0),
        }
    }

    /// destroy the child device
    pub(crate) async fn destroy(&self) -> Result<(), NexusBdevError> {
        if self.device.is_some() {
            self.set_state(ChildState::Destroying);
            info!("{} destroying underlying block device", self.name);
            let rc = device_destroy(&self.name).await;
            info!(
                "{} underlying block device destroyed, rc = {:?}",
                self.name, rc
            );
            rc
        } else {
            warn!(
                "{}: destroying nexus child without associated block device",
                self.name
            );
            Ok(())
        }
    }

    /// Check if the child is in a state that can service I/O.
    /// When out-of-sync, the child is still accessible (can accept I/O)
    /// because:
    /// 1. An added child starts in the out-of-sync state and may require its
    ///    label and metadata to be updated
    /// 2. It needs to be rebuilt
    fn check_accessible(&self) -> Result<(), ChildError> {
        if self.state() == ChildState::Open
            || self.state() == ChildState::Faulted(Reason::OutOfSync)
        {
            Ok(())
        } else {
            error!(
                "{}: nexus child is inaccessible (state={})",
                self.name,
                self.state()
            );
            Err(ChildError::ChildInaccessible {})
        }
    }

    /// Return reference to child's block device.
    pub(crate) fn get_device(
        &self,
    ) -> Result<&Box<dyn BlockDevice>, ChildError> {
        self.device.as_ref().ok_or(ChildError::ChildInaccessible {})
    }

    pub(crate) fn get_dev(
        &self,
    ) -> Result<(&Box<dyn BlockDevice>, Box<dyn BlockDeviceHandle>), ChildError>
    {
        self.check_accessible()?;
        Ok((self.get_device().unwrap(), self.get_io_handle().unwrap()))
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
                // A local child is not exported over nvme or iscsi.
                let local =
                    dev.driver_name() != "nvme" && dev.driver_name() != "iscsi";
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
}

/// Looks up a child based on the underlying block device name.
pub fn lookup_nexus_child(bdev_name: &str) -> Option<&mut NexusChild> {
    for nexus in instances() {
        for child in &mut nexus.children {
            if child.device.is_some()
                && child.device.as_ref().unwrap().device_name() == bdev_name
            {
                return Some(child);
            }
        }
    }
    None
}
