//!
//! This file implements operations to the child bdevs from the context of its
//! parent.
//!
//! `register_children` and `register_child` are should only be used when
//! building up a new nexus
//!
//! `offline_child` and `online_child` should be used to include the child into
//! the IO path of the nexus currently, online of a child will default the nexus
//! into the degraded mode as it (may) require a rebuild. This will be changed
//! in the near future -- online child will not determine if it SHOULD online
//! but simply does what its told. Therefore, the callee must be careful when
//! using this method.
//!
//! 'fault_child` will do the same as `offline_child` except, it will not close
//! the child.
//!
//! `add_child` will construct a new `NexusChild` and add the bdev given by the
//! uri to the nexus. The nexus will transition to degraded mode as the new
//! child requires rebuild first. If the rebuild flag is set then the rebuild
//! is also started otherwise it has to be started through `start_rebuild`.
//!
//! When reconfiguring the nexus, we traverse all our children, create new IO
//! channels for all children that are in the open state.

use std::{cmp::min, pin::Pin};

use futures::channel::oneshot;
use snafu::ResultExt;

use super::{
    nexus_err,
    nexus_lookup,
    nexus_lookup_mut,
    ChildState,
    ChildSyncState,
    Error,
    FaultReason,
    IOLogChannel,
    IoMode,
    Nexus,
    NexusChild,
    NexusOperation,
    NexusPauseState,
    NexusState,
    NexusStatus,
    PersistOp,
};

use crate::{
    bdev::{dev::device_name, device_create, device_destroy, device_lookup},
    bdev_api::BdevError,
    core::{
        device_cmd_queue,
        DeviceCommand,
        DeviceEventListener,
        DeviceEventType,
        Reactors,
        VerboseError,
    },
    eventing::{EventMetaGen, EventWithMeta},
    subsys::NvmfSubsystem,
};

use events_api::event::EventAction;

use spdk_rs::{
    ffihelper::cb_arg,
    ChannelTraverseStatus,
    IoDeviceChannelTraverse,
};

impl<'n> Nexus<'n> {
    /// Create and register a single child to nexus, only allowed during the
    /// nexus init phase
    pub async fn new_child(
        mut self: Pin<&mut Self>,
        uri: &str,
    ) -> Result<(), BdevError> {
        assert_eq!(*self.state.lock(), NexusState::Init);

        info!("{:?}: adding child: '{}'...", self, uri);

        let nexus_name = self.nexus_name().to_owned();
        let device_name = device_create(uri).await?;

        let c = NexusChild::new(
            uri.to_string(),
            nexus_name,
            device_lookup(&device_name),
        );

        info!("{:?}: added to nexus", c);

        unsafe {
            self.as_mut().child_add_unsafe(c);
        }

        Ok(())
    }

    /// add a new child to an existing nexus. note that the child is added and
    /// opened but not taking part of any new IO's that are submitted to the
    /// nexus.
    ///
    /// The child may require a rebuild first, so the nexus will
    /// transition to degraded mode when the addition has been successful.
    /// The rebuild flag dictates wether we attempt to start the rebuild or not
    /// If the rebuild fails to start the child remains degraded until such
    /// time the rebuild is retried and complete
    pub async fn add_child(
        mut self: Pin<&mut Self>,
        uri: &str,
        norebuild: bool,
    ) -> Result<NexusStatus, Error> {
        self.check_nexus_operation(NexusOperation::ReplicaAdd)?;

        let status = self.as_mut().add_child_only(uri).await?;

        if !norebuild {
            match self.start_rebuild(uri).await {
                Err(e) => {
                    // todo: CAS-253 retry starting the rebuild again when ready
                    error!(
                        "Child added but rebuild failed to start: {}",
                        e.verbose()
                    );
                    match self.child(uri) {
                        Ok(child) => {
                            child
                                .close_faulted(FaultReason::RebuildFailed)
                                .await
                        }
                        Err(e) => error!(
                            "Failed to find newly added child {}, error: {}",
                            uri,
                            e.verbose()
                        ),
                    };
                }
                Ok(_) => {
                    if let Ok(child) = self.child(uri) {
                        self.event(EventAction::OnlineChild, child.meta())
                            .generate();
                    }
                }
            }
        }
        Ok(status)
    }

    /// The child may require a rebuild first, so the nexus will
    /// transition to degraded mode when the addition has been successful.
    async fn add_child_only(
        mut self: Pin<&mut Self>,
        uri: &str,
    ) -> Result<NexusStatus, Error> {
        self.check_nexus_operation(NexusOperation::ReplicaAdd)?;

        let name =
            device_create(uri).await.context(nexus_err::CreateChild {
                name: self.name.clone(),
            })?;

        assert!(self.num_blocks() > 0);
        assert!(self.block_len() > 0);

        let child_bdev = match device_lookup(&name) {
            Some(child) => {
                if child.block_len() != self.block_len()
                    || self
                        .min_num_blocks()
                        .map_or(true, |n| n > child.num_blocks())
                {
                    if let Err(err) = device_destroy(uri).await {
                        error!(
                            "Failed to destroy child bdev with wrong geometry: {}",
                            err.to_string()
                        );
                    }

                    return Err(Error::ChildGeometry {
                        child: name,
                        name: self.name.clone(),
                    });
                } else {
                    child
                }
            }
            None => {
                return Err(Error::ChildMissing {
                    child: name,
                    name: self.name.clone(),
                })
            }
        };

        if self.lookup_child_by_device(&name).is_some() {
            return Err(Error::ChildAlreadyExists {
                child: name,
                name: self.name.to_owned(),
            });
        }

        let mut child = NexusChild::new(
            uri.to_owned(),
            self.nexus_name().to_owned(),
            Some(child_bdev),
        );

        // it can never take part in the IO path
        // of the nexus until it's rebuilt from a healthy child.
        let mut res = child.open(self.req_size(), ChildSyncState::OutOfSync);

        if res.is_ok() {
            // we have created the bdev, and created a nexusChild struct. To
            // make use of the device itself the
            // data and metadata must be validated. The child
            // will be added and marked as faulted, once the rebuild has
            // completed the device can transition to online
            if let Err(e) = child.reservation_acquire(&self.nvme_params).await {
                res = Err(e);
            }
        }

        match res {
            Ok(child_uri) => {
                let healthy = child.is_healthy();

                // Register event listener for newly added child.
                child.set_event_listener(self.get_event_sink());

                unsafe {
                    self.as_mut().child_add_unsafe(child);
                }

                match self
                    .persist(PersistOp::AddChild {
                        child_uri,
                        healthy,
                    })
                    .await
                {
                    Ok(_) => Ok(self.status()),
                    Err(e) => {
                        error!(
                            "{self:?}: failed to add child '{uri}' \
                            because of persistent store update failure: {e}"
                        );
                        unsafe {
                            self.as_mut().child_remove_unsafe(uri);
                        }
                        Err(e)
                    }
                }
            }
            Err(e) => {
                if let Err(err) = device_destroy(uri).await {
                    error!(
                        "{:?}: failed to destroy child '{}' which \
                        failed to open: {}",
                        self,
                        uri,
                        err.to_string()
                    );
                }
                Err(e).context(nexus_err::OpenChild {
                    child: uri.to_owned(),
                    name: self.name.clone(),
                })
            }
        }
    }

    /// Checks if the nexus contains the given child uri.
    pub fn contains_child_uri(&self, uri: &str) -> bool {
        self.children_iter().any(|c| c.uri() == uri)
    }
    /// Checks if the nexus contains the given child name.
    pub fn contains_child_name(&self, name: &str) -> bool {
        self.children_iter()
            .any(|c| device_name(c.uri()).ok().as_deref() == Some(name))
    }

    /// Destroy child with given uri.
    /// If the child does not exist the method returns success.
    pub async fn remove_child(
        mut self: Pin<&mut Self>,
        uri: &str,
    ) -> Result<(), Error> {
        info!("{:?}: remove child request: '{}'", self, uri);

        self.check_nexus_operation(NexusOperation::ReplicaRemove)?;

        self.check_child_remove_operation(uri)?;

        if self.lookup_child(uri).is_none() {
            return Ok(());
        }

        // Pause subsystem and rebuild jobs.
        debug!("{self:?}: remove child '{uri}': pausing...");
        let paused = self.pause_rebuild_jobs(uri).await;
        if let Err(e) = self.as_mut().pause().await {
            error!(
                "{self:?}: remove child '{uri}': failed to pause subsystem: {e}"
            );
            paused.resume().await;
            return Ok(());
        }
        debug!("{self:?}: remove child '{uri}': pausing ok");

        // Update persistent store.
        if let Err(e) = self
            .persist(PersistOp::RemoveChild {
                child_uri: uri.to_string(),
            })
            .await
        {
            error!(
                "{self:?}: failed to remove child '{uri}' because of \
                persistent store update failure: {e}"
            );
            return Err(e);
        }

        // Close and remove the child.
        let res = match self.lookup_child(uri) {
            Some(child) => {
                // Detach the child from the I/O path, and close its handles.
                if let Some(device) = child.get_device_name() {
                    self.detach_device(&device).await;
                    self.disconnect_all_detached_devices().await;
                }

                // Close child's device.
                let res = child.close().await.map_err(|e| Error::CloseChild {
                    name: self.name.clone(),
                    child: uri.to_owned(),
                    source: e,
                });

                // Remove the child from the child list.
                unsafe {
                    self.as_mut()
                        .unpin_mut()
                        .children
                        .retain(|c| c.uri() != uri);
                }

                res
            }
            None => Ok(()),
        };

        // Resume subsystem and paused rebuild jobs.
        debug!("{self:?}: remove child '{uri}': resuming...");
        if let Err(e) = self.as_mut().resume().await {
            error!(
                "{self:?}: remove child '{uri}': failed \
                to resume subsystem: {e}"
            );
        }
        paused.resume().await;
        debug!("{self:?}: remove child '{uri}': resuming ok");

        res
    }

    /// Faults a child with the given reason.
    pub async fn fault_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
        reason: FaultReason,
    ) -> Result<NexusStatus, Error> {
        info!(
            "{:?}: fault ({}) child request for '{}'",
            self, reason, child_uri
        );

        // Check that the nexus allows such operation.
        self.check_nexus_operation(NexusOperation::ReplicaFault)?;

        // Check that the child exists and can be removed.
        self.check_child_remove_operation(child_uri)?;

        // Get child's device name.
        let dev_name = self.get_child_device_name(child_uri)?;

        // Stop running rebuild jobs.
        let paused = self.as_mut().pause_rebuild_jobs(child_uri).await;

        // Fault and retire.
        self.as_mut().retire_child_device(&dev_name, reason, false);

        let res = Ok(self.status());

        // Restart rebuild jobs.
        paused.resume().await;

        res
    }

    /// Checks that the given child can be removed or offlined.
    fn check_child_remove_operation(
        &self,
        child_uri: &str,
    ) -> Result<(), Error> {
        let _ = self.child(child_uri)?;

        if self.child_count() == 1 {
            return Err(Error::RemoveLastChild {
                name: self.name.clone(),
                child: child_uri.to_owned(),
            });
        }

        let healthy = self
            .children_iter()
            .filter(|c| c.is_healthy())
            .collect::<Vec<_>>();

        if healthy.len() == 1 && healthy[0].uri() == child_uri {
            // the last healthy child cannot be faulted
            return Err(Error::RemoveLastHealthyChild {
                name: self.name.clone(),
                child: child_uri.to_owned(),
            });
        }

        Ok(())
    }

    /// Onlines a child by re-opening its underlying block device and rebuilding
    /// the data from an existing child.
    pub async fn online_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<NexusStatus, Error> {
        let nexus_name = self.name.clone();
        let nexus_size = self.req_size();

        self.check_nexus_operation(NexusOperation::ReplicaOnline)?;

        info!("{:?}: online child request: '{}'", self, child_uri);

        let child = unsafe { self.as_mut().child_mut_unsafe(child_uri)? };

        if child.state() == ChildState::Open {
            warn!("{:?}: child already online", child);
            return Ok(self.status());
        }

        child
            .online(nexus_size)
            .await
            .context(nexus_err::OnlineChild {
                child: child_uri.to_owned(),
                name: nexus_name.clone(),
            })?;

        // Acquire reservations.
        if let Err(e) = child.reservation_acquire(&self.nvme_params).await {
            child.close().await.ok();

            return Err(e).context(nexus_err::OnlineChild {
                child: child_uri.to_owned(),
                name: nexus_name.clone(),
            });
        }

        // Register event listener for onlined child.
        child.set_event_listener(self.get_event_sink());

        // Start rebuild.
        if let Err(e) = self.start_rebuild(child_uri).await {
            child.close().await.ok();
            return Err(e);
        }

        self.event(EventAction::OnlineChild, child.meta())
            .generate();

        Ok(self.status())
    }

    /// Unconditionally closes all children of this nexus.
    pub(crate) async fn close_children(&self) {
        info!("{self:?}: closing {n} children...", n = self.children.len());

        let mut failed = 0;
        for child in self.children_iter() {
            if child.close().await.is_err() {
                failed += 1;
            }
        }

        if failed == 0 {
            info!("{self:?}: all children closed");
        } else {
            error!("{self:?}: failed to close some of the children");
        }
    }

    /// Tries to open all the child devices.
    /// Opens children, determines and validates block size and block count
    /// of underlying devices.
    pub(crate) async fn try_open_children(
        mut self: Pin<&mut Self>,
    ) -> Result<(), Error> {
        info!("{:?}: opening nexus children...", self);

        let name = self.name.clone();

        let size = self.req_size();

        // Take the child vec, try open and re-add.
        // NOTE: self.child_count is not affected by this algorithm!
        // let children = std::mem::take(&mut self.children);
        let mut error = None;
        let evt_listener = self.as_mut().get_event_sink();

        unsafe {
            for child in self.as_mut().children_iter_mut() {
                match child.open(size, ChildSyncState::Synced) {
                    Ok(_) => {
                        child.set_event_listener(evt_listener.clone());
                    }
                    Err(err) => {
                        error = Some(err);
                    }
                }
            }
        }

        // TODO:
        // Depending on IO consistency policies, we might be able to go online
        // even if some of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.
        if let Some(error) = error {
            // Close any children that WERE successfully opened.
            for child in self.children_iter() {
                if child.is_opened() {
                    child.close().await.ok();
                }
            }

            return Err(Error::NexusIncomplete {
                name,
                reason: error.to_string(),
            });
        }

        // acquire a write exclusive reservation on all children,
        // if any one fails, close all children.
        let mut write_ex_err: Result<(), Error> = Ok(());
        for child in self.children_iter() {
            if let Err(error) =
                child.reservation_acquire(&self.nvme_params).await
            {
                write_ex_err = Err(Error::ChildWriteExclusiveResvFailed {
                    source: error,
                    child: child.uri().to_owned(),
                    name: self.name.clone(),
                });
                break;
            }
        }

        if let Err(e) = write_ex_err {
            self.close_children().await;
            return Err(e);
        }

        let mut new_alignment = self.alignment();

        for child in self.children_iter() {
            let alignment = child.get_device().as_ref().unwrap().alignment();
            if new_alignment < alignment {
                info!(
                    "{:?}: child has alignment {}, updating \
                        required_alignment from {}",
                    child, alignment, new_alignment
                );
                new_alignment = alignment;
            }
        }

        if self.alignment() != new_alignment {
            unsafe {
                self.as_mut().set_required_alignment(new_alignment as u8);
            }
        }

        info!(
            "{:?}: all nexus children successfully opened: required_alignment={}",
            self, new_alignment,
        );
        Ok(())
    }

    /// Closes a child by its device name.
    pub async fn close_child(&self, device_name: &str) -> Result<(), Error> {
        info!("{self:?}: destroying child device: '{device_name}'");

        self.child_by_device(device_name)?
            .close()
            .await
            .map_err(|source| Error::CloseChild {
                source,
                child: device_name.to_string(),
                name: self.name.to_string(),
            })
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blkcnt of all online children as
    /// they MAY vary in size.
    pub(crate) fn min_num_blocks(&self) -> Option<u64> {
        self.children_iter()
            .filter(|c| c.is_healthy())
            .map(|c| c.get_device().unwrap().num_blocks())
            .reduce(min)
    }

    /// Looks up a child by device name.
    pub fn lookup_child_by_device(
        &self,
        device_name: &str,
    ) -> Option<&NexusChild<'n>> {
        self.children_iter()
            .find(|c| c.match_device_name(device_name))
    }

    /// Looks up a child by its UUID.
    pub fn child_by_uuid(
        &self,
        device_uuid: &str,
    ) -> Result<&NexusChild<'n>, Error> {
        let dev = self.children_iter().find(|c| match c.get_uuid() {
            Some(u) => u.eq(device_uuid),
            None => false,
        });
        dev.ok_or_else(|| Error::ChildNotFound {
            child: device_uuid.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Looks up a child by device name.
    /// Returns an error if child is not found.
    pub fn child_by_device(
        &self,
        device_name: &str,
    ) -> Result<&NexusChild<'n>, Error> {
        self.lookup_child_by_device(device_name).ok_or_else(|| {
            Error::ChildNotFound {
                child: device_name.to_owned(),
                name: self.name.clone(),
            }
        })
    }

    /// Looks up a child by device name and returns a mutable reference.
    pub(crate) fn lookup_child_by_device_mut(
        self: Pin<&mut Self>,
        device_name: &str,
    ) -> Option<&mut NexusChild<'n>> {
        unsafe {
            self.children_iter_mut()
                .find(|c| c.match_device_name(device_name))
        }
    }

    /// Looks up a child by device name and returns a mutable reference.
    /// Returns an error if child is not found.
    #[allow(dead_code)]
    pub(crate) fn child_by_device_mut(
        self: Pin<&mut Self>,
        device_name: &str,
    ) -> Result<&mut NexusChild<'n>, Error> {
        let nexus_name = self.name.clone();
        self.lookup_child_by_device_mut(device_name).ok_or_else(|| {
            Error::ChildNotFound {
                child: device_name.to_owned(),
                name: nexus_name,
            }
        })
    }

    /// Looks up a child by its URI.
    pub fn lookup_child(&self, child_uri: &str) -> Option<&NexusChild<'n>> {
        self.children_iter().find(|c| c.uri() == child_uri)
    }

    /// Looks up a child by its URI.
    /// Returns an error if child is not found.
    pub fn child(&self, child_uri: &str) -> Result<&NexusChild<'n>, Error> {
        self.lookup_child(child_uri)
            .ok_or_else(|| Error::ChildNotFound {
                child: child_uri.to_owned(),
                name: self.name.clone(),
            })
    }

    /// Looks up a child by its URI and returns a mutable reference.
    pub fn lookup_child_mut(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Option<&mut NexusChild<'n>> {
        unsafe { self.children_iter_mut().find(|c| c.uri() == child_uri) }
    }

    /// Looks up a child by its URI and returns a mutable reference.
    /// Returns an error if child is not found.
    pub fn child_mut(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<&mut NexusChild<'n>, Error> {
        let nexus_name = self.name.clone();
        self.lookup_child_mut(child_uri)
            .ok_or_else(|| Error::ChildNotFound {
                child: child_uri.to_owned(),
                name: nexus_name,
            })
    }

    /// Version of `child_mut` that returns a reference to a child in
    /// static lifetime, allowing to manipulate a child and the nexus in the
    /// same scope.
    unsafe fn child_mut_unsafe(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<&'static mut NexusChild<'static>, Error> {
        self.child_mut(child_uri).map(|c| {
            std::mem::transmute::<
                &mut NexusChild<'n>,
                &'static mut NexusChild<'static>,
            >(c)
        })
    }

    /// Looks up a child by its URI and returns child device name.
    pub fn get_child_device_name(
        &self,
        child_uri: &str,
    ) -> Result<String, Error> {
        self.child(child_uri)?.get_device_name().ok_or_else(|| {
            Error::ChildDeviceNotOpen {
                child: child_uri.to_owned(),
                name: self.name.clone(),
            }
        })
    }

    /// Returns the list of URIs of all children.
    pub(crate) fn child_devices(&self) -> Vec<String> {
        self.children_iter()
            .filter_map(|c| c.get_device_name())
            .collect()
    }

    /// Returns the list of URIs of all children.
    pub(crate) fn child_uris(&self) -> Vec<String> {
        self.children_iter().map(|c| c.uri().to_owned()).collect()
    }
}

impl<'n> DeviceEventListener for Nexus<'n> {
    fn handle_device_event(&self, evt: DeviceEventType, dev_name: &str) {
        match evt {
            DeviceEventType::DeviceRemoved
            | DeviceEventType::LoopbackRemoved => {
                Reactors::master().send_future(Nexus::child_remove_routine(
                    self.name.clone(),
                    dev_name.to_owned(),
                ));
            }
            DeviceEventType::AdminCommandCompletionFailed => {
                info!(
                    "{:?}: admin command completion failure event: \
                    retiring child '{}'",
                    self, dev_name
                );
                self.retire_child_device(
                    dev_name,
                    FaultReason::AdminCommandFailed,
                    false,
                );
            }
            DeviceEventType::AdminQNoticeCtrlFailed => {
                Reactors::master().send_future(Nexus::disconnect_failed_child(
                    self.name.clone(),
                    dev_name.to_owned(),
                ));
            }

            _ => {
                warn!(
                    "{:?}: ignoring event '{:?}' for device '{}'",
                    self, evt, dev_name
                );
            }
        }
    }

    fn get_listener_name(&self) -> String {
        self.name.to_string()
    }
}

impl<'n> Nexus<'n> {
    /// Faults the device by its name, with the given fault reason.
    /// The faulted device is scheduled to be retired.
    pub(crate) fn retire_child_device(
        &self,
        child_device: &str,
        reason: FaultReason,
        retry: bool,
    ) -> Option<IOLogChannel> {
        let Some(c) = self.lookup_child_by_device(child_device) else {
            error!(
                "{self:?}: trying to retire a child device which \
                cannot be not found '{child_device}'"
            );

            return None;
        };

        // We must start I/O log _before_ changing the state of the child.
        // Otherwise, any reconfiguration (Nexus::reconfigure()) that may run
        // in parallel, would skip connecting both child's device as a writer
        // and child's I/O log.
        let has_io_log = c.start_io_log();

        // Fail and retire an open child.
        if Ok(ChildState::Open)
            == c.state
                .compare_exchange(ChildState::Open, ChildState::Faulted(reason))
        {
            if has_io_log {
                warn!("{c:?}: faulted with {reason}, will retire");
            } else {
                warn!(
                    "{c:?}: faulted with {reason}, will retire; \
                    child is currently unsync, disabling I/O logging"
                );
            }

            // The child state was not faulted yet, so this is the first
            // I/O to this child for which we
            // encountered an error.
            Reactors::master().send_future(Nexus::child_retire_routine(
                self.name.clone(),
                child_device.to_owned(),
                retry,
            ));

            // Set the timestamp of this child fault.
            c.set_fault_timestamp();
        } else {
            warn!("{c:?}: faulted with {reason}, already retired/retiring");
        }

        // Here, we tell all the channels to reconnect the I/O logs, including
        // the newly created one.
        //
        // Since it is done asynchronously via `traverse_io_channels`, there is
        // a possibility that a parallel write I/O would arrive to another
        // channel before the new log is connected to that channel.
        //
        // However, this won't cause a data loss because:
        //
        // A) If such I/O succeeds, the data goes through to the device.
        //
        // B) If such I/O fails, `retire_child_device` would be called again,
        // and the I/O would end up logged.
        if has_io_log {
            self.traverse_io_channels(
                (),
                |chan, _| {
                    chan.reconnect_io_logs();
                    ChannelTraverseStatus::Ok
                },
                |_, _| {},
            );

            c.io_log_channel()
        } else {
            None
        }
    }

    /// Returns list of I/O log channels of all children for the current core.
    pub(super) fn io_log_channels(&self) -> Vec<IOLogChannel> {
        self.children_iter()
            .filter(|c| !c.is_rebuilding())
            .filter_map(|c| c.io_log_channel())
            .collect()
    }

    /// Handle child device removal.
    async fn child_remove_routine(nexus_name: String, child_device: String) {
        if let Some(mut nexus) = nexus_lookup_mut(&nexus_name) {
            match nexus.as_mut().lookup_child_by_device_mut(&child_device) {
                Some(child) => {
                    info!(
                        nexus_name,
                        child_device, "Unplugging nexus child device",
                    );
                    child.unplug().await;
                }
                None => {
                    warn!(
                        nexus_name,
                        child_device, "Nexus child device not found",
                    );
                }
            }
        } else {
            warn!(
                nexus_name,
                child_device, "Removing nexus child: nexus already gone",
            );
        }
    }

    /// Disconnect a failed child from the given nexus.
    async fn disconnect_failed_child(nexus_name: String, dev: String) {
        let Some(nex) = nexus_lookup_mut(&nexus_name) else {
            warn!(
                "Nexus '{nexus_name}': retiring failed device '{dev}': \
                nexus already gone"
            );
            return;
        };

        info!("Nexus '{nexus_name}': disconnect handlers for controller failed device: '{dev}'");

        if nex.io_subsystem_state() == Some(NexusPauseState::Pausing) {
            nex.traverse_io_channels_async((), |channel, _| {
                channel.disconnect_detached_devices(|h| {
                    h.get_device().device_name() == dev && h.is_ctrlr_failed()
                });
            })
            .await;
        }
    }

    /// Retires a child device for the given nexus.
    async fn child_retire_routine(
        nexus_name: String,
        dev: String,
        retry: bool,
    ) {
        let Some(mut nex) = nexus_lookup_mut(&nexus_name) else {
            warn!(
                "Nexus '{nexus_name}': retiring device '{dev}': \
                nexus already gone"
            );
            return;
        };

        // Error indicates it is already paused and another
        // thread is processing the fault.
        if let Err(err) = nex.as_mut().do_child_retire(dev.clone()).await {
            if retry {
                warn!(
                    "{nex:?}: retire failed (double pause), retrying: {err}",
                    err = err.verbose()
                );

                assert!(Reactors::is_master());

                Reactors::current().send_future(Nexus::child_retire_routine(
                    nexus_name, dev, retry,
                ));
            } else {
                warn!(
                    "{nex:?}: retire failed (double pause): {err}",
                    err = err.verbose()
                );
            }
            return;
        }

        if matches!(nex.status(), NexusStatus::Faulted) {
            error!("{nex:?}: failed to retire '{dev}': nexus is faulted");
        }
    }

    /// Retires a child with the given device.
    async fn do_child_retire(
        mut self: Pin<&mut Self>,
        dev: String,
    ) -> Result<(), Error> {
        warn!("{self:?}: retiring child device '{dev}'...");

        // Update persistent store. To prevent data inconsistency across
        // replicas, this must be done before disconnecting the
        // device from the I/O channels and acknowledging the I/O to the client.
        // If the persistent store update fails, the nexus is shutting down and
        // we don't proceed further at this place.
        if self.child_retire_persist(&dev).await.is_err() {
            return Ok(());
        }

        // Detach the device from all the channels.
        //
        // After disconnecting, the device will no longer be used by the
        // channels, and all I/Os failing due to this device will eventually
        // resubmit and succeeded (if any healthy children are left).
        //
        // Device disconnection is done in two steps (detach, then disconnect)
        // in order to prevent an I/O race when retiring a device.
        self.detach_device(&dev).await;

        // Disconnect the devices with failed controllers _before_ pause,
        // otherwise pause would get stuck. Keep all controllers which are _not_
        // failed (e.g., in the case I/O failed due to ENOSPC).
        self.traverse_io_channels_async((), |channel, _| {
            channel.disconnect_detached_devices(|h| h.is_ctrlr_failed());
        })
        .await;

        // Disconnect, destroy and close the device. The subsystem must be
        // paused to do this properly.
        {
            debug!("{self:?}: retire: pausing...");
            let res = self.as_mut().pause().await;
            match &res {
                Ok(_) => debug!("{self:?}: retire: pausing ok"),
                Err(e) => warn!("{self:?}: retire: pausing failed: {e}"),
            };

            // Disconnect the all previously detached device handles. This has
            // to be done after the pause to prevent an I/O race.
            self.disconnect_all_detached_devices().await;

            res?;

            self.child_retire_destroy_device(&dev).await;

            debug!("{self:?}: resuming...");
            self.as_mut().resume().await?;
            debug!("{self:?}: resuming ok");
        }

        warn!("{self:?}: child device '{dev}' retired");

        Ok(())
    }

    /// Persist current nexus state during device retire procedure.
    async fn child_retire_persist(&self, dev: &str) -> Result<(), Error> {
        let Some(child) = self.lookup_child_by_device(dev) else {
            error!("{self:?}: child device to retire is not found: '{dev}'");
            return Ok(());
        };

        let uri = child.uri();

        debug!("{self:?}: retire device '{dev}': updating persistent store...");

        // Do not persist child state in case it's the last healthy child of the
        // nexus: let Control Plane reconstruct the nexus using this device as
        // the replica with the most recent user data.
        self.persist(PersistOp::UpdateCond {
            child_uri: uri.to_owned(),
            healthy: child.is_healthy(),
            predicate: &|nexus_info| {
                // Determine the amount of healthy replicas in the persistent
                // state and check against the last healthy
                // replica remaining.
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
                            "{self:?}: no healthy replicas persent \
                            in persistent store when retiring replica '{uri}':
                            not persisting the replica state",
                        );
                        false
                    }
                    1 => {
                        warn!(
                            "{self:?}: retiring the last healthy \
                            replica '{uri}', not persisting the replica state",
                        );
                        false
                    }
                    _ => true,
                }
            },
        })
        .await?;

        debug!("{self:?}: retire device '{dev}': persistent store updated");

        Ok(())
    }

    /// Detaches the device's handles from all I/O channels.
    ///
    /// The detached handles must be disconnected and dropped by a
    /// `disconnect_detached_devices()` call.
    ///
    /// Device disconnection is done in two steps (detach, than disconnect) in
    /// order to prevent an I/O race when retiring a device.
    pub(crate) async fn detach_device(&self, dev: &str) {
        if !self.has_io_device {
            return;
        }

        debug!("{self:?}: detaching '{dev}' from all channels...");

        self.traverse_io_channels_async(dev, |channel, dev| {
            channel.detach_device(dev);
        })
        .await;

        debug!("{self:?}: '{dev}' detached from all I/O channels");
    }

    /// Disconnects all the detached devices on all I/O channels by dropping
    /// their handles.
    pub(crate) async fn disconnect_all_detached_devices(&self) {
        debug!("{self:?}: disconnecting all detached devices ...");

        self.traverse_io_channels_async((), |channel, _| {
            channel.disconnect_detached_devices(|_| true);
        })
        .await;

        debug!("{self:?}: disconnected all detached devices");
    }

    /// Destroys the device being retired.
    async fn child_retire_destroy_device(&self, dev: &str) {
        let Some(child) = self.lookup_child_by_device(dev) else {
            error!("{self:?}: child device to retire is not found: '{dev}'");
            return;
        };

        // Cancel rebuild job for this child, if any.
        if let Some(job) = child.rebuild_job() {
            debug!("{self:?}: retire: stopping rebuild job...");
            if let either::Either::Left(terminated) = job.force_fail() {
                Reactors::master().send_future(async move {
                    terminated.await.ok();
                });
            }
        }

        debug!("{child:?}: retire: enqueuing device '{dev}' to retire");

        device_cmd_queue().enqueue(DeviceCommand::RetireDevice {
            nexus_name: self.name.clone(),
            child_device: dev.to_string(),
        });
    }

    /// Sets the current nexus I/O mode for all channels.
    pub(crate) async fn set_nexus_io_mode(&self, mode: IoMode) {
        if !self.has_io_device {
            return;
        }

        debug!("{self:?}: set I/O mode to {mode:?} ...");

        self.traverse_io_channels_async(mode, |channel, mode| {
            channel.set_io_mode(*mode);
        })
        .await;

        debug!("{self:?}: set I/O mode to {mode:?}: done");
    }

    /// TODO
    pub(super) fn try_self_shutdown(&self) {
        let nexus_name = self.nexus_name().to_owned();

        Reactors::master().send_future(async move {
            if let Some(nexus) = nexus_lookup(&nexus_name) {
                // Check against concurrent graceful nexus shutdown
                // initiated by user and mark nexus as being shutdown.
                {
                    let mut s = nexus.state.lock();
                    match *s {
                        NexusState::Shutdown | NexusState::ShuttingDown => {
                            info!(
                                nexus_name,
                                "Nexus is under user-triggered shutdown, \
                                    skipping self shutdown"
                            );
                            return;
                        }
                        nexus_state => {
                            info!(
                                nexus_name,
                                nexus_state=%nexus_state,
                                "Initiating self shutdown for nexus"
                            );
                        }
                    };
                    *s = NexusState::ShuttingDown;
                }

                // Step 1: Close I/O channels for all children.
                for dev in nexus.child_devices() {
                    nexus.detach_device(&dev).await;
                    nexus.disconnect_all_detached_devices().await;

                    device_cmd_queue().enqueue(DeviceCommand::RetireDevice {
                        nexus_name: nexus.name.clone(),
                        child_device: dev.clone(),
                    });
                }

                // Step 2: abort all frozen I/Os.
                debug!("{nexus:?}: aborting all frozen I/Os");
                nexus
                    .traverse_io_channels_async((), |channel, _| {
                        channel.abort_frozen();
                    })
                    .await;

                // Step 3: cancel all active rebuild jobs.
                let child_uris = nexus.child_uris();
                for child in child_uris {
                    nexus.cancel_rebuild_jobs(&child).await;
                }

                // Step 4: close all children.
                nexus.close_children().await;

                // Step 5: Mark nexus as shutdown.
                // Note: we don't persist nexus's state in ETCd as nexus
                // might be recreated on onother node.
                *nexus.state.lock() = NexusState::Shutdown;
            }
        });
    }

    /// Resets nexus nvme children in case of initiator timeout.
    pub(crate) async fn reset_all_children(nexus_name: String) {
        let mut nex = match nexus_lookup_mut(&nexus_name) {
            Some(v) => v,
            None => {
                warn!("Reset nexus children request: '{nexus_name}' not found");
                return;
            }
        };

        info!("{nex:?}: resetting nexus children ...");

        // Pause the nexus.
        if let Err(e) = nex.as_mut().pause().await {
            error!("{nex:?}: failed to pause: {e}");
            return;
        }

        debug!("{nex:?}: paused for children reset");

        for child in nex.children_iter() {
            if !(child.is_healthy() || child.is_opened_unsync()) {
                continue;
            }

            let dev = match child.get_device() {
                Ok(dev) => dev,
                Err(e) => {
                    error!("{child:?}: failed to get device: {e:?}");
                    continue;
                }
            };

            if dev.driver_name() != "nvme" {
                debug!("{child:?}: driver is not NVMe-oF, skipping reset");
                continue;
            }

            debug!(
                "{child:?}: resetting child: {drv}/{prod}",
                drv = dev.driver_name(),
                prod = dev.product_name(),
            );

            let (s, r) = oneshot::channel::<bool>();

            let dev_name = dev.device_name();
            NvmfSubsystem::reset_controller(&dev_name, cb_arg(s)).await;

            if let Ok(res) = r.await {
                if !res {
                    error!("{child:?}: failed to reset");
                }
            }
        }

        // Resume the nexus.
        if let Err(e) = nex.as_mut().resume().await {
            error!("{nex:?}: failed to resume: {e}");
        } else {
            debug!("{nex:?}: resumed after children reset");
        }

        if !nex.as_mut().set_open_state() {
            error!("{nex:?}: failed to set nexus status open");
        }

        info!("{nex:?}: resetting nexus children done");
    }
}
