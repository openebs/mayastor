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

use futures::{channel::oneshot, future::join_all};
use snafu::ResultExt;

use super::{
    nexus_err,
    nexus_lookup_mut,
    ChildState,
    DrEvent,
    Error,
    Nexus,
    NexusChannel,
    NexusChild,
    NexusState,
    NexusStatus,
    PersistOp,
    Reason,
};

use crate::{
    bdev::{device_create, device_destroy, device_lookup},
    bdev_api::BdevError,
    core::{
        device_cmd_queue,
        partition,
        DeviceCommand,
        DeviceEventListener,
        DeviceEventType,
        Reactors,
        VerboseError,
    },
};

use spdk_rs::{ChannelTraverseStatus, IoDeviceChannelTraverse};

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
        let status = self.as_mut().add_child_only(uri).await?;

        if !norebuild {
            if let Err(e) = self.as_mut().start_rebuild(uri).await {
                // todo: CAS-253 retry starting the rebuild again when ready
                error!(
                    "Child added but rebuild failed to start: {}",
                    e.verbose()
                );
                match self.child_mut(uri) {
                    Ok(child) => child.fault(Reason::RebuildFailed).await,
                    Err(e) => error!(
                        "Failed to find newly added child {}, error: {}",
                        uri,
                        e.verbose()
                    ),
                };
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

        if self.lookup_child_device(&name).is_some() {
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
        let mut res =
            child.open(self.req_size(), ChildState::Faulted(Reason::OutOfSync));

        if res.is_ok() {
            // we have created the bdev, and created a nexusChild struct. To
            // make use of the device itself the
            // data and metadata must be validated. The child
            // will be added and marked as faulted, once the rebuild has
            // completed the device can transition to online
            if let Err(e) = child
                .acquire_write_exclusive(
                    self.nvme_params.resv_key,
                    self.nvme_params.preempt_key,
                )
                .await
            {
                res = Err(e);
            }
        }

        match res {
            Ok(child_uri) => {
                let child_state = child.state();

                // Register event listener for newly added child.
                child.set_event_listener(self.get_event_sink());

                unsafe {
                    self.as_mut().child_add_unsafe(child);
                }

                self.persist(PersistOp::AddChild {
                    child_uri,
                    child_state,
                })
                .await;

                Ok(self.status())
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

    /// Destroy child with given uri.
    /// If the child does not exist the method returns success.
    pub async fn remove_child(
        mut self: Pin<&mut Self>,
        uri: &str,
    ) -> Result<(), Error> {
        info!("{:?}: remove child request: '{}'", self, uri);

        if self.child_count() == 1 {
            return Err(Error::DestroyLastChild {
                name: self.name.clone(),
                child: uri.to_owned(),
            });
        }

        let healthy_children = self
            .children_iter()
            .filter(|c| c.is_healthy())
            .collect::<Vec<_>>();

        let have_healthy_children = !healthy_children.is_empty();
        let other_healthy_children = healthy_children
            .into_iter()
            .filter(|c| c.uri() != uri)
            .count()
            > 0;

        if have_healthy_children && !other_healthy_children {
            return Err(Error::DestroyLastHealthyChild {
                name: self.name.clone(),
                child: uri.to_owned(),
            });
        }

        let paused = self.as_mut().pause_rebuild_jobs(uri).await;

        let idx = match self.children_iter().position(|c| c.uri() == uri) {
            None => {
                paused.resume().await;
                return Ok(());
            }
            Some(val) => val,
        };

        let res = unsafe {
            self.as_mut().child_at_mut(idx).close().await.map_err(|e| {
                Error::CloseChild {
                    name: self.name.clone(),
                    child: self.child_at(idx).uri().to_string(),
                    source: e,
                }
            })
        };

        if res.is_ok() {
            let child_state = self.child_at(idx).state();

            unsafe {
                self.as_mut().child_remove_at_unsafe(idx);
            }

            self.persist(PersistOp::Update {
                child_uri: uri.to_string(),
                child_state,
            })
            .await;
        }

        paused.resume().await;

        res
    }

    /// offline a child device and reconfigure the IO channels
    pub(crate) async fn offline_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<NexusStatus, Error> {
        info!("{:?}: offline child request: '{}'", self, child_uri);

        let paused = self.as_mut().pause_rebuild_jobs(child_uri).await;

        let res = match self.as_mut().child_mut(child_uri) {
            Ok(child) => {
                child.offline().await;
                self.reconfigure(DrEvent::ChildOffline).await;
                Ok(self.status())
            }
            Err(e) => Err(e),
        };

        paused.resume().await;

        res
    }

    /// Faults a child device and reconfigures the IO channels.
    pub async fn fault_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
        reason: Reason,
    ) -> Result<(), Error> {
        info!("{:?}: fault child request for '{}'", self, child_uri);

        if self.children().len() < 2 {
            return Err(Error::RemoveLastChild {
                name: self.name.clone(),
                child: child_uri.to_owned(),
            });
        }

        let healthy = self
            .children_iter()
            .filter(|c| c.state() == ChildState::Open)
            .collect::<Vec<_>>();

        if healthy.len() == 1 && healthy[0].uri() == child_uri {
            // the last healthy child cannot be faulted
            return Err(Error::FaultingLastHealthyChild {
                name: self.name.clone(),
                child: child_uri.to_owned(),
            });
        }

        let paused = self.as_mut().pause_rebuild_jobs(child_uri).await;

        let res = match self.as_mut().child_mut(child_uri) {
            Ok(child) => {
                match child.state() {
                    ChildState::Faulted(current_reason) => {
                        if current_reason != reason
                            && reason == Reason::ByClient
                        {
                            child.fault(reason).await;
                        } else {
                            warn!("{:?}: already faulted", child);
                        }
                    }
                    _ => {
                        child.fault(reason).await;
                        self.reconfigure(DrEvent::ChildFault).await;
                    }
                }
                Ok(())
            }
            Err(e) => Err(e),
        };

        paused.resume().await;

        res
    }

    /// Retires a child immediately.
    #[allow(dead_code)]
    pub(crate) async fn retire_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<NexusStatus, Error> {
        info!("{:?}: retire child request: '{}'", self, child_uri);

        // Check that device does exist.
        let ch = self.as_mut().child_mut(child_uri)?;

        if let Some(dev_name) = ch.get_device_name() {
            self.as_mut().retire_child_device(
                &dev_name,
                Reason::IoError,
                false,
            );
        } else {
            warn!("{:?}: child is not open, won't retire", ch);
        }

        Ok(self.status())
    }

    /// online a child and reconfigure the IO channels. The child is already
    /// registered, but simply not opened. This can be required in case where
    /// a child is misbehaving.
    pub async fn online_child(
        mut self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<NexusStatus, Error> {
        let nexus_name = self.name.clone();
        let nexus_size = self.req_size();

        info!("{:?}: online child request: '{}'", self, child_uri);

        let child = self.as_mut().child_mut(child_uri)?;

        child
            .online(nexus_size)
            .await
            .context(nexus_err::OnlineChild {
                child: child_uri.to_owned(),
                name: nexus_name,
            })?;

        self.as_mut().start_rebuild(child_uri).await.map(|_| {})?;

        Ok(self.status())
    }

    /// Close each child that belongs to this nexus.
    pub(crate) async fn close_children(mut self: Pin<&mut Self>) {
        let futures =
            unsafe { self.as_mut().children_iter_mut().map(|c| c.close()) };
        let results = join_all(futures).await;
        if results.iter().any(|c| c.is_err()) {
            error!("{:?}: failed to close children", self);
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

        if self.children().is_empty() {
            return Err(Error::NexusIncomplete {
                name,
            });
        }

        // Determine Nexus block size and data start and end offsets.
        let mut start_blk = 0;
        let mut end_blk = 0;
        let mut blk_size = 0;

        for child in self.children_iter() {
            let dev = match child.get_device() {
                Ok(dev) => dev,
                Err(_) => {
                    return Err(Error::NexusIncomplete {
                        name,
                    })
                }
            };

            let nb = dev.num_blocks();
            let bs = dev.block_len();

            if blk_size == 0 {
                blk_size = bs;
            } else if bs != blk_size {
                return Err(Error::MixedBlockSizes {
                    name: self.name.clone(),
                });
            }

            match partition::calc_data_partition(self.req_size(), nb, bs) {
                Some((start, end)) => {
                    if start_blk == 0 {
                        start_blk = start;
                        end_blk = end;
                    } else {
                        end_blk = min(end_blk, end);

                        if start_blk != start {
                            return Err(Error::ChildGeometry {
                                child: child.uri().to_owned(),
                                name,
                            });
                        }
                    }
                }
                None => {
                    return Err(Error::ChildTooSmall {
                        child: child.uri().to_owned(),
                        name,
                        num_blocks: nb,
                        block_size: bs,
                    })
                }
            }
        }

        unsafe {
            self.as_mut().set_data_ent_offset(start_blk);
            self.as_mut().set_block_len(blk_size as u32);
            self.as_mut().set_num_blocks(end_blk - start_blk);
        }

        let size = self.req_size();

        // Take the child vec, try open and re-add.
        // NOTE: self.child_count is not affected by this algorithm!
        // let children = std::mem::take(&mut self.children);
        let mut failed = false;
        let evt_listener = self.as_mut().get_event_sink();

        unsafe {
            for child in self.as_mut().children_iter_mut() {
                if child.open(size, ChildState::Open).is_ok() {
                    child.set_event_listener(evt_listener.clone());
                } else {
                    failed = true;
                }
            }
        }

        // TODO:
        // Depending on IO consistency policies, we might be able to go online
        // even if some of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.
        if failed {
            // Close any children that WERE succesfully opened.
            unsafe {
                for child in self.as_mut().children_iter_mut() {
                    if child.state() == ChildState::Open {
                        if let Err(error) = child.close().await {
                            error!(
                                "{:?}: child failed to close: {}",
                                child,
                                error.verbose()
                            );
                        }
                    }
                }
            }

            return Err(Error::NexusIncomplete {
                name,
            });
        }

        // acquire a write exclusive reservation on all children,
        // if any one fails, close all children.
        let mut write_ex_err: Result<(), Error> = Ok(());
        for child in self.children_iter() {
            if let Err(error) = child
                .acquire_write_exclusive(
                    self.nvme_params.resv_key,
                    self.nvme_params.preempt_key,
                )
                .await
            {
                write_ex_err = Err(Error::ChildWriteExclusiveResvFailed {
                    source: error,
                    child: child.uri().to_owned(),
                    name: self.name.clone(),
                });
                break;
            }
        }

        if let Err(error) = write_ex_err {
            unsafe {
                for child in self.as_mut().children_iter_mut() {
                    if let Err(error) = child.close().await {
                        error!(
                            "{:?}: child failed to close: {}",
                            child,
                            error.verbose()
                        );
                    }
                }
            }
            return Err(error);
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
            "{:?}: children opened, updated specs: start_blk={}, end_blk={}, \
                block_len={}, required_alignment={}",
            self, start_blk, end_blk, blk_size, new_alignment
        );

        Ok(())
    }

    /// TODO
    pub async fn destroy_child_device(
        &self,
        device_name: &str,
    ) -> Result<(), Error> {
        info!("{:?}: destroying child device: '{}'", self, device_name);

        if let Some(child) = self.lookup_child_device(device_name) {
            child
                .destroy_device()
                .await
                .map_err(|source| Error::DestroyChild {
                    source,
                    child: device_name.to_string(),
                    name: self.name.to_string(),
                })
        } else {
            Err(Error::ChildNotFound {
                child: device_name.to_string(),
                name: self.name.to_string(),
            })
        }
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blkcnt of all online children as
    /// they MAY vary in size.
    pub(crate) fn min_num_blocks(&self) -> Option<u64> {
        self.children_iter()
            .filter(|c| c.state() == ChildState::Open)
            .map(|c| c.get_device().unwrap().num_blocks())
            .reduce(min)
    }

    /// Looks up a child based on the underlying block device name.
    pub fn lookup_child_device(
        &self,
        device_name: &str,
    ) -> Option<&NexusChild<'n>> {
        self.children_iter()
            .find(|c| c.match_device_name(device_name))
    }

    /// Looks up a child based on the underlying block device name and
    /// returns a mutable reference.
    pub fn lookup_child_device_mut(
        self: Pin<&mut Self>,
        device_name: &str,
    ) -> Option<&mut NexusChild<'n>> {
        unsafe {
            self.children_iter_mut()
                .find(|c| c.match_device_name(device_name))
        }
    }

    /// Looks up a child by its URI.
    pub fn lookup_child(&self, child_uri: &str) -> Option<&NexusChild<'n>> {
        self.children_iter().find(|c| c.uri() == child_uri)
    }

    /// Looks up a child by its URI and returns a mutable reference.
    pub fn lookup_child_mut(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Option<&mut NexusChild<'n>> {
        unsafe { self.children_iter_mut().find(|c| c.uri() == child_uri) }
    }

    /// Looks up a child by its URI.
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

    /// TODO
    pub fn children_uris(&self) -> Vec<String> {
        self.children_iter().map(|c| c.uri().to_owned()).collect()
    }
}

impl<'n> DeviceEventListener for Nexus<'n> {
    fn handle_device_event(
        mut self: Pin<&mut Self>,
        evt: DeviceEventType,
        dev_name: &str,
    ) {
        match evt {
            DeviceEventType::DeviceRemoved
            | DeviceEventType::LoopbackRemoved => {
                match self.as_mut().lookup_child_device_mut(dev_name) {
                    Some(child) => {
                        info!(
                            "{:?}: device remove event: unplugging \
                            child",
                            child,
                        );
                        child.unplug();
                    }
                    None => {
                        warn!(
                            "{:?}: device remove event: child device '{}' \
                            not found",
                            self, dev_name
                        );
                    }
                }
            }
            DeviceEventType::AdminCommandCompletionFailed => {
                info!(
                    "{:?}: admin command completion failure event: \
                    retiring child '{}'",
                    self, dev_name
                );
                self.retire_child_device(
                    dev_name,
                    Reason::AdminCommandFailed,
                    false,
                );
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

/// TODO
struct UpdateFailFastCtx {
    sender: oneshot::Sender<bool>,
    child_device: String,
}

/// TODO
fn update_failfast_cb(
    channel: &mut NexusChannel,
    ctx: &mut UpdateFailFastCtx,
) -> ChannelTraverseStatus {
    channel.disconnect_device(&ctx.child_device);
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
    /// Marks a child device as faulted.
    /// Returns true if the child was in open state, false otherwise.
    fn child_io_faulted(
        self: Pin<&mut Self>,
        device_name: &str,
        reason: Reason,
    ) -> bool {
        match self.lookup_child_device(device_name) {
            Some(c) => {
                debug!("{:?}: faulting with {}...", c, reason);

                if Ok(ChildState::Open)
                    == c.state.compare_exchange(
                        ChildState::Open,
                        ChildState::Faulted(reason),
                    )
                {
                    warn!("{:?}: I/O faulted; will retire", c);
                    true
                } else {
                    warn!("{:?}: I/O faulted; child was already faulted", c);
                    false
                }
            }
            None => {
                error!(
                    "{:?}: trying to fault a child device which \
                        cannot be not found '{}'",
                    self, device_name
                );
                false
            }
        }
    }

    /// TODO
    pub(crate) fn retire_child_device(
        mut self: Pin<&mut Self>,
        child_device: &str,
        reason: Reason,
        retry: bool,
    ) {
        // check if this child needs to be retired
        let need_retire = self.as_mut().child_io_faulted(child_device, reason);

        // The child state was not faulted yet, so this is the first I/O
        // to this child for which we encountered an error.
        if need_retire {
            Reactors::master().send_future(Nexus::child_retire_routine(
                self.name.clone(),
                child_device.to_owned(),
                retry,
            ));
        }
    }

    /// Retire a child for this nexus.
    async fn child_retire_routine(
        nexus_name: String,
        child_device: String,
        retry: bool,
    ) {
        if let Some(mut nexus) = nexus_lookup_mut(&nexus_name) {
            // Error indicates it is already paused and another
            // thread is processing the fault
            if let Err(err) =
                nexus.as_mut().do_child_retire(child_device.clone()).await
            {
                if retry {
                    warn!(
                        "{:?}: retire failed (double pause), retrying: {}",
                        nexus,
                        err.verbose()
                    );

                    assert!(Reactors::is_master());

                    Reactors::current().send_future(
                        Nexus::child_retire_routine(
                            nexus_name,
                            child_device,
                            retry,
                        ),
                    );
                } else {
                    warn!(
                        "{:?}: retire failed (double pause): {}",
                        nexus,
                        err.verbose()
                    );
                }
                return;
            }

            if matches!(nexus.status(), NexusStatus::Faulted) {
                error!(
                    "{:?}: failed to retire '{}': nexus is faulted",
                    nexus, child_device
                );
            }
        } else {
            warn!(
                "Nexus '{}': retiring device '{}': nexus already gone",
                nexus_name, child_device
            );
        }
    }

    /// Retires a child with the given device.
    async fn do_child_retire(
        mut self: Pin<&mut Self>,
        device_name: String,
    ) -> Result<(), Error> {
        warn!("{:?}: retiring child device '{}'...", self, device_name);

        self.disconnect_all_channels(device_name.clone()).await?;

        debug!("{:?}: pausing...", self);
        self.as_mut().pause().await?;
        debug!("{:?}: pausing ok", self);

        if let Some(child) = self.lookup_child_device(&device_name) {
            let uri = child.uri();

            // Schedule the deletion of the child eventhough etcd has not been
            // updated yet we do not need to wait for that to
            // complete anyway.
            debug!("{:?}: enqueuing remove device '{}'", child, device_name);
            device_cmd_queue().enqueue(DeviceCommand::RemoveDevice {
                nexus_name: self.name.clone(),
                child_device: device_name.clone(),
            });

            // Do not persist child state in case it's the last healthy child of
            // the nexus: let Control Plane reconstruct the nexus
            // using this device as the replica with the most recent
            // user data.
            self.persist(PersistOp::UpdateCond {
                child_uri: uri.to_owned(),
                child_state: child.state(),
                predicate: &|nexus_info| {
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
                                &device_name, uri,
                            );
                            false
                        }
                        1 => {
                            warn!(
                                "nexus {}: retiring the last healthy replica {}, not persisting the replica state",
                                &device_name, uri,
                            );
                            false
                        },
                        _ => true,
                    }
                }
            }).await;
        } else {
            error!(
                "{:?}: child device to retire is not found: '{}'",
                self, device_name
            );
        }

        debug!("{:?}: resuming...", self);
        let r = self.as_mut().resume().await;
        debug!("{:?}: resuming ok", self);
        r
    }

    // TODO
    async fn disconnect_all_channels(
        &self,
        child_device: String,
    ) -> Result<(), Error> {
        let (sender, r) = oneshot::channel::<bool>();

        let ctx = UpdateFailFastCtx {
            sender,
            child_device: child_device.clone(),
        };

        if self.has_io_device {
            info!(
                "{:?}: disconnecting all channels from '{}'...",
                self, child_device
            );

            self.traverse_io_channels(
                update_failfast_cb,
                update_failfast_done,
                ctx,
            );

            r.await
                .expect("disconnect_all_children() sender already dropped");

            info!(
                "{:?}: device '{}' disconnected from all I/O channels",
                self, child_device
            );
        }

        Ok(())
    }
}
