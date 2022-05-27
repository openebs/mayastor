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

use futures::future::join_all;
use snafu::ResultExt;

use super::{
    fault_nexus_child,
    nexus_iter_mut,
    ChildState,
    CreateChild,
    DrEvent,
    Error,
    Nexus,
    NexusChild,
    NexusState,
    NexusStatus,
    OpenChild,
    Reason,
    VerboseError,
};

use crate::{
    bdev::{
        device_create,
        device_destroy,
        device_lookup,
        nexus::nexus_persistence::PersistOp,
    },
    core::{partition, DeviceEventListener, DeviceEventType, Reactors},
    nexus_uri::NexusBdevError,
};

impl<'n> Nexus<'n> {
    /// register children with the nexus, only allowed during the nexus init
    /// phase
    pub fn register_children(mut self: Pin<&mut Self>, dev_name: &[String]) {
        assert_eq!(*self.state.lock(), NexusState::Init);

        unsafe {
            self.as_mut().get_unchecked_mut().child_count =
                dev_name.len() as u32;
        }

        let nexus_name = self.name.clone();
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name, c);

                unsafe {
                    self.as_mut().get_unchecked_mut().children.push(
                        NexusChild::new(
                            c.clone(),
                            nexus_name.clone(),
                            device_lookup(c),
                        ),
                    )
                }
            })
            .for_each(drop);
    }

    /// Create and register a single child to nexus, only allowed during the
    /// nexus init phase
    pub async fn create_and_register(
        mut self: Pin<&mut Self>,
        uri: &str,
    ) -> Result<(), NexusBdevError> {
        assert_eq!(*self.state.lock(), NexusState::Init);
        let name = device_create(uri).await?;
        let nexus_name = self.name.clone();

        unsafe {
            self.as_mut()
                .get_unchecked_mut()
                .children
                .push(NexusChild::new(
                    uri.to_string(),
                    nexus_name,
                    device_lookup(&name),
                ));
            self.as_mut().get_unchecked_mut().child_count += 1;
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
                match self.get_child_by_name(uri) {
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
        let name = device_create(uri).await.context(CreateChild {
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
                            err
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

        if self.lookup_child(&name).is_some() {
            return Err(Error::ChildAlreadyExists {
                child: name,
                name: self.name.to_owned(),
            });
        }

        let mut child = NexusChild::new(
            uri.to_owned(),
            self.name.clone(),
            Some(child_bdev),
        );

        let mut child_name = child.open(self.req_size);

        if let Ok(ref name) = child_name {
            // we have created the bdev, and created a nexusChild struct. To
            // make use of the device itself the
            // data and metadata must be validated. The child
            // will be added and marked as faulted, once the rebuild has
            // completed the device can transition to online
            info!("{}: child opened successfully {}", self.name, name);

            if let Err(e) = child
                .acquire_write_exclusive(
                    self.nvme_params.resv_key,
                    self.nvme_params.preempt_key,
                )
                .await
            {
                child_name = Err(e);
            }
        }

        match child_name {
            Ok(cn) => {
                // it can never take part in the IO path
                // of the nexus until it's rebuilt from a healthy child.
                child.fault(Reason::OutOfSync).await;
                let child_state = child.state();

                // Register event listener for newly added child.
                child.set_event_listener(self.get_event_sink());

                unsafe {
                    self.as_mut().get_unchecked_mut().children.push(child);
                    self.as_mut().get_unchecked_mut().child_count += 1;
                }

                self.persist(PersistOp::AddChild((cn, child_state))).await;

                Ok(self.status())
            }
            Err(e) => {
                if let Err(err) = device_destroy(uri).await {
                    error!(
                        "Failed to destroy child which failed to open: {}",
                        err
                    );
                }
                Err(e).context(OpenChild {
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
        if self.child_count == 1 {
            return Err(Error::DestroyLastChild {
                name: self.name.clone(),
                child: uri.to_owned(),
            });
        }

        let healthy_children = self
            .children
            .iter()
            .filter(|c| c.is_healthy())
            .collect::<Vec<_>>();

        let have_healthy_children = !healthy_children.is_empty();
        let other_healthy_children = healthy_children
            .into_iter()
            .filter(|c| c.get_name() != uri)
            .count()
            > 0;

        if have_healthy_children && !other_healthy_children {
            return Err(Error::DestroyLastHealthyChild {
                name: self.name.clone(),
                child: uri.to_owned(),
            });
        }

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(uri).await;

        let idx = match self.children.iter().position(|c| c.get_name() == uri) {
            None => return Ok(()),
            Some(val) => val,
        };

        unsafe {
            if let Err(e) = self.as_mut().get_unchecked_mut().children[idx]
                .close()
                .await
            {
                return Err(Error::CloseChild {
                    name: self.name.clone(),
                    child: self.children[idx].get_name().to_string(),
                    source: e,
                });
            }
        }

        let child_state = self.children[idx].state();

        unsafe {
            self.as_mut().get_unchecked_mut().children.remove(idx);
            self.as_mut().get_unchecked_mut().child_count -= 1;
        }

        self.persist(PersistOp::Update((uri.to_string(), child_state)))
            .await;

        self.start_rebuild_jobs(cancelled_rebuilding_children).await;
        Ok(())
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        mut self: Pin<&mut Self>,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        trace!("{}: Offline child request for {}", self.name, name);

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(name).await;

        unsafe {
            if let Some(child) = self
                .as_mut()
                .get_unchecked_mut()
                .children
                .iter_mut()
                .find(|c| c.get_name() == name)
            {
                child.offline().await;
            } else {
                return Err(Error::ChildNotFound {
                    name: self.name.clone(),
                    child: name.to_owned(),
                });
            }
        }

        self.reconfigure(DrEvent::ChildOffline).await;
        self.as_mut()
            .start_rebuild_jobs(cancelled_rebuilding_children)
            .await;

        Ok(self.status())
    }

    /// fault a child device and reconfigure the IO channels
    pub async fn fault_child(
        mut self: Pin<&mut Self>,
        name: &str,
        reason: Reason,
    ) -> Result<(), Error> {
        trace!("{}: fault child request for {}", self.name, name);

        if self.child_count < 2 {
            return Err(Error::RemoveLastChild {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        let healthy_children = self
            .children
            .iter()
            .filter(|c| c.state() == ChildState::Open)
            .collect::<Vec<_>>();

        if healthy_children.len() == 1 && healthy_children[0].get_name() == name
        {
            // the last healthy child cannot be faulted
            return Err(Error::FaultingLastHealthyChild {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(name).await;

        let result = unsafe {
            match self
                .as_mut()
                .get_unchecked_mut()
                .children
                .iter_mut()
                .find(|c| c.get_name() == name)
            {
                Some(child) => {
                    match child.state() {
                        ChildState::Faulted(_) => {}
                        _ => {
                            child.fault(reason).await;
                            self.reconfigure(DrEvent::ChildFault).await;
                        }
                    }
                    Ok(())
                }
                None => Err(Error::ChildNotFound {
                    name: self.name.clone(),
                    child: name.to_owned(),
                }),
            }
        };

        // start rebuilding the children that previously had their rebuild jobs
        // cancelled, in spite of whether or not the child was correctly faulted
        self.start_rebuild_jobs(cancelled_rebuilding_children).await;
        result
    }

    /// online a child and reconfigure the IO channels. The child is already
    /// registered, but simply not opened. This can be required in case where
    /// a child is misbehaving.
    pub async fn online_child(
        mut self: Pin<&mut Self>,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        let nexus_name = self.name.clone();
        let nexus_size = self.req_size;

        trace!("{} Online child request", nexus_name);

        unsafe {
            if let Some(child) = self
                .as_mut()
                .get_unchecked_mut()
                .children
                .iter_mut()
                .find(|c| c.get_name() == name)
            {
                child.online(nexus_size).await.context(OpenChild {
                    child: name.to_owned(),
                    name: nexus_name,
                })?;
                self.as_mut().start_rebuild(name).await.map(|_| {})?;
                Ok(self.status())
            } else {
                Err(Error::ChildNotFound {
                    name: nexus_name,
                    child: name.to_owned(),
                })
            }
        }
    }

    /// Close each child that belongs to this nexus.
    pub(crate) async fn close_children(mut self: Pin<&mut Self>) {
        let futures = unsafe {
            self.as_mut()
                .get_unchecked_mut()
                .children
                .iter_mut()
                .map(|c| c.close())
        };
        let results = join_all(futures).await;
        if results.iter().any(|c| c.is_err()) {
            error!("{}: Failed to close children", self.name);
        }
    }

    /// Tries to open all the child devices.
    /// Opens children, determines and validates block size and block count
    /// of underlying devices.
    pub(crate) async fn try_open_children(
        mut self: Pin<&mut Self>,
    ) -> Result<(), Error> {
        let name = self.name.clone();

        if self.children.is_empty() {
            return Err(Error::NexusIncomplete {
                name,
            });
        }

        // Determine Nexus block size and data start and end offsets.
        let mut start_blk = 0;
        let mut end_blk = 0;
        let mut blk_size = 0;

        for child in self.children.iter() {
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

            match partition::calc_data_partition(self.req_size, nb, bs) {
                Some((start, end)) => {
                    if start_blk == 0 {
                        start_blk = start;
                        end_blk = end;
                    } else {
                        end_blk = min(end_blk, end);

                        if start_blk != start {
                            return Err(Error::ChildGeometry {
                                child: child.name.clone(),
                                name,
                            });
                        }
                    }
                }
                None => {
                    return Err(Error::ChildTooSmall {
                        child: child.name.clone(),
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

        let size = self.req_size;

        // Take the child vec, try open and re-add.
        // NOTE: self.child_count is not affected by this algorithm!
        // let children = std::mem::take(&mut self.children);
        let mut failed = false;
        let evt_listener = self.as_mut().get_event_sink();

        unsafe {
            for child in self.as_mut().get_unchecked_mut().children.iter_mut() {
                match child.open(size) {
                    Ok(child_name) => {
                        info!(
                            "{}: successfully opened child {}",
                            name, child_name
                        );
                        child.set_event_listener(evt_listener.clone());
                    }
                    Err(error) => {
                        error!(
                            "{}: failed to open child {}: {}",
                            name,
                            child.name,
                            error.verbose()
                        );
                        failed = true;
                    }
                };
            }
        }

        // TODO:
        // Depending on IO consistency policies, we might be able to go online
        // even if some of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.
        if failed {
            // Close any children that WERE succesfully opened.
            unsafe {
                for child in
                    self.as_mut().get_unchecked_mut().children.iter_mut()
                {
                    if child.state() == ChildState::Open {
                        if let Err(error) = child.close().await {
                            error!(
                                "{}: failed to close child {}: {}",
                                name,
                                child.name,
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
        for child in self.children.iter() {
            if let Err(error) = child
                .acquire_write_exclusive(
                    self.nvme_params.resv_key,
                    self.nvme_params.preempt_key,
                )
                .await
            {
                write_ex_err = Err(Error::ChildWriteExclusiveResvFailed {
                    source: error,
                    child: child.name.clone(),
                    name: self.name.clone(),
                });
                break;
            }
        }

        if let Err(error) = write_ex_err {
            unsafe {
                for child in
                    self.as_mut().get_unchecked_mut().children.iter_mut()
                {
                    if let Err(error) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            name,
                            child.name,
                            error.verbose()
                        );
                    }
                }
            }
            return Err(error);
        }

        let mut new_alignment = self.alignment();

        for child in self.children.iter() {
            let alignment = child.get_device().as_ref().unwrap().alignment();
            if new_alignment < alignment {
                info!(
                    "{}: child {} has alignment {}, updating \
                        required_alignment from {}",
                    name, child.name, alignment, new_alignment
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
            "{}: updated specs: start_blk={}, end_blk={}, \
                block_len={}, required_alignment={}",
            name, start_blk, end_blk, blk_size, new_alignment
        );

        Ok(())
    }

    /// TODO
    pub async fn destroy_child(&self, name: &str) -> Result<(), Error> {
        if let Some(child) = self.lookup_child(name) {
            child.destroy().await.map_err(|source| Error::DestroyChild {
                source,
                child: name.to_string(),
                name: self.name.to_string(),
            })
        } else {
            Err(Error::ChildNotFound {
                child: name.to_string(),
                name: self.name.to_string(),
            })
        }
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blkcnt of all online children as
    /// they MAY vary in size.
    pub(crate) fn min_num_blocks(&self) -> Option<u64> {
        self.children
            .iter()
            .filter(|c| c.state() == ChildState::Open)
            .map(|c| c.get_device().unwrap().num_blocks())
            .reduce(min)
    }

    /// Looks up a child based on the underlying block device name.
    pub fn lookup_child(&self, device_name: &str) -> Option<&NexusChild> {
        self.children
            .iter()
            .find(|c| c.match_device_name(device_name))
    }

    /// Looks up a child based on the underlying block device name and
    /// returns a mutable reference.
    pub fn lookup_child_mut(
        self: Pin<&mut Self>,
        device_name: &str,
    ) -> Option<&mut NexusChild<'n>> {
        let n = unsafe { Pin::get_unchecked_mut(self) };
        n.children
            .iter_mut()
            .find(|c| c.match_device_name(device_name))
    }

    /// Looks up a child by its URL.
    pub fn get_child_by_name(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<&mut NexusChild<'n>, Error> {
        let nexus_name = self.name.clone();
        let n = unsafe { Pin::get_unchecked_mut(self) };
        match n.children.iter_mut().find(|c| c.get_name() == name) {
            Some(child) => Ok(child),
            None => Err(Error::ChildNotFound {
                child: name.to_owned(),
                name: nexus_name,
            }),
        }
    }
}

impl<'n> DeviceEventListener for Nexus<'n> {
    fn handle_device_event(
        self: Pin<&mut Self>,
        evt: DeviceEventType,
        dev_name: &str,
    ) {
        match evt {
            DeviceEventType::DeviceRemoved => {
                match self.lookup_child_mut(dev_name) {
                    Some(child) => {
                        info!(
                            "{}: removing child {} in response to device removal event",
                            child.get_nexus_name(),
                            child.get_name(),
                        );
                        child.remove();
                    }
                    None => {
                        warn!(
                            "No nexus child exists for device {}, ignoring device removal event",
                            dev_name
                        );
                    }
                }
            }
            DeviceEventType::AdminCommandCompletionFailed => {
                let cn = &dev_name;
                for mut nexus in nexus_iter_mut() {
                    if fault_nexus_child(nexus.as_mut(), cn) {
                        info!(
                            "{}: retiring child {} in response to admin command completion failure event",
                            nexus.name,
                            dev_name,
                        );

                        let child_dev = dev_name.to_string();
                        Reactors::master().send_future(async move {
                            // Error indicates it is already paused and another
                            // thread is processing the fault
                            let child_dev2 = child_dev.clone();
                            if let Err(e) = nexus.child_retire(child_dev).await
                            {
                                warn!(
                                    "retiring child {} returned {}",
                                    child_dev2, e
                                );
                            }
                        });
                        return;
                    }
                }
                warn!(
                    "No nexus child exists for device {}, ignoring admin command completion failure event",
                    dev_name
                );
            }
            _ => {
                info!("Ignoring {:?} event for device {}", evt, dev_name);
            }
        }
    }

    fn get_listener_name(&self) -> String {
        self.name.to_string()
    }
}
