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

use futures::future::join_all;
use snafu::ResultExt;

use crate::{
    bdev::{
        device_create,
        device_destroy,
        device_lookup,
        lookup_nexus_child,
        nexus::{
            nexus_bdev::{
                CreateChild,
                Error,
                Nexus,
                NexusState,
                NexusStatus,
                OpenChild,
            },
            nexus_channel::DrEvent,
            nexus_child::{ChildState, NexusChild},
            nexus_child_status_config::ChildStatusConfig,
        },
        Reason,
        VerboseError,
    },
    core::DeviceEventType,
    nexus_uri::NexusBdevError,
};

impl Nexus {
    /// register children with the nexus, only allowed during the nexus init
    /// phase
    pub fn register_children(&mut self, dev_name: &[String]) {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);
        self.child_count = dev_name.len() as u32;
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name, c);
                self.children.push(NexusChild::new(
                    c.clone(),
                    self.name.clone(),
                    device_lookup(c),
                ))
            })
            .for_each(drop);
    }

    /// Create and register a single child to nexus, only allowed during the
    /// nexus init phase
    pub async fn create_and_register(
        &mut self,
        uri: &str,
    ) -> Result<(), NexusBdevError> {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);
        let name = device_create(&uri).await?;
        self.children.push(NexusChild::new(
            uri.to_string(),
            self.name.clone(),
            device_lookup(&name),
        ));

        self.child_count += 1;
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
        &mut self,
        uri: &str,
        norebuild: bool,
    ) -> Result<NexusStatus, Error> {
        let status = self.add_child_only(uri).await?;

        if !norebuild {
            if let Err(e) = self.start_rebuild(&uri).await {
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
        &mut self,
        uri: &str,
    ) -> Result<NexusStatus, Error> {
        let name = device_create(&uri).await.context(CreateChild {
            name: self.name.clone(),
        })?;

        let child_bdev = match device_lookup(&name) {
            Some(child) => {
                if child.block_len() as u32 != self.bdev.block_len()
                    || self.min_num_blocks() > child.num_blocks()
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

        let mut child = NexusChild::new(
            uri.to_owned(),
            self.name.clone(),
            Some(child_bdev),
        );
        match child.open(self.size) {
            Ok(name) => {
                // we have created the bdev, and created a nexusChild struct. To
                // make use of the device itself the
                // data and metadata must be validated. The child
                // will be added and marked as faulted, once the rebuild has
                // completed the device can transition to online
                info!("{}: child opened successfully {}", self.name, name);

                // it can never take part in the IO path
                // of the nexus until it's rebuilt from a healthy child.
                child.fault(Reason::OutOfSync).await;
                if ChildStatusConfig::add(&child).is_err() {
                    error!("Failed to add child status information");
                }

                // Register event listener for newly added child.
                self.register_child_event_listener(&child);

                self.children.push(child);
                self.child_count += 1;

                if let Err(e) = self.sync_labels().await {
                    error!("Failed to sync labels {:?}", e);
                    // todo: how to signal this?
                }

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

    fn register_child_event_listener(&self, child: &NexusChild) {
        let dev = child
            .get_device()
            .expect("No block device associated with nexus child");

        dev.add_event_listener(Nexus::child_event_listener)
            .map_err(|err| {
                error!(
                    ?err,
                    "{}: failed to register event listener for child {}",
                    self.name,
                    child.get_name(),
                );
                err
            })
            .unwrap();
    }

    /// Destroy child with given uri.
    /// If the child does not exist the method returns success.
    pub async fn remove_child(&mut self, uri: &str) -> Result<(), Error> {
        if self.child_count == 1 {
            return Err(Error::DestroyLastChild {
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

        if let Err(e) = self.children[idx].close().await {
            return Err(Error::CloseChild {
                name: self.name.clone(),
                child: self.children[idx].get_name().to_string(),
                source: e,
            });
        }

        self.children.remove(idx);
        self.child_count -= 1;

        // Update child status to remove this child
        NexusChild::save_state_change();

        self.start_rebuild_jobs(cancelled_rebuilding_children).await;
        Ok(())
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        &mut self,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        trace!("{}: Offline child request for {}", self.name, name);

        let cancelled_rebuilding_children =
            self.cancel_child_rebuild_jobs(name).await;

        if let Some(child) =
            self.children.iter_mut().find(|c| c.get_name() == name)
        {
            child.offline().await;
        } else {
            return Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        self.reconfigure(DrEvent::ChildOffline).await;
        self.start_rebuild_jobs(cancelled_rebuilding_children).await;

        Ok(self.status())
    }

    /// fault a child device and reconfigure the IO channels
    pub async fn fault_child(
        &mut self,
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

        let result =
            match self.children.iter_mut().find(|c| c.get_name() == name) {
                Some(child) => {
                    match child.state() {
                        ChildState::Faulted(_) => {}
                        _ => {
                            child.fault(reason).await;
                            NexusChild::save_state_change();
                            self.reconfigure(DrEvent::ChildFault).await;
                        }
                    }
                    Ok(())
                }
                None => Err(Error::ChildNotFound {
                    name: self.name.clone(),
                    child: name.to_owned(),
                }),
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
        &mut self,
        name: &str,
    ) -> Result<NexusStatus, Error> {
        trace!("{} Online child request", self.name);

        if let Some(child) =
            self.children.iter_mut().find(|c| c.get_name() == name)
        {
            child.online(self.size).await.context(OpenChild {
                child: name.to_owned(),
                name: self.name.clone(),
            })?;
            self.start_rebuild(name).await.map(|_| {})?;
            Ok(self.status())
        } else {
            Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            })
        }
    }

    /// Close each child that belongs to this nexus.
    pub(crate) async fn close_children(&mut self) {
        let futures = self.children.iter_mut().map(|c| c.close());
        let results = join_all(futures).await;
        if results.iter().any(|c| c.is_err()) {
            error!("{}: Failed to close children", self.name);
        }
    }

    /// Listener for nexus child events.
    fn child_event_listener(event: DeviceEventType, device: &str) {
        match event {
            DeviceEventType::DeviceRemoved => {
                match lookup_nexus_child(device) {
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
                            device
                        );
                    }
                }
            }
            _ => {
                info!("Ignoring {:?} event for device {}", event, device);
            }
        }
    }

    /// try to open all the child devices
    pub(crate) async fn try_open_children(&mut self) -> Result<(), Error> {
        if self.children.is_empty()
            || self.children.iter().any(|c| c.get_device().is_err())
        {
            return Err(Error::NexusIncomplete {
                name: self.name.clone(),
            });
        }

        let blk_size = self.children[0].get_device().unwrap().block_len();

        if self
            .children
            .iter()
            .any(|b| b.get_device().unwrap().block_len() != blk_size)
        {
            return Err(Error::MixedBlockSizes {
                name: self.name.clone(),
            });
        }

        self.bdev.set_block_len(blk_size as u32);

        let size = self.size;

        let (open, error): (Vec<_>, Vec<_>) = self
            .children
            .iter_mut()
            .map(|c| c.open(size))
            .partition(Result::is_ok);

        // depending on IO consistency policies, we might be able to go online
        // even if one of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.
        if !error.is_empty() {
            for open_child in open {
                let name = open_child.unwrap();
                if let Some(child) =
                    self.children.iter_mut().find(|c| c.get_name() == name)
                {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            self.name,
                            name,
                            e.verbose()
                        );
                    }
                } else {
                    error!("{}: child {} failed to open", self.name, name);
                }
            }
            return Err(Error::NexusIncomplete {
                name: self.name.clone(),
            });
        }

        self.children
            .iter()
            .map(|c| c.get_device().as_ref().unwrap().alignment())
            .collect::<Vec<_>>()
            .iter()
            .map(|s| {
                if self.bdev.alignment() < *s {
                    trace!(
                        "{}: child has alignment {}, updating required_alignment from {}",
                        self.name, *s, self.bdev.alignment()
                    );
                    unsafe {
                        (*self.bdev.as_ptr()).required_alignment = *s as u8;
                    }
                }
            })
            .for_each(drop);

        // Register event listeners for child devices.
        self.children.iter().for_each(|ch| {
            self.register_child_event_listener(ch);
        });

        Ok(())
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blockcnt of all online children as
    /// they MAY vary in size.
    pub(crate) fn min_num_blocks(&self) -> u64 {
        let mut blockcnt = std::u64::MAX;
        self.children
            .iter()
            .filter(|c| c.state() == ChildState::Open)
            .map(|c| c.get_device().unwrap().num_blocks())
            .collect::<Vec<_>>()
            .iter()
            .map(|s| {
                if *s < blockcnt {
                    blockcnt = *s;
                }
            })
            .for_each(drop);
        blockcnt
    }

    /// lookup a child by its name
    pub fn child_lookup(&self, name: &str) -> Option<&NexusChild> {
        self.children
            .iter()
            .filter(|c| c.get_device().is_ok())
            .find(|c| c.get_device().unwrap().device_name() == name)
    }

    pub fn get_child_by_name(
        &mut self,
        name: &str,
    ) -> Result<&mut NexusChild, Error> {
        match self.children.iter_mut().find(|c| c.get_name() == name) {
            Some(child) => Ok(child),
            None => Err(Error::ChildNotFound {
                child: name.to_owned(),
                name: self.name.clone(),
            }),
        }
    }
}
