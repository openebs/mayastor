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
//! child requires rebuild first.
//!
//! When reconfiguring the nexus, we traverse all our children, create new IO
//! channels for all children that are in the open state.

use crossbeam::channel::Receiver;
use futures::future::join_all;
use snafu::ResultExt;

use crate::{
    bdev::nexus::{
        nexus_bdev::{
            nexus_lookup, CreateChild, DestroyChild, Error, Nexus, NexusState,
            OpenChild, ReadLabel, StartRebuild,
        },
        nexus_channel::DREvent,
        nexus_child::{ChildState, NexusChild},
        nexus_label::NexusLabel,
    },
    core::{Bdev, Reactors},
    nexus_uri::{bdev_create, bdev_destroy, BdevCreateDestroy},
    rebuild::{RebuildActions, RebuildState, RebuildTask},
};

impl Nexus {
    /// register children with the nexus, only allowed during the nexus init
    /// phase
    pub fn register_children(&mut self, dev_name: &[String]) {
        assert_eq!(self.state, NexusState::Init);
        self.child_count = dev_name.len() as u32;
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name, c);
                self.children.push(NexusChild::new(
                    c.clone(),
                    self.name.clone(),
                    Bdev::lookup_by_name(c),
                ))
            })
            .for_each(drop);
    }

    /// register a single child to nexus, only allowed during the nexus init
    /// phase
    pub async fn register_child(
        &mut self,
        uri: &str,
    ) -> Result<(), BdevCreateDestroy> {
        assert_eq!(self.state, NexusState::Init);
        let name = bdev_create(&uri).await?;
        self.children.push(NexusChild::new(
            uri.to_string(),
            self.name.clone(),
            Bdev::lookup_by_name(&name),
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
    pub async fn add_child(&mut self, uri: &str) -> Result<NexusState, Error> {
        let name = bdev_create(&uri).await.context(CreateChild {
            name: self.name.clone(),
        })?;

        trace!("adding child {} to nexus {}", name, self.name);

        let child_bdev = match Bdev::lookup_by_name(&name) {
            Some(child) => {
                if child.block_len() != self.bdev.block_len()
                    || self.min_num_blocks() < child.num_blocks()
                {
                    if let Err(err) = bdev_destroy(uri).await {
                        error!(
                            "Failed to destroy child bdev with wrong geometry: {}",
                            err
                        );
                    }
                    return Err(Error::ChildGeometry {
                        child: child.name(),
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

                // mark faulted so that it can never take part in the IO path of
                // the nexus until brought online.
                child.state = ChildState::Faulted;

                self.children.push(child);
                self.child_count += 1;
                self.set_state(NexusState::Degraded);

                if let Err(e) = self.sync_labels().await {
                    error!("Failed to sync labels {:?}", e);
                    // todo: how to signal this?
                }

                Ok(self.state)
            }
            Err(e) => {
                if let Err(err) = bdev_destroy(uri).await {
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

    pub async fn start_rebuild_rpc(
        &mut self,
        destination: &str,
    ) -> Result<(), Error> {
        if let Err(e) = self.start_rebuild(destination).await {
            Err(e)
        } else {
            Ok(())
        }
    }

    pub async fn start_rebuild(
        &mut self,
        destination: &str,
    ) -> Result<Receiver<RebuildState>, Error> {
        trace!("{}: start rebuild request for {}", self.name, destination);

        let source = match self
            .children
            .iter_mut()
            .find(|c| c.state == ChildState::Open)
        {
            Some(child) => child.name.clone(),
            None => {
                return Err(Error::OpenChildNotFound {
                    name: self.name.clone(),
                })
            }
        };

        if let Some(dst_child) =
            self.children.iter_mut().find(|c| c.name == destination)
        {
            self.rebuilds.push(
                RebuildTask::new(
                    self.name.clone(),
                    source,
                    destination.to_string(),
                    self.data_ent_offset,
                    self.bdev.num_blocks() + self.data_ent_offset,
                    |nexus, task| {
                        Reactors::current().send_future(async move {
                            Nexus::complete_rebuild(nexus, task).await;
                        });
                    },
                )
                .context(StartRebuild {
                    child: destination.to_string(),
                    name: self.name.clone(),
                })?,
            );

            dst_child.repairing = true;

            match self
                .rebuilds
                .iter_mut()
                .find(|t| t.destination == destination)
            {
                Some(task) => Ok(task.start()),
                None => Err(Error::CompleteRebuild {
                    child: destination.to_string(),
                    name: self.name.clone(),
                    reason: "rebuild task not found in the nexus".to_string(),
                }),
            }
        } else {
            Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: destination.to_owned(),
            })
        }
    }

    /// On rebuild task completion it updates the child state and removes the
    /// rebuild task in case of failure the child is left in a Faulted State
    async fn on_rebuild_complete(&mut self, task: String) -> Result<(), Error> {
        let task_index =
            match self.rebuilds.iter().position(|t| t.destination == task) {
                Some(task_index) => task_index,
                None => {
                    return Err(Error::CompleteRebuild {
                        child: task,
                        name: self.name.clone(),
                        reason: "rebuild task not found in the nexus"
                            .to_string(),
                    });
                }
            };

        let task = self.rebuilds.remove(task_index);

        let recovered_child = match self
            .children
            .iter_mut()
            .find(|c| c.name == task.destination)
        {
            Some(child) => child,
            None => {
                return Err(Error::CompleteRebuild {
                    child: task.destination,
                    name: self.name.clone(),
                    reason: "Missing destination child".to_string(),
                });
            }
        };

        recovered_child.repairing = false;

        if task.state == RebuildState::Completed {
            recovered_child.state = ChildState::Open;

            // child can now be part of the IO path
            self.reconfigure(DREvent::ChildOnline).await;

            // Actually we'd have to check if all other children are healthy
            // and if not maybe we can start the other rebuild's?
            self.set_state(NexusState::Online);
        } else {
            error!(
                "Rebuild task for child {} of nexus {} failed with state {:?}",
                &task.destination, &self.name, task.state
            );
        }

        Ok(())
    }

    pub async fn complete_rebuild(nexus: String, task: String) {
        info!(
            "nexus {} received complete_rebuild from task {}",
            nexus, task
        );

        let nexus = nexus_lookup(&nexus).unwrap();
        if let Err(e) = nexus.on_rebuild_complete(task).await {
            error!("{}", e);
        }
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

        let idx = match self.children.iter().position(|c| c.name == uri) {
            None => return Ok(()),
            Some(val) => val,
        };

        self.children[idx].close();
        assert_eq!(self.children[idx].state, ChildState::Closed);

        let mut child = self.children.remove(idx);
        self.child_count -= 1;
        child.destroy().await.context(DestroyChild {
            name: self.name.clone(),
            child: uri,
        })
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, Error> {
        trace!("{}: Offline child request for {}", self.name, name);

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.close();
        } else {
            return Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            });
        }

        self.reconfigure(DREvent::ChildOffline).await;
        Ok(self.set_state(NexusState::Degraded))
    }

    /// online a child and reconfigure the IO channels. The child is already
    /// registered, but simpy not opened. This can be required in case where
    /// a child is misbehaving.
    pub async fn online_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, Error> {
        trace!("{} Online child request", self.name);

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            if child.state != ChildState::Closed {
                Err(Error::ChildNotClosed {
                    name: self.name.clone(),
                    child: name.to_owned(),
                })
            } else {
                child.open(self.size).context(OpenChild {
                    child: name.to_owned(),
                    name: self.name.clone(),
                })?;
                self.reconfigure(DREvent::ChildOnline).await;
                //TODO should be rebuilding
                Ok(self.set_state(NexusState::Degraded))
            }
        } else {
            Err(Error::ChildNotFound {
                name: self.name.clone(),
                child: name.to_owned(),
            })
        }
    }
    /// destroy all children that are part of this nexus closes any child
    /// that might be open first
    pub(crate) async fn destroy_children(&mut self) {
        let futures = self.children.iter_mut().map(|c| c.destroy());
        let results = join_all(futures).await;
        if results.iter().any(|c| c.is_err()) {
            error!("{}: Failed to destroy child", self.name);
        }
    }

    /// Add a child to the configuration when an example callback is run.
    /// The nexus is not opened implicitly, call .open() for this manually.
    pub fn examine_child(&mut self, name: &str) -> bool {
        for mut c in &mut self.children {
            if c.name == name && c.state == ChildState::Init {
                if let Some(bdev) = Bdev::lookup_by_name(name) {
                    debug!("{}: Adding child {}", self.name, name);
                    c.bdev = Some(bdev);
                    return true;
                }
            }
        }
        false
    }

    /// try to open all the child devices
    pub(crate) fn try_open_children(&mut self) -> Result<(), Error> {
        if self.children.is_empty()
            || self.children.iter().any(|c| c.bdev.is_none())
        {
            return Err(Error::NexusIncomplete {
                name: self.name.clone(),
            });
        }

        let blk_size = self.children[0].bdev.as_ref().unwrap().block_len();

        if self
            .children
            .iter()
            .any(|b| b.bdev.as_ref().unwrap().block_len() != blk_size)
        {
            return Err(Error::MixedBlockSizes {
                name: self.name.clone(),
            });
        }

        self.bdev.set_block_len(blk_size);

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
            open.into_iter()
                .map(Result::unwrap)
                .map(|name| {
                    if let Some(child) =
                        self.children.iter_mut().find(|c| c.name == name)
                    {
                        let _ = child.close();
                    } else {
                        error!("{}: child {} failed to open", self.name, name);
                    }
                })
                .for_each(drop);

            return Err(Error::NexusIncomplete {
                name: self.name.clone(),
            });
        }

        self.children
            .iter()
            .map(|c| c.bdev.as_ref().unwrap().alignment())
            .collect::<Vec<_>>()
            .iter()
            .map(|s| {
                if self.bdev.alignment() < *s {
                    trace!(
                        "{}: child has alignment {}, updating required_alignment from {}",
                        self.name, *s, self.bdev.alignment()
                    );
                    unsafe {
                        (*self.bdev.as_ptr()).required_alignment = *s;
                    }
                }
            })
            .for_each(drop);
        Ok(())
    }

    /// read labels from the children devices, we fail the operation if:
    ///
    /// (1) a child does not have valid label
    /// (2) if any label does not match the label of the first child

    pub async fn update_child_labels(&mut self) -> Result<NexusLabel, Error> {
        let mut futures = Vec::new();
        self.children
            .iter_mut()
            .map(|child| futures.push(child.probe_label()))
            .for_each(drop);

        let (ok_res, mut err_res): (Vec<_>, Vec<_>) =
            join_all(futures).await.into_iter().partition(Result::is_ok);
        if let Some(Err(err)) = err_res.pop() {
            // pick the first error
            return Err(err).context(ReadLabel {
                name: self.name.clone(),
            });
        }

        let mut ret: Vec<NexusLabel> =
            ok_res.into_iter().map(Result::unwrap).collect();

        // verify that all labels are equal
        if ret.iter().skip(1).any(|e| e != &ret[0]) {
            return Err(Error::CheckLabels {
                name: self.name.clone(),
            });
        }

        Ok(ret.pop().unwrap())
    }

    /// The nexus is allowed to be smaller then the underlying child devices
    /// this function returns the smallest blockcnt of all online children as
    /// they MAY vary in size.
    pub(crate) fn min_num_blocks(&self) -> u64 {
        let mut blockcnt = std::u64::MAX;
        self.children
            .iter()
            .filter(|c| c.state == ChildState::Open)
            .map(|c| c.bdev.as_ref().unwrap().num_blocks())
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
}
