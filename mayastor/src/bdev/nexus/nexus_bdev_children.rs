use futures::future::join_all;

use crate::bdev::{
    bdev_lookup_by_name,
    nexus::{
        self,
        nexus_bdev::{bdev_create, bdev_destroy, Nexus, NexusState},
        nexus_channel::DREvent,
        nexus_child::{ChildState, NexusChild},
        nexus_label::NexusLabel,
        Error,
    },
};

impl Nexus {
    /// Add the child bdevs to the nexus instance in the "init state"
    /// this function should be used when bdevs are added asynchronously
    /// like for example, when parsing the init file. The examine callback
    /// will iterate through the list and invoke nexus::online once completed

    pub fn add_children(&mut self, dev_name: &[String]) {
        self.child_count = dev_name.len() as u32;
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name(), c);
                self.children.push(NexusChild::new(
                    c.clone(),
                    self.name.clone(),
                    bdev_lookup_by_name(c),
                ))
            })
            .for_each(drop);
    }

    /// create a bdev based on its URL and add it to the nexus
    pub async fn register_child(&mut self, uri: &str) -> Result<String, Error> {
        let name = bdev_create(&uri).await?;
        self.children.push(NexusChild::new(
            uri.to_string(),
            self.name.clone(),
            bdev_lookup_by_name(&name),
        ));

        self.child_count += 1;

        Ok(name)
    }

    pub async fn add_child(&mut self, uri: &str) -> Result<String, Error> {
        let name = bdev_create(&uri).await?;

        if let Some(child) = bdev_lookup_by_name(&name) {
            if child.block_len() != self.bdev.block_len()
                || self.min_num_blocks() < child.num_blocks()
            {
                error!(
                    ":{} child {} has invalid geometry",
                    self.name,
                    child.name()
                );
                bdev_destroy(uri).await?;
            }
        }

        trace!("adding child {} to nexus {}", name, self.name);

        let child = bdev_lookup_by_name(&name);
        if child.is_none() {
            error!(":{} child should be there but its not!", self.name);
            return Err(Error::Internal("child does not exist".into()));
        };

        let mut child = NexusChild::new(name, self.name.clone(), child);

        match child.open(self.size) {
            Ok(name) => {
                info!(":{} child opened successfully {}", self.name, name);
                self.children.push(child);
                self.child_count += 1;
                self.sync_labels().await?;
            }
            Err(_) => {
                error!("{}: failed to open child ", self.name);
                bdev_destroy(&uri).await?;
            }
        }

        Ok(uri.into())
    }

    /// Destroy child with given uri.
    /// If the child does not exist the method returns success.
    pub async fn destroy_child(&mut self, uri: &str) -> Result<(), Error> {
        let idx = match self.children.iter().position(|c| c.name == uri) {
            None => return Ok(()),
            Some(val) => val,
        };
        let mut child = self.children.remove(idx);
        child.destroy().await?;
        self.child_count -= 1;
        Ok(())
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{}: Offline child request for {}", self.name(), name);
        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.close()?;
        } else {
            return Err(Error::NotFound);
        }

        self.reconfigure(DREvent::ChildOffline).await;
        Ok(self.set_state(NexusState::Degraded))
    }

    /// online a child and reconfigure the IO channels
    pub async fn online_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{} Online child request", self.name());

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.open(self.size)?;
            self.reconfigure(DREvent::ChildOnline).await;
            if self.is_healthy() {
                Ok(self.set_state(NexusState::Online))
            } else {
                Ok(NexusState::Degraded)
            }
        } else {
            Err(Error::NotFound)
        }
    }

    /// fault a child and reconfigure the IO channels
    pub async fn fault_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{} fault child request", self.name());

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.state = ChildState::Faulted;
            self.reconfigure(DREvent::ChildFault).await;
            Ok(self.set_state(NexusState::Degraded))
        } else {
            Err(Error::NotFound)
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
                if let Some(bdev) = bdev_lookup_by_name(name) {
                    debug!("{}: Adding child {}", self.name, name);
                    c.bdev = Some(bdev);
                    return true;
                }
            }
        }
        false
    }

    /// try to open all the child devices
    pub(crate) fn try_open_children(&mut self) -> Result<(), nexus::Error> {
        if self.children.is_empty()
            || self.children.iter().any(|c| c.bdev.is_none())
        {
            debug!("{}: config incomplete deferring open", self.name);
            return Err(Error::NexusIncomplete);
        }

        let blk_size = self.children[0].bdev.as_ref().unwrap().block_len();

        if self
            .children
            .iter()
            .any(|b| b.bdev.as_ref().unwrap().block_len() != blk_size)
        {
            error!("{}: children have mixed block sizes", self.name);
            return Err(Error::Invalid(
                "children have mixed block sizes".into(),
            ));
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
                        error!("{}: child opened but found!", self.name());
                    }
                })
                .for_each(drop);

            return Err(Error::NexusIncomplete);
        }

        self.children
            .iter()
            .map(|c| c.bdev.as_ref().unwrap().alignment())
            .collect::<Vec<_>>()
            .iter()
            .map(|s| {
                if self.bdev.alignment() < *s {
                    unsafe {
                        (*self.bdev.inner).required_alignment = *s;
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

        let (ret, err): (Vec<_>, Vec<_>) =
            join_all(futures).await.into_iter().partition(Result::is_ok);
        if !err.is_empty() {
            return Err(Error::Internal(
                "failed to probe all child labels".into(),
            ));
        }

        let mut ret: Vec<NexusLabel> =
            ret.into_iter().map(Result::unwrap).collect();

        // verify that all labels are equal
        if ret.iter().skip(1).any(|e| e != &ret[0]) {
            return Err(Error::Invalid("GPT labels differ".into()));
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
