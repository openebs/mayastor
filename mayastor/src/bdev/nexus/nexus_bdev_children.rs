use crate::bdev::{
    bdev_lookup_by_name,
    nexus::{self, nexus_child::NexusChild, Error},
};

use crate::descriptor::IoChannel;

use crate::nexus_uri::BdevType;

use spdk_sys::{spdk_get_io_channel, spdk_put_io_channel};

use crate::bdev::nexus::{
    nexus_bdev::{Nexus, NexusState},
    nexus_channel::DREvent,
    nexus_child::ChildState,
    nexus_label::NexusLabel,
    nexus_uri::nexus_parse_uri,
};
use futures::future::join_all;

impl Nexus {
    fn hold_io_channel(&self) -> IoChannel {
        unsafe { IoChannel::new(self.as_ptr()) }
    }

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
    pub async fn create_and_add_child(
        &mut self,
        uri: &str,
    ) -> Result<String, Error> {
        let bdev_type = nexus_parse_uri(uri)?;

        // workaround until we can get async fn trait
        let name = match bdev_type {
            BdevType::Aio(args) => args.create().await?,
            BdevType::Iscsi(args) => args.create().await?,
            BdevType::Nvmf(args) => args.create().await?,
            BdevType::Bdev(name) => name,
        };

        self.children.push(NexusChild::new(
            uri.to_string(),
            self.name.clone(),
            bdev_lookup_by_name(&name),
        ));

        self.child_count += 1;

        Ok(name)
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
            {
                let _ch = self.hold_io_channel();
                self.reconfigure(DREvent::ChildOffline).await;
            }
            self.set_state(NexusState::Degraded);
            Ok(NexusState::Degraded)
        } else {
            Err(Error::NotFound)
        }
    }

    /// online a child and reconfigure the IO channels
    pub async fn online_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{} Online child request", self.name());

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.open(self.size)?;

            // TODO we need to get a reference to a channel before we can
            // process it would be nice to abstract this like
            // self.channel_{hold/release}

            let ch = unsafe { spdk_get_io_channel(self.as_ptr()) };
            self.reconfigure(DREvent::ChildOnline).await;
            unsafe { spdk_put_io_channel(ch) };
            if self.is_healthy() {
                self.set_state(NexusState::Online);
                Ok(NexusState::Online)
            } else {
                Ok(NexusState::Degraded)
            }
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
