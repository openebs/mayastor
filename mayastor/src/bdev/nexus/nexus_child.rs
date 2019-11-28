use std::{fmt::Display, ops::Neg};

use serde::{export::Formatter, Serialize};

use spdk_sys::{
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_get_io_channel,
    spdk_bdev_module_release_bdev,
    spdk_io_channel,
};

use crate::{
    bdev::{
        nexus::{
            self,
            nexus_label::{GPTHeader, GptEntry, NexusLabel},
            nexus_module::NEXUS_MODULE,
            Error,
        },
        Bdev,
    },
    descriptor::{Descriptor, DmaBuf},
    nexus_uri::{nexus_parse_uri, BdevType},
};

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ChildState {
    /// child has not been opened but we are in the process of opening it
    Init,
    /// can not add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// The child has been closed by its parent
    Closed,
    /// a fatal error (or errors) have occurred on this child
    Faulted,
}

impl ToString for ChildState {
    fn to_string(&self) -> String {
        match *self {
            ChildState::Init => "init",
            ChildState::ConfigInvalid => "configInvalid",
            ChildState::Open => "open",
            ChildState::Faulted => "faulted",
            ChildState::Closed => "closed",
        }
        .parse()
        .unwrap()
    }
}

#[derive(Debug, Serialize)]
pub struct NexusChild {
    /// name of the parent this child belongs too
    pub(crate) parent: String,
    ///  name of the child itself
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    /// descriptor obtained after opening a device
    pub(crate) desc: *mut spdk_bdev_desc,
    #[serde(skip_serializing)]
    /// channel on which we submit the IO
    pub(crate) ch: *mut spdk_io_channel,
    /// current state of the child
    pub(crate) state: ChildState,
    #[serde(skip_serializing)]
    pub(crate) descriptor: Option<Descriptor>,
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        if self.bdev.is_some() {
            let bdev = self.bdev.as_ref().unwrap();
            writeln!(
                f,
                "{}: {:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state,
                bdev.num_blocks(),
                bdev.block_len(),
            )
        } else {
            writeln!(f, "{}: state {:?}", self.name, self.state)
        }
    }
}

impl NexusChild {
    /// open the child bdev, assumes RW
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
    ) -> Result<String, nexus::Error> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        if let Some(bdev) = self.bdev.as_ref() {
            let child_size = bdev.size_in_bytes();
            if parent_size > child_size {
                error!(
                    "{}: child {} to small  ({} vs {})",
                    self.parent, self.name, parent_size, child_size,
                );

                self.state = ChildState::ConfigInvalid;
                return Err(nexus::Error::Invalid(
                    "requested nexus size is larger than some of its children"
                        .into(),
                ));
            }

            let mut rc = unsafe {
                spdk_sys::spdk_bdev_open(
                    bdev.inner,
                    true,
                    None,
                    std::ptr::null_mut(),
                    &mut self.desc,
                )
            };

            if rc != 0 {
                error!("{}: Failed to open child {}", self.parent, self.name);
                self.state = ChildState::Faulted;
                self.desc = std::ptr::null_mut();
                return Err(match rc.neg() {
                    libc::EPERM => nexus::Error::ReadOnly,
                    libc::ENOTSUP => nexus::Error::InvalidThread,
                    _ => nexus::Error::from(rc),
                });
            }

            rc = unsafe {
                spdk_sys::spdk_bdev_module_claim_bdev(
                    bdev.inner,
                    std::ptr::null_mut(),
                    &NEXUS_MODULE.module as *const _ as *mut _,
                )
            };

            if rc != 0 {
                self.state = ChildState::Faulted;
                error!("{}: Failed to claim device {}", self.parent, self.name);
                unsafe { spdk_bdev_close(self.desc) }
                self.desc = std::ptr::null_mut();
                return Err(match rc.neg() {
                    libc::EPERM => nexus::Error::AlreadyClaimed,
                    _ => nexus::Error::from(rc),
                });
            }

            self.state = ChildState::Open;

            // used for internal IOS like updating labels
            self.descriptor = Some(Descriptor {
                desc: self.desc,
                ch: self.get_io_channel(),
            });

            debug!("{}: child {} opened successfully", self.parent, self.name);

            Ok(self.name.clone())
        } else {
            Err(Error::NexusIncomplete)
        }
    }

    /// close the bdev -- we have no means of determining if this succeeds
    pub(crate) fn close(&mut self) -> Result<ChildState, nexus::Error> {
        trace!("{}: Closing child {}", self.parent, self.name);
        if self.state == ChildState::Faulted {
            assert_eq!(self.desc, std::ptr::null_mut());
            self.clear();
            return Ok(self.state);
        }

        if let Some(bdev) = self.bdev.as_ref() {
            unsafe {
                spdk_bdev_module_release_bdev(bdev.inner);
            }
        }

        let _ = self.descriptor.take();
        // we leave the bdev structure around for when we want reopen it
        self.state = ChildState::Closed;
        Ok(self.state)
    }

    pub(crate) fn clear(&mut self) -> bool {
        self.state = ChildState::Init;
        true
    }

    pub(crate) fn get_io_channel(&self) -> *mut spdk_sys::spdk_io_channel {
        assert_eq!(self.state, ChildState::Open);
        unsafe { spdk_bdev_get_io_channel(self.desc) }
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: std::ptr::null_mut(),
            ch: std::ptr::null_mut(),
            state: ChildState::Init,
            descriptor: None,
        }
    }

    /// destroy the child bdev
    pub(crate) async fn destroy(&mut self) -> Result<(), nexus::Error> {
        if self.state == ChildState::Open {
            self.close().unwrap();
        }

        if let Ok(child_type) = nexus_parse_uri(&self.name) {
            match child_type {
                BdevType::Aio(args) => args.destroy().await,
                BdevType::Iscsi(args) => args.destroy().await,
                BdevType::Nvmf(args) => args.destroy(),
                BdevType::Bdev(_name) => Ok(()),
            }
        } else {
            // a bdev type we dont support is being used by the nexus
            Err(nexus::Error::Invalid(format!(
                "requested bdev: {} type is not supported by the nexus",
                self.name
            )))
        }
    }

    pub fn can_rw(&self) -> bool {
        self.state == ChildState::Open
    }

    pub async fn probe_label(&mut self) -> Result<NexusLabel, nexus::Error> {
        if !self.can_rw() {
            info!(
                "{}: Trying to read from closed child: {}",
                self.parent, self.name
            );
            // TODO add better errors
            return Err(nexus::Error::Invalid(format!(
                "{}: child {} is read only",
                self.parent, self.name
            )));
        }

        let bdev = self.bdev.as_ref();
        let desc = self.descriptor.as_ref();

        if bdev.is_none() || desc.is_none() {
            return Err(Error::Invalid("Bdev is invalid".into()));
        }

        let bdev = bdev.unwrap();
        let desc = desc.unwrap();

        let block_size = bdev.block_len();

        let primary = u64::from(block_size);
        let secondary = bdev.num_blocks() - 1;

        let mut buf = desc.dma_malloc(block_size as usize)?;

        self.read_at(primary, &mut buf).await?;
        let mut label = GPTHeader::from_slice(buf.as_slice());

        if label.is_err() {
            info!("{}: {}: Primary label is invalid!", self.parent, self.name);
            self.read_at(secondary, &mut buf).await?;
            label = GPTHeader::from_slice(buf.as_slice());
        }

        if label.is_err() {
            info!(
                "{}: {}: Primary and backup label are invalid!",
                self.parent, self.name
            );
            return Err(Error::Invalid(format!(
                "{}: {}: Primary and backup label are invalid!",
                self.parent, self.name
            )));
        }

        let label = label.unwrap();

        // determine number of blocks we need to read for the partition table
        let num_blocks =
            ((label.entry_size * label.num_entries) / block_size) + 1;

        let mut buf = desc.dma_malloc((num_blocks * block_size) as usize)?;

        self.read_at(label.lba_table * u64::from(block_size), &mut buf)
            .await?;

        let mut partitions =
            GptEntry::from_slice(&buf.as_slice(), label.num_entries)?;

        if GptEntry::checksum(&partitions) != label.table_crc {
            info!("{}: {}: Partition crc invalid!", self.parent, self.name);
            return Err(Error::Invalid(format!(
                "{}: {}: Partition crc invalid!",
                self.parent, self.name
            )));
        }

        // some tools write 128 partition entries, even though only two are
        // created, in any case we are only ever interested in the first two
        // partitions, so we drain the others.
        let parts = partitions.drain(.. 2).collect::<Vec<_>>();

        let nl = NexusLabel {
            primary: label,
            partitions: parts,
        };

        Ok(nl)
    }

    /// write the contents of the buffer to  this child
    pub async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, Error> {
        if let Some(desc) = self.descriptor.as_ref() {
            Ok(desc.write_at(offset, buf).await?)
        } else {
            Err(Error::Internal("Invalid descriptor for bdev".into()))
        }
    }

    /// read from this child device into the given buffer
    pub async fn read_at(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<usize, Error> {
        if let Some(desc) = self.descriptor.as_ref() {
            Ok(desc.read_at(offset, buf).await?)
        } else {
            Err(Error::Internal("Invalid descriptor for bdev".into()))
        }
    }

    /// get a dma buffer that is aligned to this child
    pub fn get_buf(&self, size: usize) -> Option<DmaBuf> {
        match self.descriptor.as_ref() {
            Some(descriptor) => match descriptor.dma_malloc(size) {
                Ok(buf) => Some(buf),
                Err(..) => None,
            },
            None => None,
        }
    }
}
