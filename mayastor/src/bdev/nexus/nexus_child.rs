use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use spdk_sys::{spdk_bdev_module_release_bdev, spdk_io_channel};

use crate::{
    bdev::nexus::nexus_label::{GPTHeader, GptEntry, NexusLabel},
    core::{Bdev, BdevHandle, CoreError, Descriptor, DmaBuf, DmaError},
    nexus_uri::{bdev_destroy, BdevCreateDestroy},
};

#[derive(Debug, Snafu)]
pub enum ChildError {
    #[snafu(display("Child is not closed"))]
    ChildNotClosed {},
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
    #[snafu(display("Child is read-only"))]
    ChildReadOnly {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Failed to allocate buffer for label"))]
    LabelAlloc { source: DmaError },
    #[snafu(display("Failed to read label from child"))]
    LabelRead { source: ChildIoError },
    #[snafu(display("Primary and backup labels are invalid"))]
    LabelInvalid {},
    #[snafu(display("Failed to allocate buffer for partition table"))]
    PartitionTableAlloc { source: DmaError },
    #[snafu(display("Failed to read partition table from child"))]
    PartitionTableRead { source: ChildIoError },
    #[snafu(display("Invalid partition table"))]
    InvalidPartitionTable {},
    #[snafu(display("Invalid partition table checksum"))]
    PartitionTableChecksum {},
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
    #[snafu(display("Failed to create a BdevHandle for child"))]
    HandleCreate { source: CoreError },
}

#[derive(Debug, Snafu)]
pub enum ChildIoError {
    #[snafu(display("Error writing to {}", name))]
    WriteError { source: CoreError, name: String },
    #[snafu(display("Error reading from {}", name))]
    ReadError { source: CoreError, name: String },
    #[snafu(display("Invalid descriptor for child bdev {}", name))]
    InvalidDescriptor { name: String },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ChildState {
    /// child has not been opened, but we are in the process of opening it
    Init,
    /// cannot add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// The child has been closed by its parent
    Closed,
    /// a non-fatal have occurred on this child
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
    /// Name of the child is the URI used to create it.
    /// Note that bdev name can differ from it!
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    /// channel on which we submit the IO
    pub(crate) ch: *mut spdk_io_channel,
    #[serde(skip_serializing)]
    pub(crate) desc: Option<Arc<Descriptor>>,
    /// current state of the child
    pub(crate) state: ChildState,
    pub(crate) repairing: bool,
    /// descriptor obtained after opening a device
    #[serde(skip_serializing)]
    pub(crate) bdev_handle: Option<BdevHandle>,
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
    /// Open the child in RW mode and claim the device to be ours. If the child
    /// is already opened by someone else (i.e one of the targets) it will
    /// error out.
    ///
    /// only devices in the closed or Init state can be opened.
    pub(crate) fn open(
        &mut self,
        parent_size: u64,
    ) -> Result<String, ChildError> {
        trace!("{}: Opening child device {}", self.parent, self.name);

        if self.state != ChildState::Closed && self.state != ChildState::Init {
            return Err(ChildError::ChildNotClosed {});
        }

        if self.bdev.is_none() {
            return Err(ChildError::OpenWithoutBdev {});
        }

        let bdev = self.bdev.as_ref().unwrap();

        let child_size = bdev.size_in_bytes();
        if parent_size > child_size {
            error!(
                "{}: child to small parent size: {} child size: {}",
                self.name, parent_size, child_size
            );
            self.state = ChildState::ConfigInvalid;
            return Err(ChildError::ChildTooSmall {
                parent_size,
                child_size,
            });
        }

        self.desc = Some(Arc::new(
            Bdev::open_by_name(&bdev.name(), true).context(OpenChild {})?,
        ));

        self.bdev_handle = Some(
            BdevHandle::try_from(self.desc.as_ref().unwrap().clone()).unwrap(),
        );

        self.state = ChildState::Open;

        debug!("{}: child {} opened successfully", self.parent, self.name);

        Ok(self.name.clone())
    }

    /// return a descriptor to this child
    pub fn get_descriptor(&self) -> Result<Arc<Descriptor>, CoreError> {
        if let Some(ref d) = self.desc {
            Ok(d.clone())
        } else {
            Err(CoreError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// close the bdev -- we have no means of determining if this succeeds
    pub(crate) fn close(&mut self) -> ChildState {
        trace!("{}: Closing child {}", self.parent, self.name);

        if let Some(bdev) = self.bdev.as_ref() {
            unsafe {
                if !(*bdev.as_ptr()).internal.claim_module.is_null() {
                    spdk_bdev_module_release_bdev(bdev.as_ptr());
                }
            }
        }

        // just to be explicit
        let hdl = self.bdev_handle.take();
        let desc = self.desc.take();
        drop(hdl);
        drop(desc);

        // we leave the child structure around for when we want reopen it
        self.state = ChildState::Closed;
        self.state
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: None,
            ch: std::ptr::null_mut(),
            state: ChildState::Init,
            bdev_handle: None,
            repairing: false,
        }
    }

    /// destroy the child bdev
    pub(crate) async fn destroy(&mut self) -> Result<(), BdevCreateDestroy> {
        assert_eq!(self.state, ChildState::Closed);
        if let Some(_bdev) = &self.bdev {
            bdev_destroy(&self.name).await
        } else {
            warn!("Destroy child without bdev");
            Ok(())
        }
    }

    /// returns if a child can be written too
    pub fn can_rw(&self) -> bool {
        self.state == ChildState::Open || self.state == ChildState::Faulted
    }

    pub async fn probe_label(&mut self) -> Result<NexusLabel, ChildError> {
        if !self.can_rw() {
            info!(
                "{}: Trying to read from closed child: {}",
                self.parent, self.name
            );
            return Err(ChildError::ChildReadOnly {});
        }

        let bdev = self.bdev.as_ref();
        let desc = self.bdev_handle.as_ref();

        if bdev.is_none() || desc.is_none() {
            return Err(ChildError::ChildInvalid {});
        }

        let bdev = bdev.unwrap();
        let desc = desc.unwrap();

        let block_size = bdev.block_len();

        let primary = u64::from(block_size);
        let secondary = bdev.num_blocks() - 1;

        let mut buf = desc
            .dma_malloc(block_size as usize)
            .context(LabelAlloc {})?;

        self.read_at(primary, &mut buf)
            .await
            .context(LabelRead {})?;

        let mut label = GPTHeader::from_slice(buf.as_slice());
        if label.is_err() {
            warn!(
                "{}: {}: The primary label is invalid!",
                self.parent, self.name
            );
            self.read_at(secondary, &mut buf)
                .await
                .context(LabelRead {})?;
            label = GPTHeader::from_slice(buf.as_slice());
        }

        let label = match label {
            Ok(label) => label,
            Err(_) => return Err(ChildError::LabelInvalid {}),
        };

        // determine number of blocks we need to read from the partition table
        let num_blocks =
            ((label.entry_size * label.num_entries) / block_size) + 1;

        let mut buf = desc
            .dma_malloc((num_blocks * block_size) as usize)
            .context(PartitionTableAlloc {})?;

        self.read_at(label.lba_table * u64::from(block_size), &mut buf)
            .await
            .context(PartitionTableRead {})?;

        let mut partitions =
            match GptEntry::from_slice(&buf.as_slice(), label.num_entries) {
                Ok(parts) => parts,
                Err(_) => return Err(ChildError::InvalidPartitionTable {}),
            };

        if GptEntry::checksum(&partitions) != label.table_crc {
            return Err(ChildError::PartitionTableChecksum {});
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

    /// write the contents of the buffer to this child
    pub async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, ChildIoError> {
        if let Some(desc) = self.bdev_handle.as_ref() {
            Ok(desc.write_at(offset, buf).await.context(WriteError {
                name: self.name.clone(),
            })?)
        } else {
            Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }

    /// read from this child device into the given buffer
    pub async fn read_at(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<usize, ChildIoError> {
        if let Some(desc) = self.bdev_handle.as_ref() {
            Ok(desc.read_at(offset, buf).await.context(ReadError {
                name: self.name.clone(),
            })?)
        } else {
            Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            })
        }
    }
}
