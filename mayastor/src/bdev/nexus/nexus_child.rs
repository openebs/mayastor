use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use spdk_sys::{spdk_bdev_module_release_bdev, spdk_io_channel};

use crate::{
    bdev::nexus::nexus_label::{
        GPTHeader,
        GptEntry,
        LabelData,
        NexusLabel,
        Pmbr,
    },
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
    #[snafu(display("Label is invalid"))]
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
        trace!("destroying child {:?}", self);
        assert_eq!(self.state, ChildState::Closed);
        if let Some(_bdev) = &self.bdev {
            bdev_destroy(&self.name).await
        } else {
            warn!("Destroy child without bdev");
            Ok(())
        }
    }

    /// returns if a child can be written to
    pub fn can_rw(&self) -> bool {
        self.state == ChildState::Open || self.state == ChildState::Faulted
    }

    /// read and validate this child's label
    pub async fn probe_label(&mut self) -> Result<NexusLabel, ChildError> {
        if !self.can_rw() {
            info!(
                "{}: Trying to read from closed child: {}",
                self.parent, self.name
            );
            return Err(ChildError::ChildReadOnly {});
        }

        let bdev = match self.bdev.as_ref() {
            Some(bdev) => bdev,
            None => {
                return Err(ChildError::ChildInvalid {});
            }
        };

        let desc = match self.bdev_handle.as_ref() {
            Some(desc) => desc,
            None => {
                return Err(ChildError::ChildInvalid {});
            }
        };

        let block_size = bdev.block_len();

        //
        // Protective MBR
        let mut buf = desc
            .dma_malloc(block_size as usize)
            .context(LabelAlloc {})?;
        let mbr = self.read_mbr(&mut buf).await?;

        let mut header: Vec<GPTHeader> = Vec::with_capacity(2);
        let active: GPTHeader;

        //
        // GPT header(s)
        match self.read_header(u64::from(block_size), &mut buf).await {
            Ok(primary) => {
                active = primary;
                header.push(primary);
                match self
                    .read_header(
                        (bdev.num_blocks() - 1) * u64::from(block_size),
                        &mut buf,
                    )
                    .await
                {
                    Ok(secondary) => {
                        header.push(secondary);
                        self.check_consistency(&primary, &secondary)?;
                    }
                    Err(error) => {
                        // The secondary GPT header is invalid.
                        // Issue a warning but otherwise carry on.
                        warn!(
                            "{}: {}: The secondary GPT header is invalid: {}",
                            self.parent, self.name, error
                        );
                        // Recreate secondary from primary.
                        header.push(primary.to_backup());
                    }
                }
            }
            Err(error) => {
                warn!(
                    "{}: {}: The primary GPT header is invalid: {}",
                    self.parent, self.name, error
                );
                // The primary GPT header is invalid.
                // Try the secondary and see if we are able to proceed.
                match self
                    .read_header(
                        (bdev.num_blocks() - 1) * u64::from(block_size),
                        &mut buf,
                    )
                    .await
                {
                    Ok(secondary) => {
                        warn!("{}: {}: Recreating primary GPT header from secondary!", self.parent, self.name);
                        active = secondary;
                        // Recreate primary from secondary.
                        header.push(secondary.to_primary());
                        header.push(secondary);
                    }
                    Err(error) => {
                        warn!(
                            "{}: {}: The secondary GPT header is invalid: {}",
                            self.parent, self.name, error
                        );
                        warn!("{}: {}: Both primary and secondary GPT headers are invalid!", self.parent, self.name);
                        return Err(ChildError::LabelInvalid {});
                    }
                }
            }
        }

        if mbr.entries[0].num_sectors != 0xffff_ffff
            && mbr.entries[0].num_sectors as u64 != header[0].lba_alt
        {
            warn!("{}: {}: The protective MBR disk size does not match the GPT disk size!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        //
        // Partition table
        let mut buf = desc
            .dma_malloc(NexusChild::get_size(
                active.entry_size * active.num_entries,
                block_size,
            ) as usize)
            .context(PartitionTableAlloc {})?;
        let mut partitions = self
            .read_partitions(u64::from(block_size), &mut buf, &active)
            .await?;

        header[0].checksum();
        header[1].checksum();

        // Some tools always write 128 partition entries, even though most
        // are not used. In any case we are only ever interested
        // in the first two partitions, so we drain the others.
        let entries = partitions.drain(.. 2).collect::<Vec<GptEntry>>();

        Ok(NexusLabel {
            mbr,
            primary: header[0],
            partitions: entries,
            secondary: header[1],
        })
    }

    fn get_size(size: u32, block_size: u32) -> u32 {
        match size % block_size {
            0 => size,
            n => size + block_size - n,
        }
    }

    async fn read_mbr(&self, buf: &mut DmaBuf) -> Result<Pmbr, ChildError> {
        self.read_at(0, buf).await.context(LabelRead {})?;
        match Pmbr::from_slice(&buf.as_slice()[440 .. 512]) {
            Ok(mbr) => Ok(mbr),
            Err(error) => {
                warn!(
                    "{}: {}: The protective MBR is invalid: {}",
                    self.parent, self.name, error
                );
                Err(ChildError::LabelInvalid {})
            }
        }
    }

    async fn read_header(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<GPTHeader, ChildError> {
        self.read_at(offset, buf).await.context(LabelRead {})?;
        match GPTHeader::from_slice(buf.as_slice()) {
            Ok(header) => Ok(header),
            Err(_) => Err(ChildError::LabelInvalid {}),
        }
    }

    async fn read_partitions(
        &self,
        block_size: u64,
        buf: &mut DmaBuf,
        header: &GPTHeader,
    ) -> Result<Vec<GptEntry>, ChildError> {
        self.read_at(header.lba_table * block_size, buf)
            .await
            .context(PartitionTableRead {})?;
        match GptEntry::from_slice(&buf.as_slice(), header.num_entries) {
            Ok(partitions) => {
                if GptEntry::checksum(&partitions) != header.table_crc {
                    warn!("{}: {}: The calculated and stored partition table checksums differ!", self.parent, self.name);
                    return Err(ChildError::PartitionTableChecksum {});
                }
                Ok(partitions)
            }
            Err(error) => {
                warn!(
                    "{}: {}: The partition table is invalid: {}",
                    self.parent, self.name, error
                );
                Err(ChildError::InvalidPartitionTable {})
            }
        }
    }

    fn check_consistency(
        &self,
        primary: &GPTHeader,
        secondary: &GPTHeader,
    ) -> Result<(), ChildError> {
        if primary.guid != secondary.guid {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: GUIDs differ!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }
        if primary.lba_start != secondary.lba_start
            || primary.lba_end != secondary.lba_end
        {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: disk sizes differ!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }
        if primary.table_crc != secondary.table_crc {
            warn!("{}: {}: The primary and secondary GPT headers are inconsistent: stored partition table checksums differ!", self.parent, self.name);
            return Err(ChildError::InvalidPartitionTable {});
        }
        Ok(())
    }

    /// write both (primary and secondary) labels to this child
    pub async fn write_labels(
        &self,
        primary: &LabelData,
        secondary: &LabelData,
    ) -> Result<(), ChildIoError> {
        self.write_at(primary.offset, &primary.buf).await?;
        self.write_at(secondary.offset, &secondary.buf).await?;
        Ok(())
    }

    /// write a single (primary or secondary) label to this child
    pub async fn write_label(
        &self,
        label: &LabelData,
    ) -> Result<(), ChildIoError> {
        self.write_at(label.offset, &label.buf).await?;
        Ok(())
    }

    /// write the contents of the buffer to this child
    pub async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, ChildIoError> {
        match self.bdev_handle.as_ref() {
            Some(desc) => {
                Ok(desc.write_at(offset, buf).await.context(WriteError {
                    name: self.name.clone(),
                })?)
            }
            None => Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            }),
        }
    }

    /// read from this child device into the given buffer
    pub async fn read_at(
        &self,
        offset: u64,
        buf: &mut DmaBuf,
    ) -> Result<usize, ChildIoError> {
        match self.bdev_handle.as_ref() {
            Some(desc) => {
                Ok(desc.read_at(offset, buf).await.context(ReadError {
                    name: self.name.clone(),
                })?)
            }
            None => Err(ChildIoError::InvalidDescriptor {
                name: self.name.clone(),
            }),
        }
    }
}
