use std::{convert::TryFrom, fmt::Display, sync::Arc};

use nix::errno::Errno;
use serde::{export::Formatter, Serialize};
use snafu::{ResultExt, Snafu};

use spdk_sys::{spdk_bdev_module_release_bdev, spdk_io_channel};

use crate::{
    bdev::nexus::{
        nexus_label::{
            GPTHeader,
            GptEntry,
            LabelError,
            NexusChildLabel,
            NexusLabel,
            NexusLabelStatus,
            Unsigned,
        },
        nexus_metadata::{MetaDataError, MetaDataHeader, NexusMetaData},
        nexus_state::NexusConfig,
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
    #[snafu(display("Child is closed"))]
    ChildClosed {},
    #[snafu(display("Invalid state of child"))]
    ChildInvalid {},
    #[snafu(display("Opening child bdev without bdev pointer"))]
    OpenWithoutBdev {},
    #[snafu(display("Failed to create a BdevHandle for child"))]
    HandleCreate { source: CoreError },

    #[snafu(display("Failed to read MBR from child"))]
    MbrRead { source: ChildIoError },
    #[snafu(display("MBR is invalid"))]
    MbrInvalid { source: LabelError },

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
    #[snafu(display("Partition table is invalid"))]
    PartitionTableInvalid { source: LabelError },

    #[snafu(display("Failed to allocate buffer for metadata"))]
    MetaDataHeaderAlloc { source: DmaError },
    #[snafu(display("Failed to read metadata from child"))]
    MetaDataHeaderRead { source: ChildIoError },
    #[snafu(display("Metadata is invalid"))]
    MetaDataHeaderInvalid { source: MetaDataError },

    #[snafu(display("Failed to allocate buffer for metadata"))]
    MetaDataIndexAlloc { source: DmaError },
    #[snafu(display("Failed to read metadata from child"))]
    MetaDataIndexRead { source: ChildIoError },
    #[snafu(display("Metadata is invalid"))]
    MetaDataIndexInvalid { source: MetaDataError },

    #[snafu(display("Failed to allocate buffer for metadata"))]
    MetaDataObjectAlloc { source: DmaError },
    #[snafu(display("Failed to read metadata from child"))]
    MetaDataObjectRead { source: ChildIoError },
    #[snafu(display("Metadata is invalid"))]
    MetaDataObjectInvalid { source: MetaDataError },
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

    pub fn get_dev(&self) -> Result<(&Bdev, &BdevHandle), ChildError> {
        if !self.can_rw() {
            info!("{}: Closed child: {}", self.parent, self.name);
            return Err(ChildError::ChildClosed {});
        }

        if let Some(bdev) = &self.bdev {
            if let Some(desc) = &self.bdev_handle {
                return Ok((bdev, desc));
            }
        }

        Err(ChildError::ChildInvalid {})
    }

    /// read and validate this child's label
    pub async fn probe_label(&self) -> Result<NexusLabel, ChildError> {
        let (bdev, desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        //
        // Protective MBR
        let mut buf = desc
            .dma_malloc(block_size as usize)
            .context(LabelAlloc {})?;
        self.read_at(0, &mut buf).await.context(MbrRead {})?;
        let mbr = NexusLabel::read_mbr(&buf).context(MbrInvalid {})?;

        let status: NexusLabelStatus;
        let primary: GPTHeader;
        let secondary: GPTHeader;
        let active: &GPTHeader;

        //
        // GPT header(s)

        // Get primary.
        self.read_at(block_size, &mut buf)
            .await
            .context(LabelRead {})?;
        match NexusLabel::read_primary_header(&buf) {
            Ok(header) => {
                primary = header;
                active = &primary;
                // Get secondary.
                let offset = (bdev.num_blocks() - 1) * block_size;
                self.read_at(offset, &mut buf).await.context(LabelRead {})?;
                match NexusLabel::read_secondary_header(&buf) {
                    Ok(header) => {
                        // Both primary and secondary GPT headers are valid.
                        // Check if they are consistent with each other.
                        match NexusLabel::check_consistency(&primary, &header) {
                            Ok(()) => {
                                // All good.
                                secondary = header;
                                status = NexusLabelStatus::Both;
                            }
                            Err(error) => {
                                warn!("{}: {}: The primary and secondary GPT headers are inconsistent: {}", self.parent, self.name, error);
                                warn!("{}: {}: Recreating secondary GPT header from primary!", self.parent, self.name);
                                secondary = primary.to_backup();
                                status = NexusLabelStatus::Primary;
                            }
                        }
                    }
                    Err(error) => {
                        warn!(
                            "{}: {}: The secondary GPT header is invalid: {}",
                            self.parent, self.name, error
                        );
                        warn!("{}: {}: Recreating secondary GPT header from primary!", self.parent, self.name);
                        secondary = primary.to_backup();
                        status = NexusLabelStatus::Primary;
                    }
                }
            }
            Err(error) => {
                warn!(
                    "{}: {}: The primary GPT header is invalid: {}",
                    self.parent, self.name, error
                );
                // Get secondary and see if we are able to proceed.
                let offset = (bdev.num_blocks() - 1) * block_size;
                self.read_at(offset, &mut buf).await.context(LabelRead {})?;
                match NexusLabel::read_secondary_header(&buf) {
                    Ok(header) => {
                        secondary = header;
                        active = &secondary;
                        warn!("{}: {}: Recreating primary GPT header from secondary!", self.parent, self.name);
                        primary = secondary.to_primary();
                        status = NexusLabelStatus::Secondary;
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
            && mbr.entries[0].num_sectors as u64 != primary.lba_alt
        {
            warn!("{}: {}: The protective MBR disk size does not match the GPT disk size!", self.parent, self.name);
            return Err(ChildError::LabelInvalid {});
        }

        //
        // Partition table
        let blocks = Unsigned::get_aligned_blocks(
            (active.entry_size * active.num_entries) as u64,
            block_size,
        );
        let mut buf = desc
            .dma_malloc((blocks * block_size) as usize)
            .context(PartitionTableAlloc {})?;
        let offset = active.lba_table * block_size;
        self.read_at(offset, &mut buf)
            .await
            .context(PartitionTableRead {})?;
        let mut partitions = NexusLabel::read_partitions(&buf, active)
            .context(PartitionTableInvalid {})?;

        // Some tools always write 128 partition entries, even though most
        // are not used. In any case we are only ever interested
        // in the first two partitions, so we drain the others.
        let entries = partitions.drain(.. 2).collect::<Vec<GptEntry>>();

        Ok(NexusLabel {
            status,
            mbr,
            primary,
            partitions: entries,
            secondary,
        })
    }

    pub async fn probe_index(
        &self,
        partition_lba: u64,
    ) -> Result<NexusMetaData, ChildError> {
        let (bdev, desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        //
        // Header
        let blocks = Unsigned::get_aligned_blocks(
            MetaDataHeader::METADATA_HEADER_SIZE as u64,
            block_size,
        );
        let mut buf = desc
            .dma_malloc((blocks * block_size) as usize)
            .context(MetaDataHeaderAlloc {})?;
        self.read_at((partition_lba + 1) * block_size, &mut buf)
            .await
            .context(MetaDataHeaderRead {})?;
        let header = NexusMetaData::read_header(&buf)
            .context(MetaDataHeaderInvalid {})?;

        //
        // Index
        let index = if header.used_entries > 0 {
            let blocks = Unsigned::get_aligned_blocks(
                (header.used_entries * header.entry_size) as u64,
                block_size,
            );
            let mut buf = desc
                .dma_malloc((blocks * block_size) as usize)
                .context(MetaDataIndexAlloc {})?;
            self.read_at(
                (header.self_lba + header.index_start) * block_size,
                &mut buf,
            )
            .await
            .context(MetaDataIndexRead {})?;
            NexusMetaData::read_index(&buf, &header)
                .context(MetaDataIndexInvalid {})?
        } else {
            NexusMetaData::empty_index(&header)
                .context(MetaDataIndexInvalid {})?
        };

        Ok(NexusMetaData {
            header,
            index,
        })
    }

    pub async fn probe_config_object(
        &self,
        metadata: &NexusMetaData,
        n: u32,
    ) -> Result<NexusConfig, ChildError> {
        if n >= metadata.header.used_entries {
            return Err(ChildError::LabelInvalid {});
        }

        let entry = &metadata.index[n as usize];

        let (bdev, desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        let blocks = entry.data_end - entry.data_start + 1;
        let mut buf = desc
            .dma_malloc((blocks * block_size) as usize)
            .context(MetaDataObjectAlloc {})?;
        self.read_at(
            (metadata.header.self_lba + entry.data_start) * block_size,
            &mut buf,
        )
        .await
        .context(MetaDataObjectRead {})?;

        Ok(NexusMetaData::read_config_object(&buf, entry)
            .context(MetaDataObjectInvalid {})?)
    }

    pub async fn probe_all_config_objects(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<Vec<NexusConfig>, ChildError> {
        let (bdev, desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        let mut list: Vec<NexusConfig> = Vec::new();

        for entry in &metadata.index {
            let blocks = entry.data_end - entry.data_start + 1;
            let mut buf = desc
                .dma_malloc((blocks * block_size) as usize)
                .context(MetaDataObjectAlloc {})?;
            self.read_at(
                (metadata.header.self_lba + entry.data_start) * block_size,
                &mut buf,
            )
            .await
            .context(MetaDataObjectRead {})?;
            list.push(
                NexusMetaData::read_config_object(&buf, &entry)
                    .context(MetaDataObjectInvalid {})?,
            );
        }

        Ok(list)
    }

    /// return this child and its label
    pub async fn get_label(&self) -> NexusChildLabel<'_> {
        let label = match self.probe_label().await {
            Ok(label) => Some(label),
            Err(error) => {
                warn!(
                    "{}: {}: Error probing label: {}",
                    self.parent, self.name, error
                );
                None
            }
        };

        NexusChildLabel {
            child: self,
            label,
        }
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
