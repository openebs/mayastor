//! Support for manipulating nexus "metadata".
//! Simple API for adding and retrieving data from the "MayaMeta" partition.
//! The raw disk partition is accessed directly - there is no filesystem
//! present.
//!
//! The data layout is as follows:
//!  - The first block of the partition is not used.
//!  - The second block contains the MetaDataIndex (currently 80 bytes) while
//!    the remainder of the block is padded with zeros.
//!  - The data in the "index" starts at the third block.
//!  - This and each subsequent block contains a single MetaDataObject entry.
//!  - The size of a MetaDataObject entry therefore cannot exceed the block_size
//!    of the device.
//!  - Once the index is "full", when a new entry is added, the oldest entry is
//!    (silently) removed in order to make space for the new entry.
//!
//! ## Example
//!
//! Sample code to create a new index containing 32 entries
//! on a specified child:
//!
//!    let child = nexus.children[0];
//!    let mut index = MetaDataIndex::new(
//!        nexus_guid,
//!        child_guid,
//!        child.metadata_index_lba,
//!        32,
//!    );
//!    let now = SystemTime::now();
//!    NexusMetaData::create_index(&child, &mut index, &now).await?;
//!
//! Sample code to add a new MetaDataObject entry to a child's index:
//!
//!    let now = SystemTime::now();
//!    let mut object = MetaDataObject::new();
//!    object.children.push(MetaDataChildEntry {
//!        guid: Guid::new_random(),
//!        state: 0,
//!    });
//!    object.generation = 1;
//!    object.timestamp = NexusMetaData::timestamp(&now);
//!    NexusMetaData::add(&child, &mut object, &now).await?;
//!
//! And code to retrieve the latest MetaDataObject entry from a child's index:
//!
//!    let object = NexusMetaData::last(&child).await?;

use std::{
    cmp::min,
    io::Cursor,
    mem::size_of,
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{deserialize_from, serialize, serialize_into, Error};

use crc::{crc32, Hasher32};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::{
    bdev::nexus::{
        nexus_child::NexusChild,
        nexus_label::{GptGuid as Guid, NexusLabel},
    },
    core::{CoreError, DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum MetaDataError {
    #[snafu(display("Serialization error: {}", source))]
    SerializeError { source: Error },
    #[snafu(display("Deserialization error: {}", source))]
    DeserializeError { source: Error },
    #[snafu(display(
        "Failed to allocate buffer for reading {}: {}",
        name,
        source
    ))]
    ReadAlloc { source: DmaError, name: String },
    #[snafu(display(
        "Failed to allocate buffer for writing {}: {}",
        name,
        source
    ))]
    WriteAlloc { source: DmaError, name: String },
    #[snafu(display("Error reading {}: {}", name, source))]
    ReadError { source: CoreError, name: String },
    #[snafu(display("Error writing {}: {}", name, source))]
    WriteError { source: CoreError, name: String },
    #[snafu(display(
        "Failed to obtain BdevHandle for child {}: {}",
        name,
        source
    ))]
    HandleError { source: CoreError, name: String },
    #[snafu(display("Incorrect MetaDataObject signature"))]
    ObjectSignature {},
    #[snafu(display("Incorrect MetaDataObject checksum"))]
    ObjectChecksum {},
    #[snafu(display("Incorrect child table checksum"))]
    ChildTableChecksum {},
    #[snafu(display("Maximum number of children exceeded"))]
    ChildTableSize {},
    #[snafu(display("Incorrect MetaDataIndex signature"))]
    IndexSignature {},
    #[snafu(display("Incorrect MetaDataIndex size"))]
    IndexSize {},
    #[snafu(display("Incorrect MetaDataIndex checksum"))]
    IndexChecksum {},
    #[snafu(display("Incorrect MetaDataIndex self address"))]
    IndexSelfAddress {},
    #[snafu(display("GUID does not match MetaDataIndex"))]
    IndexGuid {},
    #[snafu(display("MetaDataIndex address is not set for child {}", name))]
    IndexAddressNotSet { name: String },
    #[snafu(display("MetaDataIndex not found"))]
    MissingIndex {},
    #[snafu(display("Missing partition: {}", name))]
    MissingPartition { name: String },
}

#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MetaDataChildEntry {
    pub guid: Guid,
    pub state: u16,
}

impl MetaDataChildEntry {
    /// Calculate checksum over the child entries
    pub fn checksum(children: &[MetaDataChildEntry]) -> Result<u32, Error> {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        let count = children.len() as u32;
        for child in children {
            digest.write(&serialize(child)?);
        }
        if count < MetaDataObject::MAX_CHILD_ENTRIES {
            let pad = serialize(&MetaDataChildEntry::default())?;
            for _ in count .. MetaDataObject::MAX_CHILD_ENTRIES {
                digest.write(&pad);
            }
        }
        Ok(digest.sum32())
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct MetaDataHeader {
    signature: [u8; 8],
    self_checksum: u32,
    generation: u64,
    timestamp: u128,
    table_crc: u32,
    count: u32,
}

impl MetaDataHeader {
    /// Checksum the header with the checksum field itself set to 0
    fn checksum(&mut self) -> Result<u32, Error> {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(&self)?);
        Ok(self.self_checksum)
    }
}

#[derive(Debug, PartialEq)]
pub struct MetaDataObject {
    pub signature: [u8; 8],
    pub self_checksum: u32,
    pub generation: u64,
    pub timestamp: u128,
    pub table_crc: u32,
    pub children: Vec<MetaDataChildEntry>,
}

impl MetaDataObject {
    /// Observe that sizeof(MetaDataHeader) + 26 * sizeof(MetaDataChildEntry) is
    /// exactly 512. While it may be smaller, the value of this expression
    /// must NEVER exceed 512.
    pub const MAX_CHILD_ENTRIES: u32 = 26;
    pub const OBJECT_SIGNATURE: [u8; 8] =
        [0x4d, 0x61, 0x79, 0x61, 0x44, 0x61, 0x74, 0x61];

    /// Convert a slice into a MetaDataObject and validate
    fn from_slice(slice: &[u8]) -> Result<MetaDataObject, MetaDataError> {
        let mut reader = Cursor::new(slice);
        let mut header: MetaDataHeader =
            deserialize_from(&mut reader).context(DeserializeError {})?;

        if header.signature != MetaDataObject::OBJECT_SIGNATURE {
            return Err(MetaDataError::ObjectSignature {});
        }

        if header.count > MetaDataObject::MAX_CHILD_ENTRIES {
            return Err(MetaDataError::ChildTableSize {});
        }

        let mut children: Vec<MetaDataChildEntry> =
            Vec::with_capacity(header.count as usize);

        for _ in 0 .. header.count {
            children.push(
                deserialize_from(&mut reader).context(DeserializeError {})?,
            );
        }

        if header.table_crc
            != MetaDataChildEntry::checksum(&children)
                .context(SerializeError {})?
        {
            return Err(MetaDataError::ChildTableChecksum {});
        }

        let checksum = header.self_checksum;

        if checksum != header.checksum().context(SerializeError {})? {
            return Err(MetaDataError::ObjectChecksum {});
        }

        let object = MetaDataObject {
            signature: header.signature,
            self_checksum: header.self_checksum,
            generation: header.generation,
            timestamp: header.timestamp,
            table_crc: header.table_crc,
            children,
        };

        Ok(object)
    }

    pub fn checksum(&mut self) -> Result<u32, Error> {
        let mut header = MetaDataHeader::from(&*self);
        self.self_checksum = header.checksum()?;
        Ok(self.self_checksum)
    }

    /// Perform basic checks to ensure object is valid,
    /// and then calculate checksums.
    pub fn validate(&mut self) -> Result<(), MetaDataError> {
        if self.signature != MetaDataObject::OBJECT_SIGNATURE {
            return Err(MetaDataError::ObjectSignature {});
        }

        if self.children.len() as u32 > MetaDataObject::MAX_CHILD_ENTRIES {
            return Err(MetaDataError::ChildTableSize {});
        }

        self.table_crc = MetaDataChildEntry::checksum(&self.children)
            .context(SerializeError {})?;
        self.checksum().context(SerializeError {})?;

        Ok(())
    }

    pub fn new() -> MetaDataObject {
        MetaDataObject {
            signature: MetaDataObject::OBJECT_SIGNATURE,
            self_checksum: 0,
            generation: 0,
            timestamp: 0,
            table_crc: 0,
            children: Vec::new(),
        }
    }
}

impl Default for MetaDataObject {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&MetaDataObject> for MetaDataHeader {
    fn from(object: &MetaDataObject) -> MetaDataHeader {
        MetaDataHeader {
            signature: object.signature,
            self_checksum: object.self_checksum,
            generation: object.generation,
            timestamp: object.timestamp,
            table_crc: object.table_crc,
            count: object.children.len() as u32,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetaDataIndex {
    pub signature: [u8; 8],
    pub index_size: u32,
    pub self_checksum: u32,
    pub parent: Guid,
    pub guid: Guid,
    pub generation: u64,
    pub timestamp: u128,
    pub self_lba: u64,
    pub start_lba: u64,
    pub current_entry: u64,
    pub used_entries: u64,
    pub total_entries: u64,
}

impl MetaDataIndex {
    pub const METADATA_INDEX_SIZE: u32 = size_of::<MetaDataIndex>() as u32;
    pub const INDEX_SIGNATURE: [u8; 8] =
        [0x4d, 0x61, 0x79, 0x61, 0x44, 0x61, 0x74, 0x61];

    /// Convert a slice into a MetaDataIndex and validate
    pub fn from_slice(slice: &[u8]) -> Result<MetaDataIndex, MetaDataError> {
        let mut index: MetaDataIndex =
            deserialize_from(&mut Cursor::new(slice))
                .context(DeserializeError {})?;

        if index.signature != MetaDataIndex::INDEX_SIGNATURE {
            return Err(MetaDataError::IndexSignature {});
        }

        if index.index_size != MetaDataIndex::METADATA_INDEX_SIZE {
            return Err(MetaDataError::IndexSize {});
        }

        let checksum = index.self_checksum;

        if checksum != index.checksum().context(SerializeError {})? {
            return Err(MetaDataError::IndexChecksum {});
        }

        Ok(index)
    }

    /// Checksum the index with the checksum field itself set to 0
    pub fn checksum(&mut self) -> Result<u32, Error> {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(self)?);
        Ok(self.self_checksum)
    }

    pub fn new(
        parent: Guid,
        guid: Guid,
        index_lba: u64,
        total_entries: u64,
    ) -> MetaDataIndex {
        MetaDataIndex {
            signature: MetaDataIndex::INDEX_SIGNATURE,
            index_size: MetaDataIndex::METADATA_INDEX_SIZE,
            self_checksum: 0,
            parent,
            guid,
            generation: 0,
            timestamp: 0,
            self_lba: index_lba,
            start_lba: index_lba + 1,
            current_entry: 0,
            used_entries: 0,
            total_entries,
        }
    }
}

pub struct NexusMetaData;

impl NexusMetaData {
    fn timestamp(time: &SystemTime) -> u128 {
        time.duration_since(UNIX_EPOCH).unwrap().as_micros()
    }

    fn read_index(
        buf: &DmaBuf,
        index_lba: u64,
    ) -> Result<MetaDataIndex, MetaDataError> {
        let index = MetaDataIndex::from_slice(buf.as_slice())?;

        if index.self_lba != index_lba {
            return Err(MetaDataError::IndexSelfAddress {});
        }

        Ok(index)
    }

    fn read_object(buf: &DmaBuf) -> Result<MetaDataObject, MetaDataError> {
        MetaDataObject::from_slice(buf.as_slice())
    }

    fn write_index(
        buf: &mut DmaBuf,
        index: &mut MetaDataIndex,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        buf.fill(0);
        let mut writer = Cursor::new(buf.as_mut_slice());

        index.generation += 1;
        index.timestamp = NexusMetaData::timestamp(now);
        index.checksum().context(SerializeError {})?;

        serialize_into(&mut writer, index).context(SerializeError {})
    }

    fn write_object(
        buf: &mut DmaBuf,
        object: &MetaDataObject,
    ) -> Result<(), MetaDataError> {
        buf.fill(0);
        let mut writer = Cursor::new(buf.as_mut_slice());

        let header = MetaDataHeader::from(object);
        serialize_into(&mut writer, &header).context(SerializeError {})?;

        for child in object.children.iter() {
            serialize_into(&mut writer, child).context(SerializeError {})?;
        }

        Ok(())
    }

    /// Get the location of the MetaDataIndex
    pub(crate) fn get_index_lba(
        label: &NexusLabel,
    ) -> Result<u64, MetaDataError> {
        match label.get_partition("MayaMeta") {
            Some(entry) => Ok(entry.ent_start + 1),
            None => Err(MetaDataError::MissingPartition {
                name: String::from("MayaMeta"),
            }),
        }
    }

    /// Check for a valid index and create a new one if none exists
    pub(crate) async fn check_or_initialise_index(
        child: &mut NexusChild,
        parent: Guid,
        label: &NexusLabel,
        total_entries: u64,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        let index_lba = NexusMetaData::get_index_lba(label)?;

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        if let Ok(index) = MetaDataIndex::from_slice(buf.as_slice()) {
            if index.self_lba != index_lba {
                return Err(MetaDataError::IndexSelfAddress {});
            }

            if child.guid != index.guid {
                // Set GUID for this child to match that stored in the index
                child.guid = index.guid;
                info!(
                    "setting GUID to {} for child {} to match index",
                    child.guid, child.name
                );
            }

            return Ok(());
        }

        let guid = Guid::from(Uuid::from(bdev.uuid()));
        let mut index =
            MetaDataIndex::new(parent, guid, index_lba, total_entries);

        info!("writing new index to child {}", child.name);

        NexusMetaData::write_index(&mut buf, &mut index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        if child.guid != guid {
            // Set GUID for this child to match that of the associated bdev
            child.guid = guid;
            info!(
                "setting GUID to {} for child {} to match bdev",
                child.guid, child.name
            );
        }

        Ok(())
    }

    /// Create a new index (overwriting any existing one)
    pub async fn initialise_index(
        child: &mut NexusChild,
        parent: Guid,
        label: &NexusLabel,
        total_entries: u64,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        let index_lba = NexusMetaData::get_index_lba(label)?;

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(WriteAlloc {
            name: String::from("index"),
        })?;

        let guid = Guid::from(Uuid::from(bdev.uuid()));
        let mut index =
            MetaDataIndex::new(parent, guid, index_lba, total_entries);

        info!("writing new index to child {}", child.name);

        NexusMetaData::write_index(&mut buf, &mut index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        if child.guid != guid {
            // Set GUID for this child to match that of the associated bdev
            child.guid = guid;
            info!(
                "setting GUID to {} for child {} to match bdev",
                child.guid, child.name
            );
        }

        Ok(())
    }

    /// Check that a valid MetaDataIndex exists on the MayaMeta partition,
    /// and that it is consistent with the NexusChild object.
    pub(crate) async fn validate_index(
        child: &NexusChild,
    ) -> Result<(), MetaDataError> {
        if let Some(index) = NexusMetaData::get_index(child).await? {
            if child.guid == index.guid {
                return Ok(());
            }

            return Err(MetaDataError::IndexGuid {});
        }

        Err(MetaDataError::MissingIndex {})
    }

    /// Write a new index to the MayaMeta partition.
    pub async fn create_index(
        child: &NexusChild,
        index: &mut MetaDataIndex,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(WriteAlloc {
            name: String::from("index"),
        })?;

        info!("writing new index to child {}", child.name);

        NexusMetaData::write_index(&mut buf, index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        Ok(())
    }

    /// Retrieve existing index from the MayaMeta partition.
    pub async fn get_index(
        child: &NexusChild,
    ) -> Result<Option<MetaDataIndex>, MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        if let Ok(index) = MetaDataIndex::from_slice(buf.as_slice()) {
            if index.self_lba != child.metadata_index_lba {
                return Err(MetaDataError::IndexSelfAddress {});
            }
            return Ok(Some(index));
        }

        Ok(None)
    }

    /// Add (append) a new entry to the index.
    pub async fn add(
        child: &NexusChild,
        object: &mut MetaDataObject,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        object.validate()?;

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let mut index =
            NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        if index.used_entries == 0 {
            index.current_entry = 0;
            index.used_entries = 1;
        } else {
            index.current_entry += 1;

            if index.current_entry == index.total_entries {
                index.current_entry = 0;
            }

            if index.used_entries < index.total_entries {
                index.used_entries += 1;
            }
        }

        NexusMetaData::write_object(&mut buf, object)?;
        handle
            .write_at(
                (index.start_lba + index.current_entry) * block_size,
                &buf,
            )
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        NexusMetaData::write_index(&mut buf, &mut index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        Ok(())
    }

    /// Update (overwrite) the most recent index entry.
    pub async fn update(
        child: &NexusChild,
        object: &mut MetaDataObject,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        object.validate()?;

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let mut index =
            NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        if index.used_entries == 0 {
            index.current_entry = 0;
            index.used_entries = 1;
        }

        NexusMetaData::write_object(&mut buf, object)?;
        handle
            .write_at(
                (index.start_lba + index.current_entry) * block_size,
                &buf,
            )
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        NexusMetaData::write_index(&mut buf, &mut index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        Ok(())
    }

    /// Remove the most recent entry from the index.
    pub async fn remove(
        child: &NexusChild,
        now: &SystemTime,
    ) -> Result<Option<MetaDataObject>, MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let mut index =
            NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        if index.used_entries == 0 {
            return Ok(None);
        }

        index.used_entries -= 1;

        handle
            .read_at(
                (index.start_lba + index.current_entry) * block_size,
                &mut buf,
            )
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;
        let object = NexusMetaData::read_object(&buf)?;

        if index.current_entry > 0 {
            index.current_entry -= 1;
        } else {
            index.current_entry = index.total_entries - 1;
        }

        NexusMetaData::write_index(&mut buf, &mut index, now)?;
        handle
            .write_at(index.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: child.name.clone(),
            })?;

        Ok(Some(object))
    }

    /// Get the most recent entry but do not remove it from the index.
    pub async fn last(
        child: &NexusChild,
    ) -> Result<Option<MetaDataObject>, MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let index = NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        if index.used_entries == 0 {
            return Ok(None);
        }

        handle
            .read_at(
                (index.start_lba + index.current_entry) * block_size,
                &mut buf,
            )
            .await
            .context(ReadError {
                name: String::from("object"),
            })?;
        let object = NexusMetaData::read_object(&buf)?;

        Ok(Some(object))
    }

    /// Return list of the most recent entries but do not modify the index.
    pub async fn get(
        child: &NexusChild,
        count: u64,
    ) -> Result<Vec<MetaDataObject>, MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let index = NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        let used = min(count, index.used_entries);

        if used == 0 {
            return Ok(Vec::new());
        }

        let mut list: Vec<MetaDataObject> = Vec::with_capacity(used as usize);

        let start = if used > index.current_entry + 1 {
            index.total_entries + index.current_entry + 1 - used
        } else {
            index.current_entry + 1 - used
        };

        if start > index.current_entry {
            for offset in start .. index.total_entries {
                handle
                    .read_at((index.start_lba + offset) * block_size, &mut buf)
                    .await
                    .context(ReadError {
                        name: String::from("object"),
                    })?;
                list.push(NexusMetaData::read_object(&buf)?);
            }

            for offset in 0 ..= index.current_entry {
                handle
                    .read_at((index.start_lba + offset) * block_size, &mut buf)
                    .await
                    .context(ReadError {
                        name: String::from("object"),
                    })?;
                list.push(NexusMetaData::read_object(&buf)?);
            }
        } else {
            for offset in start ..= index.current_entry {
                handle
                    .read_at((index.start_lba + offset) * block_size, &mut buf)
                    .await
                    .context(ReadError {
                        name: String::from("object"),
                    })?;
                list.push(NexusMetaData::read_object(&buf)?);
            }
        }

        Ok(list)
    }

    /// Purge oldest entries from index.
    pub async fn purge(
        child: &NexusChild,
        retain: u64,
        now: &SystemTime,
    ) -> Result<u64, MetaDataError> {
        if child.metadata_index_lba == 0 {
            return Err(MetaDataError::IndexAddressNotSet {
                name: child.name.clone(),
            });
        }

        let handle = child.handle().context(HandleError {
            name: child.name.clone(),
        })?;

        let bdev = handle.get_bdev();
        let block_size = u64::from(bdev.block_len());

        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("index"),
        })?;

        handle
            .read_at(child.metadata_index_lba * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;

        let mut index =
            NexusMetaData::read_index(&buf, child.metadata_index_lba)?;

        if retain < index.used_entries {
            let removed = index.used_entries - retain;
            index.used_entries = retain;

            NexusMetaData::write_index(&mut buf, &mut index, now)?;
            handle
                .write_at(index.self_lba * block_size, &buf)
                .await
                .context(WriteError {
                    name: child.name.clone(),
                })?;

            return Ok(removed);
        }

        Ok(0)
    }
}
