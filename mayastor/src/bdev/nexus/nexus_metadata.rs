//! Support for manipulating nexus "metadata".
//! Simple API for adding and retrieving data from the "MayaMeta" partition.
//! The raw disk partition is accessed directly - there is no filesystem
//! present.
//!
//! The data layout is as follows:
//!  - The first block of the partition is not used.
//!  - The second block contains a MetaDataHeader (currently 72 bytes) while the
//!    remainder of the block is padded with zeros.
//!  - The "index" starts at the third block and contains a fixed number of
//!    MetaDataIndexEntry entries, each of which contains the address of an
//!    object that has been written to the partition.
//!  - The first usable "data" block is the first block following the index
//!    (whose size is aligned to the blocksize of the disk).
//!
//! ## Example
//! Sample code to create a new index and add a config object:
//!
//!    let config: NexusConfig = NexusConfig::Version1(NexusConfigVersion1 {
//!       ...
//!    });
//!    let now = SystemTime::now();
//!    let child = &mut nexus.children[0];
//!    let mut metadata = child.create_metadata().await?;
//!    child.append_config_object(&mut metadata, &config, &now).await?;
//!
//! Sample code to retrieve the latest config object from an existing index:
//!
//!    let metadata = child.get_metadata().await?;
//!    let config = child.get_latest_config_object(&metadata).await?;
use std::{
    io::{Cursor, Seek, SeekFrom},
    str::FromStr,
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use bincode::{
    deserialize_from,
    serialize,
    serialize_into,
    serialized_size,
    Error,
};
use crc::{crc32, Hasher32};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::{
    bdev::nexus::{
        nexus_bdev::Nexus,
        nexus_child::{ChildError, NexusChild},
        nexus_label::{Aligned, GptEntry, GptGuid, LabelError},
        nexus_metadata_content::NexusConfig,
    },
    core::{CoreError, DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum MetaDataError {
    #[snafu(display("{}", source))]
    NexusChildError { source: ChildError },
    #[snafu(display("Error probing disk label: {}", source))]
    ProbeLabelError { source: LabelError },
    #[snafu(display("Error reading {}: {}", name, source))]
    ReadError { name: String, source: CoreError },
    #[snafu(display("Error writing {}: {}", name, source))]
    WriteError { name: String, source: CoreError },
    #[snafu(display(
        "Failed to allocate buffer for reading {}: {}",
        name,
        source
    ))]
    ReadAlloc { name: String, source: DmaError },
    #[snafu(display(
        "Failed to allocate buffer for writing {}: {}",
        name,
        source
    ))]
    WriteAlloc { name: String, source: DmaError },
    #[snafu(display("Serialization error: {}", source))]
    SerializeError { source: Error },
    #[snafu(display("Deserialization error: {}", source))]
    DeserializeError { source: Error },
    #[snafu(display(
        "Incorrect MetaData header size: actual={} expected={}",
        actual_size,
        expected_size
    ))]
    HeaderSize {
        actual_size: u32,
        expected_size: u32,
    },
    #[snafu(display("Incorrect MetaData header signature"))]
    HeaderSignature {},
    #[snafu(display("Incorrect MetaData header checksum"))]
    HeaderChecksum {},
    #[snafu(display("Incorrect MetaData index checksum"))]
    IndexChecksum {},
    #[snafu(display("Incorrect MetaData configuration object checksum"))]
    ObjectChecksum {},
    #[snafu(display("MetaData index is inconsistent"))]
    IndexInconsistent {},
    #[snafu(display("MetaData index ({}) out of range ({})", selected, used))]
    IndexOutOfRange { selected: u32, used: u32 },
    #[snafu(display(
        "Number of objects ({}) exceeds index size ({})",
        used,
        size
    ))]
    IndexSizeExceeded { used: u32, size: u32 },
    #[snafu(display("Not enough space left on MetaData partition"))]
    PartitionSizeExceeded {},
    #[snafu(display("MetaData partition is missing or invalid"))]
    MissingPartition {},
    #[snafu(display("Error calculating timestamp: {}", source))]
    TimeStampError { source: SystemTimeError },
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Copy, Clone)]
pub struct MetaDataHeader {
    /// Signature identifying this as a MetaDataHeader object
    pub signature: [u8; 8],
    /// Size of this header in bytes
    pub header_size: u32,
    /// CRC-32 checksum of this header
    pub self_checksum: u32,
    /// Current object generation counter
    pub generation: u64,
    /// Absolute location (LBA) of this header on disk
    pub self_lba: u64,
    /// Offset of start of index table relative to self_lba
    pub index_start: u64,
    /// Number of valid entries in the index
    pub used_entries: u32,
    /// Maximum number of entries that the index can contain
    pub max_entries: u32,
    /// Size of an index entry in bytes
    pub entry_size: u32,
    /// CRC-32 checksum of the index table
    pub index_checksum: u32,
    /// Offset of first usable data block relative to self_lba
    pub data_start: u64,
    /// Offset of last usable data block relative to self_lba
    pub data_end: u64,
}

impl MetaDataHeader {
    pub const METADATA_HEADER_SIZE: u32 = 72;
    pub const MAX_INDEX_ENTRIES: u32 = 32;
    pub const INDEX_ENTRY_SIZE: u32 = 44;

    /// Convert a slice into a MetaDataHeader and validate
    pub fn from_slice(slice: &[u8]) -> Result<MetaDataHeader, MetaDataError> {
        let mut header: MetaDataHeader =
            deserialize_from(&mut Cursor::new(slice))
                .context(DeserializeError {})?;

        if header.header_size != MetaDataHeader::METADATA_HEADER_SIZE {
            return Err(MetaDataError::HeaderSize {
                actual_size: header.header_size,
                expected_size: MetaDataHeader::METADATA_HEADER_SIZE,
            });
        }

        if header.signature != [0x4d, 0x61, 0x79, 0x61, 0x44, 0x61, 0x74, 0x61]
        {
            return Err(MetaDataError::HeaderSignature {});
        }

        let checksum = header.self_checksum;

        if header.checksum() != checksum {
            return Err(MetaDataError::HeaderChecksum {});
        }

        Ok(header)
    }

    /// Checksum the header with the checksum field itself set to 0
    pub fn checksum(&mut self) -> u32 {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(self).unwrap());
        self.self_checksum
    }

    /// Generate a new MetaDataHeader based on the partition information and the
    /// underlying block_size
    pub fn new(block_size: u32, partition: &GptEntry) -> MetaDataHeader {
        let index_start = Aligned::get_blocks(
            MetaDataHeader::METADATA_HEADER_SIZE,
            block_size,
        );

        let data_start = index_start
            + Aligned::get_blocks(
                MetaDataHeader::MAX_INDEX_ENTRIES
                    * MetaDataHeader::INDEX_ENTRY_SIZE,
                block_size,
            );

        MetaDataHeader {
            signature: [0x4d, 0x61, 0x79, 0x61, 0x44, 0x61, 0x74, 0x61],
            header_size: MetaDataHeader::METADATA_HEADER_SIZE,
            self_checksum: 0,
            generation: 0,
            self_lba: partition.ent_start + 1, /* skip the first block of the
                                                * partition */
            index_start: index_start as u64,
            used_entries: 0,
            max_entries: MetaDataHeader::MAX_INDEX_ENTRIES,
            entry_size: MetaDataHeader::INDEX_ENTRY_SIZE,
            index_checksum: 0,
            data_start: data_start as u64,
            data_end: partition.ent_end - partition.ent_start - 1,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct MetaDataIndexEntry {
    /// Current object revision
    pub revision: u64,
    /// When this entry was created (number of microseconds since UNIX_EPOCH)
    pub timestamp: u128,
    /// CRC-32 checksum of all the blocks comprising the stored object
    pub data_checksum: u32,
    /// Location of first data block (relative to self_lba in the
    /// MetaDataHeader)
    pub data_start: u64,
    /// Location of last data block (relative to self_lba in the
    /// MetaDataHeader)
    pub data_end: u64,
}

impl MetaDataIndexEntry {
    /// Convert a slice into an index array
    pub fn from_slice(
        slice: &[u8],
        used_entries: u32,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        let mut index: Vec<MetaDataIndexEntry> = Vec::new();
        let mut reader = Cursor::new(slice);
        for _ in 0 .. used_entries {
            index.push(
                deserialize_from(&mut reader).context(DeserializeError {})?,
            );
        }
        Ok(index)
    }

    /// Calculate checksum of an index array
    pub fn checksum(index: &[MetaDataIndexEntry]) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        for entry in index {
            digest.write(&serialize(entry).unwrap());
        }
        digest.sum32()
    }
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct NexusMetaData {
    pub header: MetaDataHeader,
    pub index: Vec<MetaDataIndexEntry>,
}

impl NexusMetaData {
    /// Construct a MetaDataHeader from raw data
    fn read_header(buf: &DmaBuf) -> Result<MetaDataHeader, MetaDataError> {
        MetaDataHeader::from_slice(buf.as_slice())
    }

    /// Construct index array from raw data
    fn read_index(
        buf: &DmaBuf,
        header: &MetaDataHeader,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        let index = MetaDataIndexEntry::from_slice(
            buf.as_slice(),
            header.used_entries,
        )?;
        let checksum = MetaDataIndexEntry::checksum(&index);
        if checksum != header.index_checksum {
            return Err(MetaDataError::IndexChecksum {});
        }
        Ok(index)
    }

    /// Create an "empty" index array.
    // This is called when the header indicates that there are no entries in
    // the index, in which case there is nothing further to be read from disk.
    // However we still want to ensure that the checksum is valid in this case.
    fn empty_index(
        header: &MetaDataHeader,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        if header.index_checksum != 0 {
            return Err(MetaDataError::IndexChecksum {});
        }
        Ok(Vec::new())
    }

    /// Construct a (config) object from raw data
    fn read_config_object(
        buf: &DmaBuf,
        entry: &MetaDataIndexEntry,
    ) -> Result<NexusConfig, MetaDataError> {
        let checksum = crc32::checksum_ieee(buf.as_slice());
        if checksum != entry.data_checksum {
            return Err(MetaDataError::ObjectChecksum {});
        }
        NexusConfig::from_slice(buf.as_slice()).context(DeserializeError {})
    }
}

impl NexusChild {
    /// Read the Metadata header + index from disk
    async fn probe_index(
        &self,
        partition_lba: u64,
    ) -> Result<NexusMetaData, MetaDataError> {
        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        //
        // Header
        let blocks = Aligned::get_blocks(
            MetaDataHeader::METADATA_HEADER_SIZE as u64,
            block_size,
        );
        let mut buf =
            hndl.dma_malloc(blocks * block_size).context(ReadAlloc {
                name: String::from("header"),
            })?;
        hndl.read_at((partition_lba + 1) * block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("header"),
            })?;
        let header = NexusMetaData::read_header(&buf)?;

        //
        // Index
        let index = if header.used_entries > 0 {
            let blocks = Aligned::get_blocks(
                (header.used_entries * header.entry_size) as u64,
                block_size,
            );
            let mut buf =
                hndl.dma_malloc(blocks * block_size).context(ReadAlloc {
                    name: String::from("index"),
                })?;
            hndl.read_at(
                (header.self_lba + header.index_start) * block_size,
                &mut buf,
            )
            .await
            .context(ReadError {
                name: String::from("index"),
            })?;
            NexusMetaData::read_index(&buf, &header)?
        } else {
            NexusMetaData::empty_index(&header)?
        };

        Ok(NexusMetaData {
            header,
            index,
        })
    }

    /// Read the selected config object from disk.
    async fn probe_config_object(
        &self,
        metadata: &NexusMetaData,
        selected: u32,
    ) -> Result<NexusConfig, MetaDataError> {
        if selected >= metadata.header.used_entries {
            return Err(MetaDataError::IndexOutOfRange {
                selected,
                used: metadata.header.used_entries,
            });
        }

        let entry = &metadata.index[selected as usize];

        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        let blocks = entry.data_end - entry.data_start + 1;
        let mut buf =
            hndl.dma_malloc(blocks * block_size).context(ReadAlloc {
                name: String::from("object"),
            })?;
        hndl.read_at(
            (metadata.header.self_lba + entry.data_start) * block_size,
            &mut buf,
        )
        .await
        .context(ReadError {
            name: String::from("object"),
        })?;

        Ok(NexusMetaData::read_config_object(&buf, entry)?)
    }

    /// Read all config objects currently present on disk.
    pub async fn probe_all_config_objects(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<Vec<NexusConfig>, MetaDataError> {
        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        let mut list: Vec<NexusConfig> = Vec::new();

        for entry in &metadata.index {
            let blocks = entry.data_end - entry.data_start + 1;
            let mut buf =
                hndl.dma_malloc(blocks * block_size).context(ReadAlloc {
                    name: String::from("object"),
                })?;
            hndl.read_at(
                (metadata.header.self_lba + entry.data_start) * block_size,
                &mut buf,
            )
            .await
            .context(ReadError {
                name: String::from("object"),
            })?;
            list.push(NexusMetaData::read_config_object(&buf, &entry)?);
        }

        Ok(list)
    }

    /// Write the Metadata header + index to disk
    async fn write_index(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<(), MetaDataError> {
        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        let blocks = metadata.header.index_start
            + Aligned::get_blocks(
                (metadata.header.max_entries * metadata.header.entry_size)
                    as u64,
                block_size,
            );
        let mut buf = DmaBuf::new(blocks * block_size, bdev.alignment())
            .context(WriteAlloc {
                name: String::from("index"),
            })?;
        let mut writer = Cursor::new(buf.as_mut_slice());

        // Header
        serialize_into(&mut writer, &metadata.header)
            .context(SerializeError {})?;

        // Index
        writer
            .seek(SeekFrom::Start(metadata.header.index_start * block_size))
            .unwrap();
        for entry in &metadata.index {
            serialize_into(&mut writer, entry).context(SerializeError {})?;
        }

        hndl.write_at(metadata.header.self_lba * block_size, &buf)
            .await
            .context(WriteError {
                name: String::from("index"),
            })?;

        Ok(())
    }

    /// Write a config object to disk.
    /// Also add a corresponding entry to the index and update the header.
    /// Will fail if there is insufficent space remaining on the disk.
    async fn write_config_object(
        &self,
        metadata: &mut NexusMetaData,
        config: &NexusConfig,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        if metadata.header.used_entries >= metadata.header.max_entries {
            return Err(MetaDataError::IndexSizeExceeded {
                used: metadata.header.used_entries + 1,
                size: metadata.header.max_entries,
            });
        }

        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .context(TimeStampError {})?
            .as_micros();

        let start: u64;

        if let Some(entry) = metadata.index.last() {
            start = entry.data_end + 1;
            if start > metadata.header.data_end {
                return Err(MetaDataError::PartitionSizeExceeded {});
            }
        } else {
            start = metadata.header.data_start;
        }

        let blocks = Aligned::get_blocks(
            serialized_size(config).context(SerializeError {})?,
            block_size,
        );
        let next = start + blocks;

        if next > metadata.header.data_end + 1 {
            return Err(MetaDataError::PartitionSizeExceeded {});
        }

        let mut buf = DmaBuf::new(block_size * blocks, bdev.alignment())
            .context(WriteAlloc {
                name: String::from("object"),
            })?;
        let mut writer = Cursor::new(buf.as_mut_slice());

        serialize_into(&mut writer, config).context(SerializeError {})?;
        let checksum = crc32::checksum_ieee(buf.as_slice());

        hndl.write_at((metadata.header.self_lba + start) * block_size, &buf)
            .await
            .context(WriteError {
                name: String::from("object"),
            })?;

        metadata.header.generation += 1;
        metadata.header.used_entries += 1;

        metadata.index.push(MetaDataIndexEntry {
            revision: metadata.header.generation,
            timestamp,
            data_checksum: checksum,
            data_start: start,
            data_end: next - 1,
        });

        Ok(())
    }

    /// Write an array of config object to disk.
    /// Also generate a suitable index.
    /// Will fail if there is insufficent space on the disk.
    pub async fn write_all_config_objects(
        &self,
        header: &MetaDataHeader,
        list: &[NexusConfig],
        generation: u64,
        now: &SystemTime,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        if list.len() > header.max_entries as usize {
            return Err(MetaDataError::IndexSizeExceeded {
                used: list.len() as u32,
                size: header.max_entries,
            });
        }

        let mut index: Vec<MetaDataIndexEntry> = Vec::new();

        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;

        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .context(TimeStampError {})?
            .as_micros();

        let mut start = header.data_start;
        let mut revision = generation;

        for config in list {
            let blocks = Aligned::get_blocks(
                serialized_size(config).context(SerializeError {})?,
                block_size,
            );
            let next = start + blocks;

            if next > header.data_end + 1 {
                return Err(MetaDataError::PartitionSizeExceeded {});
            }

            let mut buf = DmaBuf::new(blocks * block_size, bdev.alignment())
                .context(WriteAlloc {
                    name: String::from("object"),
                })?;
            let mut writer = Cursor::new(buf.as_mut_slice());

            serialize_into(&mut writer, config).context(SerializeError {})?;
            let checksum = crc32::checksum_ieee(buf.as_slice());

            hndl.write_at((header.self_lba + start) * block_size, &buf)
                .await
                .context(WriteError {
                    name: String::from("object"),
                })?;

            revision += 1;

            index.push(MetaDataIndexEntry {
                revision,
                timestamp,
                data_checksum: checksum,
                data_start: start,
                data_end: next - 1,
            });

            start = next;
        }

        Ok(index)
    }

    /// Determines if the data defined by the index is currently fragmented.
    /// Returns an error if the index is inconsistent.
    fn fragmented(
        mut start: u64,
        index: &[MetaDataIndexEntry],
    ) -> Result<bool, MetaDataError> {
        for entry in index {
            if entry.data_start < start {
                return Err(MetaDataError::IndexInconsistent {});
            }
            if entry.data_start > start {
                return Ok(true);
            }
            start = entry.data_end + 1;
        }
        Ok(false)
    }

    /// Defragment the data defined by the index, and update the index in situ.
    async fn compact(
        &mut self,
        metadata: &mut NexusMetaData,
    ) -> Result<(), MetaDataError> {
        let (bdev, hndl) = self.get_dev().context(NexusChildError {})?;
        let block_size = bdev.block_len() as u64;
        let alignment = bdev.alignment();

        let self_lba = metadata.header.self_lba;
        let mut start = metadata.header.data_start;

        for entry in &mut metadata.index {
            if entry.data_start > start {
                let blocks = entry.data_end - entry.data_start;
                let mut buf = DmaBuf::new((blocks + 1) * block_size, alignment)
                    .context(ReadAlloc {
                        name: String::from("object"),
                    })?;
                hndl.read_at(
                    (self_lba + entry.data_start) * block_size,
                    &mut buf,
                )
                .await
                .context(ReadError {
                    name: String::from("object"),
                })?;
                hndl.write_at((self_lba + start) * block_size, &buf)
                    .await
                    .context(WriteError {
                        name: String::from("object"),
                    })?;
                entry.data_start = start;
                entry.data_end = start + blocks;
            }
            start = entry.data_end + 1;
        }

        Ok(())
    }

    /// Update checksums and write out MetaData header + index to disk.
    pub async fn sync_metadata(
        &mut self,
        metadata: &mut NexusMetaData,
    ) -> Result<(), MetaDataError> {
        metadata.header.index_checksum =
            MetaDataIndexEntry::checksum(&metadata.index);
        metadata.header.checksum();
        self.write_index(&metadata).await
    }

    /// Create a new header + index on "MetaData" partition.
    pub async fn create_metadata(
        &mut self,
    ) -> Result<NexusMetaData, MetaDataError> {
        let (bdev, _hndl) = self.get_dev().context(NexusChildError {})?;

        if let Some(partition) = self
            .probe_label()
            .await
            .context(ProbeLabelError {})?
            .partitions
            .get(0)
        {
            if partition.ent_type
                == GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID).unwrap()
                && partition.ent_name.name == "MayaMeta"
            {
                let mut metadata = NexusMetaData {
                    header: MetaDataHeader::new(
                        bdev.block_len() as u32,
                        &partition,
                    ),
                    index: Vec::new(),
                };
                self.sync_metadata(&mut metadata).await?;
                return Ok(metadata);
            }
        }

        Err(MetaDataError::MissingPartition {})
    }

    /// Retrieve header + index from "MetaData" partition.
    pub async fn get_metadata(&self) -> Result<NexusMetaData, MetaDataError> {
        if let Some(partition) = self
            .probe_label()
            .await
            .context(ProbeLabelError {})?
            .partitions
            .get(0)
        {
            if partition.ent_type
                == GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID).unwrap()
                && partition.ent_name.name == "MayaMeta"
            {
                return self.probe_index(partition.ent_start).await;
            }
        }

        Err(MetaDataError::MissingPartition {})
    }

    /// Retrieve selected config object from "MetaData" partition.
    /// The "selected" parameter identifies the appropriate entry in the index
    /// array.
    pub async fn get_config_object(
        &self,
        metadata: &NexusMetaData,
        selected: u32,
    ) -> Result<Option<NexusConfig>, MetaDataError> {
        if selected < metadata.header.used_entries {
            return Ok(Some(
                self.probe_config_object(metadata, selected).await?,
            ));
        }
        Ok(None)
    }

    /// Retrieve latest config object from "MetaData" partition.
    pub async fn get_latest_config_object(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<Option<NexusConfig>, MetaDataError> {
        if metadata.header.used_entries > 0 {
            return Ok(Some(
                self.probe_config_object(
                    metadata,
                    metadata.header.used_entries - 1,
                )
                .await?,
            ));
        }
        Ok(None)
    }

    /// Remove selected config object from "MetaData" partition.
    /// The "selected" parameter identifies the appropriate entry in the index
    /// array.
    pub async fn delete_config_object(
        &mut self,
        metadata: &mut NexusMetaData,
        selected: u32,
    ) -> Result<(), MetaDataError> {
        if metadata.index.len() != metadata.header.used_entries as usize {
            return Err(MetaDataError::IndexInconsistent {});
        }

        if selected < metadata.header.used_entries {
            metadata.index.remove(selected as usize);
            metadata.header.used_entries -= 1;
        }

        self.sync_metadata(metadata).await
    }

    /// Append a new config object to "MetaData" partition.
    pub async fn append_config_object(
        &mut self,
        metadata: &mut NexusMetaData,
        config: &NexusConfig,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        if metadata.index.len() != metadata.header.used_entries as usize {
            return Err(MetaDataError::IndexInconsistent {});
        }

        if NexusChild::fragmented(metadata.header.data_start, &metadata.index)?
        {
            if let Err(error) = self.compact(metadata).await {
                warn!("Error compacting MetaData: {}", error);
            }
        }

        self.write_config_object(metadata, config, now).await?;
        self.sync_metadata(metadata).await
    }
}

impl NexusConfig {
    /// Convert a slice into a NexusConfig object
    pub fn from_slice(buf: &[u8]) -> Result<NexusConfig, Error> {
        let mut reader = Cursor::new(buf);
        let config: NexusConfig = deserialize_from(&mut reader)?;
        Ok(config)
    }
}
