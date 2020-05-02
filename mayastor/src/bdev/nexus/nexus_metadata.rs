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
        nexus_child::{ChildError, ChildIoError, NexusChild},
        nexus_label::{GptEntry, GptGuid, Unsigned},
        nexus_state::NexusConfig,
    },
    core::{DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum MetaDataError {
    #[snafu(display("Incorrect MetaData header size"))]
    HeaderSize {},
    #[snafu(display("Incorrect MetaData header signature"))]
    HeaderSignature {},
    #[snafu(display("Incorrect MetaData header checksum"))]
    HeaderChecksum {},
    #[snafu(display("Incorrect MetaData index checksum"))]
    IndexChecksum {},
    #[snafu(display("Incorrect MetaData configuration object checksum"))]
    ObjectChecksum {},

    #[snafu(display("MetaData serialization error"))]
    SerializeError { source: Error },
    #[snafu(display("MetaData deserialization error"))]
    DeserializeError { source: Error },

    #[snafu(display("Failed to allocate index: {}", source))]
    IndexAlloc { source: DmaError },
    #[snafu(display("Failed to write index: {}", source))]
    IndexWrite { source: ChildIoError },

    #[snafu(display("MetaData index is inconsistent"))]
    IndexInconsistent {},
    #[snafu(display(
        "Number of objects {} exceeds index size {}",
        count,
        size
    ))]
    IndexSizeExceeded { count: u32, size: u32 },
    #[snafu(display("Not enough space left on MetaData partition"))]
    PartitionSizeExceeded {},

    #[snafu(display("Failed to allocate configuration object: {}", source))]
    ObjectAlloc { source: DmaError },
    #[snafu(display("Failed to read configuration object: {}", source))]
    ObjectRead { source: ChildIoError },
    #[snafu(display("Failed to write configuration object: {}", source))]
    ObjectWrite { source: ChildIoError },

    #[snafu(display("MetaData partition is missing or invalid"))]
    MissingPartition {},

    #[snafu(display("Error calculating timestamp: {}", source))]
    TimeStampError { source: SystemTimeError },

    #[snafu(display("{}", error))]
    NexusChildError { error: String },
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
    pub const MAX_INDEX_ENTRIES: u32 = 28;
    pub const INDEX_ENTRY_SIZE: u32 = 36;

    pub fn from_slice(slice: &[u8]) -> Result<MetaDataHeader, MetaDataError> {
        let mut header: MetaDataHeader =
            deserialize_from(&mut Cursor::new(slice))
                .context(DeserializeError {})?;

        if header.header_size != MetaDataHeader::METADATA_HEADER_SIZE {
            return Err(MetaDataError::HeaderSize {});
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

    pub fn checksum(&mut self) -> u32 {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(self).unwrap());
        self.self_checksum
    }

    pub fn new(block_size: u32, partition: &GptEntry) -> MetaDataHeader {
        let index_start = Unsigned::get_aligned_blocks(
            MetaDataHeader::METADATA_HEADER_SIZE,
            block_size,
        );

        let data_start = index_start
            + Unsigned::get_aligned_blocks(
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
    pub revision: u64,
    pub timestamp: u64,
    pub data_checksum: u32,
    pub data_start: u64,
    pub data_end: u64,
}

impl MetaDataIndexEntry {
    ///
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

    ///
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
    pub fn read_header(buf: &DmaBuf) -> Result<MetaDataHeader, MetaDataError> {
        MetaDataHeader::from_slice(buf.as_slice())
    }

    /// Construct index from raw data
    pub fn read_index(
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

    /// Construct and validate an empty index
    pub fn empty_index(
        header: &MetaDataHeader,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        if header.index_checksum != 0 {
            return Err(MetaDataError::IndexChecksum {});
        }
        Ok(Vec::new())
    }

    /// Construct a (config) object from raw data
    pub fn read_config_object(
        buf: &DmaBuf,
        entry: &MetaDataIndexEntry,
    ) -> Result<NexusConfig, MetaDataError> {
        let checksum = crc32::checksum_ieee(buf.as_slice());
        if checksum != entry.data_checksum {
            return Err(MetaDataError::ObjectChecksum {});
        }
        Ok(NexusConfig::from_slice(buf.as_slice())
            .context(DeserializeError {})?)
    }
}

impl NexusChild {
    async fn write_index(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<(), MetaDataError> {
        let (bdev, _desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        let blocks = metadata.header.index_start
            + Unsigned::get_aligned_blocks(
                (metadata.header.max_entries * metadata.header.entry_size)
                    as u64,
                block_size,
            );
        let mut buf =
            DmaBuf::new((blocks * block_size) as usize, bdev.alignment())
                .context(IndexAlloc {})?;
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

        self.write_at(metadata.header.self_lba * block_size, &buf)
            .await
            .context(IndexWrite {})?;

        Ok(())
    }

    pub async fn write_config_object(
        &self,
        metadata: &mut NexusMetaData,
        config: &NexusConfig,
        now: &SystemTime,
    ) -> Result<(), MetaDataError> {
        if metadata.header.used_entries >= metadata.header.max_entries {
            return Err(MetaDataError::IndexSizeExceeded {
                count: metadata.header.used_entries + 1,
                size: metadata.header.max_entries,
            });
        }

        let (bdev, _desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .context(TimeStampError {})?
            .as_secs();

        let start: u64;

        if let Some(entry) = metadata.index.last() {
            start = entry.data_end + 1;
            if start > metadata.header.data_end {
                return Err(MetaDataError::PartitionSizeExceeded {});
            }
        } else {
            start = metadata.header.data_start;
        }

        let blocks = Unsigned::get_aligned_blocks(
            serialized_size(config).context(SerializeError {})?,
            block_size,
        );
        let next = start + blocks;

        if next > metadata.header.data_end + 1 {
            return Err(MetaDataError::PartitionSizeExceeded {});
        }

        let mut buf =
            DmaBuf::new((blocks * block_size) as usize, bdev.alignment())
                .context(ObjectAlloc {})?;
        let mut writer = Cursor::new(buf.as_mut_slice());

        serialize_into(&mut writer, config).context(SerializeError {})?;
        let checksum = crc32::checksum_ieee(buf.as_slice());

        self.write_at((metadata.header.self_lba + start) * block_size, &buf)
            .await
            .context(ObjectWrite {})?;

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

    pub async fn write_all_config_objects(
        &self,
        header: &MetaDataHeader,
        list: &[NexusConfig],
        generation: u64,
        now: &SystemTime,
    ) -> Result<Vec<MetaDataIndexEntry>, MetaDataError> {
        if list.len() > header.max_entries as usize {
            return Err(MetaDataError::IndexSizeExceeded {
                count: list.len() as u32,
                size: header.max_entries,
            });
        }

        let mut index: Vec<MetaDataIndexEntry> = Vec::new();

        let (bdev, _desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;

        let timestamp = now
            .duration_since(UNIX_EPOCH)
            .context(TimeStampError {})?
            .as_secs();

        let mut start = header.data_start;
        let mut revision = generation;

        for config in list {
            let blocks = Unsigned::get_aligned_blocks(
                serialized_size(config).context(SerializeError {})?,
                block_size,
            );
            let next = start + blocks;

            if next > header.data_end + 1 {
                return Err(MetaDataError::PartitionSizeExceeded {});
            }

            let mut buf =
                DmaBuf::new((blocks * block_size) as usize, bdev.alignment())
                    .context(ObjectAlloc {})?;
            let mut writer = Cursor::new(buf.as_mut_slice());

            serialize_into(&mut writer, config).context(SerializeError {})?;
            let checksum = crc32::checksum_ieee(buf.as_slice());

            self.write_at((header.self_lba + start) * block_size, &buf)
                .await
                .context(ObjectWrite {})?;

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

    // Determines if the data defined by the index is currently fragmented.
    // Returns an error if the index is inconsistent.
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

    // Defragment the data defined by the index, and update the index in situ.
    async fn compact(
        &mut self,
        metadata: &mut NexusMetaData,
    ) -> Result<(), MetaDataError> {
        let (bdev, _desc) = self.get_dev()?;
        let block_size = bdev.block_len() as u64;
        let alignment = bdev.alignment();

        let self_lba = metadata.header.self_lba;
        let mut start = metadata.header.data_start;

        for entry in &mut metadata.index {
            if entry.data_start > start {
                let blocks = entry.data_end - entry.data_start;
                let mut buf = DmaBuf::new(
                    ((blocks + 1) * block_size) as usize,
                    alignment,
                )
                .context(ObjectAlloc {})?;
                self.read_at(
                    (self_lba + entry.data_start) * block_size,
                    &mut buf,
                )
                .await
                .context(ObjectRead {})?;
                self.write_at((self_lba + start) * block_size, &buf)
                    .await
                    .context(ObjectWrite {})?;
                entry.data_start = start;
                entry.data_end = start + blocks;
            }
            start = entry.data_end + 1;
        }

        Ok(())
    }

    // Update checksums and write out MetaData header + index to disk.
    pub async fn sync_metadata(
        &mut self,
        metadata: &mut NexusMetaData,
    ) -> Result<(), MetaDataError> {
        metadata.header.index_checksum =
            MetaDataIndexEntry::checksum(&metadata.index);
        metadata.header.checksum();
        self.write_index(&metadata).await
    }

    // Create a new header + index on "MetaData" partition.
    pub async fn create_metadata(
        &mut self,
    ) -> Result<NexusMetaData, MetaDataError> {
        let (bdev, _desc) = self.get_dev()?;

        if let Some(partition) = self.probe_label().await?.partitions.get(0) {
            if partition.ent_type
                == GptGuid::from_str("27663382-e5e6-11e9-81b4-ca5ca5ca5ca5")
                    .unwrap()
                && partition.ent_name.name == "MayaMeta"
            {
                let mut metadata = NexusMetaData {
                    header: MetaDataHeader::new(bdev.block_len(), &partition),
                    index: Vec::new(),
                };
                self.sync_metadata(&mut metadata).await?;
                return Ok(metadata);
            }
        }

        Err(MetaDataError::MissingPartition {})
    }

    // Retrieve header + index from "MetaData" partition.
    pub async fn get_metadata(&self) -> Result<NexusMetaData, MetaDataError> {
        if let Some(partition) = self.probe_label().await?.partitions.get(0) {
            if partition.ent_type
                == GptGuid::from_str("27663382-e5e6-11e9-81b4-ca5ca5ca5ca5")
                    .unwrap()
                && partition.ent_name.name == "MayaMeta"
            {
                return Ok(self.probe_index(partition.ent_start).await?);
            }
        }

        Err(MetaDataError::MissingPartition {})
    }

    // Retrieve specified config object from "MetaData" partition.
    pub async fn get_config_object(
        &self,
        metadata: &NexusMetaData,
        n: u32,
    ) -> Result<Option<NexusConfig>, MetaDataError> {
        if n < metadata.header.used_entries {
            return Ok(Some(self.probe_config_object(metadata, n).await?));
        }
        Ok(None)
    }

    // Retrieve latest config object from "MetaData" partition.
    pub async fn get_latest_config_object(
        &self,
        metadata: &NexusMetaData,
    ) -> Result<Option<NexusConfig>, MetaDataError> {
        if metadata.header.used_entries > 0 {
            let n = metadata.header.used_entries - 1;
            return Ok(Some(self.probe_config_object(metadata, n).await?));
        }
        Ok(None)
    }

    // Remove specified config object from "MetaData" partition.
    pub async fn delete_config_object(
        &mut self,
        metadata: &mut NexusMetaData,
        n: u32,
    ) -> Result<(), MetaDataError> {
        if metadata.index.len() != metadata.header.used_entries as usize {
            return Err(MetaDataError::IndexInconsistent {});
        }

        if n < metadata.header.used_entries {
            metadata.index.remove(n as usize);
            metadata.header.used_entries -= 1;
            return self.sync_metadata(metadata).await;
        }
        Ok(())
    }

    // Append a new config object to "MetaData" partition.
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
    pub fn from_slice(buf: &[u8]) -> Result<NexusConfig, Error> {
        let mut reader = Cursor::new(buf);
        let config: NexusConfig = deserialize_from(&mut reader)?;
        Ok(config)
    }
}

// ChildError already has a variant with a MetaDataError source.
// Attempting to add a variant to MetaDataError with a ChildError source
// creates a circular definition, resulting in the compiler error:
//   "cycle detected when processing ChildError"
// This is resolved by providing an explicit conversion.
impl std::convert::From<ChildError> for MetaDataError {
    fn from(error: ChildError) -> MetaDataError {
        MetaDataError::NexusChildError {
            error: format!("{}", error),
        }
    }
}
