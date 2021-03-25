//! GPT labeling for Nexus devices. The primary partition
//! (/dev/x1) will be used for meta data during, rebuild. The second
//! partition contains the file system.
//!
//! The nexus will adjust internal data structures to offset the IO to the
//! right partition. put differently, when connecting to this device via
//! NVMF or iSCSI it will show up as device with just one partition.
//!
//! When the nexus is removed from the data path and other initiations are
//! used, the data is still accessible and thus removes us has a hard
//! dependency in the data path.
//!
//! # Example:
//!
//! ```bash
//! $ rm /code/disk1.img; truncate -s 1GiB /code/disk1.img
//! $ mayastor-client nexus create $UUID 1GiB aio:////code/disk1.img?blk_size=512
//! $ sgdisk -p /code/disk1.img
//! Disk /code//disk1.img: 2097152 sectors, 1024.0 MiB
//! Sector size (logical): 512 bytes
//! Disk identifier (GUID): EAB49A2F-EFEA-45E6-9A1B-61FECE3426DD
//! Partition table holds up to 128 entries
//! Main partition table begins at sector 2 and ends at sector 33
//! First usable sector is 2048, last usable sector is 2097118
//! Partitions will be aligned on 2048-sector boundaries
//! Total free space is 0 sectors (0 bytes)
//!
//! Number  Start (sector)    End (sector)  Size       Code  Name
//!  1            2048           10239   4.0 MiB     FFFF  MayaMeta
//!  2           10240         2097118   1019.0 MiB  FFFF  MayaData
//! ```
//!
//! Notice how two partitions have been created when accessing the disk
//! when shared by the nexus:
//!
//! ```bash
//! $ mayastor-client nexus share $UUID
//! "/dev/nbd0"
//!
//! TODO: also note how it complains about a MBR
//!
//! $ lsblk
//! NAME    MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
//! sda       8:0    0   50G  0 disk
//! ├─sda1    8:1    0 41.5G  0 part /
//! ├─sda2    8:2    0    7M  0 part [SWAP]
//! └─sda3    8:3    0  511M  0 part /boot
//! sr0      11:0    1 1024M  0 rom
//! nbd0     43:0    0 1019M  0 disk
//! nvme0n1 259:0    0  200G  0 disk /code
//!
//! The nbd0 zero device does not show the partitions when mounting
//! it without the nexus in the data path, there would be two paritions
//! ```
use bincode::{deserialize_from, serialize, serialize_into, Error};
use crc::{crc32, Hasher32};
use serde::{
    de::{Deserialize, Deserializer, SeqAccess, Unexpected, Visitor},
    ser::{Serialize, SerializeTuple, Serializer},
};
use snafu::{ResultExt, Snafu};
use std::{
    cmp::min,
    convert::From,
    fmt::{self, Display},
    io::{Cursor, Seek, SeekFrom},
    str::FromStr,
};
use uuid::{self, parser, Uuid};

use crate::{
    bdev::nexus::{nexus_bdev::Nexus, nexus_child::NexusChild},
    core::{CoreError, DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum LabelError {
    #[snafu(display("Serialization error: {}", source))]
    SerializeError { source: Error },
    #[snafu(display(
        "Failed to allocate buffer for reading from child {}: {}",
        name,
        source
    ))]
    ReadAlloc { source: DmaError, name: String },
    #[snafu(display(
        "Failed to allocate buffer for writing to child {}: {}",
        name,
        source
    ))]
    WriteAlloc { source: DmaError, name: String },
    #[snafu(display("Error reading from child {}: {}", name, source))]
    ReadError { source: CoreError, name: String },
    #[snafu(display("Error writing to child {}: {}", name, source))]
    WriteError { source: CoreError, name: String },
    #[snafu(display("Label is invalid: {}", source))]
    InvalidLabel { source: ProbeError },
    #[snafu(display(
        "Failed to obtain BdevHandle for child {}: {}",
        name,
        source
    ))]
    HandleError { source: CoreError, name: String },
    #[snafu(display(
        "Device is too small to accomodate Metadata partition: blocks={}",
        blocks
    ))]
    DeviceTooSmall { blocks: u64 },
    #[snafu(display("The written label could not be read from disk, likely the child {} is a null device", name))]
    ReReadError { name: String },
}

#[derive(Debug, Snafu)]
pub enum ProbeError {
    #[snafu(display("Deserialization error: {}", source))]
    DeserializeError { source: Error },
    #[snafu(display("Incorrect MBR signature"))]
    MbrSignature {},
    #[snafu(display("Disk size in MBR does not match size in GPT header"))]
    MbrSize {},
    #[snafu(display("Incorrect GPT header signature"))]
    GptSignature {},
    #[snafu(display(
        "Incorrect GPT header size: actual={} expected={}",
        actual_size,
        expected_size
    ))]
    GptHeaderSize {
        actual_size: u32,
        expected_size: u32,
    },
    #[snafu(display("Incorrect GPT header checksum"))]
    GptChecksum {},
    #[snafu(display("Incorrect GPT partition table checksum"))]
    PartitionTableChecksum {},
    #[snafu(display("Disk GUIDs differ"))]
    CompareDiskGuid {},
    #[snafu(display("Disk sizes differ"))]
    CompareDiskSize {},
    #[snafu(display("GPT stored partition table checksums differ"))]
    ComparePartitionTableChecksum {},
    #[snafu(display("GPT partition table location is incorrect"))]
    PartitionTableLocation {},
    #[snafu(display("Missing partition: {}", name))]
    MissingPartition { name: String },
    #[snafu(display("Primary GTP header location is incorrect"))]
    PrimaryLocation {},
    #[snafu(display("Secondary GTP header location is incorrect"))]
    SecondaryLocation {},
    #[snafu(display("Location of first usable block is incorrect"))]
    FirstUsableBlock {},
    #[snafu(display("Location of last usable block is incorrect"))]
    LastUsableBlock {},
    #[snafu(display("Partition table exceeds maximum size"))]
    PartitionTableSize {},
    #[snafu(display("Insufficient space reserved for partition table"))]
    PartitionTableSpace {},
    #[snafu(display("Partition starts before first usable block"))]
    PartitionStart {},
    #[snafu(display("Partition ends after last usable block"))]
    PartitionEnd {},
    #[snafu(display("Partition has negative size"))]
    NegativePartitionSize {},
    #[snafu(display("GPT header locations are inconsistent"))]
    CompareHeaderLocation {},
    #[snafu(display("Number of partition table entries differ"))]
    ComparePartitionEntryCount {},
    #[snafu(display("Partition table entry sizes differ"))]
    ComparePartitionEntrySize {},
    #[snafu(display("Incorrect partition layout"))]
    IncorrectPartitions {},
    #[snafu(display("Label is invalid"))]
    LabelRedundancy {},
}

pub struct LabelConfig {
    disk_guid: GptGuid,
    meta_guid: GptGuid,
    data_guid: GptGuid,
}

impl LabelConfig {
    fn new(guid: GptGuid) -> LabelConfig {
        LabelConfig {
            disk_guid: guid,
            meta_guid: GptGuid::new_random(),
            data_guid: GptGuid::new_random(),
        }
    }
}

impl Nexus {
    /// Partition Type GUID for our "MayaMeta" partition.
    pub const METADATA_PARTITION_TYPE_ID: &'static str =
        "27663382-e5e6-11e9-81b4-ca5ca5ca5ca5";
    pub const METADATA_PARTITION_SIZE: u64 = 4 * 1024 * 1024;

    /// Generate a new nexus label based on the nexus configuration.
    pub(crate) fn generate_label(
        config: &LabelConfig,
        block_size: u32,
        data_blocks: u64,
        total_blocks: u64,
    ) -> Result<NexusLabel, LabelError> {
        // (Protective) MBR
        let mut pmbr = Pmbr::default();
        pmbr.entries[0].protect(total_blocks);

        // Primary GPT header
        let mut header =
            GptHeader::new(block_size, total_blocks, config.disk_guid);

        // Partition table
        let partitions = Nexus::create_maya_partitions(
            config,
            &header,
            block_size,
            data_blocks,
        )?;

        header.table_crc = GptEntry::checksum(&partitions, header.num_entries);
        header.checksum();

        // Secondary GPT header
        let backup = header.to_backup();

        Ok(NexusLabel {
            status: NexusLabelStatus::Neither,
            mbr: pmbr,
            primary: header,
            partitions,
            secondary: backup,
        })
    }

    /// Create partition table entries for the MayaMeta and
    /// MayaData partitions based on the nexus configuration.
    #[allow(clippy::vec_init_then_push)]
    fn create_maya_partitions(
        config: &LabelConfig,
        header: &GptHeader,
        block_size: u32,
        data_blocks: u64,
    ) -> Result<Vec<GptEntry>, LabelError> {
        let metadata_size = Aligned::get_blocks(
            Nexus::METADATA_PARTITION_SIZE,
            u64::from(block_size),
        );
        let data = header.lba_start + metadata_size;

        if data > header.lba_end {
            // Device is too small to accomodate Metadata partition
            return Err(LabelError::DeviceTooSmall {
                blocks: header.lba_alt + 1,
            });
        }

        let mut partitions: Vec<GptEntry> = Vec::with_capacity(2);

        partitions.push(GptEntry {
            ent_type: GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID)
                .unwrap(),
            ent_guid: config.meta_guid,
            ent_start: header.lba_start,
            ent_end: data - 1,
            ent_attr: 0,
            ent_name: "MayaMeta".into(),
        });

        partitions.push(GptEntry {
            ent_type: GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID)
                .unwrap(),
            ent_guid: config.data_guid,
            ent_start: data,
            ent_end: min(data + data_blocks - 1, header.lba_end),
            ent_attr: 0,
            ent_name: "MayaData".into(),
        });

        Ok(partitions)
    }
}

/// based on RFC4122
#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone, Copy)]
pub struct GptGuid {
    pub time_low: u32,
    pub time_mid: u16,
    pub time_high: u16,
    pub node: [u8; 8],
}

impl From<Uuid> for GptGuid {
    fn from(uuid: Uuid) -> GptGuid {
        let fields = uuid.as_fields();
        GptGuid {
            time_low: fields.0,
            time_mid: fields.1,
            time_high: fields.2,
            node: *fields.3,
        }
    }
}

impl From<GptGuid> for Uuid {
    fn from(guid: GptGuid) -> Uuid {
        Uuid::from_fields(
            guid.time_low,
            guid.time_mid,
            guid.time_high,
            &guid.node,
        )
        .unwrap()
    }
}

impl FromStr for GptGuid {
    type Err = parser::ParseError;

    fn from_str(uuid: &str) -> Result<Self, Self::Err> {
        Ok(GptGuid::from(Uuid::from_str(uuid)?))
    }
}

impl std::fmt::Display for GptGuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Uuid::from(*self).to_string())
    }
}

impl GptGuid {
    pub(crate) fn new_random() -> Self {
        GptGuid::from(Uuid::new_v4())
    }
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Copy, Clone)]
pub struct GptHeader {
    /// GPT signature (must be "EFI PART").
    pub signature: [u8; 8],
    /// 00 00 01 00 up til version 2.17
    pub revision: [u8; 4],
    /// GPT header size (92 bytes)
    pub header_size: u32,
    /// CRC32 of the header.
    pub self_checksum: u32,
    pub reserved: [u8; 4],
    /// primary lba where the header
    pub lba_self: u64,
    /// alternative lba
    pub lba_alt: u64,
    /// first usable lba
    pub lba_start: u64,
    /// last usable lba
    pub lba_end: u64,
    /// 16 bytes representing the GUID of the GPT.
    pub guid: GptGuid,
    /// lba of where to find the partition table
    pub lba_table: u64,
    /// number of partitions, most tools set this to 128
    pub num_entries: u32,
    /// Size of element
    pub entry_size: u32,
    /// CRC32 checksum of the partition array.
    pub table_crc: u32,
}

impl GptHeader {
    pub const PARTITION_TABLE_SIZE: u64 = 128 * 128;

    /// converts a slice into a gpt header and verifies the validity of the data
    pub fn from_slice(slice: &[u8]) -> Result<GptHeader, ProbeError> {
        let mut reader = Cursor::new(slice);
        let mut gpt: GptHeader =
            deserialize_from(&mut reader).context(DeserializeError {})?;

        if gpt.header_size != 92 {
            return Err(ProbeError::GptHeaderSize {
                actual_size: gpt.header_size,
                expected_size: 92,
            });
        }

        if gpt.signature != [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54]
            || gpt.revision != [0x00, 0x00, 0x01, 0x00]
        {
            return Err(ProbeError::GptSignature {});
        }

        let checksum = gpt.self_checksum;

        if gpt.checksum() != checksum {
            return Err(ProbeError::GptChecksum {});
        }

        Ok(gpt)
    }

    /// checksum the header with the checksum field itself set to 0
    pub fn checksum(&mut self) -> u32 {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(&self).unwrap());
        self.self_checksum
    }

    // Create a new GPT header for a device with specified size
    pub fn new(block_size: u32, num_blocks: u64, guid: GptGuid) -> Self {
        let partition_size = Aligned::get_blocks(
            GptHeader::PARTITION_TABLE_SIZE,
            u64::from(block_size),
        );

        let start = u64::from((1 << 20) / block_size);

        GptHeader {
            signature: [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54],
            revision: [0x00, 0x00, 0x01, 0x00],
            header_size: 92,
            self_checksum: 0,
            reserved: [0; 4],
            lba_self: 1,
            lba_alt: num_blocks - 1,
            lba_start: start,
            lba_end: num_blocks - partition_size - 2,
            guid,
            lba_table: 2,
            num_entries: 2,
            entry_size: 128,
            table_crc: 0,
        }
    }

    // Create a reference GPT header for a device of sufficient
    // size to have the requisite number of data blocks
    pub fn reference(block_size: u32, data_blocks: u64, guid: GptGuid) -> Self {
        let partition_size = Aligned::get_blocks(
            GptHeader::PARTITION_TABLE_SIZE,
            u64::from(block_size),
        );

        let metadata_size = Aligned::get_blocks(
            Nexus::METADATA_PARTITION_SIZE,
            u64::from(block_size),
        );

        let start = u64::from((1 << 20) / block_size);
        let table = start + metadata_size + data_blocks;
        let last = table + partition_size;

        GptHeader {
            signature: [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54],
            revision: [0x00, 0x00, 0x01, 0x00],
            header_size: 92,
            self_checksum: 0,
            reserved: [0; 4],
            lba_self: 1,
            lba_alt: last,
            lba_start: start,
            lba_end: table - 1,
            guid,
            lba_table: 2,
            num_entries: 2,
            entry_size: 128,
            table_crc: 0,
        }
    }

    pub fn to_backup(&self) -> Self {
        let mut secondary = *self;
        secondary.lba_self = self.lba_alt;
        secondary.lba_alt = self.lba_self;
        secondary.lba_table = self.lba_end + 1;
        secondary.checksum();
        secondary
    }

    pub fn to_primary(&self) -> Self {
        let mut primary = *self;
        primary.lba_self = self.lba_alt;
        primary.lba_alt = self.lba_self;
        primary.lba_table = self.lba_alt + 1;
        primary.checksum();
        primary
    }
}

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone)]
pub struct GptEntry {
    /// GUID type, some of them are assigned/reserved for example to Linux
    pub ent_type: GptGuid,
    /// entry GUID, can be anything typically random
    pub ent_guid: GptGuid,
    /// start lba for this entry
    pub ent_start: u64,
    /// end lba for this entry
    pub ent_end: u64,
    /// entry attributes, according to do the docs bit 0 MUST be zero
    pub ent_attr: u64,
    /// UTF-16 name of the partition entry,
    /// DO NOT confuse this with filesystem labels!
    pub ent_name: GptName,
}

impl GptEntry {
    /// converts a slice into a partition table
    pub fn from_slice(
        slice: &[u8],
        count: u32,
    ) -> Result<Vec<GptEntry>, ProbeError> {
        let mut reader = Cursor::new(slice);
        let mut partitions: Vec<GptEntry> = Vec::with_capacity(count as usize);
        for _ in 0 .. count {
            partitions.push(
                deserialize_from(&mut reader).context(DeserializeError {})?,
            );
        }
        Ok(partitions)
    }

    /// calculate the checksum over the partition table
    pub fn checksum(partitions: &[GptEntry], size: u32) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        let count = partitions.len() as u32;
        for entry in partitions {
            digest.write(&serialize(entry).unwrap());
        }
        if count < size {
            let pad = serialize(&GptEntry::default()).unwrap();
            for _ in count .. size {
                digest.write(&pad);
            }
        }
        digest.sum32()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum NexusLabelStatus {
    /// Both primary and secondary labels are synced with disk.
    Both,
    /// Only primary label is synced with disk.
    Primary,
    /// Only secondary label is synced with disk.
    Secondary,
    /// Neither primary or secondary labels are synced with disk.
    Neither,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
/// The nexus label is standard GPT label (such that you can use it without us
/// in the data path) The only thing that is really specific to us is the
/// ent_type GUID if we see that attached to a partition, we assume the data in
/// that partition is ours. In the data we will have more magic markers to
/// confirm the assumption but this is step one.
pub struct NexusLabel {
    /// The status of the Nexus labels
    pub status: NexusLabelStatus,
    /// The protective MBR
    pub mbr: Pmbr,
    /// The main GPT header
    pub primary: GptHeader,
    /// Vector of GPT entries where the first element is considered to be ours
    pub partitions: Vec<GptEntry>,
    /// The backup GPT header
    pub secondary: GptHeader,
}

impl NexusLabel {
    /// update label with new disk guid
    fn set_guid(&mut self, guid: GptGuid) {
        self.primary.guid = guid;
        self.primary.checksum();
        self.secondary = self.primary.to_backup();
        self.status = NexusLabelStatus::Neither;
    }

    /// locate a partition by name
    fn get_partition(&self, name: &str) -> Option<&GptEntry> {
        self.partitions
            .iter()
            .find(|entry| entry.ent_name.name == name)
    }

    #[allow(dead_code)]
    /// returns the offset of the first metadata block
    pub(crate) fn metadata_offset(&self) -> Result<u64, ProbeError> {
        match self.get_partition("MayaMeta") {
            Some(entry) => Ok(entry.ent_start),
            None => Err(ProbeError::MissingPartition {
                name: "MayaMeta".into(),
            }),
        }
    }

    #[allow(dead_code)]
    /// returns the offset of the first data block
    pub(crate) fn data_offset(&self) -> Result<u64, ProbeError> {
        match self.get_partition("MayaData") {
            Some(entry) => Ok(entry.ent_start),
            None => Err(ProbeError::MissingPartition {
                name: "MayaData".into(),
            }),
        }
    }

    #[allow(dead_code)]
    /// returns the total number of metadata blocks
    pub(crate) fn metadata_block_count(&self) -> Result<u64, ProbeError> {
        match self.get_partition("MayaMeta") {
            Some(entry) => Ok(entry.ent_end - entry.ent_start + 1),
            None => Err(ProbeError::MissingPartition {
                name: "MayaMeta".into(),
            }),
        }
    }

    /// returns the total number of data blocks
    pub(crate) fn data_block_count(&self) -> Result<u64, ProbeError> {
        match self.get_partition("MayaData") {
            Some(entry) => Ok(entry.ent_end - entry.ent_start + 1),
            None => Err(ProbeError::MissingPartition {
                name: "MayaData".into(),
            }),
        }
    }

    /// get current label config
    pub fn get_label_config(&self) -> Option<LabelConfig> {
        if let Some(meta) = self.get_partition("MayaMeta") {
            if let Some(data) = self.get_partition("MayaData") {
                return Some(LabelConfig {
                    disk_guid: self.primary.guid,
                    meta_guid: meta.ent_guid,
                    data_guid: data.ent_guid,
                });
            }
        }
        None
    }
}

impl Display for NexusLabel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "GUID: {}", self.primary.guid.to_string())?;

        writeln!(
            f,
            "Primary GPT header crc32: {:08x}",
            self.primary.self_checksum
        )?;
        writeln!(f, "LBA primary GPT header: {}", self.primary.lba_self)?;
        writeln!(f, "LBA primary partition table: {}", self.primary.lba_table)?;

        writeln!(
            f,
            "Secondary GPT header crc32: {:08x}",
            self.secondary.self_checksum
        )?;
        writeln!(f, "LBA secondary GPT header: {}", self.secondary.lba_self)?;
        writeln!(
            f,
            "LBA secondary partition table: {}",
            self.secondary.lba_table
        )?;

        writeln!(f, "Partition table crc32: {:08x}", self.primary.table_crc)?;
        writeln!(f, "LBA first usable block: {}", self.primary.lba_start)?;
        writeln!(f, "LBA last usable block: {}", self.primary.lba_end)?;

        for i in 0 .. self.partitions.len() {
            writeln!(f, "  Partition {}", i)?;
            writeln!(
                f,
                "    GUID: {}",
                self.partitions[i].ent_guid.to_string()
            )?;
            writeln!(
                f,
                "    Type GUID: {}",
                self.partitions[i].ent_type.to_string()
            )?;
            writeln!(f, "    LBA start: {}", self.partitions[i].ent_start)?;
            writeln!(f, "    LBA end: {}", self.partitions[i].ent_end)?;
            writeln!(f, "    Name: {}", self.partitions[i].ent_name.name)?;
        }

        Ok(())
    }
}

// For arrays bigger than 32 elements, things start to get unimplemented
// in terms of derive and what not. So we create our own "newtype" struct,
// and tell serde how to use it during serializing/deserializing.
#[derive(Debug, PartialEq, Default, Clone)]
pub struct GptName {
    pub name: String,
}

struct GpEntryNameVisitor;

impl<'a> Deserialize<'a> for GptName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_tuple_struct("GptName", 36, GpEntryNameVisitor)
    }
}

impl Serialize for GptName {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // we can't use serialize_type_struct here as we want exactly 72 bytes
        let mut s = serializer.serialize_tuple(36)?;
        let mut out: Vec<u16> = vec![0; 36];
        for (i, o) in self.name.encode_utf16().zip(out.iter_mut()) {
            *o = i;
        }

        out.iter().for_each(|e| s.serialize_element(&e).unwrap());
        s.end()
    }
}
impl<'a> Visitor<'a> for GpEntryNameVisitor {
    type Value = GptName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Invalid GPT partition name")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<GptName, A::Error>
    where
        A: SeqAccess<'a>,
    {
        let mut out = Vec::new();
        let mut end = false;
        loop {
            match seq.next_element()? {
                Some(0) => {
                    end = true;
                }
                Some(e) if !end => out.push(e),
                _ => break,
            }
        }

        if end {
            Ok(GptName::from(String::from_utf16_lossy(&out)))
        } else {
            Err(serde::de::Error::invalid_value(Unexpected::Seq, &self))
        }
    }
}

impl From<String> for GptName {
    fn from(name: String) -> GptName {
        GptName {
            name,
        }
    }
}

impl From<&str> for GptName {
    fn from(name: &str) -> GptName {
        GptName::from(String::from(name))
    }
}

/// Although we don't use it, we must have a protective MBR to avoid systems
/// to get confused about what's on the disk. Utils like sgdisk work fine
/// without an MBR (but will warn) but as we want to be able to access the
/// partitions with the nexus out of the data path, will create one here.
///
/// The struct should have a 440 byte code section here as well, this is
/// omitted to make serialisation a bit easier.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Pmbr {
    /// signature to uniquely ID the disk we do not use this
    disk_signature: u32,
    reserved: u16,
    /// number of partition entries
    entries: [MbrEntry; 4],
    /// must be set to [0x55, 0xaa]
    signature: [u8; 2],
}

/// the MBR partition entry
#[derive(Copy, Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct MbrEntry {
    /// attributes of this MBR partition we set these all to zero, which
    /// includes the boot flag.
    attributes: u8,
    /// start in CHS format
    chs_start: [u8; 3],
    /// type of partition, in our case always 0xEE
    ent_type: u8,
    /// end of the partition
    chs_last: [u8; 3],
    /// lba start
    lba_start: u32,
    /// last sector of this partition
    num_sectors: u32,
}

impl MbrEntry {
    // Set this MBR partition entry to represent
    // a protective MBR partition of given size.
    fn protect(&mut self, num_blocks: u64) {
        self.attributes = 0x00; // NOT bootable
        self.ent_type = 0xee; // protective MBR partition
        self.chs_start = [0x00, 0x02, 0x00]; // CHS address 0/0/2
        self.chs_last = [0xff, 0xff, 0xff]; // CHS address 1023/255/63

        // The partition starts immediately after the MBR
        self.lba_start = 1;

        // The partition size must accurately reflect
        // the disk size where possible.
        if num_blocks > u32::max_value().into() {
            // If the size (in blocks) is too large to fit into 32 bits,
            // then set the size to 0xffff_ffff
            self.num_sectors = u32::max_value();
        } else {
            // Do not count the first block that contains the MBR
            self.num_sectors = (num_blocks - 1) as u32;
        }
    }
}

impl Pmbr {
    /// converts a slice into a MBR and validates the signature
    pub fn from_slice(slice: &[u8]) -> Result<Pmbr, ProbeError> {
        let mut reader = Cursor::new(slice);
        let mbr: Pmbr =
            deserialize_from(&mut reader).context(DeserializeError {})?;

        if mbr.signature != [0x55, 0xaa] {
            return Err(ProbeError::MbrSignature {});
        }

        Ok(mbr)
    }
}

impl Default for Pmbr {
    fn default() -> Self {
        Pmbr {
            disk_signature: 0,
            reserved: 0,
            entries: [MbrEntry::default(); 4],
            signature: [0x55, 0xaa],
        }
    }
}

impl NexusLabel {
    /// construct a Pmbr from raw data
    fn read_mbr(buf: &DmaBuf) -> Result<Pmbr, ProbeError> {
        Pmbr::from_slice(&buf.as_slice()[440 .. 512])
    }

    /// construct a GPT header from raw data
    fn read_header(buf: &DmaBuf) -> Result<GptHeader, ProbeError> {
        GptHeader::from_slice(buf.as_slice())
    }

    /// construct and validate primary GPT header
    fn read_primary_header(
        buf: &DmaBuf,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<GptHeader, ProbeError> {
        let header = NexusLabel::read_header(buf)?;
        NexusLabel::validate_primary_header(&header, block_size, num_blocks)?;
        Ok(header)
    }

    /// construct and validate secondary GPT header
    fn read_secondary_header(
        buf: &DmaBuf,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<GptHeader, ProbeError> {
        let header = NexusLabel::read_header(buf)?;
        NexusLabel::validate_secondary_header(&header, block_size, num_blocks)?;
        Ok(header)
    }

    /// construct and validate partition table
    fn read_partitions(
        buf: &DmaBuf,
        header: &GptHeader,
    ) -> Result<Vec<GptEntry>, ProbeError> {
        let partitions =
            GptEntry::from_slice(buf.as_slice(), header.num_entries)?;
        NexusLabel::validate_partitions(&partitions, header)?;
        Ok(partitions)
    }

    /// check that primary GPT header is valid and consistent
    fn validate_primary_header(
        primary: &GptHeader,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<(), ProbeError> {
        if primary.lba_self != 1 {
            return Err(ProbeError::PrimaryLocation {});
        }
        if primary.lba_alt + 1 != num_blocks {
            return Err(ProbeError::SecondaryLocation {});
        }
        if primary.lba_end >= primary.lba_alt {
            return Err(ProbeError::LastUsableBlock {});
        }
        if primary.lba_table != primary.lba_self + 1 {
            return Err(ProbeError::PartitionTableLocation {});
        }
        if (primary.num_entries * primary.entry_size) as u64
            > GptHeader::PARTITION_TABLE_SIZE
        {
            return Err(ProbeError::PartitionTableSize {});
        }
        if primary.lba_table
            + Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size)
            > primary.lba_start
        {
            return Err(ProbeError::PartitionTableSpace {});
        }
        Ok(())
    }

    /// check that secondary GPT header is valid and consistent
    fn validate_secondary_header(
        secondary: &GptHeader,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<(), ProbeError> {
        if secondary.lba_alt != 1 {
            return Err(ProbeError::PrimaryLocation {});
        }
        if secondary.lba_self + 1 != num_blocks {
            return Err(ProbeError::SecondaryLocation {});
        }
        if secondary.lba_alt >= secondary.lba_start {
            return Err(ProbeError::FirstUsableBlock {});
        }
        if secondary.lba_table != secondary.lba_end + 1 {
            return Err(ProbeError::PartitionTableLocation {});
        }
        if (secondary.num_entries * secondary.entry_size) as u64
            > GptHeader::PARTITION_TABLE_SIZE
        {
            return Err(ProbeError::PartitionTableSize {});
        }
        if secondary.lba_table
            + Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size)
            > secondary.lba_self
        {
            return Err(ProbeError::PartitionTableSpace {});
        }
        Ok(())
    }

    /// check that partition table entries are valid and consistent
    fn validate_partitions(
        partitions: &[GptEntry],
        header: &GptHeader,
    ) -> Result<(), ProbeError> {
        for entry in partitions {
            if 0 < entry.ent_start && entry.ent_start < header.lba_start {
                return Err(ProbeError::PartitionStart {});
            }
            if entry.ent_start > entry.ent_end {
                return Err(ProbeError::NegativePartitionSize {});
            }
            if entry.ent_end > header.lba_end {
                return Err(ProbeError::PartitionEnd {});
            }
        }
        if GptEntry::checksum(partitions, header.num_entries)
            != header.table_crc
        {
            return Err(ProbeError::PartitionTableChecksum {});
        }
        Ok(())
    }

    /// check that primary and secondary GPT headers
    /// are consistent with each other
    fn consistency_check(
        primary: &GptHeader,
        secondary: &GptHeader,
    ) -> Result<(), ProbeError> {
        if primary.lba_self != secondary.lba_alt {
            return Err(ProbeError::CompareHeaderLocation {});
        }
        if primary.lba_alt != secondary.lba_self {
            return Err(ProbeError::CompareHeaderLocation {});
        }
        if primary.lba_start != secondary.lba_start {
            return Err(ProbeError::FirstUsableBlock {});
        }
        if primary.lba_end != secondary.lba_end {
            return Err(ProbeError::LastUsableBlock {});
        }
        if primary.guid != secondary.guid {
            return Err(ProbeError::CompareDiskGuid {});
        }
        if primary.num_entries != secondary.num_entries {
            return Err(ProbeError::ComparePartitionEntryCount {});
        }
        if primary.entry_size != secondary.entry_size {
            return Err(ProbeError::ComparePartitionEntrySize {});
        }
        if primary.table_crc != secondary.table_crc {
            return Err(ProbeError::ComparePartitionTableChecksum {});
        }
        Ok(())
    }
}

impl NexusChild {
    /// read and validate this child's label
    pub async fn probe_label(&self) -> Result<NexusLabel, LabelError> {
        let handle = self.get_io_handle().context(HandleError {
            name: self.get_name().clone(),
        })?;

        let bdev = handle.get_device();
        let block_size = u64::from(bdev.block_len());
        let num_blocks = bdev.num_blocks();

        // Protective MBR
        let mut buf = handle.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("header"),
        })?;
        handle.read_at(0, &mut buf).await.context(ReadError {
            name: String::from("MBR"),
        })?;
        let mbr = NexusLabel::read_mbr(&buf).context(InvalidLabel {})?;

        // GPT headers

        let status: NexusLabelStatus;
        let primary: GptHeader;
        let secondary: GptHeader;
        let active: &GptHeader;

        // Get primary GPT header.
        handle
            .read_at(block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("primary GPT header"),
            })?;
        match NexusLabel::read_primary_header(&buf, block_size, num_blocks) {
            Ok(header) => {
                primary = header;
                active = &primary;
                // Get secondary GPT header.
                let offset = (num_blocks - 1) * block_size;
                handle.read_at(offset, &mut buf).await.context(ReadError {
                    name: String::from("secondary GPT header"),
                })?;
                match NexusLabel::read_secondary_header(
                    &buf, block_size, num_blocks,
                ) {
                    Ok(header) => {
                        NexusLabel::consistency_check(&primary, &header)
                            .context(InvalidLabel {})?;
                        // All good - primary and secondary GTP headers
                        // are valid and consistent with each other.
                        secondary = header;
                        status = NexusLabelStatus::Both;
                    }
                    Err(_) => {
                        // Secondary GPT header is either not present
                        // or invalid. Construct new secondary
                        // GPT header from primary.
                        secondary = primary.to_backup();
                        status = NexusLabelStatus::Primary;
                    }
                }
            }
            Err(error) => {
                // Primary GPT header is either not present or invalid.
                // See if we can obtain a valid secondary GPT header.
                let offset = (num_blocks - 1) * block_size;
                handle.read_at(offset, &mut buf).await.context(ReadError {
                    name: String::from("secondary GPT header"),
                })?;
                match NexusLabel::read_secondary_header(
                    &buf, block_size, num_blocks,
                ) {
                    Ok(header) => {
                        secondary = header;
                        active = &secondary;
                        // Construct new primary GPT header from secondary.
                        primary = secondary.to_primary();
                        status = NexusLabelStatus::Secondary;
                    }
                    Err(_) => {
                        // Neither primary or secondary GPT header
                        // is present or valid.
                        return Err(LabelError::InvalidLabel {
                            source: error,
                        });
                    }
                }
            }
        }

        // The disk size recorded in protective MBR
        // must be consistent with GPT header.
        if mbr.entries[0].num_sectors != 0xffff_ffff
            && u64::from(mbr.entries[0].num_sectors) != primary.lba_alt
        {
            return Err(LabelError::InvalidLabel {
                source: ProbeError::MbrSize {},
            });
        }

        // Partition table
        let blocks = Aligned::get_blocks(
            u64::from(active.entry_size * active.num_entries),
            block_size,
        );
        let mut buf =
            handle.dma_malloc(blocks * block_size).context(ReadAlloc {
                name: String::from("partition table"),
            })?;
        let offset = active.lba_table * block_size;
        handle.read_at(offset, &mut buf).await.context(ReadError {
            name: String::from("partition table"),
        })?;
        let mut partitions = NexusLabel::read_partitions(&buf, active)
            .context(InvalidLabel {})?;

        // There can be up to 128 partition entries stored on disk,
        // even though most are not used. Retain only those entries
        // that actually define partitions.
        partitions.retain(|entry| entry.ent_start > 0 && entry.ent_end > 0);

        Ok(NexusLabel {
            status,
            mbr,
            primary,
            partitions,
            secondary,
        })
    }

    // Check for the presence of "MayaMeta" and "MayaData" partitions
    fn check_maya_partitions(
        reference: &[GptEntry],
        label: &NexusLabel,
        block_size: u32,
    ) -> bool {
        match label.get_partition("MayaMeta") {
            Some(entry) => {
                if entry.ent_start != reference[0].ent_start {
                    return false;
                }
                if entry.ent_end != reference[0].ent_end {
                    return false;
                }
                if (entry.ent_end - entry.ent_start + 1) * u64::from(block_size)
                    < Nexus::METADATA_PARTITION_SIZE
                {
                    return false;
                }
            }
            None => {
                return false;
            }
        }

        if let Some(entry) = label.get_partition("MayaData") {
            if entry.ent_start == reference[1].ent_start {
                return true;
            }
        }

        false
    }

    /// Create a new label on this child
    async fn create_label(
        &mut self,
        config: &LabelConfig,
        block_size: u32,
        data_blocks: u64,
        total_blocks: u64,
    ) -> Result<NexusLabel, LabelError> {
        info!("creating new label for child {}", self.get_name());
        let label = Nexus::generate_label(
            config,
            block_size,
            data_blocks,
            total_blocks,
        )?;
        self.write_label(&label).await?;
        Ok(label)
    }

    /// Create or Update label on this child as and when necessary
    async fn update_label(
        &mut self,
        reference: &[GptEntry],
        config: &LabelConfig,
        block_size: u32,
        data_blocks: u64,
        total_blocks: u64,
    ) -> Result<NexusLabel, LabelError> {
        match self.probe_label().await {
            Ok(mut label)
                if NexusChild::check_maya_partitions(
                    reference, &label, block_size,
                ) =>
            {
                // Use existing label
                if label.primary.guid != config.disk_guid {
                    info!("updating existing label for child {}: setting guid to {}", self.get_name(), config.disk_guid);
                    label.set_guid(config.disk_guid);
                }
                self.write_label(&label).await?;
                Ok(label)
            }
            Ok(_) => {
                // Replace existing label
                self.create_label(config, block_size, data_blocks, total_blocks)
                    .await
            }
            Err(LabelError::InvalidLabel {
                ..
            }) => {
                // Create new label
                self.create_label(config, block_size, data_blocks, total_blocks)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    /// Validate label on this child
    async fn validate_label(
        &self,
        reference: &[GptEntry],
        block_size: u32,
    ) -> Result<NexusLabel, LabelError> {
        let label = self.probe_label().await?;

        if !NexusChild::check_maya_partitions(reference, &label, block_size) {
            return Err(LabelError::InvalidLabel {
                source: ProbeError::IncorrectPartitions {},
            });
        }

        if label.status != NexusLabelStatus::Both {
            return Err(LabelError::InvalidLabel {
                source: ProbeError::LabelRedundancy {},
            });
        }

        Ok(label)
    }
}

impl Nexus {
    /// Validate label on each child device
    pub(crate) async fn validate_child_labels(
        &mut self,
    ) -> Result<(), LabelError> {
        let guid = GptGuid::from(Uuid::from_bytes(self.bdev.uuid().as_bytes()));
        let config = LabelConfig::new(guid);

        let block_size = self.bdev.block_len();
        let nexus_blocks = self.size / u64::from(block_size);
        let mut min_blocks = nexus_blocks;

        // Generate "reference" partition table entries
        let header = GptHeader::reference(block_size, nexus_blocks, guid);
        let reference = Nexus::create_maya_partitions(
            &config,
            &header,
            block_size,
            nexus_blocks,
        )?;
        let data_offset = reference[1].ent_start;

        for child in self.children.iter_mut() {
            let handle = child.get_io_handle().context(HandleError {
                name: child.get_name().clone(),
            })?;

            let bdev = handle.get_device();
            let label = child
                .validate_label(&reference, bdev.block_len() as u32)
                .await?;
            let data_blocks =
                label.data_block_count().context(InvalidLabel {})?;

            // Adjust size of data partition if necessary
            if data_blocks < min_blocks {
                min_blocks = data_blocks;
            }
        }

        // Update the nexus size
        self.data_ent_offset = data_offset;
        self.bdev.set_block_count(min_blocks);

        Ok(())
    }

    // Get configuration from first valid label with specified disk guid
    async fn find_label_config(
        &self,
        guid: GptGuid,
    ) -> Result<Option<LabelConfig>, LabelError> {
        for child in self.children.iter() {
            match child.probe_label().await {
                Ok(label) => {
                    if label.primary.guid != guid {
                        continue;
                    }
                    if let Some(config) = label.get_label_config() {
                        return Ok(Some(config));
                    }
                }
                Err(LabelError::InvalidLabel {
                    ..
                }) => {
                    // Label is most likely not present or possibly invalid.
                    continue;
                }
                Err(error) => {
                    // Any other errors are fatal.
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    /// Create or Update label on each child device as and when necessary
    pub(crate) async fn update_child_labels(
        &mut self,
    ) -> Result<(), LabelError> {
        let guid = GptGuid::from(Uuid::from_bytes(self.bdev.uuid().as_bytes()));
        let config = self
            .find_label_config(guid)
            .await?
            .unwrap_or_else(|| LabelConfig::new(guid));

        let block_size = self.bdev.block_len();
        let nexus_blocks = self.size / u64::from(block_size);

        // Generate "reference" partition table entries
        let header = GptHeader::reference(block_size, nexus_blocks, guid);
        let reference = Nexus::create_maya_partitions(
            &config,
            &header,
            block_size,
            nexus_blocks,
        )?;

        for child in self.children.iter_mut() {
            let handle = child.get_io_handle().context(HandleError {
                name: child.get_name().clone(),
            })?;

            let bdev = handle.get_device();
            child
                .update_label(
                    &reference,
                    &config,
                    bdev.block_len() as u32,
                    nexus_blocks,
                    bdev.num_blocks(),
                )
                .await?;
        }

        Ok(())
    }

    /// Create a new label on each child device.
    /// DO NOT check for existing labels and ALWAYS write a new label.
    pub(crate) async fn create_child_labels(
        &mut self,
    ) -> Result<(), LabelError> {
        let guid = GptGuid::from(Uuid::from_bytes(self.bdev.uuid().as_bytes()));
        let config = LabelConfig::new(guid);

        let block_size = self.bdev.block_len();
        let nexus_blocks = self.size / u64::from(block_size);
        let mut min_blocks = nexus_blocks;

        // Generate "reference" partition table entries
        let header = GptHeader::reference(block_size, nexus_blocks, guid);
        let reference = Nexus::create_maya_partitions(
            &config,
            &header,
            block_size,
            nexus_blocks,
        )?;
        let data_offset = reference[1].ent_start;

        for child in self.children.iter_mut() {
            let handle = child.get_io_handle().context(HandleError {
                name: child.get_name().clone(),
            })?;

            let bdev = handle.get_device();
            let label = child
                .create_label(
                    &config,
                    bdev.block_len() as u32,
                    nexus_blocks,
                    bdev.num_blocks(),
                )
                .await?;
            let data_blocks =
                label.data_block_count().context(InvalidLabel {})?;

            // Adjust size of data partition if necessary
            if data_blocks < min_blocks {
                min_blocks = data_blocks;
            }
        }

        // Update the nexus size
        self.data_ent_offset = data_offset;
        self.bdev.set_block_count(min_blocks);

        Ok(())
    }
}

struct LabelData {
    offset: u64,
    buf: DmaBuf,
}

impl NexusChild {
    /// generate raw data for (primary) label ready to be written to disk
    fn get_primary_data(
        &self,
        label: &NexusLabel,
    ) -> Result<LabelData, LabelError> {
        let handle = self.get_io_handle().context(HandleError {
            name: self.get_name().clone(),
        })?;

        let bdev = handle.get_device();
        let block_size = u64::from(bdev.block_len());

        let mut buf =
            DmaBuf::new(label.primary.lba_start * block_size, bdev.alignment())
                .context(WriteAlloc {
                    name: String::from("primary"),
                })?;

        let mut writer = Cursor::new(buf.as_mut_slice());

        // Protective MBR
        writer.seek(SeekFrom::Start(440)).unwrap();
        serialize_into(&mut writer, &label.mbr).context(SerializeError {})?;

        // Primary GPT header
        writer
            .seek(SeekFrom::Start(label.primary.lba_self * block_size))
            .unwrap();
        serialize_into(&mut writer, &label.primary)
            .context(SerializeError {})?;

        // Primary partition table
        writer
            .seek(SeekFrom::Start(label.primary.lba_table * block_size))
            .unwrap();
        for entry in label.partitions.iter() {
            serialize_into(&mut writer, &entry).context(SerializeError {})?;
        }

        Ok(LabelData {
            offset: 0,
            buf,
        })
    }

    /// generate raw data for (secondary) label ready to be written to disk
    fn get_secondary_data(
        &self,
        label: &NexusLabel,
    ) -> Result<LabelData, LabelError> {
        let handle = self.get_io_handle().context(HandleError {
            name: self.get_name().clone(),
        })?;

        let bdev = handle.get_device();
        let block_size = u64::from(bdev.block_len());

        let mut buf = DmaBuf::new(
            (label.secondary.lba_self - label.secondary.lba_table + 1)
                * block_size,
            bdev.alignment(),
        )
        .context(WriteAlloc {
            name: String::from("secondary"),
        })?;

        let mut writer = Cursor::new(buf.as_mut_slice());

        // Secondary partition table
        for entry in label.partitions.iter() {
            serialize_into(&mut writer, &entry).context(SerializeError {})?;
        }

        // Secondary GPT header
        writer
            .seek(SeekFrom::Start(
                (label.secondary.lba_self - label.secondary.lba_table)
                    * block_size,
            ))
            .unwrap();
        serialize_into(&mut writer, &label.secondary)
            .context(SerializeError {})?;

        Ok(LabelData {
            offset: label.secondary.lba_table * block_size,
            buf,
        })
    }

    /// write the contents of the buffer to this child
    async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<u64, LabelError> {
        let handle = self.get_io_handle().context(HandleError {
            name: self.get_name().clone(),
        })?;

        Ok(handle.write_at(offset, buf).await.context(WriteError {
            name: self.get_name().clone(),
        })?)
    }

    pub async fn write_label(
        &self,
        label: &NexusLabel,
    ) -> Result<(), LabelError> {
        match label.status {
            NexusLabelStatus::Both => {
                // Nothing to do as both labels on disk are valid.
            }
            NexusLabelStatus::Primary => {
                // Only write out secondary as disk already has valid primary.
                info!("writing secondary label to child {}", self.get_name());
                let secondary = self.get_secondary_data(label)?;
                self.write_at(secondary.offset, &secondary.buf).await?;
            }
            NexusLabelStatus::Secondary => {
                // Only write out primary as disk already has valid secondary.
                info!("writing primary label to child {}", self.get_name());
                let primary = self.get_primary_data(label)?;
                self.write_at(primary.offset, &primary.buf).await?;
            }
            NexusLabelStatus::Neither => {
                // Write out both labels.
                info!("writing label to child {}", self.get_name());
                let primary = self.get_primary_data(label)?;
                let secondary = self.get_secondary_data(label)?;
                self.write_at(primary.offset, &primary.buf).await?;
                self.write_at(secondary.offset, &secondary.buf).await?;
            }
        }

        Ok(())
    }
}

pub trait Aligned {
    /// Return the (appropriately aligned) number of blocks
    /// representing this size.
    fn get_blocks(size: Self, block_size: Self) -> Self;
}

impl Aligned for u32 {
    fn get_blocks(size: u32, block_size: u32) -> u32 {
        let blocks = size / block_size;
        match size % block_size {
            0 => blocks,
            _ => blocks + 1,
        }
    }
}

impl Aligned for u64 {
    fn get_blocks(size: u64, block_size: u64) -> u64 {
        let blocks = size / block_size;
        match size % block_size {
            0 => blocks,
            _ => blocks + 1,
        }
    }
}
