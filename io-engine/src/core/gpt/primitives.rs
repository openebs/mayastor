//! Generic GPT (GUID Partition Table) primitives.
//!
//! This module contains only the standards-based pieces of GPT handling —
//! header, partition entry, protective MBR, label container, probing,
//! serialisation and validation — with no knowledge of any particular
//! partition layout. The versioned MayaMeta/MayaData layout is
//! built on top of these primitives in [`super::label`].
//!
//! The on-disk format follows the UEFI specification, ie:
//! +---------------------------------------------------------------+
//! |                        LBA 0 (512 B)                          |
//! |                     Protective MBR (PMBR)                     |
//! |        - MBR signature                                        |
//! |        - One partition entry of type 0xEE covering disk       |
//! +---------------------------------------------------------------+
//! |                        LBA 1 (512 B)                          |
//! |                     Primary GPT Header                        |
//! |   "EFI PART" signature | Disk GUID | CRC32 | Table location   |
//! +---------------------------------------------------------------+
//! |                  LBA 2 ... LBA 33 (or more)                   |
//! |                 GPT Partition Entry Array                     |
//! |   [Entry 0] Partition Type GUID | Unique GUID | LBAs | Name   |
//! |   [Entry 1] ...                                               |
//! |   Typically 128 entries × 128 bytes each                      |
//! +---------------------------------------------------------------+
//! |                                                               |
//! |                     Usable Disk Space                         |
//! |                 (Defined by GPT Header)                       |
//! |                                                               |
//! |   +-------------------------------------------------------+   |
//! |   | EFI System Partition (ESP)                            |   |
//! |   | FAT32, ~100–300 MB                                    |   |
//! |   |   /EFI/BOOT/BOOTX64.EFI                               |   |
//! |   |   /EFI/<vendor>/...                                   |   |
//! |   +-------------------------------------------------------+   |
//! |                                                               |
//! |   +-------------------------------------------------------+   |
//! |   | Microsoft Reserved Partition (MSR) (Windows only)     |   |
//! |   | ~16 MB, no filesystem                                 |   |
//! |   +-------------------------------------------------------+   |
//! |                                                               |
//! |   +-------------------------------------------------------+   |
//! |   | OS Partition (NTFS/ext4/etc.)                         |   |
//! |   +-------------------------------------------------------+   |
//! |                                                               |
//! |   +-------------------------------------------------------+   |
//! |   | Recovery / OEM / Vendor Partitions                    |   |
//! |   +-------------------------------------------------------+   |
//! |                                                               |
//! +---------------------------------------------------------------+
//! |            Backup GPT Partition Entry Array (N-32...)         |
//! +---------------------------------------------------------------+
//! |                     Backup GPT Header (Last LBA)              |
//! +---------------------------------------------------------------+

use std::{
    convert::From,
    fmt,
    io::{Cursor, Seek, SeekFrom},
    ops::{Deref, DerefMut},
    str::FromStr,
};

use crate::core::{BlockDeviceHandle, CoreError};

use bincode::{deserialize_from, serialize, serialize_into, Error};
use crc::{crc32, Hasher32};
use serde::{
    de::{Deserializer, SeqAccess, Unexpected, Visitor},
    ser::{SerializeTuple, Serializer},
    Deserialize, Serialize,
};
use snafu::{ResultExt, Snafu};
use spdk_rs::{DmaBuf, DmaError};
use uuid::{self, Uuid};

/// The GPT label error type.
#[derive(Debug, Snafu)]
pub enum LabelError {
    #[snafu(display("Serialization error: {source}"))]
    SerializeError { source: Error },
    #[snafu(display("Failed to allocate buffer for reading {part}: {source}"))]
    ReadAlloc { source: DmaError, part: String },
    #[snafu(display("Failed to allocate buffer for writing {what}: {source}"))]
    WriteAlloc { source: DmaError, what: String },
    #[snafu(display("Error reading {what}: {source}"))]
    ReadError { source: CoreError, what: String },
    #[snafu(display("Error writing {what}: {source}"))]
    WriteError { source: CoreError, what: String },
    #[snafu(display("Label is invalid: {source}"))]
    InvalidLabel { source: ProbeError },
    #[snafu(display("Failed to obtain BlockDeviceHandle: {source}"))]
    HandleError { source: CoreError },
    #[snafu(display(
        "Device is too small to accommodate the requested partition layout: \
         size = {num_blocks} x {block_size}"
    ))]
    DeviceTooSmall { num_blocks: u64, block_size: u64 },
    #[snafu(display("{source}"))]
    SeekError { source: std::io::Error },
}

/// The GPT probe error type.
#[derive(Debug, Snafu)]
pub enum ProbeError {
    #[snafu(display("Serialization error: {source}"))]
    ChecksumSerializeError { source: Error },
    #[snafu(display("Deserialization error: {source}"))]
    DeserializeError { source: Error },
    #[snafu(display("Incorrect MBR signature"))]
    MbrSignature {},
    #[snafu(display("Disk size in MBR does not match size in GPT header"))]
    MbrSize {},
    #[snafu(display("Incorrect GPT header signature"))]
    GptSignature {},
    #[snafu(display("Incorrect GPT header revision"))]
    GptRevision {},
    #[snafu(display("Incorrect GPT header size: actual={actual_size}, expected={expected_size}"))]
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
    #[snafu(display("Missing partition: {name}"))]
    MissingPartition { name: String },
    #[snafu(display("Primary GPT header location is incorrect"))]
    PrimaryLocation {},
    #[snafu(display("Secondary GPT header location is incorrect"))]
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

impl From<ProbeError> for LabelError {
    fn from(error: ProbeError) -> LabelError {
        LabelError::InvalidLabel { source: error }
    }
}

/// Based on RFC4122.
#[derive(Debug, serde::Deserialize, PartialEq, Default, serde::Serialize, Clone, Copy)]
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
        Uuid::from_fields(guid.time_low, guid.time_mid, guid.time_high, &guid.node)
    }
}

impl FromStr for GptGuid {
    type Err = uuid::Error;

    fn from_str(uuid: &str) -> Result<Self, Self::Err> {
        Ok(GptGuid::from(Uuid::from_str(uuid)?))
    }
}

impl fmt::Display for GptGuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Uuid::from(*self))
    }
}

impl GptGuid {
    pub fn new_random() -> Self {
        GptGuid::from(Uuid::new_v4())
    }
}

/// The GPT header structure, as defined in the UEFI specification.
#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Copy, Clone)]
pub struct GptHeader {
    /// GPT signature (must be "EFI PART").
    pub signature: [u8; 8],
    /// 00 00 01 00 up til version 2.17.
    pub revision: [u8; 4],
    /// GPT header size (92 bytes).
    pub header_size: u32,
    /// CRC32 of the header.
    pub self_checksum: u32,
    pub reserved: [u8; 4],
    /// Primary lba where the header.
    pub lba_self: u64,
    /// Alternative lba.
    pub lba_alt: u64,
    /// First usable lba.
    pub lba_start: u64,
    /// Last usable lba.
    pub lba_end: u64,
    /// 16 bytes representing the GUID of the GPT.
    pub guid: GptGuid,
    /// Lba of where to find the partition table.
    pub lba_table: u64,
    /// Number of partitions, most tools set this to 128.
    pub num_entries: u32,
    /// Size of element.
    pub entry_size: u32,
    /// CRC32 checksum of the partition array.
    pub table_crc: u32,
}

impl GptHeader {
    /// The size of the partition table in bytes: 128 entries × 128 bytes each,
    /// the minimum required by the UEFI specification.
    pub const PARTITION_TABLE_SIZE: u64 = 128 * 128;
    /// The byte offset of the first usable LBA (1 MiB). Aligning the first
    /// usable sector to 1 MiB is the conventional default used by most GPT
    /// tools (e.g. fdisk, gdisk) to ensure optimal I/O alignment on SSDs and
    /// RAID arrays.
    pub const DATA_OFFSET: u64 = 1024 * 1024;
    /// The GPT header size in bytes, as defined by the UEFI specification.
    /// The remainder of the first LBA (sector) following the header is reserved.
    pub const HEADER_SIZE: u32 = 92;
    /// The revision of the GPT header, as defined in the UEFI specification.
    pub const HEADER_REVISION: [u8; 4] = [0x00, 0x00, 0x01, 0x00];
    /// The signature of the GPT header, as defined in the UEFI specification.
    pub const HEADER_SIGNATURE: [u8; 8] = [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54];

    /// Converts a slice into a GPT header and verifies the validity of the data.
    pub fn from_slice(slice: &[u8]) -> Result<GptHeader, ProbeError> {
        let mut reader = Cursor::new(slice);

        let mut header: GptHeader = deserialize_from(&mut reader).context(DeserializeSnafu)?;

        if header.header_size != GptHeader::HEADER_SIZE {
            return Err(ProbeError::GptHeaderSize {
                actual_size: header.header_size,
                expected_size: GptHeader::HEADER_SIZE,
            });
        }

        if header.signature != GptHeader::HEADER_SIGNATURE {
            return Err(ProbeError::GptSignature {});
        }

        if header.revision != GptHeader::HEADER_REVISION {
            return Err(ProbeError::GptRevision {});
        }

        let checksum = header.self_checksum;

        if checksum != header.checksum().context(ChecksumSerializeSnafu)? {
            return Err(ProbeError::GptChecksum {});
        }

        Ok(header)
    }

    /// Checksums the header with the checksum field itself set to 0.
    pub fn checksum(&mut self) -> Result<u32, Error> {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(&self)?);
        Ok(self.self_checksum)
    }

    // Creates a new GPT header for a device with specified size.
    pub fn new(guid: GptGuid, block_size: u64, num_blocks: u64) -> Self {
        let partition_blocks = Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size);

        let data_start = Aligned::get_blocks(GptHeader::DATA_OFFSET, block_size);

        GptHeader {
            signature: GptHeader::HEADER_SIGNATURE,
            revision: GptHeader::HEADER_REVISION,
            header_size: GptHeader::HEADER_SIZE,
            self_checksum: 0,
            reserved: [0; 4],
            lba_self: 1,
            lba_alt: num_blocks - 1,
            lba_start: data_start,
            lba_end: num_blocks - partition_blocks - 2,
            guid,
            lba_table: 2,
            num_entries: 128,
            entry_size: 128,
            table_crc: 0,
        }
    }

    /// Derives the secondary GPT header from this (primary) header.
    ///
    /// The secondary header mirrors the primary: `lba_self`/`lba_alt` are
    /// swapped so the secondary points to itself at the last LBA of the disk.
    /// The secondary partition table is placed immediately after `lba_end`
    /// (the last usable LBA), growing backwards toward the secondary header.
    pub fn as_secondary(&self) -> Result<GptHeader, Error> {
        let mut secondary = *self;
        secondary.lba_self = self.lba_alt;
        secondary.lba_alt = self.lba_self;
        secondary.lba_table = self.lba_end + 1;
        secondary.checksum()?;
        Ok(secondary)
    }

    /// Derives the primary GPT header from this (secondary) header.
    ///
    /// This is the inverse of [`as_secondary`](Self::as_secondary): must be
    /// called on a secondary header. `lba_self`/`lba_alt` are swapped so the
    /// result points to LBA 1 (the primary header location), and the primary
    /// partition table is placed at `lba_self + 1` (LBA 2, immediately after
    /// the primary header).
    pub fn as_primary(&self) -> Result<GptHeader, Error> {
        let mut primary = *self;
        primary.lba_self = self.lba_alt;
        primary.lba_alt = self.lba_self;
        primary.lba_table = self.lba_alt + 1;
        primary.checksum()?;
        Ok(primary)
    }
}

// For arrays bigger than 32 elements, things start to get unimplemented
// in terms of derive and what not. So we create our own "newtype" struct,
// and tell serde how to use it during serializing/deserializing.
#[derive(Debug, PartialEq, Default, Clone)]
pub struct GptName {
    pub name: String,
}
impl std::fmt::Display for GptName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
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
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
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
        GptName { name }
    }
}
impl From<&str> for GptName {
    fn from(name: &str) -> GptName {
        GptName::from(String::from(name))
    }
}

/// The GPT partition entry structure, as defined in the UEFI specification.
#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone)]
pub struct GptEntry {
    /// GUID type, some of them are assigned/reserved for example to Linux.
    pub ent_type: GptGuid,
    /// Entry GUID, can be anything typically random.
    pub ent_guid: GptGuid,
    /// Start lba for this entry.
    pub ent_start: u64,
    /// End lba for this entry.
    pub ent_end: u64,
    /// Entry attributes, according to do the docs bit 0 MUST be zero.
    pub ent_attr: u64,
    /// UTF-16 name of the partition entry,
    /// DO NOT confuse this with filesystem labels!
    pub ent_name: GptName,
}

impl GptEntry {
    /// Converts a slice into a partition table.
    pub fn from_slice(slice: &[u8], count: u32) -> Result<Vec<GptEntry>, ProbeError> {
        let mut reader = Cursor::new(slice);
        let mut partitions: Vec<GptEntry> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            partitions.push(deserialize_from(&mut reader).context(DeserializeSnafu)?);
        }
        Ok(partitions)
    }

    /// Checks if the partition entry is unused, which is indicated by a zeroed GUID.
    pub fn is_unused(&self) -> bool {
        self.ent_guid == GptGuid::default()
    }

    /// Calculates the checksum over the partition table.
    pub fn checksum(partitions: &[GptEntry], size: u32) -> Result<u32, Error> {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        let count = partitions.len() as u32;
        for entry in partitions {
            digest.write(&serialize(entry)?);
        }
        if count < size {
            let pad = serialize(&GptEntry::default())?;
            for _ in count..size {
                digest.write(&pad);
            }
        }
        Ok(digest.sum32())
    }
}

/// Although we don't use it, we must have a protective MBR to avoid systems
/// to get confused about what's on the disk. Utils like sgdisk work fine
/// without an MBR (but will warn) but as we want to be able to access the
/// partitions out of the data path, will create one here.
///
/// The struct should have a 440 byte code section here as well,
/// however this is omitted to make serialisation a bit easier.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Pmbr {
    /// Signature to uniquely ID the disk - we do not use this.
    disk_signature: u32,
    reserved: u16,
    /// Number of partition entries.
    pub(super) entries: [MbrEntry; 4],
    /// Must be set to [0x55, 0xaa].
    signature: [u8; 2],
}

/// The MBR partition entry structure, as defined in the UEFI specification.
#[derive(Copy, Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MbrEntry {
    /// Attributes of this MBR partition we set these all to zero, which
    /// includes the boot flag.
    attributes: u8,
    /// Start in CHS format.
    chs_start: [u8; 3],
    /// Type of partition, in our case always 0xEE.
    ent_type: u8,
    /// End of the partition.
    chs_last: [u8; 3],
    /// Lba start.
    lba_start: u32,
    /// Last sector of this partition.
    num_sectors: u32,
}

impl MbrEntry {
    /// Set this MBR partition entry to represent a protective MBR partition of given size.
    pub fn protect(&mut self, num_blocks: u64) {
        self.attributes = 0x00; // NOT bootable
        self.ent_type = 0xee; // protective MBR partition
        self.chs_start = [0x00, 0x02, 0x00]; // CHS address 0/0/2
        self.chs_last = [0xff, 0xff, 0xff]; // CHS address 1023/255/63

        // The partition starts immediately after the MBR
        self.lba_start = 1;

        // The partition size must accurately reflect
        // the disk size where possible.
        if num_blocks > u32::MAX as u64 {
            // If the size (in blocks) is too large to fit into 32 bits,
            // then set the size to 0xffff_ffff
            self.num_sectors = u32::MAX;
        } else {
            // Do not count the first block that contains the MBR
            self.num_sectors = (num_blocks - 1) as u32;
        }
    }
}

impl Pmbr {
    /// The signature of the protective MBR, as defined in the UEFI specification.
    pub const PMBR_SIGNATURE: [u8; 2] = [0x55, 0xaa];

    /// Converts a slice into a MBR and validates the signature.
    pub fn from_slice(slice: &[u8]) -> Result<Pmbr, ProbeError> {
        let mut reader = Cursor::new(slice);

        let mbr: Pmbr = deserialize_from(&mut reader).context(DeserializeSnafu)?;

        if mbr.signature != Pmbr::PMBR_SIGNATURE {
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
            signature: Pmbr::PMBR_SIGNATURE,
        }
    }
}

/// The status of the GPT labels on disk.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum LabelStatus {
    /// Both primary and secondary labels are synced with disk.
    Both,
    /// Only primary label is synced with disk.
    Primary,
    /// Only secondary label is synced with disk.
    Secondary,
    /// Neither primary or secondary labels are synced with disk.
    Neither,
}

/// A standard GPT label: protective MBR, primary header, partition table
/// and secondary header.
///
/// The container is layout-agnostic; consumers may attach any meaningful
/// interpretation to the partitions via sibling modules (e.g.
/// [`super::label`] for the versioned MayaMeta/MayaData layout).
#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct GptLabel {
    /// The status of the labels.
    pub status: LabelStatus,
    /// Block size of underlying device.
    pub block_size: u64,
    /// The protective MBR.
    pub mbr: Pmbr,
    /// The main GPT header.
    pub primary: GptHeader,
    /// Vector of GPT partition entries (only entries that actually define a
    /// partition; padding entries are dropped).
    pub partitions: Vec<GptEntry>,
    /// The backup GPT header.
    pub secondary: GptHeader,
}

impl GptLabel {
    /// Build a new label from a freshly-constructed primary header and a
    /// set of partition entries.
    ///
    /// Computes the partition-table CRC, the header self-CRC and derives
    /// the secondary header from the primary. The protective MBR is
    /// populated to match `num_blocks`.
    pub fn new(
        guid: GptGuid,
        block_size: u64,
        num_blocks: u64,
        partitions: Vec<GptEntry>,
    ) -> Result<GptLabel, LabelError> {
        let mut pmbr = Pmbr::default();
        pmbr.entries[0].protect(num_blocks);

        let mut header = GptHeader::new(guid, block_size, num_blocks);
        header.table_crc =
            GptEntry::checksum(&partitions, header.num_entries).context(SerializeSnafu)?;
        header.checksum().context(SerializeSnafu)?;
        let secondary = header.as_secondary().context(SerializeSnafu)?;

        Ok(GptLabel {
            status: LabelStatus::Neither,
            block_size,
            mbr: pmbr,
            primary: header,
            partitions,
            secondary,
        })
    }

    /// Probe the disk for a [`GptLabel`].
    pub async fn probe_label<T: GptDiskOps>(handle: &T) -> Result<GptLabel, LabelError> {
        let GptDiskProps {
            block_size,
            num_blocks,
        } = handle.props();

        // Note that PMBR is 512B even on larger sector disks, but we must allocate block_size
        // bytes to read it, as the underlying device may not support smaller reads.
        let mut buf = handle.buffer_alloc(block_size).context(ReadAllocSnafu {
            part: String::from("MBR"),
        })?;
        handle.read_at(0, &mut buf).await.context(ReadSnafu {
            what: String::from("MBR"),
        })?;
        let mbr = GptLabel::read_mbr(&buf)?;

        // GPT headers
        let status: LabelStatus;
        let primary: GptHeader;
        let secondary: GptHeader;
        let active: &GptHeader;

        // Get primary GPT header.
        handle
            .read_at(block_size, &mut buf)
            .await
            .context(ReadSnafu {
                what: String::from("primary GPT header"),
            })?;
        match GptLabel::read_primary_header(&buf, block_size, num_blocks) {
            Ok(header) => {
                primary = header;
                active = &primary;
                // Get secondary GPT header.
                let offset = (num_blocks - 1) * block_size;
                handle.read_at(offset, &mut buf).await.context(ReadSnafu {
                    what: String::from("secondary GPT header"),
                })?;
                match GptLabel::read_secondary_header(&buf, block_size, num_blocks) {
                    Ok(header) => {
                        GptLabel::consistency_check(&primary, &header)?;
                        // All good - primary and secondary GPT headers
                        // are valid and consistent with each other.
                        secondary = header;
                        status = LabelStatus::Both;
                    }
                    Err(_) => {
                        // Secondary GPT header is either not present
                        // or invalid. Construct new secondary GPT header from primary.
                        secondary = primary.as_secondary().context(SerializeSnafu)?;
                        status = LabelStatus::Primary;
                    }
                }
            }
            Err(error) => {
                // Primary GPT header is either not present or invalid.
                // See if we can obtain a valid secondary GPT header.
                let offset = (num_blocks - 1) * block_size;
                handle.read_at(offset, &mut buf).await.context(ReadSnafu {
                    what: String::from("secondary GPT header"),
                })?;
                match GptLabel::read_secondary_header(&buf, block_size, num_blocks) {
                    Ok(header) => {
                        secondary = header;
                        active = &secondary;
                        // Construct new primary GPT header from secondary.
                        primary = secondary.as_primary().context(SerializeSnafu)?;
                        status = LabelStatus::Secondary;
                    }
                    Err(_) => {
                        // Neither primary or secondary GPT header is present or valid.
                        return Err(LabelError::InvalidLabel { source: error });
                    }
                }
            }
        }

        // The disk size recorded in protective MBR must be consistent with GPT header.
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
        let mut buf = handle
            .buffer_alloc(blocks * block_size)
            .context(ReadAllocSnafu {
                part: String::from("partition table"),
            })?;
        let offset = active.lba_table * block_size;
        handle.read_at(offset, &mut buf).await.context(ReadSnafu {
            what: String::from("partition table"),
        })?;
        let mut partitions = GptLabel::read_partitions(&buf, active)?;

        // There can be up to 128 partition entries stored on disk,
        // even though most are not used. Retain only those entries
        // that actually define partitions.
        partitions.retain(|entry| entry.ent_start > 0 || entry.ent_end > 0);

        Ok(GptLabel {
            status,
            block_size,
            mbr,
            primary,
            partitions,
            secondary,
        })
    }

    /// Locate a partition by name.
    pub fn partition(&self, name: &str) -> Option<&GptEntry> {
        self.partitions
            .iter()
            .find(|entry| entry.ent_name.name == name)
    }

    /// Returns the offset (in bytes) of the specified partition.
    pub fn partition_offset(&self, name: &str) -> Result<u64, ProbeError> {
        match self.partition(name) {
            Some(entry) => Ok(entry.ent_start * self.block_size),
            None => Err(ProbeError::MissingPartition {
                name: String::from(name),
            }),
        }
    }

    /// Returns the size (in bytes) of the specified partition.
    ///
    /// Per the UEFI spec, `ent_end` is the last LBA of the partition
    /// (inclusive), so the block count is `ent_end - ent_start + 1`.
    pub fn partition_size(&self, name: &str) -> Result<u64, ProbeError> {
        match self.partition(name) {
            Some(entry) => Ok((entry.ent_end - entry.ent_start + 1) * self.block_size),
            None => Err(ProbeError::MissingPartition {
                name: String::from(name),
            }),
        }
    }
}

impl fmt::Display for GptLabel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "GUID: {}", self.primary.guid)?;

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

        for (i, part) in self.partitions.iter().enumerate() {
            writeln!(f, "  Partition {i}")?;
            writeln!(f, "    GUID: {}", part.ent_guid)?;
            writeln!(f, "    Type GUID: {}", part.ent_type)?;
            writeln!(f, "    LBA start: {}", part.ent_start)?;
            writeln!(f, "    LBA end: {}", part.ent_end)?;
            writeln!(f, "    Name: {}", part.ent_name.name)?;
        }

        Ok(())
    }
}

impl GptLabel {
    /// Construct a Pmbr from raw data.
    fn read_mbr(buf: &impl GptBuffer) -> Result<Pmbr, ProbeError> {
        Pmbr::from_slice(&buf.as_slice()[440..512])
    }

    /// Construct a GPT header from raw data.
    fn read_header(buf: &impl GptBuffer) -> Result<GptHeader, ProbeError> {
        GptHeader::from_slice(buf.as_slice())
    }

    /// Construct and validate primary GPT header.
    fn read_primary_header(
        buf: &impl GptBuffer,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<GptHeader, ProbeError> {
        let header = GptLabel::read_header(buf)?;
        GptLabel::validate_primary_header(&header, block_size, num_blocks)?;
        Ok(header)
    }

    /// Construct and validate secondary GPT header.
    fn read_secondary_header(
        buf: &impl GptBuffer,
        block_size: u64,
        num_blocks: u64,
    ) -> Result<GptHeader, ProbeError> {
        let header = GptLabel::read_header(buf)?;
        GptLabel::validate_secondary_header(&header, block_size, num_blocks)?;
        Ok(header)
    }

    /// Construct and validate partition table.
    fn read_partitions(
        buf: &impl GptBuffer,
        header: &GptHeader,
    ) -> Result<Vec<GptEntry>, ProbeError> {
        let partitions = GptEntry::from_slice(buf.as_slice(), header.num_entries)?;
        GptLabel::validate_partitions(&partitions, header)?;
        Ok(partitions)
    }

    /// Check that primary GPT header is valid and consistent.
    pub fn validate_primary_header(
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
        if (primary.num_entries * primary.entry_size) as u64 > GptHeader::PARTITION_TABLE_SIZE {
            return Err(ProbeError::PartitionTableSize {});
        }
        if primary.lba_table + Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size)
            > primary.lba_start
        {
            return Err(ProbeError::PartitionTableSpace {});
        }
        Ok(())
    }

    /// Check that secondary GPT header is valid and consistent.
    pub fn validate_secondary_header(
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
        if (secondary.num_entries * secondary.entry_size) as u64 > GptHeader::PARTITION_TABLE_SIZE {
            return Err(ProbeError::PartitionTableSize {});
        }
        if secondary.lba_table + Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size)
            > secondary.lba_self
        {
            return Err(ProbeError::PartitionTableSpace {});
        }
        Ok(())
    }

    /// Check that partition table entries are valid and consistent.
    pub fn validate_partitions(
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
        if header.table_crc
            != GptEntry::checksum(partitions, header.num_entries).context(ChecksumSerializeSnafu)?
        {
            return Err(ProbeError::PartitionTableChecksum {});
        }
        Ok(())
    }

    /// Check that primary and secondary GPT headers are consistent with each other.
    pub fn consistency_check(primary: &GptHeader, secondary: &GptHeader) -> Result<(), ProbeError> {
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

/// Properties of the disk needed to probe and read GPT labels.
#[derive(Debug, Clone, Copy)]
pub struct GptDiskProps {
    /// Logical block size of the underlying device.
    pub block_size: u64,
    /// Number of blocks on the underlying device.
    pub num_blocks: u64,
}

/// Trait for disk operations needed to probe and read GPT labels.
///
/// Implementations decouple the GPT (de)serialisation code from any
/// particular I/O backend (SPDK, plain files, etc.) so the label routines
/// can be reused outside of the io-engine core.
#[async_trait::async_trait(?Send)]
pub trait GptDiskOps {
    /// Backend-specific buffer type used for I/O.
    type Buffer: GptBuffer;

    /// Allocate an I/O buffer of `size` bytes suitable for use with
    /// [`GptDiskOps::read_at`].
    fn buffer_alloc(&self, size: u64) -> Result<Self::Buffer, DmaError>;
    /// Return the geometry (block size, block count) of the underlying disk.
    fn props(&self) -> GptDiskProps;
    /// Read `buffer.len()` bytes starting at byte `offset` into `buffer`.
    /// Returns the number of bytes read.
    async fn read_at(&self, offset: u64, buffer: &mut Self::Buffer) -> Result<u64, CoreError>;
}

#[async_trait::async_trait(?Send)]
impl GptDiskOps for Box<dyn BlockDeviceHandle> {
    type Buffer = DmaBuf;

    fn buffer_alloc(&self, size: u64) -> Result<Self::Buffer, DmaError> {
        self.dma_malloc(size)
    }
    fn props(&self) -> GptDiskProps {
        let bdev = self.get_device();
        GptDiskProps {
            block_size: bdev.block_len(),
            num_blocks: bdev.num_blocks(),
        }
    }
    async fn read_at(&self, offset: u64, buffer: &mut Self::Buffer) -> Result<u64, CoreError> {
        self.read_at(offset, buffer).await
    }
}
#[async_trait::async_trait(?Send)]
impl GptDiskOps for dyn BlockDeviceHandle {
    type Buffer = DmaBuf;

    fn buffer_alloc(&self, size: u64) -> Result<Self::Buffer, DmaError> {
        self.dma_malloc(size)
    }
    fn props(&self) -> GptDiskProps {
        let bdev = self.get_device();
        GptDiskProps {
            block_size: bdev.block_len(),
            num_blocks: bdev.num_blocks(),
        }
    }
    async fn read_at(&self, offset: u64, buffer: &mut Self::Buffer) -> Result<u64, CoreError> {
        #[allow(deprecated)]
        self.read_at(offset, buffer).await
    }
}

/// A trait to abstract over the buffer type used for reading/writing GPT data.
pub trait GptBuffer {
    /// View the buffer's contents as an immutable byte slice.
    fn as_slice(&self) -> &[u8];
    /// View the buffer's contents as a mutable byte slice.
    fn as_mut_slice(&mut self) -> &mut [u8];
}
impl GptBuffer for DmaBuf {
    fn as_slice(&self) -> &[u8] {
        self.deref().as_slice()
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.deref_mut().as_mut_slice()
    }
}

/// A structure to hold the raw label data and the offset at which it should be written to disk.
pub struct LabelData<T: GptBuffer> {
    /// Byte offset on the disk at which `buf` should be written.
    pub offset: u64,
    /// Serialised label data ready to be written verbatim to disk.
    pub buf: T,
}
impl GptLabel {
    /// Generate raw data for (primary) label ready to be written to disk.
    pub fn primary_data<T: GptDiskOps + ?Sized>(
        &self,
        handle: &T,
    ) -> Result<LabelData<T::Buffer>, LabelError> {
        let mut buf = handle
            .buffer_alloc(self.primary.lba_start * self.block_size)
            .context(WriteAllocSnafu {
                what: String::from("primary"),
            })?;

        let mut writer = Cursor::new(buf.as_mut_slice());

        // Protective MBR
        writer.seek(SeekFrom::Start(440)).context(SeekSnafu)?;
        serialize_into(&mut writer, &self.mbr).context(SerializeSnafu)?;

        // Primary GPT header
        writer
            .seek(SeekFrom::Start(self.primary.lba_self * self.block_size))
            .context(SeekSnafu)?;
        serialize_into(&mut writer, &self.primary).context(SerializeSnafu)?;

        // Primary partition table
        writer
            .seek(SeekFrom::Start(self.primary.lba_table * self.block_size))
            .context(SeekSnafu)?;
        for entry in self.partitions.iter() {
            serialize_into(&mut writer, &entry).context(SerializeSnafu)?;
        }

        Ok(LabelData { offset: 0, buf })
    }

    /// Generate raw data for (secondary) label ready to be written to disk.
    pub fn secondary_data<T: GptDiskOps + ?Sized>(
        &self,
        handle: &T,
    ) -> Result<LabelData<T::Buffer>, LabelError> {
        let len_bytes = (self.secondary.lba_self - self.secondary.lba_table + 1) * self.block_size;
        let mut buf = handle.buffer_alloc(len_bytes).context(WriteAllocSnafu {
            what: String::from("secondary"),
        })?;

        let mut writer = Cursor::new(buf.as_mut_slice());

        // Secondary partition table
        for entry in self.partitions.iter() {
            serialize_into(&mut writer, &entry).context(SerializeSnafu)?;
        }

        // Secondary GPT header
        writer
            .seek(SeekFrom::Start(
                (self.secondary.lba_self - self.secondary.lba_table) * self.block_size,
            ))
            .context(SeekSnafu)?;
        serialize_into(&mut writer, &self.secondary).context(SerializeSnafu)?;

        let offset = self.secondary.lba_table * self.block_size;
        Ok(LabelData { offset, buf })
    }
}

/// A trait to calculate the number of blocks needed
/// to represent a given size, aligned to a block size.
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
