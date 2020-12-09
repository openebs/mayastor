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
use futures::future::join_all;
use serde::{
    de::{Deserialize, Deserializer, SeqAccess, Unexpected, Visitor},
    ser::{Serialize, SerializeTuple, Serializer},
};
use snafu::{ResultExt, Snafu};
use std::{
    fmt::{self, Display},
    io::{Cursor, Seek, SeekFrom},
    str::FromStr,
};
use uuid::{self, parser, Uuid};

use crate::{
    bdev::nexus::{
        nexus_bdev::Nexus,
        nexus_child::{ChildError, NexusChild},
    },
    core::{CoreError, DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum LabelError {
    #[snafu(display("{}", source))]
    NexusChildError { source: ChildError },
    #[snafu(display("Error reading {}", name))]
    ReadError { name: String, source: CoreError },
    #[snafu(display("Write error"))]
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
    #[snafu(display("Label probe error"))]
    ProbeError {},
    #[snafu(display("Label is invalid"))]
    LabelInvalid {},
    #[snafu(display("Incorrect MBR signature"))]
    MbrSignature {},
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
    CompareDiskGUID {},
    #[snafu(display("Disk sizes differ"))]
    CompareDiskSize {},
    #[snafu(display("GPT stored partition table checksums differ"))]
    ComparePartitionTableChecksum {},
    #[snafu(display("Alternate GPT locations are incorrect"))]
    BackupLocation {},
    #[snafu(display("GPT partition table location is incorrect"))]
    PartitionTableLocation {},
    #[snafu(display("Could not get handle for child bdev {}", name,))]
    HandleCreate { name: String, source: ChildError },
    #[snafu(display("The written label could not be read from disk, likely the child {} is a null device", name))]
    ReReadError { name: String },
}

struct LabelData {
    offset: u64,
    buf: DmaBuf,
}

impl Nexus {
    /// Partition Type GUID for our "MayaMeta" partition.
    pub const METADATA_PARTITION_TYPE_ID: &'static str =
        "27663382-e5e6-11e9-81b4-ca5ca5ca5ca5";

    /// Generate a new nexus label based on the nexus configuration.
    /// The meta partition is fixed in size and aligned to a 1MB boundary.
    pub(crate) fn generate_label(&mut self) -> NexusLabel {
        let block_size: u32 = self.bdev.block_len();
        let num_blocks: u64 = self.min_num_blocks();

        //
        // (Protective) MBR
        let mut pmbr = Pmbr::default();

        pmbr.entries[0].ent_type = 0xee; // indicates this is a protective MBR partition
        pmbr.entries[0].attributes = 0x00;
        pmbr.entries[0].chs_start = [0x00, 0x02, 0x00];
        pmbr.entries[0].chs_last = [0xff, 0xff, 0xff];

        // the partition must accurately reflect the disk size where possible.
        // if the size (in blocks) is too large to fit into 32 bits,
        // we set the size to 0xffff_ffff

        pmbr.entries[0].lba_start = 1; // "partition" starts immediately after the MBR
        pmbr.entries[0].num_sectors = if num_blocks > u32::max_value().into() {
            u32::max_value()
        } else {
            (num_blocks as u32) - 1 // do not count the first block that
                                    // contains the MBR
        };

        pmbr.signature = [0x55, 0xaa];

        //
        // Primary GPT header
        let mut header = GPTHeader::new(
            block_size,
            num_blocks,
            Uuid::from_bytes(self.bdev.uuid().as_bytes()),
        );

        //
        // Partition table
        let mut entries =
            vec![GptEntry::default(); header.num_entries as usize];

        entries[0] = GptEntry {
            ent_type: GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID)
                .unwrap(),
            ent_guid: GptGuid::new_random(),
            // 1MB aligned
            ent_start: header.lba_start,
            // 4MB
            ent_end: header.lba_start + u64::from((4 << 20) / block_size) - 1,
            ent_attr: 0,
            ent_name: GptName {
                name: "MayaMeta".into(),
            },
        };

        entries[1] = GptEntry {
            ent_type: GptGuid::from_str(Nexus::METADATA_PARTITION_TYPE_ID)
                .unwrap(),
            ent_guid: GptGuid::new_random(),
            ent_start: entries[0].ent_end + 1,
            ent_end: header.lba_end,
            ent_attr: 0,
            ent_name: GptName {
                name: "MayaData".into(),
            },
        };

        header.table_crc = GptEntry::checksum(&entries);
        header.checksum();

        //
        // Secondary GPT header
        let backup = header.to_backup();

        NexusLabel {
            status: NexusLabelStatus::Neither,
            mbr: pmbr,
            primary: header,
            partitions: entries,
            secondary: backup,
        }
    }

    fn get_primary_data(
        &self,
        label: &NexusLabel,
    ) -> Result<LabelData, LabelError> {
        let block_size = self.bdev.block_len() as u64;
        let blocks = Aligned::get_blocks(
            GPTHeader::PARTITION_TABLE_SIZE as u64,
            block_size,
        );
        // allocate 2 extra blocks for the MBR and GPT header respectively
        let mut buf =
            DmaBuf::new((blocks + 2) * block_size, self.bdev.alignment())
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
        for entry in &label.partitions {
            serialize_into(&mut writer, &entry).context(SerializeError {})?;
        }

        Ok(LabelData {
            offset: 0,
            buf,
        })
    }

    fn get_secondary_data(
        &self,
        label: &NexusLabel,
    ) -> Result<LabelData, LabelError> {
        let block_size = self.bdev.block_len() as u64;
        let blocks = Aligned::get_blocks(
            GPTHeader::PARTITION_TABLE_SIZE as u64,
            block_size,
        );
        // allocate 1 extra block for the GPT header
        let mut buf =
            DmaBuf::new((blocks + 1) * block_size, self.bdev.alignment())
                .context(WriteAlloc {
                    name: String::from("secondary"),
                })?;
        let mut writer = Cursor::new(buf.as_mut_slice());

        // Secondary partition table
        for entry in &label.partitions {
            serialize_into(&mut writer, &entry).context(SerializeError {})?;
        }

        // Secondary GPT header
        writer
            .seek(SeekFrom::Start(GPTHeader::PARTITION_TABLE_SIZE))
            .unwrap();
        serialize_into(&mut writer, &label.secondary)
            .context(SerializeError {})?;

        Ok(LabelData {
            offset: label.secondary.lba_table * block_size,
            buf,
        })
    }

    pub async fn write_labels(
        &self,
        target: &NexusLabel,
        list: &[NexusChildLabel<'_>],
    ) -> Result<(), LabelError> {
        let primary = self.get_primary_data(target)?;
        let secondary = self.get_secondary_data(target)?;

        let mut futures = Vec::new();

        for label in list {
            match label.get_label_status() {
                NexusLabelStatus::Both => {
                    // Nothing to do as both labels are already valid.
                }
                NexusLabelStatus::Primary => {
                    // Only write out secondary as primary is already valid.
                    futures.push(
                        label.child.write_at(secondary.offset, &secondary.buf),
                    );
                }
                NexusLabelStatus::Secondary => {
                    // Only write out primary as secondary is already valid.
                    futures.push(
                        label.child.write_at(primary.offset, &primary.buf),
                    );
                }
                NexusLabelStatus::Neither => {
                    futures.push(
                        label.child.write_at(primary.offset, &primary.buf),
                    );
                    futures.push(
                        label.child.write_at(secondary.offset, &secondary.buf),
                    );
                }
            }
        }

        for result in join_all(futures).await {
            if let Err(error) = result {
                // return the first error
                return Err(error);
            }
        }

        // check if we can read the labels
        for label in list {
            if let Err(error) = label.child.probe_label().await {
                warn!(
                    "{}: {}: Error validating newly written disk label: {}",
                    label.child.parent, label.child.name, error
                );
                return Err(LabelError::ProbeError {});
            }
            info!(
                "{}: {}: Disk label written",
                label.child.parent, label.child.name
            );
        }

        Ok(())
    }

    pub async fn write_all_labels(
        &self,
        label: &NexusLabel,
    ) -> Result<(), LabelError> {
        let primary = self.get_primary_data(label)?;
        let secondary = self.get_secondary_data(label)?;

        let mut futures = Vec::new();

        for child in &self.children {
            futures.push(child.write_at(primary.offset, &primary.buf));
            futures.push(child.write_at(secondary.offset, &secondary.buf));
        }

        for result in join_all(futures).await {
            if let Err(error) = result {
                // return the first error
                return Err(error);
            }
        }

        // check if we can read the labels
        for child in &self.children {
            if let Err(error) = child.probe_label().await {
                warn!(
                    "{}: {}: Error validating newly written disk label: {}",
                    child.parent, child.name, error
                );
                return Err(LabelError::ReReadError {
                    name: child.name.clone(),
                });
            }
            info!("{}: {}: Disk label written", child.parent, child.name);
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone, Copy)]
/// based on RFC4122
pub struct GptGuid {
    pub time_low: u32,
    pub time_mid: u16,
    pub time_high: u16,
    pub node: [u8; 8],
}
impl std::str::FromStr for GptGuid {
    type Err = parser::ParseError;

    fn from_str(uuid: &str) -> Result<Self, Self::Err> {
        let fields = uuid::Uuid::from_str(uuid)?;
        let fields = fields.as_fields();

        Ok(GptGuid {
            time_low: fields.0,
            time_mid: fields.1,
            time_high: fields.2,
            node: *fields.3,
        })
    }
}

impl std::fmt::Display for GptGuid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            uuid::Uuid::from_fields(
                self.time_low,
                self.time_mid,
                self.time_high,
                &self.node,
            )
            .unwrap()
            .to_string()
        )
    }
}

impl GptGuid {
    pub(crate) fn new_random() -> Self {
        let fields = uuid::Uuid::new_v4();
        let fields = fields.as_fields();
        GptGuid {
            time_low: fields.0,
            time_mid: fields.1,
            time_high: fields.2,
            node: *fields.3,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Copy, Clone)]
pub struct GPTHeader {
    /// GPT signature (must be "EFI PART").
    pub signature: [u8; 8],
    /// 00 00 01 00 up til version 2.17
    pub revision: [u8; 4],
    /// GPT header size (92 bytes)
    pub header_size: u32,
    /// CRC32  of the header.
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

impl GPTHeader {
    pub const PARTITION_TABLE_SIZE: u64 = 128 * 128;

    /// converts a slice into a gpt header and verifies the validity of the data
    pub fn from_slice(slice: &[u8]) -> Result<GPTHeader, LabelError> {
        let mut reader = Cursor::new(slice);
        let mut gpt: GPTHeader =
            deserialize_from(&mut reader).context(DeserializeError {})?;

        if gpt.header_size != 92 {
            return Err(LabelError::GptHeaderSize {
                actual_size: gpt.header_size,
                expected_size: 92,
            });
        }

        if gpt.signature != [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54]
            || gpt.revision != [0x00, 0x00, 0x01, 0x00]
        {
            return Err(LabelError::GptSignature {});
        }

        let checksum = gpt.self_checksum;

        if gpt.checksum() != checksum {
            return Err(LabelError::GptChecksum {});
        }

        Ok(gpt)
    }

    /// checksum the header with the checksum field itself set to 0
    pub fn checksum(&mut self) -> u32 {
        self.self_checksum = 0;
        self.self_checksum = crc32::checksum_ieee(&serialize(&self).unwrap());
        self.self_checksum
    }

    pub fn new(blk_size: u32, num_blocks: u64, guid: uuid::Uuid) -> Self {
        let fields = guid.as_fields();
        GPTHeader {
            signature: [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54],
            revision: [0x00, 0x00, 0x01, 0x00],
            header_size: 92,
            self_checksum: 0,
            reserved: [0; 4],
            lba_self: 1,
            lba_alt: num_blocks - 1,
            lba_start: u64::from((1 << 20) / blk_size),
            lba_end: (num_blocks - 1)
                - (GPTHeader::PARTITION_TABLE_SIZE / u64::from(blk_size))
                - 1,
            guid: GptGuid {
                time_low: fields.0,
                time_mid: fields.1,
                time_high: fields.2,
                node: *fields.3,
            },
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
    /// utf16 name of the partition entry, do not confuse this fs labels!
    pub ent_name: GptName,
}

impl GptEntry {
    /// converts a slice into a partition array
    pub fn from_slice(
        slice: &[u8],
        parts: u32,
    ) -> Result<Vec<GptEntry>, LabelError> {
        let mut reader = Cursor::new(slice);
        let mut part_vec = Vec::new();
        // TODO 128 should be passed in as a argument
        for _ in 0 .. parts {
            part_vec.push(
                deserialize_from(&mut reader).context(DeserializeError {})?,
            );
        }
        Ok(part_vec)
    }

    /// calculate the checksum over the partitions table
    pub fn checksum(parts: &[GptEntry]) -> u32 {
        let mut digest = crc32::Digest::new(crc32::IEEE);
        for p in parts {
            digest.write(&serialize(p).unwrap());
        }
        digest.sum32()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub enum NexusLabelStatus {
    /// Both primary and secondary labels are valid
    Both,
    /// Only primary label is valid
    Primary,
    /// Only secondary label is valid
    Secondary,
    /// Neither primary or secondary labels are valid
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
    pub primary: GPTHeader,
    /// Vector of GPT entries where the first element is considered to be ours
    pub partitions: Vec<GptEntry>,
    /// The backup GPT header
    pub secondary: GPTHeader,
}

impl NexusLabel {
    /// returns the offset to the first data segment
    pub(crate) fn offset(&self) -> u64 {
        self.partitions[1].ent_start
    }

    /// returns the number of total blocks in this segment
    pub(crate) fn get_block_count(&self) -> u64 {
        self.partitions[1].ent_end - self.partitions[1].ent_start + 1
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

// for arrays bigger than 32 elements, things start to get unimplemented
// in terms of derive and what not. So we create a struct with a string,
// and tell serde how to use it during (de)serializing

struct GpEntryNameVisitor;

impl<'de> Deserialize<'de> for GptName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
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
impl<'de> Visitor<'de> for GpEntryNameVisitor {
    type Value = GptName;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Invalid GPT partition name")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<GptName, A::Error>
    where
        A: SeqAccess<'de>,
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
            Ok(GptName {
                name: String::from_utf16_lossy(&out),
            })
        } else {
            Err(serde::de::Error::invalid_value(Unexpected::Seq, &self))
        }
    }
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct GptName {
    pub name: String,
}

/// although we don't use it, we must have a protective MBR to avoid systems
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
    /// must be set to [0x55, 0xAA]
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

impl Pmbr {
    /// converts a slice into a MBR and validates the signature
    pub fn from_slice(slice: &[u8]) -> Result<Pmbr, LabelError> {
        let mut reader = Cursor::new(slice);
        let mbr: Pmbr =
            deserialize_from(&mut reader).context(DeserializeError {})?;

        if mbr.signature != [0x55, 0xaa] {
            return Err(LabelError::MbrSignature {});
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
            signature: [0x55, 0xAA],
        }
    }
}

impl NexusLabel {
    /// construct a Pmbr from raw data
    fn read_mbr(buf: &DmaBuf) -> Result<Pmbr, LabelError> {
        Pmbr::from_slice(&buf.as_slice()[440 .. 512])
    }

    /// construct a GPTHeader from raw data
    fn read_header(buf: &DmaBuf) -> Result<GPTHeader, LabelError> {
        GPTHeader::from_slice(buf.as_slice())
    }

    /// construct and validate primary GPTHeader
    fn read_primary_header(buf: &DmaBuf) -> Result<GPTHeader, LabelError> {
        let primary = NexusLabel::read_header(buf)?;
        if primary.lba_table != primary.lba_self + 1 {
            return Err(LabelError::PartitionTableLocation {});
        }
        Ok(primary)
    }

    /// construct and validate secondary GPTHeader
    fn read_secondary_header(buf: &DmaBuf) -> Result<GPTHeader, LabelError> {
        let secondary = NexusLabel::read_header(buf)?;
        if secondary.lba_table != secondary.lba_end + 1 {
            return Err(LabelError::PartitionTableLocation {});
        }
        Ok(secondary)
    }

    /// construct partition table from raw data
    fn read_partitions(
        buf: &DmaBuf,
        header: &GPTHeader,
    ) -> Result<Vec<GptEntry>, LabelError> {
        let partitions =
            GptEntry::from_slice(buf.as_slice(), header.num_entries)?;
        if GptEntry::checksum(&partitions) != header.table_crc {
            return Err(LabelError::PartitionTableChecksum {});
        }
        Ok(partitions)
    }

    /// check that primary and secondary GPTHeaders
    /// are consistent with each other
    fn check_consistency(
        primary: &GPTHeader,
        secondary: &GPTHeader,
    ) -> Result<(), LabelError> {
        if primary.guid != secondary.guid {
            return Err(LabelError::CompareDiskGUID {});
        }
        if primary.lba_start != secondary.lba_start
            || primary.lba_end != secondary.lba_end
        {
            return Err(LabelError::CompareDiskSize {});
        }
        if primary.lba_alt != secondary.lba_self
            || secondary.lba_alt != primary.lba_self
        {
            return Err(LabelError::BackupLocation {});
        }
        if primary.table_crc != secondary.table_crc {
            return Err(LabelError::ComparePartitionTableChecksum {});
        }
        Ok(())
    }
}

impl NexusChild {
    /// read and validate this child's label
    pub async fn probe_label(&self) -> Result<NexusLabel, LabelError> {
        let hndl = self.handle().context(ReadError {
            name: self.name.clone(),
        })?;
        let bdev = hndl.get_bdev();
        let block_size = hndl.get_bdev().block_len() as u64;

        // Protective MBR
        let mut buf = hndl.dma_malloc(block_size).context(ReadAlloc {
            name: String::from("header"),
        })?;
        hndl.read_at(0, &mut buf).await.context(ReadError {
            name: String::from("MBR"),
        })?;
        let mbr = NexusLabel::read_mbr(&buf)?;

        let status: NexusLabelStatus;
        let primary: GPTHeader;
        let secondary: GPTHeader;
        let active: &GPTHeader;

        //
        // GPT header(s)

        // Get primary.
        hndl.read_at(block_size, &mut buf)
            .await
            .context(ReadError {
                name: String::from("primary GPT header"),
            })?;
        match NexusLabel::read_primary_header(&buf) {
            Ok(header) => {
                primary = header;
                active = &primary;
                // Get secondary.
                let offset = (bdev.num_blocks() - 1) * block_size;
                hndl.read_at(offset, &mut buf).await.context(ReadError {
                    name: String::from("secondary GPT header"),
                })?;
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
                hndl.read_at(offset, &mut buf).await.context(ReadError {
                    name: String::from("secondary GPT header"),
                })?;
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
                        return Err(LabelError::LabelInvalid {});
                    }
                }
            }
        }

        if mbr.entries[0].num_sectors != 0xffff_ffff
            && mbr.entries[0].num_sectors as u64 != primary.lba_alt
        {
            warn!("{}: {}: The protective MBR disk size does not match the GPT disk size!", self.parent, self.name);
            return Err(LabelError::LabelInvalid {});
        }

        //
        // Partition table
        let blocks = Aligned::get_blocks(
            (active.entry_size * active.num_entries) as u64,
            block_size,
        );
        let mut buf =
            hndl.dma_malloc(blocks * block_size).context(ReadAlloc {
                name: String::from("partition table"),
            })?;
        let offset = active.lba_table * block_size;
        hndl.read_at(offset, &mut buf).await.context(ReadError {
            name: String::from("partition table"),
        })?;
        let mut partitions = NexusLabel::read_partitions(&buf, active)?;

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
    async fn write_at(
        &self,
        offset: u64,
        buf: &DmaBuf,
    ) -> Result<usize, LabelError> {
        let hdl = self.handle().context(WriteError {
            name: self.name.clone(),
        })?;
        Ok(hdl.write_at(offset, buf).await.context(WriteError {
            name: self.name.clone(),
        })?)
    }
}

pub struct NexusChildLabel<'a> {
    pub child: &'a NexusChild,
    pub label: Option<NexusLabel>,
}

impl NexusChildLabel<'_> {
    /// Return the current status of this NexusChildLabel.
    pub fn get_label_status(&self) -> NexusLabelStatus {
        match &self.label {
            Some(label) => label.status,
            None => NexusLabelStatus::Neither,
        }
    }

    /// Search for the first "valid" NexusLabel.
    /// Prefer a target label where the primary and secondary GPT headers
    /// are both valid (on disk), but a target with at least one valid
    /// GPT header (on disk) is considered acceptable.
    pub fn find_target_label(
        list: &[NexusChildLabel<'_>],
    ) -> Option<NexusLabel> {
        for status in &[
            NexusLabelStatus::Both,
            NexusLabelStatus::Primary,
            NexusLabelStatus::Secondary,
        ] {
            for label in list {
                if let Some(target) = &label.label {
                    if target.status == *status {
                        return Some(target.clone());
                    }
                }
            }
        }
        None
    }

    /// Compare all (existing) labels in list against the target NexusLabel.
    /// Return true (only) if all are identical.
    pub fn compare_labels(
        target: &NexusLabel,
        list: &[NexusChildLabel<'_>],
    ) -> bool {
        for label in list {
            if let Some(entry) = &label.label {
                if entry.mbr != target.mbr
                    || entry.primary != target.primary
                    || entry.secondary != target.secondary
                    || entry.partitions != target.partitions
                {
                    return false;
                }
            }
        }
        true
    }
}

pub trait Aligned {
    /// Return the (appropriately aligned) number of blocks representing this
    /// size.
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
