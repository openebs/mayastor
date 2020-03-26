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
//! $ mctl create gpt  -r  aio:////code/disk1.img?blk_size=512 -s 1GiB -b
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
//! $ mctl share gpt
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
use std::{
    fmt::{self, Display},
    io::{Cursor, Seek, SeekFrom},
    str::FromStr,
};

use bincode::{deserialize_from, serialize, serialize_into, Error};
use crc::{crc32, Hasher32};
use serde::{
    de::{Deserialize, Deserializer, SeqAccess, Unexpected, Visitor},
    ser::{Serialize, SerializeTuple, Serializer},
};
use snafu::{ResultExt, Snafu};
use uuid::{self, parser, Uuid};

use crate::{
    bdev::nexus::{
        nexus_bdev::Nexus,
        nexus_child::{ChildError, ChildIoError},
    },
    core::{DmaBuf, DmaError},
};

#[derive(Debug, Snafu)]
pub enum LabelError {
    #[snafu(display("Failed to allocate protective MBR"))]
    WritePmbrAlloc { source: DmaError },
    #[snafu(display("Label serialization error"))]
    SerializeError { source: Error },
    #[snafu(display("Label deserialization error"))]
    DeserializeError { source: Error },
    #[snafu(display("Write label error"))]
    WriteError { source: ChildIoError },
    #[snafu(display("Label probe error"))]
    ProbeError { source: ChildError },
    #[snafu(display("GPT header size is invalid"))]
    HeaderSize {},
    #[snafu(display("GPT label crc mismatch"))]
    CrcMismatch {},
}

impl Nexus {
    /// generate a new nexus label based on the nexus configuration. The meta
    /// partition is fixed in size and aligned to a 1MB boundary
    pub(crate) fn generate_label(&mut self) -> NexusLabel {
        let mut hdr = GPTHeader::new(
            self.bdev.block_len(),
            self.min_num_blocks(),
            Uuid::from_bytes(self.bdev.uuid().as_bytes()),
        );

        let mut entries = vec![GptEntry::default(); hdr.num_entries as usize];

        entries[0] = GptEntry {
            ent_type: GptGuid::from_str("27663382-e5e6-11e9-81b4-ca5ca5ca5ca5")
                .unwrap(),
            ent_guid: GptGuid::new_random(),
            // 1MB aligned
            ent_start: hdr.lba_start,
            // 4MB
            ent_end: hdr.lba_start
                + u64::from((4 << 20) / self.bdev.block_len())
                - 1,
            ent_attr: 0,
            ent_name: GptName {
                name: "MayaMeta".into(),
            },
        };

        entries[1] = GptEntry {
            ent_type: GptGuid::from_str("27663382-e5e6-11e9-81b4-ca5ca5ca5ca5")
                .unwrap(),
            ent_guid: GptGuid::new_random(),
            ent_start: entries[0].ent_end + 1,
            ent_end: hdr.lba_end,
            ent_attr: 0,
            ent_name: GptName {
                name: "MayaData".into(),
            },
        };

        hdr.table_crc = GptEntry::checksum(&entries);

        NexusLabel {
            primary: hdr,
            partitions: entries,
        }
    }

    /// write the protective MBR to all children.
    pub async fn write_pmbr(&mut self) -> Result<(), LabelError> {
        let mut pmbr = Pmbr::default();
        let mut buf =
            DmaBuf::new(self.bdev.block_len() as usize, self.bdev.alignment())
                .context(WritePmbrAlloc {})?;

        // the max size with MBR is 2GB, if we are smaller however, we must
        // ensure that we reflect that in the MBR as well even though its not
        // used.

        let size = self.bdev.num_blocks() * self.bdev.block_len() as u64;
        pmbr.entries[0].attributes = 0x00;
        //
        pmbr.entries[0].chs_start = [0x00, 0x02, 0x00];
        pmbr.entries[0].chs_last = [0xff, 0xff, 0xff];

        // this indicated that we are "protective MBR", saying we use all
        // use all storage on this device.

        pmbr.entries[0].ent_type = 0xee;
        pmbr.entries[0].num_sectors = if size < u32::max_value().into() {
            size as u32
        } else {
            u32::max_value()
        };

        pmbr.signature = [0x55, 0xaa];

        let mut writer = Cursor::new(buf.as_mut_slice());
        // we seek 440 into the buffer here, this makes serialisation a little
        // easier.

        writer.seek(SeekFrom::Start(440)).unwrap();
        serialize_into(&mut writer, &pmbr).context(SerializeError {})?;

        for child in &mut self.children {
            child.write_at(0, &buf).await.context(WriteError {})?;
        }

        Ok(())
    }

    /// write the gpt label to all the children.
    pub async fn write_label(
        &mut self,
        buf: &mut DmaBuf,
        label: &mut NexusLabel,
        primary: bool,
    ) -> Result<(), LabelError> {
        let blk_size = self.bdev.block_len();
        let mut writer = Cursor::new(buf.as_mut_slice());
        if primary {
            label.primary.checksum();

            serialize_into(&mut writer, &label.primary)
                .context(SerializeError {})?;

            writer.seek(SeekFrom::Start(u64::from(blk_size))).unwrap();

            for p in &label.partitions {
                serialize_into(&mut writer, &p).context(SerializeError {})?;
            }

            for child in &mut self.children {
                child
                    .write_at(u64::from(blk_size), &buf)
                    .await
                    .context(WriteError {})?;
                child.probe_label().await.context(ProbeError {})?;
            }
        } else {
            // now, write the backup label
            writer.seek(SeekFrom::Start(0)).unwrap();
            let mut backup = label.primary.to_backup();
            backup.checksum();

            for p in &label.partitions {
                serialize_into(&mut writer, &p).context(SerializeError {})?;
            }

            writer.seek(SeekFrom::Start((1 << 14) as u64)).unwrap();

            serialize_into(&mut writer, &backup).context(SerializeError {})?;

            for child in &mut self.children {
                child
                    .write_at(u64::from(blk_size) * (backup.lba_end + 1), &buf)
                    .await
                    .context(WriteError {})?;
                child.probe_label().await.context(ProbeError {})?;
            }
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
    /// converts a slice into a gpt header and verifies the validity of the data
    pub fn from_slice(slice: &[u8]) -> Result<GPTHeader, LabelError> {
        let mut reader = Cursor::new(slice);
        let mut gpt: GPTHeader = deserialize_from(&mut reader).unwrap();

        if gpt.header_size != 92
            || gpt.signature != [0x45, 0x46, 0x49, 0x20, 0x50, 0x41, 0x52, 0x54]
            || gpt.revision != [0x00, 0x00, 0x01, 0x00]
        {
            return Err(LabelError::HeaderSize {});
        }

        let crc = gpt.self_checksum;
        gpt.self_checksum = 0;
        gpt.self_checksum = crc32::checksum_ieee(&serialize(&gpt).unwrap());

        if gpt.self_checksum != crc {
            info!("GPT label crc mismatch");
            return Err(LabelError::CrcMismatch {});
        }

        if gpt.lba_self > gpt.lba_alt {
            std::mem::swap(&mut gpt.lba_self, &mut gpt.lba_alt)
        }

        Ok(gpt)
    }

    /// checksum the header with the checksum field itself set 0
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
            lba_end: ((num_blocks - 1) - u64::from((1 << 14) / blk_size)) - 1,
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
        secondary
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
        for _ in 0..parts {
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

#[derive(Debug, PartialEq, Serialize, Clone)]
/// The nexus label is standard GPT label (such that you can use it without us
/// in the data path) The only thing that is really specific to us is the
/// ent_type GUID if we see that attached to a partition, we assume the data in
/// that partition is ours. In the data we will have more magic markers to
/// confirm the assumption but this is step one.
pub struct NexusLabel {
    /// the main GPT header
    pub primary: GPTHeader,
    /// Vector of GPT entries where the first element is considered to be ours
    pub partitions: Vec<GptEntry>,
}

impl NexusLabel {
    /// returns the offset to the first data segment
    pub(crate) fn offset(&self) -> u64 {
        self.partitions[1].ent_start
    }

    /// returns the number of total blocks in this segment
    pub(crate) fn get_block_count(&self) -> u64 {
        self.partitions[1].ent_end - self.partitions[1].ent_start
    }
}

impl Display for NexusLabel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "GUID: {}", self.primary.guid.to_string())?;
        writeln!(f, "\tHeader crc32 {}", self.primary.self_checksum)?;
        writeln!(f, "\tPartition table crc32 {}", self.primary.table_crc)?;

        for i in 0..self.partitions.len() {
            writeln!(f, "\tPartition number {}", i)?;
            writeln!(f, "\tGUID: {}", self.partitions[i].ent_guid.to_string())?;
            writeln!(
                f,
                "\tType GUID: {}",
                self.partitions[i].ent_type.to_string()
            )?;
            writeln!(
                f,
                "\tLogical block start: {}, end: {}",
                self.partitions[i].ent_start, self.partitions[i].ent_end
            )?;
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
#[derive(Serialize, Deserialize)]
struct Pmbr {
    /// signature to uniquely ID the disk we do not use this
    pub disk_signature: u32,
    pub reserved: u16,
    /// number of partition entries
    pub entries: [MbrEntry; 4],
    /// must be set to [0x55, 0xAA]
    pub signature: [u8; 2],
}

/// the MBR partition entry
#[derive(Serialize, Deserialize, Copy, Clone, Default)]
pub struct MbrEntry {
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
