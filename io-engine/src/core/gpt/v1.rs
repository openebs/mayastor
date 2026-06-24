//! V1 of the on-disk label layout.
//!
//! Layout:
//! - `MayaMeta`: 4 MiB metadata partition at the GPT default first usable
//!   LBA (1 MiB aligned).
//! - `MayaData`: data partition immediately following `MayaMeta`, sized to
//!   the requested data size or the remainder of the device.

use std::str::FromStr;

use super::{
    label::{LabelVariant, LabelVersion, VersionedLabel},
    primitives::{Aligned, GptDiskProps, GptEntry, GptGuid, GptHeader, GptLabel, LabelError},
};

/// V1 layout marker. Zero-sized; used as a singleton via [`V1::INSTANCE`].
pub struct V1;

impl V1 {
    /// Partition Type GUID for the `MayaMeta` partition.
    pub const META_TYPE_GUID: &'static str = "27663382-e5e6-11e9-81b4-ca5ca5ca5ca5";
    /// Partition Type GUID for the `MayaData` partition.
    pub const DATA_TYPE_GUID: &'static str = "27663382-e5e6-11e9-81b4-ca5ca5ca5ca6";
    /// GPT partition name for the metadata partition.
    pub const META_NAME: &'static str = "MayaMeta";
    /// GPT partition name for the data partition.
    pub const DATA_NAME: &'static str = "MayaData";
    /// Size (in bytes) of the metadata partition.
    pub const META_SIZE: u64 = 4 * 1024 * 1024;

    /// Singleton used by [`super::label::LabelVersion::variant`].
    pub const INSTANCE: V1 = V1;

    fn meta_type_guid() -> GptGuid {
        GptGuid::from_str(Self::META_TYPE_GUID).expect("constant GUID must be valid")
    }

    fn data_type_guid() -> GptGuid {
        GptGuid::from_str(Self::DATA_TYPE_GUID).expect("constant GUID must be valid")
    }

    /// Generate a fresh V1 label. The data partition fills the
    /// remainder of the device after the metadata partition.
    pub fn generate(guid: GptGuid, disk: GptDiskProps) -> Result<VersionedLabel, LabelError> {
        // Transient header gives us a `lba_start`/`lba_end` to place
        // partitions; the canonical header is built inside `GptLabel::new`.
        let header = GptHeader::new(guid, disk.block_size, disk.num_blocks);
        let partitions = Self::build_partitions(&header, disk.block_size)?;
        let gpt = GptLabel::new(guid, disk.block_size, disk.num_blocks, partitions)?;
        Ok(VersionedLabel {
            version: LabelVersion::V1,
            gpt,
        })
    }

    fn build_partitions(header: &GptHeader, block_size: u64) -> Result<Vec<GptEntry>, LabelError> {
        let metadata_blocks = Aligned::get_blocks(Self::META_SIZE, block_size);

        let data_start = header.lba_start + metadata_blocks;
        if data_start > header.lba_end {
            return Err(LabelError::DeviceTooSmall {
                num_blocks: header.lba_alt + 1,
                block_size,
            });
        }

        Ok(vec![
            GptEntry {
                ent_type: Self::meta_type_guid(),
                ent_guid: GptGuid::new_random(),
                ent_start: header.lba_start,
                ent_end: data_start - 1,
                ent_attr: 0,
                ent_name: Self::META_NAME.into(),
            },
            GptEntry {
                ent_type: Self::data_type_guid(),
                ent_guid: header.guid,
                ent_start: data_start,
                ent_end: header.lba_end,
                ent_attr: 0,
                ent_name: Self::DATA_NAME.into(),
            },
        ])
    }
}

impl LabelVariant for V1 {
    fn detect(&self, label: &GptLabel) -> bool {
        label
            .partition(Self::META_NAME)
            .map(|e| e.ent_type == Self::meta_type_guid())
            .unwrap_or(false)
            && label
                .partition(Self::DATA_NAME)
                .map(|e| e.ent_type == Self::data_type_guid())
                .unwrap_or(false)
    }

    fn check(&self, label: &GptLabel) -> bool {
        let block_size = label.block_size;

        let metadata_start = Aligned::get_blocks(GptHeader::DATA_OFFSET, block_size);
        if metadata_start != label.primary.lba_start {
            return false;
        }

        let metadata_blocks = Aligned::get_blocks(Self::META_SIZE, block_size);
        let data_start = metadata_start + metadata_blocks;
        if data_start > label.primary.lba_end {
            return false;
        }

        match label.partition(Self::META_NAME) {
            Some(entry) => {
                if entry.ent_type != Self::meta_type_guid() {
                    return false;
                }
                if entry.ent_start != metadata_start {
                    return false;
                }
                if entry.ent_end != data_start - 1 {
                    return false;
                }
            }
            None => return false,
        }

        if let Some(entry) = label.partition(Self::DATA_NAME) {
            if entry.ent_type != Self::data_type_guid() {
                return false;
            }
            if entry.ent_start == data_start {
                return true;
            }
        }
        false
    }
}
