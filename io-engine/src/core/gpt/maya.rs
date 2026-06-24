//! Versioned MayaMeta/MayaData layout.
//!
//! Every version places exactly two partitions on the device: a fixed-size `MayaMeta`
//! reservation followed by a `MayaData` partition that fills the rest of the usable space.
//!
//! The two partition Type GUIDs ([`META_TYPE_GUID`] and [`DATA_TYPE_GUID`])
//! are the same across all versions.
//!
//! Versions differ only in the size of the metadata reservation, captured by [`Layout`];
//! the shared [`LabelVariant`] / `generate` / `data_extent` code in this module is
//! parameterised over a `Layout`.
//!
//! The actual versions are the inline submodules [`v1`] and [`v2`].

use std::{cmp::min, str::FromStr};

use super::{
    label::{DataExtent, LabelVariant, LabelVersion, VersionedLabel},
    primitives::{Aligned, GptDiskProps, GptEntry, GptGuid, GptHeader, GptLabel, LabelError},
};

/// Partition Type GUID for the `MayaMeta` partition.
pub const META_TYPE_GUID: &str = "27663382-e5e6-11e9-81b4-ca5ca5ca5ca5";
/// Partition Type GUID for the `MayaData` partition.
pub const DATA_TYPE_GUID: &str = "6527994e-2c5a-4eec-9613-8f5944074e8b";

const META_NAME: &str = "MayaMeta";
const DATA_NAME: &str = "MayaData";

fn meta_guid() -> GptGuid {
    GptGuid::from_str(META_TYPE_GUID).expect("constant GUID must be valid")
}

fn data_guid() -> GptGuid {
    GptGuid::from_str(DATA_TYPE_GUID).expect("constant GUID must be valid")
}

/// Per-version knobs for the dual MayaMeta/MayaData layout.
pub trait Layout {
    /// Size (in bytes) of the metadata partition.
    const META_SIZE: u64;
    /// Which [`LabelVersion`] this layout represents.
    const VERSION: LabelVersion;
}

/// Compute the data extent for `L` on a device of the given geometry.
///
/// Used both at label-generation time and at nexus-setup time (via
/// [`LabelVariant::data_extent`]).
fn data_extent<L: Layout>(
    req_size: u64,
    num_blocks: u64,
    block_size: u64,
) -> Result<DataExtent, LabelError> {
    const METADATA_RESERVATION_OFFSET: u64 = 1024 * 1024;

    let too_small = || LabelError::DeviceTooSmall {
        num_blocks,
        block_size,
    };

    let table_blocks = Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size);
    let header_blocks = Aligned::get_blocks(GptHeader::HEADER_SIZE as u64, block_size);

    // We decrement by 1 to get the last usable block index, not the count of usable blocks.
    let last_usable = num_blocks - table_blocks - header_blocks - 1;

    let first_usable_block = Aligned::get_blocks(GptHeader::DATA_OFFSET, block_size);
    let lba_start = Aligned::get_blocks(METADATA_RESERVATION_OFFSET, block_size);
    assert!(
        lba_start >= first_usable_block,
        "metadata start must be after GPT entries"
    );

    let meta_blocks = Aligned::get_blocks(L::META_SIZE, block_size);
    let data_start = lba_start + meta_blocks;

    if data_start > last_usable {
        return Err(too_small());
    }

    let max_blocks = last_usable - data_start + 1;
    let req_blocks = Aligned::get_blocks(req_size, block_size);
    let data_blocks = min(req_blocks, max_blocks);
    let data_end = data_start + data_blocks - 1;

    if data_end > last_usable || data_end < data_start {
        return Err(too_small());
    }

    Ok(DataExtent {
        start: data_start,
        end: data_end,
    })
}

/// Build a fresh `[MayaMeta, MayaData]` partition table for `L`.
fn build_partitions<L: Layout>(
    header: &GptHeader,
    block_size: u64,
    req_size: u64,
) -> Result<Vec<GptEntry>, LabelError> {
    let ext = data_extent::<L>(req_size, header.lba_alt + 1, block_size)?;

    Ok(vec![
        GptEntry {
            ent_type: meta_guid(),
            ent_guid: GptGuid::new_random(),
            ent_start: header.lba_start,
            ent_end: ext.start - 1,
            ent_attr: 0,
            ent_name: META_NAME.into(),
        },
        GptEntry {
            ent_type: data_guid(),
            ent_guid: header.guid,
            ent_start: ext.start,
            ent_end: ext.end,
            ent_attr: 0,
            ent_name: DATA_NAME.into(),
        },
    ])
}

/// Generate a fresh versioned label for `L`, sized for `req_size` bytes of
/// data (clamped to what the device can fit).
pub fn generate<L: Layout>(
    guid: GptGuid,
    disk: GptDiskProps,
    req_size: u64,
) -> Result<VersionedLabel, LabelError> {
    let header = GptHeader::new(guid, disk.block_size, disk.num_blocks);
    let partitions = build_partitions::<L>(&header, disk.block_size, req_size)?;
    let gpt = GptLabel::new(guid, disk.block_size, disk.num_blocks, partitions)?;
    Ok(VersionedLabel {
        version: L::VERSION,
        gpt,
    })
}

fn check<L: Layout>(label: &GptLabel) -> bool {
    let meta_blocks = Aligned::get_blocks(L::META_SIZE, label.block_size);

    let Some(meta) = label.partition(META_NAME) else {
        return false;
    };
    if meta.ent_type != meta_guid() || meta.ent_end - meta.ent_start + 1 != meta_blocks {
        return false;
    }

    match label.partition(DATA_NAME) {
        Some(data) => data.ent_type == data_guid() && data.ent_start == meta.ent_end + 1,
        None => false,
    }
}

/// V1 of the on-disk label layout.
///
/// 4 MiB metadata partition; data fills the rest of the usable space with
/// no extra alignment.
///
/// Device layout:
///
/// ```text
/// 0     ───── reserved for protective MBR
/// 1     ───── reserved for primary GPT header
/// 2     ──┐
///         ├── reserved for GPT entries
/// 33    ──┘
/// 34    ──┐
///         ├── unused
/// 2047  ──┘
/// 2048  ──┐
///         ├── 4M reserved for metadata
/// 10239 ──┘
/// 10240 ──┐
///         ├── available for user data (this is what the nexus bdev exposes)
/// N-35  ──┘
/// N-34  ──┘ This is actually where it was supposed to be, but there's an off by 1 bug! :)
/// N-33  ──┐
///         ├── reserved for the copy of GPT entries
/// N-2   ──┘
/// N-1   ───── last device block, reserved for secondary GPT header
/// ```
pub mod v1 {
    use super::*;

    /// V1 layout marker. Zero-sized.
    pub struct V1;

    impl Layout for V1 {
        const META_SIZE: u64 = 4 * 1024 * 1024;
        const VERSION: LabelVersion = LabelVersion::V1;
    }

    impl LabelVariant for V1 {
        fn check(&self, label: &GptLabel) -> bool {
            check::<V1>(label)
        }
        fn data_extent(
            &self,
            req_size: u64,
            num_blocks: u64,
            block_size: u64,
        ) -> Result<DataExtent, LabelError> {
            data_extent::<V1>(req_size, num_blocks, block_size)
        }
    }
}
pub mod v2 {
    /// V2 layout marker.
    pub struct V2;
}

pub use v1::V1;
pub use v2::V2;
