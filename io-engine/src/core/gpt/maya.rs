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
    const DATA_START_OFFSET: u64;
    /// Alignment (in bytes) of the data partition length. A value `<=`
    /// the device block size means no extra alignment beyond the block.
    /// When larger, the data length is rounded down to a multiple of
    /// this value, leaving a trailing void in the last usable region.
    const DATA_ALIGN_SIZE: u64;
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
    let clamped = min(req_blocks, max_blocks);

    // Round the length down to the version's data alignment (no-op for
    // V1 and any version whose alignment fits in a single block).
    let data_blocks = if L::DATA_ALIGN_SIZE > 0 {
        let cluster_blocks = Aligned::get_blocks(L::DATA_ALIGN_SIZE, block_size);
        clamped & !(cluster_blocks - 1)
    } else {
        clamped
    };

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
        const DATA_START_OFFSET: u64 = 5 * 1024 * 1024;
        const DATA_ALIGN_SIZE: u64 = 0;
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

    #[cfg(test)]
    mod tests {
        use super::*;

        const META_END_BLKS: u64 = 10239;
        const DATA_START_BLKS: u64 = META_END_BLKS + 1;

        #[test]
        fn data_extents() {
            // 12 MiB device / 512 = 24_576 blocks
            // last_usable = 24_576 - 34 = 24_542
            let num_blocks = 24_576;
            let block_size = 512;
            let gpt_reserved_blks =
                Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size) + 1;

            // pretend we want a size larger than the device so we fill it
            let extent = data_extent::<V1>(u64::MAX, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, 24_542);
            assert_eq!(extent.end, num_blocks - gpt_reserved_blks - 1);

            // request size with same size as device, should yield same result
            let extent =
                data_extent::<V1>(num_blocks * block_size, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, 24_542);

            // request size smaller size than device, should yield same result
            let extent =
                data_extent::<V1>((num_blocks - 1) * block_size, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, 24_542);

            // request size smaller size than device, should yield same result
            let extent = data_extent::<V1>(
                (24_541 - DATA_START_BLKS + 1) * block_size,
                num_blocks,
                block_size,
            )
            .unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, 24_541);

            assert_eq!(
                data_extent::<V1>(u64::MAX, gpt_reserved_blks + META_END_BLKS + 2, block_size)
                    .unwrap(),
                DataExtent {
                    start: DATA_START_BLKS,
                    end: META_END_BLKS + 1,
                }
            );

            // too small, no point in having a data partition with size 0
            assert!(matches!(
                data_extent::<V1>(u64::MAX, gpt_reserved_blks + META_END_BLKS + 1, block_size),
                Err(LabelError::DeviceTooSmall { .. })
            ));
        }

        #[test]
        fn generate_partition() {
            let disk = GptDiskProps {
                block_size: 512,
                num_blocks: 262_144,
            };
            let label = LabelVersion::V1
                .generate(GptGuid::new_random(), disk, u64::MAX)
                .unwrap();

            assert_eq!(label.version, LabelVersion::V1);
            let meta = label.gpt.partition(META_NAME).unwrap();
            assert_eq!(meta.ent_start, 2048);
            assert_eq!(meta.ent_end, 10239);

            let data = label.gpt.partition(DATA_NAME).unwrap();
            assert_eq!(data.ent_start, 10240);
            assert_eq!(data.ent_end, 262_110);

            assert!(label.check());
            assert_eq!(LabelVersion::detect(&label.gpt), Some(LabelVersion::V1));

            let disk = GptDiskProps {
                block_size: 4096,
                num_blocks: 262_144,
            };
            let label = LabelVersion::V1
                .generate(GptGuid::new_random(), disk, u64::MAX)
                .unwrap();

            assert_eq!(label.version, LabelVersion::V1);
            let meta = label.gpt.partition(META_NAME).unwrap();
            assert_eq!(meta.ent_start, 256);
            assert_eq!(meta.ent_end, 1279);

            let data = label.gpt.partition(DATA_NAME).unwrap();
            assert_eq!(data.ent_start, 1280);
            assert_eq!(data.ent_end, 262_138);

            assert!(label.check());
            assert_eq!(LabelVersion::detect(&label.gpt), Some(LabelVersion::V1));
        }
    }
}

/// V2 of the on-disk label layout.
///
/// 3 MiB metadata partition, data aligned to a 4 MiB cluster boundary with
/// a trailing 4 MiB void reserved (never allocated on a default cluster).
///
/// The metadata size is reduced to 3 MiB so that the data partition starts
/// at a 4 MiB offset, aligning user data to the default cluster size. The
/// last 4 MiB are reserved as a void so the data length is also a multiple
/// of 4 MiB; this isn't fully foolproof for larger cluster sizes, but those
/// can be addressed in a future version bump. Replacing 4 MiB with the
/// actual cluster size will also require changing the wipe of the first
/// 8 MiB when replicas are created to ensure typical fs data is cleared.
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
///         ├── 3M reserved for metadata
/// 8191  ──┘
/// 8192     ────┐
///              ├── available for user data (this is what the nexus bdev exposes)
/// N-4MiB-1 ────┘
/// N-4MiB   ──┐
///            ├── void to ensure user data is a multiple of 4 MiB (matches default cluster size)
/// N-34     ──┘
/// N-33  ──┐
///         ├── reserved for the copy of GPT entries
/// N-2   ──┘
/// N-1   ───── last device block, reserved for secondary GPT header
/// ```
pub mod v2 {
    use super::*;

    /// V2 layout marker. Zero-sized.
    pub struct V2;

    impl Layout for V2 {
        const META_SIZE: u64 = 3 * 1024 * 1024;
        const DATA_START_OFFSET: u64 = 4 * 1024 * 1024;
        const DATA_ALIGN_SIZE: u64 = 4 * 1024 * 1024;
        const VERSION: LabelVersion = LabelVersion::V2;
    }

    impl LabelVariant for V2 {
        fn check(&self, label: &GptLabel) -> bool {
            check::<V2>(label)
        }
        fn data_extent(
            &self,
            req_size: u64,
            num_blocks: u64,
            block_size: u64,
        ) -> Result<DataExtent, LabelError> {
            data_extent::<V2>(req_size, num_blocks, block_size)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const META_END_BLKS: u64 = 8191;
        const DATA_START_BLKS: u64 = META_END_BLKS + 1;

        fn data_align_blks(block_size: u64) -> u64 {
            Aligned::get_blocks(V2::DATA_ALIGN_SIZE, block_size)
        }

        #[test]
        fn data_extents() {
            // 12 MiB device / 512 = 24_576 blocks
            let num_blocks = 24_576;
            let block_size = 512;
            let aligned_end = DATA_START_BLKS + data_align_blks(block_size) - 1;

            // pretend we want a size larger than the device so we fill it
            let extent = data_extent::<V2>(u64::MAX, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, aligned_end);
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));

            // request size with same size as device, should clamp+align to same result
            let extent =
                data_extent::<V2>(num_blocks * block_size, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, aligned_end);
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));

            // request size smaller size than device, still aligns down to one cluster
            let extent =
                data_extent::<V2>((num_blocks - 1) * block_size, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, aligned_end);
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));

            // request that wouldn't quite fill last_usable: still aligns down to one cluster
            let extent = data_extent::<V2>(
                (24_541 - DATA_START_BLKS + 1) * block_size,
                num_blocks,
                block_size,
            )
            .unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, aligned_end);

            // smallest viable V2 device: needs at least one full 4 MiB cluster of
            // data, i.e. last_usable >= data_start + cluster - 1.
            // num_blocks = gpt_reserved + meta_end + 1 + cluster
            let gpt_reserved_blks =
                Aligned::get_blocks(GptHeader::PARTITION_TABLE_SIZE, block_size) + 1;
            assert_eq!(
                data_extent::<V2>(
                    u64::MAX,
                    gpt_reserved_blks + META_END_BLKS + 1 + data_align_blks(block_size),
                    block_size,
                )
                .unwrap(),
                DataExtent {
                    start: DATA_START_BLKS,
                    end: aligned_end,
                }
            );

            // one block short of fitting a full cluster -> too small
            assert!(matches!(
                data_extent::<V2>(
                    u64::MAX,
                    gpt_reserved_blks + META_END_BLKS + data_align_blks(block_size),
                    block_size,
                ),
                Err(LabelError::DeviceTooSmall { .. })
            ));

            // 13 MiB device / 512 = 26_624 blocks
            let num_blocks = 26_624;
            let block_size = 512;
            let aligned_end = DATA_START_BLKS + data_align_blks(block_size) - 1;

            // pretend we want a size larger than the device so we fill it
            let extent = data_extent::<V2>(u64::MAX, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(extent.end, aligned_end + data_align_blks(block_size));
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));
        }

        #[test]
        fn generate_partition() {
            let disk = GptDiskProps {
                block_size: 512,
                num_blocks: 262_144,
            };
            let label = LabelVersion::V2
                .generate(GptGuid::new_random(), disk, u64::MAX)
                .unwrap();

            assert_eq!(label.version, LabelVersion::V2);
            let meta = label.gpt.partition(META_NAME).unwrap();
            assert_eq!(meta.ent_start, 2048);
            assert_eq!(meta.ent_end, 8191);

            let data = label.gpt.partition(DATA_NAME).unwrap();
            assert_eq!(data.ent_start, 8192);
            assert_eq!(data.ent_end, 253_951);
            assert!(data.is_multiple_of(V2::DATA_ALIGN_SIZE / disk.block_size));

            assert!(label.check());
            assert_eq!(LabelVersion::detect(&label.gpt), Some(LabelVersion::V2));

            let disk = GptDiskProps {
                block_size: 4096,
                num_blocks: 262_144,
            };
            let label = LabelVersion::V2
                .generate(GptGuid::new_random(), disk, u64::MAX)
                .unwrap();

            assert_eq!(label.version, LabelVersion::V2);
            let meta = label.gpt.partition(META_NAME).unwrap();
            assert_eq!(meta.ent_start, 256);
            assert_eq!(meta.ent_end, 1023);

            let data = label.gpt.partition(DATA_NAME).unwrap();
            assert_eq!(data.ent_start, 1024);
            assert_eq!(data.ent_end, 261_119);
            assert!(data.is_multiple_of(V2::DATA_ALIGN_SIZE / disk.block_size));

            assert!(label.check());
            assert_eq!(LabelVersion::detect(&label.gpt), Some(LabelVersion::V2));
        }
    }
}

pub use v1::V1;
pub use v2::V2;
