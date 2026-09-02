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

fn align_down(value: u64, align: u64) -> u64 {
    value - value % align
}

fn align_up(value: u64, align: u64) -> u64 {
    match value % align {
        0 => value,
        rem => value.saturating_add(align - rem),
    }
}

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
    /// this value.
    const DATA_ALIGN_SIZE: u64;
    /// Size (in bytes) of a fixed trailing void reserved between the
    /// end of the data partition and the last usable block. Zero means
    /// no reservation (data may extend up to `last_usable`).
    const TRAILING_VOID_SIZE: u64;
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

    // Reserve a fixed trailing void (if any) from the end of the device.
    let trailing_blocks = Aligned::get_blocks(L::TRAILING_VOID_SIZE, block_size);
    let usable_end = if trailing_blocks > 0 {
        if trailing_blocks + data_start >= num_blocks {
            return Err(too_small());
        }
        // usable_end is the last block *before* the void starts.
        let void_start = num_blocks - trailing_blocks;
        void_start - 1
    } else {
        last_usable
    };

    let max_blocks = usable_end - data_start + 1;
    let req_blocks = Aligned::get_blocks(req_size, block_size);

    // Round available capacity down to fit, but round requested size up to
    // satisfy the version's data alignment.
    let data_blocks = if L::DATA_ALIGN_SIZE > 0 {
        let cluster_blocks = Aligned::get_blocks(L::DATA_ALIGN_SIZE, block_size);
        min(
            align_up(req_blocks, cluster_blocks),
            align_down(max_blocks, cluster_blocks),
        )
    } else {
        min(req_blocks, max_blocks)
    };

    if data_blocks == 0 {
        return Err(too_small());
    }

    let data_end = data_start + data_blocks - 1;
    if data_end > usable_end || data_end < data_start {
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
        const TRAILING_VOID_SIZE: u64 = 0;
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
/// 3 MiB metadata partition, data aligned down to a 1 MiB boundary with a
/// fixed 4 MiB trailing void reserved before the end of the device.
///
/// The metadata size is reduced to 3 MiB so that the data partition starts
/// at a 4 MiB offset, aligning user data to the default cluster size. A
/// fixed 4 MiB region is always reserved between the end of the data
/// partition and the last usable block; the data length itself is only
/// required to be a multiple of 1 MiB (and is therefore not necessarily a
/// multiple of 4 MiB).
///
/// Device layout:
///
/// ```text
/// 0        ───── reserved for protective MBR
/// 1        ───── reserved for primary GPT header
/// 2        ──┐
///            ├── reserved for GPT entries
/// 33       ──┘
/// 34       ──┐
///            ├── unused
/// 2047     ──┘
/// 2048     ──┐
///            ├── 3 MiB reserved for metadata
/// 8191     ──┘
/// 8192     ────┐
///              ├── available for user data, 1 MiB aligned length
///              │   (this is what the nexus bdev exposes)
/// N-4MiB-1 ────┘
/// N-4MiB   ──┐
///            ├── fixed 4 MiB void reserved before end of device
/// N-34     ──┘
/// N-33     ──┐
///            ├── reserved for the copy of GPT entries
/// N-2      ──┘
/// N-1      ───── last device block, reserved for secondary GPT header
/// ```
pub mod v2 {
    use super::*;

    /// V2 layout marker. Zero-sized.
    pub struct V2;

    impl Layout for V2 {
        const META_SIZE: u64 = 3 * 1024 * 1024;
        const DATA_START_OFFSET: u64 = 4 * 1024 * 1024;
        const DATA_ALIGN_SIZE: u64 = 1024 * 1024;
        const TRAILING_VOID_SIZE: u64 = 4 * 1024 * 1024;
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

        fn trailing_void_blks(block_size: u64) -> u64 {
            Aligned::get_blocks(V2::TRAILING_VOID_SIZE, block_size)
        }

        #[test]
        fn data_extents() {
            // 12 MiB device / 512 = 24_576 blocks
            let num_blocks = 24_576;
            let block_size = 512;
            let void = trailing_void_blks(block_size);

            // pretend we want a size larger than the device so we fill it
            let extent = data_extent::<V2>(u64::MAX, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            // fills up to `num_blocks - 4 MiB - 1`
            let max_blocks = num_blocks - void - DATA_START_BLKS;
            let aligned_blocks = max_blocks & !(data_align_blks(block_size) - 1);
            assert_eq!(extent.end, DATA_START_BLKS + aligned_blocks - 1);
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));
            // exactly 4 MiB reserved before the end of the device
            assert_eq!(num_blocks - 1 - extent.end, void);
            // 12 MiB - 4 MiB meta/offset - 4 MiB void = 4 MiB usable
            let size_bytes = (extent.end - extent.start + 1) * block_size;
            assert_eq!(size_bytes, 4 * 1024 * 1024);

            // 5 MiB request on a 12 MiB device -> clamped to 4 MiB
            let extent = data_extent::<V2>(5 * 1024 * 1024, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            let size_bytes = (extent.end - extent.start + 1) * block_size;
            assert_eq!(size_bytes, 4 * 1024 * 1024);
            assert_eq!(num_blocks - 1 - extent.end, void);

            // request 3 MiB on a 12 MiB device: fits exactly (3 MiB is 1 MiB aligned)
            let extent = data_extent::<V2>(3 * 1024 * 1024, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(
                extent.end,
                DATA_START_BLKS + (3 * 1024 * 1024) / block_size - 1
            );
            assert!(extent.is_multiple_of(V2::DATA_ALIGN_SIZE / block_size));

            // request 3.5 MiB on a 12 MiB device: aligned up to 4 MiB
            let extent =
                data_extent::<V2>(3 * 1024 * 1024 + 512 * 1024, num_blocks, block_size).unwrap();
            assert_eq!(extent.start, DATA_START_BLKS);
            assert_eq!(
                extent.end,
                DATA_START_BLKS + (4 * 1024 * 1024) / block_size - 1
            );
            let size_bytes = (extent.end - extent.start + 1) * block_size;
            assert_eq!(size_bytes, 4 * 1024 * 1024);

            // Smallest viable V2 device: 1 MiB of data plus the 4 MiB void.
            let min_num_blocks = DATA_START_BLKS + data_align_blks(block_size) + void;
            assert_eq!(
                data_extent::<V2>(u64::MAX, min_num_blocks, block_size).unwrap(),
                DataExtent {
                    start: DATA_START_BLKS,
                    end: DATA_START_BLKS + data_align_blks(block_size) - 1,
                }
            );

            // one block short -> too small
            assert!(matches!(
                data_extent::<V2>(u64::MAX, min_num_blocks - 1, block_size),
                Err(LabelError::DeviceTooSmall { .. })
            ));

            // A 13 MiB device reserves 4 MiB from the actual end, leaving 5 MiB of data.
            let num_blocks = (13 * 1024 * 1024) / block_size;
            let extent = data_extent::<V2>(u64::MAX, num_blocks, block_size).unwrap();
            let size_bytes = (extent.end - extent.start + 1) * block_size;
            assert_eq!(size_bytes, 5 * 1024 * 1024);
            assert!(
                size_bytes.is_multiple_of(1024 * 1024),
                "size must be 1 MiB aligned"
            );
            assert!(
                !size_bytes.is_multiple_of(4 * 1024 * 1024),
                "size must not be a multiple of 4 MiB for a 13 MiB device"
            );
            assert_eq!(num_blocks - 1 - extent.end, void);

            // A 15 MiB device reserves 4 MiB from the actual end, leaving 7 MiB of data.
            let num_blocks = (15 * 1024 * 1024) / block_size;
            let extent = data_extent::<V2>(u64::MAX, num_blocks, block_size).unwrap();
            let size_bytes = (extent.end - extent.start + 1) * block_size;
            assert_eq!(size_bytes, 7 * 1024 * 1024);
            assert_eq!(num_blocks - 1 - extent.end, void);
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
            // usable_end = 262_144 - 8192 - 1 = 253_951. max = 245_760 = 120 MiB,
            // already 1 MiB aligned. data_end = 253_951.
            assert_eq!(data.ent_end, 253_951);
            assert!(data.is_multiple_of(V2::DATA_ALIGN_SIZE / disk.block_size));
            // exactly 4 MiB reserved before the end of the device
            assert_eq!(
                disk.num_blocks - 1 - data.ent_end,
                4 * 1024 * 1024 / disk.block_size
            );

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
            // usable_end = 262_144 - 1024 - 1 = 261_119. max = 260_096 = 1016 MiB,
            // already 1 MiB aligned. data_end = 261_119.
            assert_eq!(data.ent_end, 261_119);
            assert!(data.is_multiple_of(V2::DATA_ALIGN_SIZE / disk.block_size));
            assert_eq!(
                disk.num_blocks - 1 - data.ent_end,
                4 * 1024 * 1024 / disk.block_size
            );

            assert!(label.check());
            assert_eq!(LabelVersion::detect(&label.gpt), Some(LabelVersion::V2));
        }
    }
}

pub use v1::V1;
pub use v2::V2;
