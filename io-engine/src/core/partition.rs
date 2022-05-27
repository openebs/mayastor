use std::cmp::min;

/// GPT partition table size in bytes.
pub const GPT_TABLE_SIZE: u64 = 128 * 128;

/// Offset for reserved metadata partition, in bytes.
pub const METADATA_RESERVATION_OFFSET: u64 = 1024 * 1024;

/// Reserved size for metadata partition, in bytes.
pub const METADATA_RESERVATION_SIZE: u64 = 4 * 1024 * 1024;

/// Start of data partition, in bytes.
pub const DATA_PARTITION_OFFSET: u64 =
    METADATA_RESERVATION_OFFSET + METADATA_RESERVATION_SIZE;

/// Calculates offsets of the first and last blocks of the data
/// partition for the given device size and block size.
///
/// Device layout:
///
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
///         ├── available for user data
/// N-34  ──┘
/// N-33  ──┐
///         ├── reserved for the copy of GPT entries
/// N-2   ──┘
/// N-1   ───── last device block, reserved for secondary GPT header
///
/// # Arguments
/// * `req_size`: Requested data partition size in bytes.
/// * `num_blocks`: Size of the device in blocks.
/// * `block_size`: Block size in bytes.
///
/// # Return
/// A tuple of first and last position of data partition, expressed in blocks,
/// if the device is large enough. `None` if the device is too small to
/// accommodate the required data layout.
pub fn calc_data_partition(
    req_size: u64,
    num_blocks: u64,
    block_size: u64,
) -> Option<(u64, u64)> {
    // Number of blocks occupied by GPT tables.
    let gpt_blocks = bytes_to_alinged_blocks(GPT_TABLE_SIZE, block_size);

    // First block of metadata reservation.
    let lba_start =
        bytes_to_alinged_blocks(METADATA_RESERVATION_OFFSET, block_size);

    // Last usable device block.
    let lba_end = num_blocks - gpt_blocks - 2;

    // Blocks used by metadata reservation.
    let meta_blocks =
        bytes_to_alinged_blocks(METADATA_RESERVATION_SIZE, block_size);

    // First block of data.
    let data_start = lba_start + meta_blocks;
    if data_start > lba_end {
        // Device is too small to accommodate Metadata reservation.
        return None;
    }

    // Number of requested data blocks.
    let req_blocks = bytes_to_alinged_blocks(req_size, block_size);

    // Last data block.
    let data_end = min(data_start + req_blocks - 1, lba_end);

    Some((data_start, data_end))
}

/// Converts an offset in bytes into offset in number of aligned blocks for the
/// given block size.
pub fn bytes_to_alinged_blocks(size: u64, block_size: u64) -> u64 {
    let blocks = size / block_size;
    match size % block_size {
        0 => blocks,
        _ => blocks + 1,
    }
}
