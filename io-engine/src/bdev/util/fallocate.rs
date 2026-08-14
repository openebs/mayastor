//! Utility functions for probing fallocate punch-hole support

use std::{os::unix::fs::FileTypeExt, path::Path};

/// Returns true if the aio bdev's `fallocate=true` trim passthrough should be
/// enabled for the given path.
///
/// Regular files support fallocate punch-hole on all mainstream filesystems.
/// For block devices the patched aio bdev issues BLKDISCARD for UNMAP and
/// BLKZEROOUT for WRITE_ZEROES and self-gates each op by the device's queue
/// limits, so we opt in whenever the device supports discard or write-zeroes.
pub(crate) fn supports_fallocate_punch_hole(path: &str) -> bool {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            debug!("failed to stat '{path}': {error}");
            return false;
        }
    };

    if metadata.file_type().is_file() {
        // Filesystem punch-hole genuinely frees space on regular files.
        return true;
    }

    if !metadata.file_type().is_block_device() {
        debug!("'{path}' is neither a regular file nor a block device");
        return false;
    }

    // Resolve any symlink (e.g. /dev/vg/lv) to the real device node
    // (e.g. /dev/dm-0) so we can find its sysfs queue limits.
    let device = match std::fs::canonicalize(path) {
        Ok(device) => device,
        Err(error) => {
            debug!("failed to canonicalize '{path}': {error}");
            return false;
        }
    };

    let Some(name) = device.file_name().and_then(|name| name.to_str()) else {
        debug!("failed to determine the device name of '{path}'");
        return false;
    };

    let queue = Path::new("/sys/block").join(name).join("queue");

    let write_zeroes_max_bytes: u64 =
        sysfs::parse_value(&queue, "write_zeroes_max_bytes").unwrap_or(0);
    let discard_max_bytes: u64 = sysfs::parse_value(&queue, "discard_max_bytes").unwrap_or(0);

    // The patched aio bdev self-gates each op by the device's queue limits
    // (BLKDISCARD for UNMAP, BLKZEROOUT for WRITE_ZEROES), so opt in whenever
    // the device supports either.
    discard_max_bytes > 0 || write_zeroes_max_bytes > 0
}

#[cfg(test)]
mod tests {
    use super::supports_fallocate_punch_hole;

    #[test]
    fn regular_file_supports_punch_hole() {
        let path = std::env::temp_dir().join(format!(
            "supports_fallocate_punch_hole-{}",
            std::process::id()
        ));
        std::fs::write(&path, b"test").unwrap();
        let supported = supports_fallocate_punch_hole(path.to_str().unwrap());
        std::fs::remove_file(&path).unwrap();
        assert!(supported);
    }

    #[test]
    fn nonexistent_path_does_not_support_punch_hole() {
        assert!(!supports_fallocate_punch_hole(
            "/nonexistent/supports_fallocate_punch_hole"
        ));
    }
}
