//! Utility functions for io_uring support

/// Returns true if the running kernel supports io_uring
pub fn kernel_support() -> bool {
    // Match SPDK_URING_QUEUE_DEPTH
    let queue_depth = 512;
    match io_uring::IoUring::new(queue_depth) {
        Ok(_ring) => true,
        Err(e) => {
            debug!("IoUring::new: {}", e);
            false
        }
    }
}
