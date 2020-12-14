use crate::{
    bdev::nexus::nexus_io::IoType,
    core::{CoreError, DmaBuf},
    nexus_uri::NexusBdevError,
};
use async_trait::async_trait;

#[derive(Debug, Default)]
pub struct BlockDeviceStats {
    pub num_read_ops: u64,
    pub num_write_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/*
 * Core trait that represents a block device.
 * TODO: Add text.
 */
pub trait BlockDevice {
    /// Returns total size in bytes of the device.
    fn size_in_bytes(&self) -> u64;

    /// Returns the size of a block of the underlying device
    fn block_len(&self) -> u64;

    /// Returns number of blocks for the device.
    fn num_blocks(&self) -> u64;

    /// Returns the UUID of the device.
    fn uuid(&self) -> String;

    /// Returns configured product name for the device.
    fn product_name(&self) -> String;

    /// Returns the name of driver module for the device.
    fn driver_name(&self) -> String;

    /// Returns the name of the device.
    fn device_name(&self) -> String;

    /// Returns aligment of the device.
    fn alignment(&self) -> u64;

    /// Checks whether target I/O type is supported by the device.
    fn io_type_supported(&self, io_type: IoType) -> bool;

    /// Obtains I/O statistics for the device.
    fn io_stats(&self) -> Result<BlockDeviceStats, NexusBdevError>;

    /// Checks if block device has been claimed.
    fn claimed_by(&self) -> Option<String>;
}

/*
 * Core trait that represents a descriptor for an opened block device.
 * TODO: Add text.
 */
pub trait BlockDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice>;
    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, NexusBdevError>;
}

/*
 * Core trait that represents a device I/O handle.
 * TODO: Add text.
 */
#[async_trait(?Send)]
pub trait BlockDeviceHandle {
    fn get_device(&self) -> Box<dyn BlockDevice>;

    // NVMe specific commands.
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError>;
}

pub trait LbaRangeController {}
