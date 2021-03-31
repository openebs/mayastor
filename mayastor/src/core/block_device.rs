use crate::core::{CoreError, DmaBuf, DmaError, IoCompletionStatus, IoType};
use async_trait::async_trait;
use merge::Merge;
use std::os::raw::c_void;

#[derive(Debug, Default, Clone, Copy, Merge)]
pub struct BlockDeviceIoStats {
    #[merge(strategy = merge::num::saturating_add)]
    pub num_read_ops: u64,
    #[merge(strategy = merge::num::saturating_add)]
    pub num_write_ops: u64,
    #[merge(strategy = merge::num::saturating_add)]
    pub bytes_read: u64,
    #[merge(strategy = merge::num::saturating_add)]
    pub bytes_written: u64,
    #[merge(strategy = merge::num::saturating_add)]
    pub num_unmap_ops: u64,
    #[merge(strategy = merge::num::saturating_add)]
    pub bytes_unmapped: u64,
}

use spdk_sys::iovec;

/*
 * Core trait that represents a block device.
 * TODO: Add text.
 */
#[async_trait(?Send)]
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
    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError>;

    /// Checks if block device has been claimed.
    fn claimed_by(&self) -> Option<String>;

    /// Open device and obtain a descriptor.
    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError>;

    /// Obtain I/O controller for device.
    fn get_io_controller(&self) -> Option<Box<dyn DeviceIoController>>;

    /// Register device event listener.
    fn add_event_listener(
        &self,
        listener: DeviceEventListener,
    ) -> Result<(), CoreError>;
}

/*
 * Core trait that represents a descriptor for an opened block device.
 * TODO: Add text.
 */
pub trait BlockDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice>;
    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError>;
    fn get_io_handle(&self) -> Result<Box<dyn BlockDeviceHandle>, CoreError>;
    fn unclaim(&self);
}

pub type DeviceEventListener = fn(DeviceEventType, &str);
pub type IoCompletionCallbackArg = *mut c_void;
pub type IoCompletionCallback = fn(
    &Box<dyn BlockDevice>,
    IoCompletionStatus,
    IoCompletionCallbackArg,
) -> ();
pub type OpCompletionCallbackArg = *mut c_void;
pub type OpCompletionCallback = fn(bool, OpCompletionCallbackArg) -> ();

/*
 * Core trait that represents a device I/O handle.
 * TODO: Add text.
 */
#[async_trait(?Send)]
pub trait BlockDeviceHandle {
    // Generic functions.
    fn get_device(&self) -> &Box<dyn BlockDevice>;
    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError>;

    // Futures-based I/O functions.
    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError>;

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError>;

    // Callback-based I/O functions.
    fn readv_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    fn writev_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    fn unmap_blocks(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    fn write_zeroes(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    // NVMe only.
    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError>;
    async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_sys::spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError>;
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError>;
    async fn create_snapshot(&self) -> Result<u64, CoreError>;
}

pub trait LbaRangeController {}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceTimeoutAction {
    /// Abort I/O operation that times out.
    Abort,
    /// Reset the  whole device in case any single command times out.
    Reset,
    /// Do not take any actions on command timeout.
    Ignore,
}

impl ToString for DeviceTimeoutAction {
    fn to_string(&self) -> String {
        match *self {
            Self::Abort => "Abort",
            Self::Reset => "Reset",
            Self::Ignore => "Ignore",
        }
        .to_string()
    }
}

pub trait DeviceIoController {
    fn get_timeout_action(&self) -> Result<DeviceTimeoutAction, CoreError>;
    fn set_timeout_action(
        &mut self,
        action: DeviceTimeoutAction,
    ) -> Result<(), CoreError>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DeviceEventType {
    DeviceRemoved,
    DeviceResized,
    MediaManagement,
}
