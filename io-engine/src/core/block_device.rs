use super::{
    CoreError,
    DeviceEventSink,
    IoCompletionStatus,
    IoType,
    SnapshotParams,
};

use spdk_rs::{DmaBuf, DmaError, IoVec};

use async_trait::async_trait;
use merge::Merge;
use nix::errno::Errno;
use std::os::raw::c_void;
use uuid::Uuid;

/// TODO
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

/// Core trait that represents a block device.
/// TODO: Add text.
#[async_trait(?Send)]
pub trait BlockDevice {
    /// Returns total size in bytes of the device.
    fn size_in_bytes(&self) -> u64;

    /// Returns the size of a block of the underlying device
    fn block_len(&self) -> u64;

    /// Returns number of blocks for the device.
    fn num_blocks(&self) -> u64;

    /// Returns the UUID of the device.
    fn uuid(&self) -> Uuid;

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

    /// Checks whether target I/O type is supported by the device.
    fn io_type_supported_by_device(&self, io_type: IoType) -> bool;

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
        listener: DeviceEventSink,
    ) -> Result<(), CoreError>;

    fn is_zoned(&self) -> bool;
    fn get_zone_size(&self) -> u64;
    fn get_num_zones(&self) -> u64;
    fn get_max_zone_append_size(&self) -> u32;
    fn get_max_open_zones(&self) -> u32;
    fn get_max_active_zones(&self) -> u32;
    fn get_optimal_open_zones(&self) -> u32;
}

/// Core trait that represents a descriptor for an opened block device.
/// TODO: Add text.
#[async_trait(?Send)]
pub trait BlockDeviceDescriptor {
    /// TODO
    fn get_device(&self) -> Box<dyn BlockDevice>;

    /// TODO
    fn device_name(&self) -> String;

    /// Consumes BlockDeviceDescriptor and returns a BlockDeviceHandle.
    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError>;

    /// Returns a BlockDeviceHandle for this descriptor without consuming it.
    fn get_io_handle(&self) -> Result<Box<dyn BlockDeviceHandle>, CoreError>;

    /// TODO
    fn unclaim(&self);

    /// TODO
    async fn get_io_handle_nonblock(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError>;
}

/// TODO
pub type IoCompletionCallbackArg = *mut c_void;

/// TODO
pub type IoCompletionCallback =
    fn(&dyn BlockDevice, IoCompletionStatus, IoCompletionCallbackArg) -> ();

/// TODO
pub type OpCompletionCallbackArg = *mut c_void;

/// TODO
pub type OpCompletionCallback = fn(bool, OpCompletionCallbackArg) -> ();

/// Read modes.
pub enum ReadMode {
    /// Normal read operation.
    Normal,
    /// Fail when reading an unwritten block of a thin-provisioned device.
    UnwrittenFail,
}

/// Core trait that represents a device I/O handle.
/// TODO: Add text.
#[async_trait(?Send)]
pub trait BlockDeviceHandle {
    // Generic functions.

    /// TODO
    fn get_device(&self) -> &dyn BlockDevice;

    /// TODO
    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError>;

    // Futures-based I/O functions.

    /// TODO
    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError>;

    /// TODO
    fn set_read_mode(&mut self, mode: ReadMode);

    /// TODO
    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError>;

    // Callback-based I/O functions.

    /// TODO
    fn readv_blocks(
        &self,
        iov: *mut IoVec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    /// TODO
    fn writev_blocks(
        &self,
        iov: *mut IoVec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    /// TODO
    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    /// TODO
    fn unmap_blocks(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    /// TODO
    fn write_zeroes(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;

    /// TODO
    fn emulate_zone_mgmt_send_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: *mut c_void,
        _buffer_size: u64,
        _cb: IoCompletionCallback,
        _cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeIoPassthruDispatch {
            source: Errno::EOPNOTSUPP,
            opcode: nvme_cmd.opc(),
        })
    }

   /// TODO
    fn emulate_zone_mgmt_recv_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: *mut c_void,
        _buffer_size: u64,
        _cb: IoCompletionCallback,
        _cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeIoPassthruDispatch {
            source: Errno::EOPNOTSUPP,
            opcode: nvme_cmd.opc(),
        })
    }

    // NVMe only.
    /// TODO
    fn submit_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: *mut c_void,
        _buffer_size: u64,
        _cb: IoCompletionCallback,
        _cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeIoPassthruDispatch {
            source: Errno::EOPNOTSUPP,
            opcode: nvme_cmd.opc(),
        })
    }

    /// TODO
    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError>;

    /// TODO
    async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError>;

    /// TODO
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError>;

    /// TODO
    async fn create_snapshot(
        &self,
        params: SnapshotParams,
    ) -> Result<u64, CoreError>;

    /// TODO
    async fn nvme_resv_register(
        &self,
        _current_key: u64,
        _new_key: u64,
        _register_action: u8,
        _cptpl: u8,
    ) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::EOPNOTSUPP,
        })
    }

    /// TODO
    async fn nvme_resv_acquire(
        &self,
        _current_key: u64,
        _preempt_key: u64,
        _acquire_action: u8,
        _resv_type: u8,
    ) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::EOPNOTSUPP,
        })
    }

    /// NVMe Reservation Release
    async fn nvme_resv_release(
        &self,
        _current_key: u64,
        _resv_type: u8,
        _release_action: u8,
    ) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::EOPNOTSUPP,
        })
    }

    /// TODO
    async fn nvme_resv_report(
        &self,
        _cdw11: u32,
        _buffer: &mut DmaBuf,
    ) -> Result<(), CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::EOPNOTSUPP,
        })
    }

    /// TODO
    async fn io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeIoPassthruDispatch {
            source: Errno::EOPNOTSUPP,
            opcode: nvme_cmd.opc(),
        })
    }

    /// TODO
    async fn host_id(&self) -> Result<[u8; 16], CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::EOPNOTSUPP,
        })
    }
    /// Flush the io in buffer to disk, for the Local Block Device.
    fn flush_io(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError>;
}

/// TODO
pub trait LbaRangeController {}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum DeviceTimeoutAction {
    /// Abort I/O operation that times out.
    Abort,
    /// Reset the  whole device in case any single command times out.
    Reset,
    /// Do not take any actions on command timeout.
    Ignore,
    /// Remove the device from the configuration
    HotRemove,
}

impl ToString for DeviceTimeoutAction {
    fn to_string(&self) -> String {
        match *self {
            Self::Abort => "Abort",
            Self::Reset => "Reset",
            Self::Ignore => "Ignore",
            Self::HotRemove => "HotRemove",
        }
        .to_string()
    }
}

/// TODO
pub trait DeviceIoController {
    /// TODO
    fn get_timeout_action(&self) -> Result<DeviceTimeoutAction, CoreError>;

    /// TODO
    fn set_timeout_action(
        &mut self,
        action: DeviceTimeoutAction,
    ) -> Result<(), CoreError>;
}
