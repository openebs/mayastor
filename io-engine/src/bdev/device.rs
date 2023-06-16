//!
//! Trait implementation for native bdev

use std::{
    collections::HashMap,
    convert::TryFrom,
    os::raw::c_void,
    sync::{Arc, Mutex},
    mem,
};

use async_trait::async_trait;
use nix::errno::Errno;
use once_cell::sync::{Lazy, OnceCell};

use spdk_rs::{
    libspdk::{
        iovec,
        spdk_bdev_flush,
        spdk_bdev_free_io,
        spdk_bdev_io,
        spdk_bdev_readv_blocks,
        spdk_bdev_reset,
        spdk_bdev_unmap_blocks,
        spdk_bdev_write_zeroes_blocks,
        spdk_bdev_writev_blocks,
        spdk_bdev_nvme_io_passthru,
        spdk_bdev_zone_info,
        spdk_bdev_get_zone_info,
        spdk_bdev_zone_management,
        SPDK_BDEV_ZONE_CLOSE,
        SPDK_BDEV_ZONE_FINISH,
        SPDK_BDEV_ZONE_OPEN,
        SPDK_BDEV_ZONE_RESET,
        SPDK_BDEV_ZONE_OFFLINE,
        SPDK_NVME_ZONE_STATE_EMPTY,
        SPDK_NVME_ZONE_STATE_IOPEN,
        SPDK_NVME_ZONE_STATE_EOPEN,
        SPDK_NVME_ZONE_STATE_CLOSED,
        SPDK_NVME_ZONE_STATE_RONLY,
        SPDK_NVME_ZONE_STATE_FULL,
        SPDK_NVME_ZONE_STATE_OFFLINE,
        SPDK_BDEV_ZONE_STATE_EMPTY,
        SPDK_BDEV_ZONE_STATE_IMP_OPEN,
        SPDK_BDEV_ZONE_STATE_FULL,
        SPDK_BDEV_ZONE_STATE_CLOSED,
        SPDK_BDEV_ZONE_STATE_READ_ONLY,
        SPDK_BDEV_ZONE_STATE_OFFLINE,
        SPDK_BDEV_ZONE_STATE_EXP_OPEN,
        SPDK_NVME_ZRA_LIST_ALL,
        SPDK_NVME_ZRA_LIST_ZSE,
        SPDK_NVME_ZRA_LIST_ZSIO,
        SPDK_NVME_ZRA_LIST_ZSEO,
        SPDK_NVME_ZRA_LIST_ZSC,
        SPDK_NVME_ZRA_LIST_ZSF,
        SPDK_NVME_ZRA_LIST_ZSRO,
        SPDK_NVME_ZRA_LIST_ZSO,
    },
    nvme_admin_opc,
    BdevOps,
    DmaBuf,
    DmaError,
    IoType,
};

use crate::{
    core::{
        mempool::MemoryPool,
        snapshot::SnapshotOps,
        Bdev,
        BdevHandle,
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        BlockDeviceIoStats,
        CoreError,
        DeviceEventDispatcher,
        DeviceEventSink,
        DeviceEventType,
        DeviceIoController,
        IoCompletionCallback,
        IoCompletionCallbackArg,
        IoCompletionStatus,
        NvmeStatus,
        ReadMode,
        SnapshotParams,
        UntypedBdev,
        UntypedBdevHandle,
        UntypedDescriptorGuard,
    },
    lvs::Lvol,
    ffihelper::FfiResult,
};

use jemalloc_sys::{
    calloc,
    free,
};

/// TODO
type EventDispatcherMap = HashMap<String, DeviceEventDispatcher>;

/// TODO
static BDEV_EVENT_DISPATCHER: Lazy<Mutex<EventDispatcherMap>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// Memory pool for bdev I/O context.
static BDEV_IOCTX_POOL: OnceCell<MemoryPool<IoCtx>> = OnceCell::new();

/// TODO
fn bdev_zone_state_to_nvme_zns_zone_state(bdev_zone_state: u32) -> Result<u32, CoreError> {
    match bdev_zone_state {
        SPDK_BDEV_ZONE_STATE_EMPTY => Ok(SPDK_NVME_ZONE_STATE_EMPTY),
        SPDK_BDEV_ZONE_STATE_IMP_OPEN => Ok(SPDK_NVME_ZONE_STATE_IOPEN),
        SPDK_BDEV_ZONE_STATE_FULL => Ok(SPDK_NVME_ZONE_STATE_FULL),
        SPDK_BDEV_ZONE_STATE_CLOSED => Ok(SPDK_NVME_ZONE_STATE_CLOSED),
        SPDK_BDEV_ZONE_STATE_READ_ONLY => Ok(SPDK_NVME_ZONE_STATE_RONLY),
        SPDK_BDEV_ZONE_STATE_OFFLINE => Ok(SPDK_NVME_ZONE_STATE_OFFLINE),
        SPDK_BDEV_ZONE_STATE_EXP_OPEN => Ok(SPDK_NVME_ZONE_STATE_EOPEN),
        _ => {
            error!("Can not map SPDK_BDEV_ZONE_STATE {} to any SPDK_NVME_ZONE_STATE", bdev_zone_state);
            Err(CoreError::NvmeIoPassthruDispatch {
                source: Errno::EINVAL,
                opcode: 122,
            })
        },
    }
}

/// TODO
fn zone_send_action_to_bdev_zone_action(zone_send_action: u8) -> Result<u32, CoreError> {
    match zone_send_action {
        0x01 => Ok(SPDK_BDEV_ZONE_CLOSE),
        0x02 => Ok(SPDK_BDEV_ZONE_FINISH),
        0x03 => Ok(SPDK_BDEV_ZONE_OPEN),
        0x04 => Ok(SPDK_BDEV_ZONE_RESET),
        0x05 => Ok(SPDK_BDEV_ZONE_OFFLINE),
        _ => {
            error!("Can not map Zone Send Action {} to any spdk_bdev_zone_action", zone_send_action);
            Err(CoreError::NvmeIoPassthruDispatch {
                source: Errno::EINVAL,
                opcode: 121,
            })
        },
    }
}

/// TODO
fn is_zra_list_matching_zone_state(zra_report_opt: u32, zns_zone_state: u32) -> bool {
    match (zra_report_opt, zns_zone_state) {
        (SPDK_NVME_ZRA_LIST_ALL, _) => true,
        (SPDK_NVME_ZRA_LIST_ZSE, SPDK_NVME_ZONE_STATE_EMPTY) => true,
        (SPDK_NVME_ZRA_LIST_ZSIO, SPDK_NVME_ZONE_STATE_IOPEN) => true,
        (SPDK_NVME_ZRA_LIST_ZSEO, SPDK_NVME_ZONE_STATE_EOPEN) => true,
        (SPDK_NVME_ZRA_LIST_ZSC, SPDK_NVME_ZONE_STATE_CLOSED) => true,
        (SPDK_NVME_ZRA_LIST_ZSF, SPDK_NVME_ZONE_STATE_FULL) => true,
        (SPDK_NVME_ZRA_LIST_ZSRO, SPDK_NVME_ZONE_STATE_RONLY) => true,
        (SPDK_NVME_ZRA_LIST_ZSO, SPDK_NVME_ZONE_STATE_OFFLINE) => true,
        _ => false,
    }
}

/// Wrapper around native SPDK block devices, which mimics target SPDK block
/// device as an abstract BlockDevice instance.
#[derive(Copy, Clone)]
pub struct SpdkBlockDevice(UntypedBdev);

impl SpdkBlockDevice {
    fn new(bdev: UntypedBdev) -> Self {
        Self(bdev)
    }

    /// Lookup existing SPDK bdev by its name.
    pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
        debug!("Searching SPDK devices for '{}'...", name);
        let bdev = UntypedBdev::lookup_by_name(name)?;
        debug!("SPDK {} device found: '{}'", bdev.driver(), bdev.name());
        Some(Box::new(SpdkBlockDevice::new(bdev)))
    }

    /// Open SPDK bdev by its name and get a block device descriptor.
    pub fn open_by_name(
        name: &str,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = UntypedBdev::open_by_name(name, read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }
}

#[async_trait(?Send)]
impl BlockDevice for SpdkBlockDevice {
    /// return the size in bytes
    fn size_in_bytes(&self) -> u64 {
        self.0.size_in_bytes()
    }
    /// returns the length of the block size in bytes
    fn block_len(&self) -> u64 {
        self.0.block_len() as u64
    }
    /// number of blocks the device has
    fn num_blocks(&self) -> u64 {
        self.0.num_blocks()
    }
    /// the UUID of the device
    fn uuid(&self) -> uuid::Uuid {
        self.0.uuid()
    }
    //// returns the product name
    fn product_name(&self) -> String {
        self.0.product_name().to_string()
    }
    //// returns the driver name of the block device
    fn driver_name(&self) -> String {
        self.0.driver().to_string()
    }
    /// returns the name of the device
    fn device_name(&self) -> String {
        self.0.name().to_string()
    }
    //// returns the alignment of the device
    fn alignment(&self) -> u64 {
        self.0.alignment()
    }
    /// returns true if the IO type is supported
    fn io_type_supported(&self, io_type: IoType) -> bool {
        match io_type {
            IoType::NvmeIo => true,
            _ => self.io_type_supported_by_device(io_type),
        }
    }

    fn io_type_supported_by_device(&self, io_type: IoType) -> bool {
        self.0.io_type_supported(io_type)
    }

    fn is_zoned(&self) -> bool {
        self.0.is_zoned()
    }

    fn get_zone_size(&self) -> u64 {
        self.0.get_zone_size()
    }

    fn get_num_zones(&self) -> u64 {
        self.0.get_num_zones()
    }

    fn get_max_zone_append_size(&self) -> u32 {
        self.0.get_max_zone_append_size()
    }

    fn get_max_open_zones(&self) -> u32 {
        self.0.get_max_open_zones()
    }

    fn get_max_active_zones(&self) -> u32 {
        self.0.get_max_active_zones()
    }

    fn get_optimal_open_zones(&self) -> u32 {
        self.0.get_optimal_open_zones()
    }

    /// returns the IO statistics
    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        self.0.stats_async().await
    }
    /// returns which module has returned driver
    fn claimed_by(&self) -> Option<String> {
        self.0.claimed_by()
    }
    /// open the device returning descriptor to the device
    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = self.0.open(read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }

    /// returns the IO controller
    fn get_io_controller(&self) -> Option<Box<dyn DeviceIoController>> {
        None
    }
    /// add a callback to be called when a particular event is received
    fn add_event_listener(
        &self,
        listener: DeviceEventSink,
    ) -> Result<(), CoreError> {
        let mut map = BDEV_EVENT_DISPATCHER.lock().expect("lock poisoned");
        let disp = map.entry(self.device_name()).or_default();
        disp.add_listener(listener);
        Ok(())
    }
}

/// Wrapper around native SPDK block device descriptor, which mimics target SPDK
/// descriptor as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceDescriptor(Arc<UntypedDescriptorGuard>);

impl From<UntypedDescriptorGuard> for SpdkBlockDeviceDescriptor {
    fn from(descr: UntypedDescriptorGuard) -> Self {
        Self(Arc::new(descr))
    }
}

#[async_trait(?Send)]
impl BlockDeviceDescriptor for SpdkBlockDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(SpdkBlockDevice::new(self.0.bdev()))
    }

    fn device_name(&self) -> String {
        self.0.bdev().name().to_string()
    }

    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(self.0)?;
        Ok(Box::new(handle))
    }

    fn get_io_handle(&self) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(self.0.clone())?;
        Ok(Box::new(handle))
    }

    async fn get_io_handle_nonblock(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(self.0.clone())?;
        Ok(Box::new(handle))
    }

    fn unclaim(&self) {
        self.0.unclaim()
    }
}

/// Wrapper around native SPDK block device I/O, which mimics target SPDK I/O
/// handle as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceHandle {
    device: SpdkBlockDevice,
    handle: UntypedBdevHandle,
}

impl TryFrom<Arc<UntypedDescriptorGuard>> for SpdkBlockDeviceHandle {
    type Error = CoreError;

    fn try_from(
        desc: Arc<UntypedDescriptorGuard>,
    ) -> Result<Self, Self::Error> {
        let handle = BdevHandle::try_from(desc)?;
        Ok(SpdkBlockDeviceHandle::from(handle))
    }
}

impl From<UntypedBdevHandle> for SpdkBlockDeviceHandle {
    fn from(handle: UntypedBdevHandle) -> Self {
        Self {
            device: SpdkBlockDevice::new(handle.get_bdev()),
            handle,
        }
    }
}

#[async_trait(?Send)]
impl BlockDeviceHandle for SpdkBlockDeviceHandle {
    fn get_device(&self) -> &dyn BlockDevice {
        &self.device
    }

    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.device.alignment())
    }

    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError> {
        self.handle.read_at(offset, buffer).await
    }

    fn set_read_mode(&mut self, mode: ReadMode) {
        self.handle.set_read_mode(mode);
    }

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        self.handle.write_at(offset, buffer).await
    }

    fn readv_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Read,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_readv_blocks(
                desc,
                chan,
                iov,
                iovcnt,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::ReadDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn writev_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Write,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_writev_blocks(
                desc,
                chan,
                iov,
                iovcnt,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::WriteDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Reset,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_reset(
                desc,
                chan,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::ResetDispatch {
                source: Errno::ENOMEM,
            })
        } else {
            Ok(())
        }
    }

    fn unmap_blocks(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Unmap,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_unmap_blocks(
                desc,
                chan,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::ENOMEM,
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn write_zeroes(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::WriteZeros,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_write_zeroes_blocks(
                desc,
                chan,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::ENOMEM,
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: opcode.into(),
        })
    }

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_cmd.opc(),
        })
    }

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_admin_opc::IDENTIFY.into(),
        })
    }

    fn emulate_zone_mgmt_send_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        buffer: *mut c_void,
        buffer_size: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        unsafe { buffer.write_bytes(0, buffer_size as usize) };

        // Read relevant fields for a 'Zone Management Send' command, see 'NVMe Zoned Namespace Command Set Specification, Revision 1.1c'
        // Bit 63:00 Dword11:Dword10 > Starting LBA
        let mut slba;
        unsafe {
            slba = ((nvme_cmd.__bindgen_anon_2.cdw11 as u64) << 32) | nvme_cmd.__bindgen_anon_1.cdw10 as u64;
        }

        // Bit 07:00 Dword 13 > Zone Send Action
        let zsa = zone_send_action_to_bdev_zone_action(nvme_cmd.cdw13 as u8).unwrap();

        // Bit 08 Dword 13 > Select All
        let select_all = nvme_cmd.cdw13 & (1 << 8) != 0;

        if select_all {
            slba = 0;
        }

        let ctx = alloc_bdev_io_ctx(
            IoType::NvmeIo,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, ch) = self.handle.io_tuple();

        let num_zones = self.device.get_num_zones();
        let zone_size = self.device.get_zone_size();

        let mut result;
        loop {
            result = unsafe {
                spdk_bdev_zone_management(
                    desc,
                    ch,
                    slba,
                    zsa,
                    Some(bdev_io_completion),
                    ctx as *mut c_void,
                )
            }.to_result(|e| CoreError::NvmeIoPassthruDispatch {
                source: Errno::from_i32(e),
                opcode: nvme_cmd.opc(),
            });
            let continue_next_zone = select_all && slba == num_zones * zone_size;
            if !continue_next_zone || result.is_err() {
                break result;
            }
            slba += zone_size;
        }
    }

    fn emulate_zone_mgmt_recv_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        buffer: *mut c_void,
        buffer_size: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {

        let ctx = alloc_bdev_io_ctx(
            IoType::NvmeIo,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, ch) = self.handle.io_tuple();

        let size_of_spdk_bdev_zone_info = mem::size_of::<spdk_bdev_zone_info>() as usize;

        // Bit 63:00 Dword11:Dword10 > Starting LBA
        let slba = unsafe { ((nvme_cmd.__bindgen_anon_2.cdw11 as u64) << 32) | nvme_cmd.__bindgen_anon_1.cdw10 as u64 };

        // Bit 07:00 Dword13 > Zone Receive Action
        let zra = nvme_cmd.cdw13 as u8;
        if zra != 0x0u8 {
            error!("Zone Management Receive 'Zone Receive Action' (cdw13) != 00h (Report Zones) not implemented");
            return Err(CoreError::NvmeIoPassthruDispatch{
                source: Errno::EOPNOTSUPP,
                opcode: nvme_cmd.opc(),
            });
        }

        // Bit 16 Dword13 > Partial Report
        let partial_report = nvme_cmd.cdw13 & (1 << 16) != 0;
        if !partial_report {
            error!("Zone Management Receive 'Partial Report' (cdw13) == 0 not implemented");
            return Err(CoreError::NvmeIoPassthruDispatch{
                source: Errno::EOPNOTSUPP,
                opcode: nvme_cmd.opc(),
            });
        }

        // Bit 15:08 Dword13 > Reporting Options
        let zra_report_opt = (nvme_cmd.cdw13 >> 8) as u8;

        let max_num_zones = self.device.get_num_zones();
        let zone_size = self.device.get_zone_size();
        let zone_report_offset = slba / zone_size;
        let max_num_zones_to_report = max_num_zones - zone_report_offset;

        // Bit 31:00 Dword12 > Number of Dwords
        let num_of_dwords = unsafe{ nvme_cmd.__bindgen_anon_3.cdw12 } + 1;
        if u64::from(((num_of_dwords * 4) - 64) / 64) < max_num_zones_to_report {
            error!("Zone Management Receive 'Number of Dwords' (cdw12) indicates to less space of the number of zones ({}) that will be reported.", max_num_zones_to_report);
            return Err(CoreError::NvmeIoPassthruDispatch{
                source: Errno::EOPNOTSUPP,
                opcode: nvme_cmd.opc(),
            });
        }

        let bdev_zone_infos;

        let ret = unsafe {
            bdev_zone_infos = calloc(max_num_zones_to_report as usize, size_of_spdk_bdev_zone_info);
            spdk_bdev_get_zone_info(
                desc,
                ch,
                slba,
                max_num_zones_to_report,
                bdev_zone_infos as *mut spdk_bdev_zone_info,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        }.to_result(|e| CoreError::NvmeIoPassthruDispatch {
            source: Errno::from_i32(e),
            opcode: nvme_cmd.opc(),
        });

        // Populate buff with the 'Extended Report Zones Data Structure' of the 'NVMe Zoned Namespace Command Set Specification, Revision 1.1c'
        unsafe { buffer.write_bytes(0, buffer_size as usize) };

        if ret.is_err() {
            unsafe { free(bdev_zone_infos) };
            return ret;
        }
        // Bytes 07:00 > Number of Zones
        // Deferred until we know how many zones we actuallay reported

        // Bytes 63:08 > Reserved
        let erzds_rsvd_offset: isize = 64;

        // Bytes 127:64 and the following 64 * (max_num_zones - 1) bytes > Zone Descriptor
        let zone_desc_size: isize = 64;

        // Zone Descriptor Extention not needed
        let zone_desc_ext_size: isize = 0;

        let mut zone = 0u64;
        let mut num_zones_reported = 0u64;

        let bdev_zone_info_c_void = unsafe { calloc(1, size_of_spdk_bdev_zone_info) };
        loop {
            if zone >= max_num_zones_to_report {
                break;
            }
            unsafe {
                // Fetch and cast the current zone info
                std::ptr::copy_nonoverlapping(
                    bdev_zone_infos.offset((zone as usize * size_of_spdk_bdev_zone_info) as isize),
                    bdev_zone_info_c_void,
                    size_of_spdk_bdev_zone_info
                );
                let bdev_zone_info: *mut spdk_bdev_zone_info = std::ptr::slice_from_raw_parts_mut(
                    bdev_zone_info_c_void,
                    size_of_spdk_bdev_zone_info
                ) as _;

                if !is_zra_list_matching_zone_state(zra_report_opt as u32, (*bdev_zone_info).state) {
                    zone += 1;
                    continue;
                }

                // Byte 00 of Zone Descriptor > Zone Type (always sequential = 0x2u8)
                let mut byte_offset: isize = 0;
                let mut zt = 0x2u8;
                std::ptr::copy_nonoverlapping(
                    &mut zt as *mut _ as *mut c_void,
                    buffer.offset(erzds_rsvd_offset + (zone as isize * (zone_desc_size + zone_desc_ext_size)) + byte_offset),
                    1
                );
                byte_offset += 1;

                // Byte 01, bits 7:4 > Zone State
                let mut zs = bdev_zone_state_to_nvme_zns_zone_state((*bdev_zone_info).state).unwrap() as u8;
                zs = zs << 4;
                std::ptr::copy_nonoverlapping(
                    &mut zs as *mut _ as *mut c_void,
                    buffer.offset(erzds_rsvd_offset + (zone as isize * (zone_desc_size + zone_desc_ext_size)) + byte_offset),
                    1
                );
                byte_offset += 1;

                //Byte 02 > Zone Attributes (always 0x0u8)
                byte_offset += 1;

                //Byte 03 > Zone Attributes Information (always 0x0u8)
                byte_offset += 1;

                //Byte 07:04 > Reserved (always 0x0u32)
                byte_offset += 4;

                //Byte 15:08 > Zone Capacity
                let mut zcap = (*bdev_zone_info).capacity;
                std::ptr::copy_nonoverlapping(
                    &mut zcap as *mut _ as *mut c_void,
                    buffer.offset(erzds_rsvd_offset + (zone as isize * (zone_desc_size + zone_desc_ext_size)) + byte_offset),
                    8
                );
                byte_offset += 8;

                //Byte 23:16 > Zone Start Logical Block Address
                let mut zslba = (*bdev_zone_info).zone_id as u64;
                std::ptr::copy_nonoverlapping(
                    &mut zslba as *mut _ as *mut c_void,
                    buffer.offset(erzds_rsvd_offset + (zone as isize * (zone_desc_size + zone_desc_ext_size)) + byte_offset),
                    8
                );
                byte_offset += 8;

                //Byte 31:24 > Write Pointer
                let mut wp = (*bdev_zone_info).write_pointer as u64;
                std::ptr::copy_nonoverlapping(
                    &mut wp as *mut _ as *mut c_void,
                    buffer.offset(erzds_rsvd_offset + (zone as isize * (zone_desc_size + zone_desc_ext_size)) + byte_offset),
                    8
                );
                //byte_offset += 8;

                // Byte 32:63 > Reserved
                zone += 1;
                num_zones_reported += 1;
            }
        }

        // Bytes 07:00 > Number of Zones
        unsafe {
            std::ptr::copy_nonoverlapping(
                &mut num_zones_reported as *mut _ as *mut c_void,
                buffer,
                mem::size_of::<u64>() as usize
            );
        }

        unsafe {
            free(bdev_zone_info_c_void);
            free(bdev_zone_infos);
        }
        ret
    }

    fn submit_io_passthru(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        buffer: *mut c_void,
        buffer_size: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {

        let ctx = alloc_bdev_io_ctx(
            IoType::NvmeIo,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, ch) = self.handle.io_tuple();

        unsafe {
            spdk_bdev_nvme_io_passthru(
                desc,
                ch,
                nvme_cmd,
                buffer,
                buffer_size,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        }.to_result(|e| CoreError::NvmeIoPassthruDispatch {
            source: Errno::from_i32(e),
            opcode: nvme_cmd.opc(),
        })
    }

    // NVMe commands are not applicable for non-NVMe devices.
    async fn create_snapshot(
        &self,
        snapshot: SnapshotParams,
    ) -> Result<u64, CoreError> {
        let bdev = self.handle.get_bdev();

        // Snapshots are supported only for LVOLs.
        if bdev.driver() != "lvol" {
            return Err(CoreError::NotSupported {
                source: Errno::ENXIO,
            });
        }

        let lvol =
            Lvol::try_from(bdev).map_err(|_e| CoreError::BdevNotFound {
                name: bdev.name().to_string(),
            })?;

        lvol.create_snapshot(snapshot).await.map_err(|e| {
            CoreError::SnapshotCreate {
                reason: e.to_string(),
            }
        })?;

        Ok(0)
    }
    // Flush the io in buffer to disk, for the Local Block Device.
    fn flush_io(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Flush,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let bdev_size = self.device.size_in_bytes();
        let rc = unsafe {
            spdk_bdev_flush(
                desc,
                chan,
                0,
                bdev_size,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::FlushDispatch {
                source: Errno::ENOMEM,
            })
        } else {
            Ok(())
        }
    }
}

/// TODO
struct IoCtx {
    device: SpdkBlockDevice,
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
}

/// TODO
#[inline]
pub fn io_type_to_err(
    op: IoType,
    source: Errno,
    offset: u64,
    len: u64,
) -> CoreError {
    match op {
        IoType::Read => CoreError::ReadDispatch {
            source,
            offset,
            len,
        },
        IoType::Write => CoreError::WriteDispatch {
            source,
            offset,
            len,
        },
        IoType::Unmap | IoType::WriteZeros => CoreError::UnmapDispatch {
            source,
            offset,
            len,
        },
        IoType::Reset => CoreError::ResetDispatch {
            source,
        },
        _ => {
            warn!("Unsupported I/O operation: {:?}", op);
            CoreError::NotSupported {
                source,
            }
        }
    }
}

/// Initialize memory pool for allocating bdev I/O contexts.
/// This must be called before the first I/O operations take place.
pub fn bdev_io_ctx_pool_init(size: u64) {
    BDEV_IOCTX_POOL.get_or_init(|| {
        MemoryPool::<IoCtx>::create("bdev_io_ctx", size).expect(
            "Failed to create memory pool [bdev_io_ctx] for bdev I/O contexts",
        )
    });
}

/// Allocate a bdev I/O context from the pool.
fn alloc_bdev_io_ctx(
    op: IoType,
    ctx: IoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut IoCtx, CoreError> {
    let pool = BDEV_IOCTX_POOL.get().unwrap();
    pool.get(ctx).ok_or_else(|| {
        io_type_to_err(op, Errno::ENOMEM, offset_blocks, num_blocks)
    })
}

/// Release the memory used by the bdev I/O context back to the pool.
fn free_bdev_io_ctx(ctx: *mut IoCtx) {
    let pool = BDEV_IOCTX_POOL.get().unwrap();
    pool.put(ctx);
}

extern "C" fn bdev_io_completion(
    child_bio: *mut spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let bio = unsafe { &mut *(ctx as *mut IoCtx) };

    // Get extended NVMe error status from original bio in case of error.
    let status = if success {
        IoCompletionStatus::Success
    } else {
        IoCompletionStatus::from(NvmeStatus::from(child_bio))
    };

    (bio.cb)(&bio.device, status, bio.cb_arg);

    // Free ctx.
    free_bdev_io_ctx(&mut *bio);

    // Free replica's bio.
    unsafe {
        spdk_bdev_free_io(child_bio);
    }
}

/// Forwards event to the high-level handler.
fn dispatch_bdev_event(event: DeviceEventType, name: &str) {
    let mut map = BDEV_EVENT_DISPATCHER.lock().expect("lock poisoned");
    if let Some(disp) = map.get_mut(name) {
        disp.dispatch_event(event, name);
    }
}

/// Called by spdk when there is an asynchronous bdev event i.e. removal.
pub fn bdev_event_callback<T: BdevOps>(
    event: spdk_rs::BdevEvent,
    bdev: spdk_rs::Bdev<T>,
) {
    let dev = Bdev::<T>::new(bdev);

    // Translate SPDK events into common device events.
    let event = match event {
        spdk_rs::BdevEvent::Remove => {
            info!("Received SPDK remove event for bdev '{}'", dev.name());
            DeviceEventType::DeviceRemoved
        }
        spdk_rs::BdevEvent::Resize => {
            warn!("Received SPDK resize event for bdev '{}'", dev.name());
            DeviceEventType::DeviceResized
        }
        spdk_rs::BdevEvent::MediaManagement => {
            warn!(
                "Received SPDK media management event for Bdev '{}'",
                dev.name()
            );
            DeviceEventType::MediaManagement
        }
    };

    dispatch_bdev_event(event, dev.name());
}

/// Dispatches a special event for loopback device removal.
pub fn dispatch_loopback_removed(name: &str) {
    dispatch_bdev_event(DeviceEventType::LoopbackRemoved, name);
}
