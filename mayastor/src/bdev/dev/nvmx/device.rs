use nix::errno::Errno;
use std::{convert::From, ptr::NonNull};

use spdk_sys::{
    self,
    spdk_nvme_ctrlr,
    spdk_nvme_ns,
    spdk_nvme_ns_get_extended_sector_size,
    spdk_nvme_ns_get_md_size,
    spdk_nvme_ns_get_num_sectors,
    spdk_nvme_ns_get_size,
    spdk_nvme_ns_get_uuid,
    spdk_nvme_ns_supports_compare,
};

use crate::{
    bdev::{
        dev::nvmx::{NvmeController, NvmeDeviceHandle, NVME_CONTROLLERS},
        nexus::nexus_io::IoType,
    },
    core::{
        uuid::Uuid,
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        BlockDeviceStats,
        CoreError,
    },
    nexus_uri::NexusBdevError,
};

pub struct NvmeBlockDevice {
    ns: NonNull<spdk_nvme_ns>,
    name: String,
}
/*
 * Descriptor for an opened NVMe device that represents a namespace for
 * an NVMe controller.
 */
pub struct NvmeDeviceDescriptor {
    ns: NonNull<spdk_nvme_ns>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    io_device_id: u64,
    name: String,
}

impl NvmeDeviceDescriptor {
    fn create(
        controller: &NvmeController,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let ns = controller.spdk_namespace();

        if ns.is_null() {
            Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            })
        } else {
            Ok(Box::new(NvmeDeviceDescriptor {
                ns: NonNull::new(ns).unwrap(),
                io_device_id: controller.id(),
                name: controller.get_name(),
                ctrlr: NonNull::new(controller.spdk_handle()).unwrap(),
            }))
        }
    }
}

impl BlockDeviceDescriptor for NvmeDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(&self.name, self.ns.as_ptr()))
    }

    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, NexusBdevError> {
        Ok(Box::new(NvmeDeviceHandle::create(
            &self.name,
            self.io_device_id,
            self.ctrlr,
            self.ns,
        )?))
    }
}

impl NvmeBlockDevice {
    pub fn open_by_name(
        name: &str,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        // TODO: Handle read_write flag properly.
        if !read_write {
            warn!("read-only mode is not supported in NvmeBlockDevice::open_by_name()");
        }

        let controllers = NVME_CONTROLLERS.read().unwrap();
        if !controllers.contains_key(name) {
            return Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            });
        }
        let controller = controllers.get(name).unwrap().lock().unwrap();
        let descr = NvmeDeviceDescriptor::create(&controller)?;
        Ok(descr)
    }

    pub fn from_ns(name: &str, ns: *mut spdk_nvme_ns) -> NvmeBlockDevice {
        NvmeBlockDevice {
            ns: NonNull::new(ns)
                .expect("nullptr dereference while accessing NVMe namespace"),
            name: String::from(name),
        }
    }
}

impl BlockDevice for NvmeBlockDevice {
    fn size_in_bytes(&self) -> u64 {
        unsafe { spdk_nvme_ns_get_size(self.ns.as_ptr()) }
    }

    fn block_len(&self) -> u64 {
        unsafe {
            spdk_nvme_ns_get_extended_sector_size(self.ns.as_ptr()) as u64
        }
    }

    fn num_blocks(&self) -> u64 {
        unsafe { spdk_nvme_ns_get_num_sectors(self.ns.as_ptr()) }
    }

    fn uuid(&self) -> String {
        let u = Uuid(unsafe { spdk_nvme_ns_get_uuid(self.ns.as_ptr()) });
        uuid::Uuid::from_bytes(u.as_bytes())
            .to_hyphenated()
            .to_string()
    }

    fn product_name(&self) -> String {
        "NVMe disk".to_string()
    }

    fn driver_name(&self) -> String {
        String::from("nvme")
    }

    fn device_name(&self) -> String {
        self.name.clone()
    }

    fn alignment(&self) -> u64 {
        1
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        let spdk_ns = self.ns.as_ptr();

        // bdev_nvme_io_type_supported
        match io_type {
            IoType::Read
            | IoType::Write
            | IoType::Reset
            | IoType::Flush
            | IoType::NvmeAdmin
            | IoType::NvmeIO
            | IoType::Abort => true,
            IoType::Compare => unsafe {
                spdk_nvme_ns_supports_compare(spdk_ns)
            },
            IoType::NvmeIOMD => {
                let t = unsafe { spdk_nvme_ns_get_md_size(spdk_ns) };
                t > 0
            }
            IoType::Unmap => false,
            IoType::WriteZeros => false,
            IoType::CompareAndWrite => false,
            _ => false,
        }
    }

    fn io_stats(&self) -> Result<BlockDeviceStats, NexusBdevError> {
        Ok(Default::default())
    }

    fn claimed_by(&self) -> Option<String> {
        None
    }
}

/*
 * Lookup target NVMeOF device by its name (starts with nvmf://).
 */
pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
    match NVME_CONTROLLERS.read().unwrap().get(name) {
        Some(ctrlr) => Some(Box::new(NvmeBlockDevice::from_ns(
            name,
            ctrlr.lock().unwrap().spdk_namespace(),
        ))),
        _ => None,
    }
}

pub fn open_by_name(
    name: &str,
    read_write: bool,
) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
    NvmeBlockDevice::open_by_name(name, read_write)
}
