use nix::errno::Errno;
use std::{convert::From, ptr::NonNull, sync::Arc};

use spdk_sys::{self, spdk_nvme_ctrlr};

use crate::{
    bdev::{
        dev::nvmx::{
            NvmeController,
            NvmeDeviceHandle,
            NvmeNamespace,
            NVME_CONTROLLERS,
        },
        nexus::nexus_io::IoType,
    },
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        BlockDeviceStats,
        CoreError,
    },
    nexus_uri::NexusBdevError,
};

pub struct NvmeBlockDevice {
    ns: Arc<NvmeNamespace>,
    name: String,
}
/*
 * Descriptor for an opened NVMe device that represents a namespace for
 * an NVMe controller.
 */
pub struct NvmeDeviceDescriptor {
    ns: Arc<NvmeNamespace>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    io_device_id: u64,
    name: String,
    prchk_flags: u32,
}

impl NvmeDeviceDescriptor {
    fn create(
        controller: &NvmeController,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        if let Some(ns) = controller.namespace() {
            Ok(Box::new(NvmeDeviceDescriptor {
                ns: Arc::clone(&ns),
                io_device_id: controller.id(),
                name: controller.get_name(),
                ctrlr: NonNull::new(controller.ctrlr_as_ptr()).unwrap(),
                prchk_flags: controller.flags(),
            }))
        } else {
            Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            })
        }
    }
}

impl BlockDeviceDescriptor for NvmeDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(&self.name, Arc::clone(&self.ns)))
    }

    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, NexusBdevError> {
        Ok(Box::new(NvmeDeviceHandle::create(
            &self.name,
            self.io_device_id,
            self.ctrlr,
            self.ns,
            self.prchk_flags,
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

    pub fn from_ns(name: &str, ns: Arc<NvmeNamespace>) -> NvmeBlockDevice {
        NvmeBlockDevice {
            ns,
            name: String::from(name),
        }
    }
}

impl BlockDevice for NvmeBlockDevice {
    fn size_in_bytes(&self) -> u64 {
        self.ns.size_in_bytes()
    }

    fn block_len(&self) -> u64 {
        self.ns.block_len()
    }

    fn num_blocks(&self) -> u64 {
        self.ns.num_blocks()
    }

    fn uuid(&self) -> String {
        self.ns.uuid()
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
        self.ns.alignment()
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        // bdev_nvme_io_type_supported
        match io_type {
            IoType::Read
            | IoType::Write
            | IoType::Reset
            | IoType::Flush
            | IoType::NvmeAdmin
            | IoType::NvmeIO
            | IoType::Abort => true,
            IoType::Compare => self.ns.supports_compare(),
            IoType::NvmeIOMD => self.ns.md_size() > 0,
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

    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        NvmeBlockDevice::open_by_name(&self.name, read_write)
    }
}

/*
 * Lookup target NVMeOF device by its name (starts with nvmf://).
 */
pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
    match NVME_CONTROLLERS.read().unwrap().get(name) {
        Some(ctrlr) => {
            if let Some(ns) = ctrlr.lock().unwrap().namespace() {
                Some(Box::new(NvmeBlockDevice::from_ns(name, ns)))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn open_by_name(
    name: &str,
    read_write: bool,
) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
    NvmeBlockDevice::open_by_name(name, read_write)
}
