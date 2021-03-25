use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use std::{
    convert::From,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use spdk_sys::{self, spdk_nvme_ctrlr};

use crate::{
    bdev::dev::nvmx::{
        NvmeController,
        NvmeControllerState,
        NvmeDeviceHandle,
        NvmeNamespace,
        NVME_CONTROLLERS,
    },
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        BlockDeviceIoStats,
        CoreError,
        DeviceEventType,
        DeviceIoController,
        DeviceTimeoutAction,
        IoType,
    },
    ffihelper::{cb_arg, done_cb},
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
                ns,
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

    fn get_io_handle(&self) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        Ok(Box::new(NvmeDeviceHandle::create(
            &self.name,
            self.io_device_id,
            self.ctrlr.clone(),
            Arc::clone(&self.ns),
            self.prchk_flags,
        )?))
    }

    fn unclaim(&self) {
        warn!("unclaim() is not implemented for NvmeDeviceDescriptor yet");
    }

    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
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

        let controller = NVME_CONTROLLERS.lookup_by_name(name).ok_or(
            CoreError::BdevNotFound {
                name: name.to_string(),
            },
        )?;

        let controller = controller.lock().expect("lock poisoned");

        // Make sure controller is available.
        if controller.get_state() == NvmeControllerState::Running {
            let descr = NvmeDeviceDescriptor::create(&controller)?;
            Ok(descr)
        } else {
            Err(CoreError::BdevNotFound {
                name: name.to_string(),
            })
        }
    }

    pub fn from_ns(name: &str, ns: Arc<NvmeNamespace>) -> NvmeBlockDevice {
        NvmeBlockDevice {
            ns,
            name: String::from(name),
        }
    }
}

#[async_trait(?Send)]
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
            | IoType::NvmeIo
            | IoType::Abort => true,
            IoType::Compare => self.ns.supports_compare(),
            IoType::NvmeIoMd => self.ns.md_size() > 0,
            IoType::Unmap => false,
            IoType::WriteZeros => false,
            IoType::CompareAndWrite => false,
            _ => false,
        }
    }

    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        let carc = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;

        let (s, r) =
            oneshot::channel::<Result<BlockDeviceIoStats, CoreError>>();
        // Schedule async I/O stats collection and wait for the result.
        {
            let controller = carc.lock().expect("lock poisoned");

            controller.get_io_stats(
                |stats, ch| {
                    done_cb(ch, stats);
                },
                cb_arg(s),
            )?;
        }

        r.await.expect("Failed awaiting at io_stats")
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

    fn get_io_controller(&self) -> Option<Box<dyn DeviceIoController>> {
        Some(Box::new(NvmeDeviceIoController::new(self.name.to_string())))
    }

    fn add_event_listener(
        &self,
        listener: fn(DeviceEventType, &str),
    ) -> Result<(), CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.clone(),
            },
        )?;
        let controller = controller.lock().expect("controller lock poisoned");
        controller.add_event_listener(listener)
    }
}

struct NvmeDeviceIoController {
    name: String,
}

impl NvmeDeviceIoController {
    pub fn new(name: String) -> Self {
        Self {
            name,
        }
    }

    fn lookup_controller(
        &self,
    ) -> Result<Arc<Mutex<NvmeController<'static>>>, CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;
        Ok(controller)
    }
}

impl DeviceIoController for NvmeDeviceIoController {
    fn get_timeout_action(&self) -> Result<DeviceTimeoutAction, CoreError> {
        let controller = self.lookup_controller()?;
        let controller = controller.lock().expect("lock poisoned");

        controller.get_timeout_action()
    }

    fn set_timeout_action(
        &mut self,
        action: DeviceTimeoutAction,
    ) -> Result<(), CoreError> {
        let controller = self.lookup_controller()?;
        let mut controller = controller.lock().expect("lock poisoned");

        controller.set_timeout_action(action)
    }
}

/*
 * Lookup target NVMeOF device by its name (starts with nvmf://).
 */
pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
    if let Some(c) = NVME_CONTROLLERS.lookup_by_name(name) {
        let controller = c.lock().expect("mutex poisoned");
        // Make sure controller is available.
        if controller.get_state() == NvmeControllerState::Running {
            let ns = controller
                .namespace()
                .expect("no namespaces for this controller");
            return Some(Box::new(NvmeBlockDevice::from_ns(name, ns)));
        }
    }
    debug!("{}: NVMe controller not found", name);
    None
}

pub fn open_by_name(
    name: &str,
    read_write: bool,
) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
    NvmeBlockDevice::open_by_name(name, read_write)
}
