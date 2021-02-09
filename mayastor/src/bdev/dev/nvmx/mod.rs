mod channel;
mod controller;
mod controller_inner;
mod controller_state;
mod device;
mod handle;
mod namespace;
mod uri;
mod utils;

pub use channel::{NvmeControllerIoChannel, NvmeIoChannel, NvmeIoChannelInner};
pub use controller::NvmeController;
pub use controller_state::NvmeControllerState;
pub use device::{lookup_by_name, open_by_name, NvmeBlockDevice};
pub use handle::NvmeDeviceHandle;
pub use namespace::NvmeNamespace;
pub(crate) use uri::NvmfDeviceTemplate;

use crate::{
    core::CoreError,
    subsys::{Config, NvmeBdevOpts},
};
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use once_cell::sync::Lazy;

#[derive(Debug)]
pub(crate) struct NVMeCtlrList<'a> {
    entries: RwLock<HashMap<String, Arc<Mutex<NvmeController<'a>>>>>,
}

impl<'a> NVMeCtlrList<'a> {
    fn write_lock(
        &self,
    ) -> RwLockWriteGuard<HashMap<String, Arc<Mutex<NvmeController<'a>>>>> {
        self.entries.write().expect("rwlock poisoned")
    }

    fn read_lock(
        &self,
    ) -> RwLockReadGuard<HashMap<String, Arc<Mutex<NvmeController<'a>>>>> {
        self.entries.read().expect("rwlock poisoned")
    }

    /// lookup a NVMe controller
    pub fn lookup_by_name<T: Into<String>>(
        &self,
        name: T,
    ) -> Option<Arc<Mutex<NvmeController<'a>>>> {
        let entries = self.read_lock();
        entries.get(&name.into()).map(|e| Arc::clone(e))
    }

    /// remove a NVMe controller from the list, when the last reference to the
    /// controller is dropped, the controller will be freed.

    pub fn remove_by_name<T: Into<String> + Display>(
        &self,
        name: T,
    ) -> Result<String, CoreError> {
        let mut entries = self.write_lock();

        if !entries.contains_key(&name.to_string()) {
            return Err(CoreError::BdevNotFound {
                name: name.into(),
            });
        }

        // Remove 'controller name -> controller' mapping.
        let e = entries.remove(&name.to_string()).unwrap();
        let controller = e.lock().unwrap();

        // Remove 'controller id->controller' mapping. This will remove the last
        // reference as causes the controller to be dropped.
        entries.remove(&controller.id().to_string());

        debug!("{}: NVMe controller has been removed from the list", name);
        Ok(name.into())
    }

    /// insert a controller into the list using the key, note that different
    /// keys may refer to the same controller
    pub fn insert_controller(
        &self,
        cid: String,
        ctl: Arc<Mutex<NvmeController<'a>>>,
    ) {
        let mut entries = self.write_lock();
        entries.insert(cid, ctl);
    }
}

impl<'a> Default for NVMeCtlrList<'a> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(
                HashMap::<String, Arc<Mutex<NvmeController>>>::new(),
            ),
        }
    }
}

static NVME_CONTROLLERS: Lazy<NVMeCtlrList> = Lazy::new(NVMeCtlrList::default);

pub fn nvme_bdev_running_config() -> &'static NvmeBdevOpts {
    &Config::get().nvme_bdev_opts
}
