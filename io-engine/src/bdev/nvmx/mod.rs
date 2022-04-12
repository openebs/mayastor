use std::{collections::HashMap, fmt::Display, sync::Arc};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use channel::{NvmeControllerIoChannel, NvmeIoChannel, NvmeIoChannelInner};
pub use controller::NvmeController;
pub use controller_state::NvmeControllerState;
pub use device::{lookup_by_name, open_by_name, NvmeBlockDevice};
pub use handle::{nvme_io_ctx_pool_init, NvmeDeviceHandle};
pub use namespace::NvmeNamespace;
pub(crate) use uri::NvmfDeviceTemplate;

use crate::{
    core::CoreError,
    subsys::{Config, NvmeBdevOpts},
};

mod channel;
mod controller;
mod controller_inner;
mod controller_state;
mod device;
mod handle;
mod namespace;
mod uri;
pub mod utils;

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub struct NVMeCtlrList<'a> {
    entries: RwLock<HashMap<String, Arc<Mutex<NvmeController<'a>>>>>,
}

impl<'a> NVMeCtlrList<'a> {
    fn write_lock(
        &self,
    ) -> RwLockWriteGuard<HashMap<String, Arc<Mutex<NvmeController<'a>>>>> {
        self.entries.write()
    }

    fn read_lock(
        &self,
    ) -> RwLockReadGuard<HashMap<String, Arc<Mutex<NvmeController<'a>>>>> {
        self.entries.read()
    }

    /// lookup a NVMe controller
    pub fn lookup_by_name<T: Into<String>>(
        &self,
        name: T,
    ) -> Option<Arc<Mutex<NvmeController<'a>>>> {
        let entries = self.read_lock();
        entries.get(&name.into()).cloned()
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
        let controller = e.lock();

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

    /// Get the names of all available NVMe controllers.
    pub fn controllers(&self) -> Vec<String> {
        let entries = self.read_lock();
        entries
            .keys()
            .map(|k| k.to_string())
            .filter(|k| k.contains("nqn")) // Filter out CIDs
            .collect::<Vec<_>>()
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

pub static NVME_CONTROLLERS: Lazy<NVMeCtlrList> =
    Lazy::new(NVMeCtlrList::default);

pub fn nvme_bdev_running_config() -> &'static NvmeBdevOpts {
    &Config::get().nvme_bdev_opts
}
