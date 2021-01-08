mod channel;
mod controller;
mod device;
mod handle;
mod namespace;
mod uri;
mod utils;

pub use channel::{NvmeControllerIoChannel, NvmeIoChannel, NvmeIoChannelInner};
pub use controller::{NvmeController, NvmeControllerState};
pub use device::{lookup_by_name, open_by_name, NvmeBlockDevice};
pub use handle::NvmeDeviceHandle;
pub use namespace::NvmeNamespace;
pub(crate) use uri::NvmfDeviceTemplate;

use crate::{core::CoreError, subsys::{Config, NvmeBdevOpts}};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

// TODO make this a struct
lazy_static! {
    pub static ref NVME_CONTROLLERS: RwLock<HashMap<String, Arc<Mutex<NvmeController>>>> =
        RwLock::new(HashMap::<String, Arc<Mutex<NvmeController>>>::new());
}

pub fn nvme_controller_lookup(
    name: &str,
) -> Option<Arc<Mutex<NvmeController>>> {
    let controllers = NVME_CONTROLLERS.read().unwrap();
    if let Some(instance) = controllers.get(name) {
        Some(Arc::clone(instance))
    } else {
        info!("NVMe controller {} not found", name);
        None
    }
}

/// NVMe controllers are stored using multiple keys to the same value. This allows
/// for easy lookup. As a consequence however, both keys must be removed in order
// for the controller to get dropped.
pub fn nvme_controller_remove(name: String) -> Result<String, CoreError>{

    debug!("{}: removing NVMe controller", name);

    let mut controllers = NVME_CONTROLLERS.write().unwrap();
    if !controllers.contains_key(&name) {
        return Err(CoreError::BdevNotFound {
            name
        });
    }

    // Remove 'controller name -> controller' mapping.
    let e = controllers.remove(&name).unwrap();
    let controller = e.lock().unwrap();

    // Remove 'controller id->controller' mapping. This will remove the last reference as
    // causes the controller to be dropped.
    controllers.remove(&controller.id().to_string());

    debug!("{}: NVMe controller has been removed from the list", name);
    Ok(name.into())

}

pub fn nvme_bdev_running_config() -> &'static NvmeBdevOpts {
    &Config::get().nvme_bdev_opts
}
