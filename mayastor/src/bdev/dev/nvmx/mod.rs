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

use crate::subsys::{Config, NvmeBdevOpts};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

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

pub fn nvme_bdev_running_config() -> &'static NvmeBdevOpts {
    &Config::get().nvme_bdev_opts
}
