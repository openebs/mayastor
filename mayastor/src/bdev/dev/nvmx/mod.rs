mod channel;
mod controller;
mod device;
mod handle;

pub use channel::{NvmeControllerIoChannel, NvmeIoChannel};
pub use controller::{
    NvmeController,
    NvmeControllerInner,
    NvmeControllerState,
    NvmfDeviceTemplate,
};
pub use device::{lookup_by_name, open_by_name, NvmeBlockDevice};
pub use handle::NvmeDeviceHandle;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

lazy_static! {
    pub static ref NVME_CONTROLLERS: RwLock<HashMap<String, Arc<Mutex<NvmeController>>>> =
        RwLock::new(HashMap::<String, Arc<Mutex<NvmeController>>>::new());
}
