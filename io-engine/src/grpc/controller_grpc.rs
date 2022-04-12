use crate::{
    bdev::{NvmeController, NvmeControllerState, NVME_CONTROLLERS},
    core::{BlockDeviceIoStats, CoreError},
    ffihelper::{cb_arg, done_cb},
};
use futures::channel::oneshot;

#[derive(Debug)]
pub struct NvmeControllerInfo {
    pub name: String,
    pub state: NvmeControllerState,
    pub size: u64,
    pub blk_size: u32,
}

impl<'a> NvmeController<'a> {
    fn to_info(&self) -> NvmeControllerInfo {
        let (size, blk_size) = self
            .namespace()
            .map_or((0, 0), |ns| (ns.size_in_bytes(), ns.block_len() as u32));

        NvmeControllerInfo {
            name: self.name.to_string(),
            state: self.get_state(),
            size,
            blk_size,
        }
    }
}

/// Returns the IO stats of the given NVMe Controller
pub async fn controller_stats(
    controller_name: &str,
) -> Result<BlockDeviceIoStats, CoreError> {
    if let Some(ctrlr) = NVME_CONTROLLERS.lookup_by_name(controller_name) {
        let (s, r) =
            oneshot::channel::<Result<BlockDeviceIoStats, CoreError>>();
        {
            let ctrlr = ctrlr.lock();
            if let Err(e) = ctrlr.get_io_stats(
                |stats, ch| {
                    done_cb(ch, stats);
                },
                cb_arg(s),
            ) {
                error!(
                    "{}: failed to get stats for NVMe controller: {:?}",
                    controller_name, e
                );
                return Err(e);
            }
        }
        r.await.expect("Failed awaiting at io_stats")
    } else {
        Err(CoreError::BdevNotFound {
            name: controller_name.to_string(),
        })
    }
}

/// Lists all the NVMe Controllers
pub async fn list_controllers() -> Vec<NvmeControllerInfo> {
    NVME_CONTROLLERS
        .controllers()
        .iter()
        .filter_map(|n| {
            NVME_CONTROLLERS
                .lookup_by_name(n)
                .map(|c| c.lock().to_info())
        })
        .collect::<Vec<_>>()
}
