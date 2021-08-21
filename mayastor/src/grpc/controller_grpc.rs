use crate::{
    bdev::{
        nexus::nexus_bdev, NvmeController, NvmeControllerState,
        NVME_CONTROLLERS,
    },
    core::{BlockDeviceIoStats, CoreError},
    ffihelper::{cb_arg, done_cb},
    grpc::{rpc_submit, GrpcResult},
};

use ::rpc::mayastor as rpc;
use futures::channel::oneshot;
use std::convert::From;
use tonic::{Response, Status};

impl<'a> NvmeController<'a> {
    fn to_grpc(&self) -> rpc::NvmeController {
        let (size, blk_size) = self
            .namespace()
            .map_or((0, 0), |ns| (ns.size_in_bytes(), ns.block_len() as u32));

        rpc::NvmeController {
            name: self.name.to_string(),
            state: rpc::NvmeControllerState::from(self.get_state()) as i32,
            size,
            blk_size,
        }
    }
}

impl From<NvmeControllerState> for rpc::NvmeControllerState {
    fn from(state: NvmeControllerState) -> Self {
        match state {
            NvmeControllerState::New => rpc::NvmeControllerState::New,
            NvmeControllerState::Initializing => {
                rpc::NvmeControllerState::Initializing
            }
            NvmeControllerState::Running => rpc::NvmeControllerState::Running,
            NvmeControllerState::Faulted(_) => {
                rpc::NvmeControllerState::Faulted
            }
            NvmeControllerState::Unconfiguring => {
                rpc::NvmeControllerState::Unconfiguring
            }
            NvmeControllerState::Unconfigured => {
                rpc::NvmeControllerState::Unconfigured
            }
        }
    }
}

impl From<BlockDeviceIoStats> for rpc::NvmeControllerIoStats {
    fn from(b: BlockDeviceIoStats) -> Self {
        Self {
            num_read_ops: b.num_read_ops,
            num_write_ops: b.num_write_ops,
            bytes_read: b.bytes_read,
            bytes_written: b.bytes_written,
            num_unmap_ops: b.num_unmap_ops,
            bytes_unmapped: b.bytes_unmapped,
        }
    }
}

pub async fn controller_stats() -> GrpcResult<rpc::StatNvmeControllersReply> {
    let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
        let mut res: Vec<rpc::NvmeControllerStats> = Vec::new();
        let controllers = NVME_CONTROLLERS.controllers();
        for name in controllers.iter() {
            if let Some(ctrlr) = NVME_CONTROLLERS.lookup_by_name(name) {
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
                            name, e
                        );
                        continue;
                    }
                }

                let stats = r.await.expect("Failed awaiting at io_stats");
                if stats.is_err() {
                    error!("{}: failed to get stats for NVMe controller", name);
                } else {
                    res.push(rpc::NvmeControllerStats {
                        name: name.to_string(),
                        stats: stats.ok().map(rpc::NvmeControllerIoStats::from),
                    });
                }
            }
        }

        Ok(rpc::StatNvmeControllersReply { controllers: res })
    })?;

    rx.await
        .map_err(|_| Status::cancelled("cancelled"))?
        .map_err(Status::from)
        .map(Response::new)
}

pub async fn list_controllers() -> GrpcResult<rpc::ListNvmeControllersReply> {
    let rx = rpc_submit::<_, _, nexus_bdev::Error>(async move {
        let controllers = NVME_CONTROLLERS
            .controllers()
            .iter()
            .filter_map(|n| {
                NVME_CONTROLLERS
                    .lookup_by_name(n)
                    .map(|c| c.lock().to_grpc())
            })
            .collect::<Vec<_>>();

        Ok(rpc::ListNvmeControllersReply { controllers })
    })?;

    rx.await
        .map_err(|_| Status::cancelled("cancelled"))?
        .map_err(Status::from)
        .map(Response::new)
}
