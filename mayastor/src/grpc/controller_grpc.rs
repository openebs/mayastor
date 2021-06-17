use crate::{
    bdev::{
        nexus::nexus_bdev,
        NvmeController,
        NvmeControllerState,
        NVME_CONTROLLERS,
    },
    grpc::{rpc_submit, GrpcResult},
};

use ::rpc::mayastor as rpc;
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

        Ok(rpc::ListNvmeControllersReply {
            controllers,
        })
    })?;

    rx.await
        .map_err(|_| Status::cancelled("cancelled"))?
        .map_err(Status::from)
        .map(Response::new)
}
