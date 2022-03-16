#![allow(clippy::vec_box)]

use futures::{future::Future, FutureExt};
use std::pin::Pin;

mod nexus_bdev;
mod nexus_bdev_children;
mod nexus_bdev_rebuild;
mod nexus_bdev_snapshot;
mod nexus_channel;
mod nexus_child;
mod nexus_io;
mod nexus_io_subsystem;
mod nexus_iter;
mod nexus_module;
mod nexus_nbd;
mod nexus_persistence;
mod nexus_share;

pub use nexus_bdev::{
    nexus_create,
    nexus_create_v2,
    Error,
    Nexus,
    NexusNvmeParams,
    NexusState,
    NexusStatus,
    NexusTarget,
    NvmeAnaState,
    VerboseError,
};
pub(crate) use nexus_bdev::{
    CreateChild,
    CreateRebuild,
    OpenChild,
    RebuildJobNotFound,
    RebuildOperation,
    RemoveRebuildJob,
    ShareNbdNexus,
    ShareNvmfNexus,
    UnshareNexus,
    NEXUS_PRODUCT_ID,
};
pub(crate) use nexus_channel::{
    fault_nexus_child,
    DrEvent,
    NexusChannel,
    NexusChannelInner,
};
pub use nexus_child::{
    lookup_nexus_child,
    ChildError,
    ChildState,
    NexusChild,
    Reason,
};
pub(crate) use nexus_io::{nexus_submit_request, NioCtx};
pub(self) use nexus_io_subsystem::NexusIoSubsystem;
pub use nexus_iter::{
    nexus_iter,
    nexus_iter_mut,
    nexus_lookup,
    nexus_lookup_mut,
    nexus_lookup_name_uuid,
    nexus_lookup_uuid_mut,
};
pub(crate) use nexus_module::{NexusModule, NEXUS_MODULE_NAME};
pub(crate) use nexus_nbd::{NbdDisk, NbdError};
pub(crate) use nexus_persistence::PersistOp;
pub use nexus_persistence::{ChildInfo, NexusInfo};

/// TODO
#[derive(Deserialize)]
struct NexusShareArgs {
    /// TODO
    name: String,
    /// TODO
    protocol: String,
    /// TODO
    cntlid_min: u16,
    /// TODO
    cntlid_max: u16,
}

/// TODO
#[derive(Serialize)]
struct NexusShareReply {
    /// TODO
    uri: String,
}

/// public function which simply calls register module
pub fn register_module() {
    nexus_module::register_module();

    use crate::{
        core::{Share, UntypedBdev},
        jsonrpc::{jsonrpc_register, Code, JsonRpcError, Result},
    };

    jsonrpc_register(
        "nexus_share",
        |args: NexusShareArgs| -> Pin<Box<dyn Future<Output = Result<NexusShareReply>>>> {
            // FIXME: shares bdev, not a nexus
            let f = async move {
                let proto = args.protocol;
                if proto != "nvmf" {
                    return Err(JsonRpcError {
                        code: Code::InvalidParams,
                        message: "invalid protocol".to_string(),
                    });
                }
                if let Some(mut bdev) = UntypedBdev::lookup_by_name(&args.name) {
                    let mut bdev = Pin::new(&mut bdev);
                    match proto.as_str() {
                        "nvmf" => {
                            bdev.as_mut().share_nvmf(Some((args.cntlid_min, args.cntlid_max)))
                                .await
                                .map_err(|e| {
                                    JsonRpcError {
                                        code: Code::InternalError,
                                        message: e.to_string(),
                                    }
                                })
                                .map(|share| {
                                    NexusShareReply {
                                        uri: bdev.share_uri().unwrap_or(share),
                                }
                            })
                        },
                        _ => unreachable!(),
                    }
                } else {
                    Err(JsonRpcError {
                        code: Code::NotFound,
                        message: "bdev not found".to_string(),
                    })
                }
            };
            Box::pin(f.boxed_local())
        },
    );
}

/// called during shutdown so that all nexus children are in Destroying state
/// so that a possible remove event from SPDK also results in bdev removal
pub async fn nexus_children_to_destroying_state() {
    info!("setting all nexus children to destroying state...");
    for nexus in nexus_iter() {
        for child in nexus.children.iter() {
            child.set_state(nexus_child::ChildState::Destroying);
        }
    }
    info!("set all nexus children to destroying state");
}
