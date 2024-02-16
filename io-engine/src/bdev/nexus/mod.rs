#![allow(clippy::vec_box)]

use std::{pin::Pin, sync::atomic::AtomicBool};

use crate::core::VerboseError;
use events_api::event::EventAction;
use futures::{future::Future, FutureExt};

mod nexus_bdev;
mod nexus_bdev_children;
mod nexus_bdev_error;
mod nexus_bdev_rebuild;
mod nexus_bdev_snapshot;
mod nexus_channel;
mod nexus_child;
mod nexus_io;
mod nexus_io_log;
mod nexus_io_subsystem;
mod nexus_iter;
mod nexus_module;
mod nexus_nbd;
mod nexus_persistence;
mod nexus_share;

use crate::{
    bdev::nexus::nexus_iter::NexusIterMut,
    eventing::{Event, EventMetaGen, EventWithMeta},
};
pub(crate) use nexus_bdev::NEXUS_PRODUCT_ID;
pub use nexus_bdev::{
    nexus_create,
    nexus_create_v2,
    Nexus,
    NexusNvmeParams,
    NexusNvmePreemption,
    NexusOperation,
    NexusState,
    NexusStatus,
    NexusTarget,
    NvmeAnaState,
    NvmeReservation,
};
pub(crate) use nexus_bdev_error::nexus_err;
pub use nexus_bdev_error::Error;
pub(crate) use nexus_channel::{DrEvent, IoMode, NexusChannel};
pub use nexus_child::{
    ChildError,
    ChildState,
    ChildStateClient,
    ChildSyncState,
    FaultReason,
    NexusChild,
};
use nexus_io::{NexusBio, NioCtx};
use nexus_io_log::{IOLog, IOLogChannel};
use nexus_io_subsystem::NexusIoSubsystem;
pub use nexus_io_subsystem::NexusPauseState;
pub use nexus_iter::{
    nexus_iter,
    nexus_iter_mut,
    nexus_lookup,
    nexus_lookup_mut,
    nexus_lookup_name_uuid,
    nexus_lookup_nqn,
    nexus_lookup_nqn_mut,
    nexus_lookup_uuid_mut,
};
pub(crate) use nexus_module::{NexusModule, NEXUS_MODULE_NAME};
pub(crate) use nexus_nbd::{NbdDisk, NbdError};
pub(crate) use nexus_persistence::PersistOp;
pub use nexus_persistence::{ChildInfo, NexusInfo};
pub(crate) use nexus_share::NexusPtpl;

pub use nexus_bdev_snapshot::{
    NexusReplicaSnapshotDescriptor,
    NexusReplicaSnapshotStatus,
    NexusSnapshotStatus,
};

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
pub fn register_module(register_json: bool) {
    nexus_module::register_module();

    if !register_json {
        return;
    }

    use crate::{
        core::{Share, ShareProps, UntypedBdev},
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
                            let share = ShareProps::new().with_range(Some((args.cntlid_min, args.cntlid_max))).with_ana(true);
                            bdev.as_mut().share_nvmf(Some(share))
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
#[allow(clippy::needless_collect)]
pub async fn shutdown_nexuses() {
    if NexusModule::current_opt().is_none() {
        info!("Skipping nexus shutdown - Nexus Module not registered...");
        return;
    }

    info!("Shutting down nexuses...");

    // TODO: We need to collect list of Nexuses before destroying them,
    // because during shutdown SPDK may destroy other Bdev (not Nexus)
    // asynchrounsly in parallel. Nexus iterator is an SPDK Bdev iterator
    // internally, so it may become invalid if another Bdev is destroyed
    // in parallel.
    // Clippy's complains about that, so it is disabled for this function.
    let nexuses: Vec<<NexusIterMut<'_> as Iterator>::Item> =
        nexus_iter_mut().collect();

    for mut nexus in nexuses.into_iter() {
        // Destroy nexus and persist its state in the ETCd.
        match nexus.as_mut().destroy_ext(true).await {
            Ok(_) => {
                Event::event(&(*nexus), EventAction::Shutdown).generate();
            }
            Err(error) => {
                error!(
                    name = nexus.name,
                    error = error.verbose(),
                    "Failed to destroy nexus"
                );
                EventWithMeta::event(
                    &(*nexus),
                    EventAction::Shutdown,
                    error.meta(),
                )
                .generate();
            }
        }
    }
    info!("All nexus have been shutdown.");
}

/// Enables/disables partial rebuild.
pub static ENABLE_PARTIAL_REBUILD: AtomicBool = AtomicBool::new(true);

/// Enables/disables nexus reset logic.
pub static ENABLE_NEXUS_RESET: AtomicBool = AtomicBool::new(false);

/// Enables/disables additional nexus I/O channel debugging.
pub static ENABLE_NEXUS_CHANNEL_DEBUG: AtomicBool = AtomicBool::new(false);

/// Whether the nexus channel should have readers/writers configured.
/// This must be set true ONLY from tests.
pub static ENABLE_IO_ALL_THRD_NX_CHAN: AtomicBool = AtomicBool::new(false);
