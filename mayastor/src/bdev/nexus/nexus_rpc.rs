use futures::{future, FutureExt};

use rpc::mayastor::{
    AddChildNexusRequest,
    Child,
    ChildNexusRequest,
    ChildState as RpcChildState,
    CreateNexusRequest,
    DestroyNexusRequest,
    ListNexusReply,
    Nexus as RpcNexus,
    NexusState as RpcNexusState,
    PauseRebuildRequest,
    PublishNexusReply,
    PublishNexusRequest,
    RebuildProgressRequest,
    RebuildStateRequest,
    RemoveChildNexusRequest,
    ResumeRebuildRequest,
    ShareProtocolNexus,
    StartRebuildRequest,
    StopRebuildRequest,
    UnpublishNexusRequest,
};

use crate::{
    bdev::nexus::{
        instances,
        nexus_bdev::{
            name_to_uuid,
            nexus_create,
            uuid_to_name,
            Error,
            Nexus,
            NexusState,
        },
        nexus_child::ChildState,
    },
    jsonrpc::jsonrpc_register,
    rebuild::RebuildTask,
};

/// Lookup a nexus by its uuid. Return error if uuid is invalid or nexus
/// not found.
fn nexus_lookup(uuid: &str) -> Result<&mut Nexus, Error> {
    let name = uuid_to_name(uuid)?;

    if let Some(nexus) = instances().iter_mut().find(|n| n.name == name) {
        Ok(nexus)
    } else {
        Err(Error::NexusNotFound {
            name: uuid.to_owned(),
        })
    }
}

pub(crate) fn register_rpc_methods() {
    // JSON rpc method to list the nexus and their states
    jsonrpc_register::<(), _, _, Error>("list_nexus", |_| {
        future::ok(ListNexusReply {
            nexus_list: instances()
                .iter()
                .map(|nexus| RpcNexus {
                    uuid: name_to_uuid(&nexus.name).to_string(),
                    size: nexus.size(),
                    state: match nexus.state {
                        NexusState::Online => RpcNexusState::NexusOnline,
                        NexusState::Faulted => RpcNexusState::NexusFaulted,
                        NexusState::Degraded => RpcNexusState::NexusDegraded,
                        NexusState::Remuling => RpcNexusState::NexusDegraded,
                        NexusState::Init => RpcNexusState::NexusDegraded,
                        NexusState::Closed => RpcNexusState::NexusDegraded,
                    } as i32,
                    children: nexus
                        .children
                        .iter()
                        .map(|child| Child {
                            uri: child.name.clone(),
                            state: match child.state {
                                ChildState::Init => {
                                    RpcChildState::ChildDegraded
                                }
                                ChildState::ConfigInvalid => {
                                    RpcChildState::ChildFaulted
                                }
                                ChildState::Open => RpcChildState::ChildOnline,
                                ChildState::Closed => {
                                    RpcChildState::ChildDegraded
                                }
                                ChildState::Faulted => {
                                    RpcChildState::ChildFaulted
                                }
                            } as i32,
                        })
                        .collect::<Vec<_>>(),
                    device_path: nexus.get_share_path().unwrap_or_default(),
                    rebuilds: RebuildTask::count() as u64,
                })
                .collect::<Vec<_>>(),
        })
        .boxed_local()
    });

    // rpc method to construct a new Nexus
    jsonrpc_register("create_nexus", |args: CreateNexusRequest| {
        let fut = async move {
            let name = match uuid_to_name(&args.uuid) {
                Ok(name) => name,
                Err(err) => return Err(err),
            };
            // TODO: get rid of hardcoded nexus block size (possibly by
            // deriving it from child bdevs's block sizes).
            nexus_create(&name, args.size, Some(&args.uuid), &args.children)
                .await
        };
        fut.boxed_local()
    });

    jsonrpc_register::<_, _, _, Error>(
        "destroy_nexus",
        |args: DestroyNexusRequest| {
            let fut = async move {
                let nexus = nexus_lookup(&args.uuid)?;
                nexus.destroy().await?;
                Ok(())
            };
            fut.boxed_local()
        },
    );

    jsonrpc_register("publish_nexus", |args: PublishNexusRequest| {
        let fut = async move {
            // the key has to be 16 characters if it contains "" we consider it
            // to be empty
            if args.key != "" && args.key.len() != 16 {
                warn!("Invalid key specified, are we under attack?!?");
                return Err(Error::InvalidKey {});
            }

            // We have no means to validate key correctness right now, this is
            // fine as we currently, do not support raw block
            // devices being consumed directly within k8s
            // the mount will fail if the key is wrong.

            let key: Option<String> =
                if args.key == "" { None } else { Some(args.key) };

            let share_protocol = match ShareProtocolNexus::from_i32(args.share)
            {
                Some(protocol) => protocol,
                None => {
                    return Err(Error::InvalidShareProtocol {
                        sp_value: args.share as i32,
                    })
                }
            };

            let nexus = nexus_lookup(&args.uuid)?;
            nexus.share(share_protocol, key).await.map(|device_path| {
                PublishNexusReply {
                    device_path,
                }
            })
        };
        fut.boxed_local()
    });

    jsonrpc_register("unpublish_nexus", |args: UnpublishNexusRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.unshare().await
        };
        fut.boxed_local()
    });

    jsonrpc_register("offline_child", |args: ChildNexusRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.offline_child(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("online_child", |args: ChildNexusRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.online_child(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("add_child_nexus", |args: AddChildNexusRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.add_child(&args.uri).await.map(|_| ())
        };
        fut.boxed_local()
    });

    jsonrpc_register("remove_child_nexus", |args: RemoveChildNexusRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.remove_child(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("start_rebuild", |args: StartRebuildRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.start_rebuild_rpc(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("stop_rebuild", |args: StopRebuildRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.stop_rebuild(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("pause_rebuild", |args: PauseRebuildRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.pause_rebuild(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("resume_rebuild", |args: ResumeRebuildRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.resume_rebuild(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("get_rebuild_state", |args: RebuildStateRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.get_rebuild_state(&args.uri).await
        };
        fut.boxed_local()
    });

    jsonrpc_register("get_rebuild_progress", |args: RebuildProgressRequest| {
        let fut = async move {
            let nexus = nexus_lookup(&args.uuid)?;
            nexus.get_rebuild_progress().await
        };
        fut.boxed_local()
    });
}
