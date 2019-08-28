use crate::{
    bdev::nexus::{
        instances,
        nexus_bdev::{nexus_create, nexus_destroy},
        Error,
    },
    jsonrpc::{jsonrpc_register, Code, JsonRpcError},
};

use crate::bdev::nexus::nexus_bdev::nexus_lookup;
use futures::{future, FutureExt};
use rpc::mayastor::{
    Child,
    ChildNexusRequest,
    CreateNexusRequest,
    DestroyNexusRequest,
    ListNexusReply,
    Nexus,
};

pub(crate) fn register_rpc_methods() {
    // JSON rpc method to list the nexus and their states
    jsonrpc_register::<(), _, _>("list_nexus", |_| {
        future::ok(ListNexusReply {
            nexus_list: instances()
                .iter()
                .map(|nexus| Nexus {
                    name: nexus.name().into(),
                    state: nexus.state.to_string(),
                    children: nexus
                        .children
                        .iter()
                        .map(|child| Child {
                            name: child.name.clone(),
                            state: child.state.to_string(),
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
        })
        .boxed_local()
    });

    // rpc method to construct a new Nexus
    jsonrpc_register("create_nexus", |args: CreateNexusRequest| {
        let fut = async move {
            match nexus_create(
                &args.name,
                args.block_len,
                args.size / u64::from(args.block_len),
                Some(args.uuid),
                &args.replicas,
            )
            .await
            {
                Ok(name) => {
                    // all rpc methods that create bdevs return the name of what
                    // they have created. This is not always the same as the
                    // name passed in as a argument.
                    Ok(name)
                }
                Err(Error::Exists) => Ok(args.name),
                Err(Error::ChildExists) => Err(JsonRpcError::new(
                    Code::InternalError,
                    "child bdev already exists",
                )),
                Err(_) => Err(JsonRpcError::new(
                    Code::InternalError,
                    "failed to create nexus",
                )),
            }
        };
        fut.boxed_local()
    });

    jsonrpc_register("destroy_nexus", |args: DestroyNexusRequest| {
        let fut = async move {
            match nexus_destroy(&args.name).await {
                Ok(name) => Ok(name),
                Err(Error::NotFound) => Ok(args.name),
                Err(e) => Err(JsonRpcError::new(
                    Code::InternalError,
                    format!("Internal error {:?}", e),
                )),
            }
        };
        fut.boxed_local()
    });

    jsonrpc_register("offline_child", |args: ChildNexusRequest| {
        let fut = async move {
            if let Some(nexus) = nexus_lookup(&args.name) {
                match nexus.offline_child(&args.child_name).await {
                    Ok(_) => Ok(args.name),
                    Err(Error::NotFound) => Ok(args.name),
                    Err(e) => Err(JsonRpcError::new(
                        Code::InternalError,
                        format!("Internal error {:?}", e),
                    )),
                }
            } else {
                Err(JsonRpcError::new(
                    Code::NotFound,
                    format!("Nexus {} not found", args.name),
                ))
            }
        };
        fut.boxed_local()
    });

    jsonrpc_register("online_child", |args: ChildNexusRequest| {
        let fut = async move {
            if let Some(nexus) = nexus_lookup(&args.name) {
                match nexus.online_child(&args.child_name).await {
                    Ok(_) => Ok(args.name),
                    Err(Error::NotFound) => Ok(args.name),
                    Err(e) => Err(JsonRpcError::new(
                        Code::InternalError,
                        format!("Internal error {:?}", e),
                    )),
                }
            } else {
                Err(JsonRpcError::new(
                    Code::NotFound,
                    format!("Nexus {} not found", args.name),
                ))
            }
        };
        fut.boxed_local()
    });
}
