use super::*;

struct Factory {}
impl HttpServiceFactory for Factory {
    fn register(self, config: &mut AppService) {
        get_replicas.register(config);
        get_replica.register(config);
        get_replica.register(config);
        get_node_replicas.register(config);
        get_node_pool_replicas.register(config);
        get_node_pool_replica.register(config);
        put_node_pool_replica.register(config);
        put_pool_replica.register(config);
        del_node_pool_replica.register(config);
        del_pool_replica.register(config);
        put_node_pool_replica_share.register(config);
        put_pool_replica_share.register(config);
        del_node_pool_replica_share.register(config);
        del_pool_replica_share.register(config);
    }
}
pub(crate) fn factory() -> impl HttpServiceFactory {
    Factory {}
}

#[get("/v0/replicas")]
async fn get_replicas() -> impl Responder {
    RestRespond::result(MessageBus::get_replicas(Filter::None).await)
}
#[get("/v0/replicas/{id}")]
async fn get_replica(
    web::Path(replica_id): web::Path<ReplicaId>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_replica(Filter::Replica(replica_id)).await,
    )
}

#[get("/v0/nodes/{id}/replicas")]
async fn get_node_replicas(
    web::Path(node_id): web::Path<NodeId>,
) -> impl Responder {
    RestRespond::result(MessageBus::get_replicas(Filter::Node(node_id)).await)
}

#[get("/v0/nodes/{node_id}/pools/{pool_id}/replicas")]
async fn get_node_pool_replicas(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_replicas(Filter::NodePool(node_id, pool_id)).await,
    )
}
#[get("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")]
async fn get_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_replica(Filter::NodePoolReplica(
            node_id, pool_id, replica_id,
        ))
        .await,
    )
}

#[put("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")]
async fn put_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
    create: web::Json<CreateReplicaBody>,
) -> impl Responder {
    put_replica(
        Filter::NodePoolReplica(node_id, pool_id, replica_id),
        create.into_inner(),
    )
    .await
}
#[put("/v0/pools/{pool_id}/replicas/{replica_id}")]
async fn put_pool_replica(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
    create: web::Json<CreateReplicaBody>,
) -> impl Responder {
    put_replica(
        Filter::PoolReplica(pool_id, replica_id),
        create.into_inner(),
    )
    .await
}

#[delete("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}")]
async fn del_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> impl Responder {
    destroy_replica(Filter::NodePoolReplica(node_id, pool_id, replica_id)).await
}
#[delete("/v0/pools/{pool_id}/replicas/{replica_id}")]
async fn del_pool_replica(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
) -> impl Responder {
    destroy_replica(Filter::PoolReplica(pool_id, replica_id)).await
}

#[put("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share/{protocol}")]
async fn put_node_pool_replica_share(
    web::Path((node_id, pool_id, replica_id, protocol)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
        Protocol,
    )>,
) -> impl Responder {
    share_replica(
        Filter::NodePoolReplica(node_id, pool_id, replica_id),
        protocol,
    )
    .await
}
#[put("/v0/pools/{pool_id}/replicas/{replica_id}/share/{protocol}")]
async fn put_pool_replica_share(
    web::Path((pool_id, replica_id, protocol)): web::Path<(
        PoolId,
        ReplicaId,
        Protocol,
    )>,
) -> impl Responder {
    share_replica(Filter::PoolReplica(pool_id, replica_id), protocol).await
}

#[delete("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share")]
async fn del_node_pool_replica_share(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> impl Responder {
    unshare_replica(Filter::NodePoolReplica(node_id, pool_id, replica_id)).await
}
#[delete("/v0/pools/{pool_id}/replicas/{replica_id}/share")]
async fn del_pool_replica_share(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
) -> impl Responder {
    unshare_replica(Filter::PoolReplica(pool_id, replica_id)).await
}

async fn put_replica(
    filter: Filter,
    body: CreateReplicaBody,
) -> impl Responder {
    let create = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
            body.bus_request(node_id, pool_id, replica_id)
        }
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return (RestError::from(error)).into(),
            };
            body.bus_request(node_id, pool_id, replica_id)
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::create_replica(create).await)
}

async fn destroy_replica(filter: Filter) -> impl Responder {
    let destroy = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
            DestroyReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return (RestError::from(error)).into(),
            };

            DestroyReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::destroy_replica(destroy).await)
}

async fn share_replica(filter: Filter, protocol: Protocol) -> impl Responder {
    let share = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => ShareReplica {
            node: node_id,
            pool: pool_id,
            uuid: replica_id,
            protocol,
        },
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return (RestError::from(error)).into(),
            };

            ShareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
                protocol,
            }
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::share_replica(share).await)
}

async fn unshare_replica(filter: Filter) -> impl Responder {
    let unshare = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
            UnshareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return (RestError::from(error)).into(),
            };

            UnshareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::unshare_replica(unshare).await)
}
