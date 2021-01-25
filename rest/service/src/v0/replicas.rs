use super::*;

pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_replicas)
        .service(get_replica)
        .service(get_node_replicas)
        .service(get_node_pool_replicas)
        .service(get_node_pool_replica)
        .service(put_node_pool_replica)
        .service(put_pool_replica)
        .service(del_node_pool_replica)
        .service(del_pool_replica)
        .service(put_node_pool_replica_share)
        .service(put_pool_replica_share)
        .service(del_node_pool_replica_share)
        .service(del_pool_replica_share);
}

#[get("/v0/replicas", tags(Replicas))]
async fn get_replicas() -> Result<Json<Vec<Replica>>, RestError> {
    RestRespond::result(MessageBus::get_replicas(Filter::None).await)
}
#[get("/v0/replicas/{id}", tags(Replicas))]
async fn get_replica(
    web::Path(replica_id): web::Path<ReplicaId>,
) -> Result<Json<Replica>, RestError> {
    RestRespond::result(
        MessageBus::get_replica(Filter::Replica(replica_id)).await,
    )
}

#[get("/v0/nodes/{id}/replicas", tags(Replicas))]
async fn get_node_replicas(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<Json<Vec<Replica>>, RestError> {
    RestRespond::result(MessageBus::get_replicas(Filter::Node(node_id)).await)
}

#[get("/v0/nodes/{node_id}/pools/{pool_id}/replicas", tags(Replicas))]
async fn get_node_pool_replicas(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> Result<Json<Vec<Replica>>, RestError> {
    RestRespond::result(
        MessageBus::get_replicas(Filter::NodePool(node_id, pool_id)).await,
    )
}
#[get(
    "/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}",
    tags(Replicas)
)]
async fn get_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> Result<Json<Replica>, RestError> {
    RestRespond::result(
        MessageBus::get_replica(Filter::NodePoolReplica(
            node_id, pool_id, replica_id,
        ))
        .await,
    )
}

#[put(
    "/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}",
    tags(Replicas)
)]
async fn put_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
    create: web::Json<CreateReplicaBody>,
) -> Result<Json<Replica>, RestError> {
    put_replica(
        Filter::NodePoolReplica(node_id, pool_id, replica_id),
        create.into_inner(),
    )
    .await
}
#[put("/v0/pools/{pool_id}/replicas/{replica_id}", tags(Replicas))]
async fn put_pool_replica(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
    create: web::Json<CreateReplicaBody>,
) -> Result<Json<Replica>, RestError> {
    put_replica(
        Filter::PoolReplica(pool_id, replica_id),
        create.into_inner(),
    )
    .await
}

#[delete(
    "/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}",
    tags(Replicas)
)]
async fn del_node_pool_replica(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> Result<Json<()>, RestError> {
    destroy_replica(Filter::NodePoolReplica(node_id, pool_id, replica_id)).await
}
#[delete("/v0/pools/{pool_id}/replicas/{replica_id}", tags(Replicas))]
async fn del_pool_replica(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
) -> Result<Json<()>, RestError> {
    destroy_replica(Filter::PoolReplica(pool_id, replica_id)).await
}

#[put("/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share/{protocol}", tags(Replicas))]
async fn put_node_pool_replica_share(
    web::Path((node_id, pool_id, replica_id, protocol)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
        Protocol,
    )>,
) -> Result<Json<String>, RestError> {
    share_replica(
        Filter::NodePoolReplica(node_id, pool_id, replica_id),
        protocol,
    )
    .await
}
#[put(
    "/v0/pools/{pool_id}/replicas/{replica_id}/share/{protocol}",
    tags(Replicas)
)]
async fn put_pool_replica_share(
    web::Path((pool_id, replica_id, protocol)): web::Path<(
        PoolId,
        ReplicaId,
        Protocol,
    )>,
) -> Result<Json<String>, RestError> {
    share_replica(Filter::PoolReplica(pool_id, replica_id), protocol).await
}

#[delete(
    "/v0/nodes/{node_id}/pools/{pool_id}/replicas/{replica_id}/share",
    tags(Replicas)
)]
async fn del_node_pool_replica_share(
    web::Path((node_id, pool_id, replica_id)): web::Path<(
        NodeId,
        PoolId,
        ReplicaId,
    )>,
) -> Result<Json<()>, RestError> {
    unshare_replica(Filter::NodePoolReplica(node_id, pool_id, replica_id)).await
}
#[delete("/v0/pools/{pool_id}/replicas/{replica_id}/share", tags(Replicas))]
async fn del_pool_replica_share(
    web::Path((pool_id, replica_id)): web::Path<(PoolId, ReplicaId)>,
) -> Result<Json<()>, RestError> {
    unshare_replica(Filter::PoolReplica(pool_id, replica_id)).await
}

async fn put_replica(
    filter: Filter,
    body: CreateReplicaBody,
) -> Result<Json<Replica>, RestError> {
    let create = match filter.clone() {
        Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
            body.bus_request(node_id, pool_id, replica_id)
        }
        Filter::PoolReplica(pool_id, replica_id) => {
            let node_id = match MessageBus::get_replica(filter).await {
                Ok(replica) => replica.node,
                Err(error) => return Err(RestError::from(error)),
            };
            body.bus_request(node_id, pool_id, replica_id)
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::create_replica(create).await)
}

async fn destroy_replica(filter: Filter) -> Result<Json<()>, RestError> {
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
                Err(error) => return Err(RestError::from(error)),
            };

            DestroyReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::destroy_replica(destroy).await)
}

async fn share_replica(
    filter: Filter,
    protocol: Protocol,
) -> Result<Json<String>, RestError> {
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
                Err(error) => return Err(RestError::from(error)),
            };

            ShareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
                protocol,
            }
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::share_replica(share).await)
}

async fn unshare_replica(filter: Filter) -> Result<Json<()>, RestError> {
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
                Err(error) => return Err(RestError::from(error)),
            };

            UnshareReplica {
                node: node_id,
                pool: pool_id,
                uuid: replica_id,
            }
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::unshare_replica(unshare).await)
}
