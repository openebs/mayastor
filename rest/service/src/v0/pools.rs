use super::*;

pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_pools)
        .service(get_pool)
        .service(get_node_pools)
        .service(get_node_pool)
        .service(put_node_pool)
        .service(del_node_pool)
        .service(del_pool);
}

#[get("/v0/pools", tags(Pools))]
async fn get_pools() -> Result<Json<Vec<Pool>>, RestError> {
    RestRespond::result(MessageBus::get_pools(Filter::None).await)
}
#[get("/v0/pools/{id}", tags(Pools))]
async fn get_pool(
    web::Path(pool_id): web::Path<PoolId>,
) -> Result<Json<Pool>, RestError> {
    RestRespond::result(MessageBus::get_pool(Filter::Pool(pool_id)).await)
}

#[get("/v0/nodes/{id}/pools", tags(Pools))]
async fn get_node_pools(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<Json<Vec<Pool>>, RestError> {
    RestRespond::result(MessageBus::get_pools(Filter::Node(node_id)).await)
}

#[get("/v0/nodes/{node_id}/pools/{pool_id}", tags(Pools))]
async fn get_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> Result<Json<Pool>, RestError> {
    RestRespond::result(
        MessageBus::get_pool(Filter::NodePool(node_id, pool_id)).await,
    )
}

#[put("/v0/nodes/{node_id}/pools/{pool_id}", tags(Pools))]
async fn put_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
    create: web::Json<CreatePoolBody>,
) -> Result<Json<Pool>, RestError> {
    let create = create.into_inner().bus_request(node_id, pool_id);
    RestRespond::result(MessageBus::create_pool(create).await)
}

#[delete("/v0/nodes/{node_id}/pools/{pool_id}", tags(Pools))]
async fn del_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> Result<Json<()>, RestError> {
    destroy_pool(Filter::NodePool(node_id, pool_id)).await
}
#[delete("/v0/pools/{pool_id}", tags(Pools))]
async fn del_pool(
    web::Path(pool_id): web::Path<PoolId>,
) -> Result<Json<()>, RestError> {
    destroy_pool(Filter::Pool(pool_id)).await
}

async fn destroy_pool(filter: Filter) -> Result<Json<()>, RestError> {
    let destroy = match filter.clone() {
        Filter::NodePool(node_id, pool_id) => DestroyPool {
            node: node_id,
            id: pool_id,
        },
        Filter::Pool(pool_id) => {
            let node_id = match MessageBus::get_pool(filter).await {
                Ok(pool) => pool.node,
                Err(error) => return Err(RestError::from(error)),
            };
            DestroyPool {
                node: node_id,
                id: pool_id,
            }
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::destroy_pool(destroy).await)
}
