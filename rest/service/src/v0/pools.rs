use super::*;

struct Factory {}
impl HttpServiceFactory for Factory {
    fn register(self, config: &mut AppService) {
        get_pools.register(config);
        get_pool.register(config);
        get_node_pools.register(config);
        get_node_pool.register(config);
        put_node_pool.register(config);
        del_node_pool.register(config);
        del_pool.register(config);
    }
}
pub(crate) fn factory() -> impl HttpServiceFactory {
    Factory {}
}

#[get("/v0/pools")]
async fn get_pools() -> impl Responder {
    RestRespond::result(MessageBus::get_pools(Filter::None).await)
}

#[get("/v0/pools/{id}")]
async fn get_pool(web::Path(pool_id): web::Path<PoolId>) -> impl Responder {
    RestRespond::result(MessageBus::get_pool(Filter::Pool(pool_id)).await)
}

#[get("/v0/nodes/{id}/pools")]
async fn get_node_pools(
    web::Path(node_id): web::Path<NodeId>,
) -> impl Responder {
    RestRespond::result(MessageBus::get_pools(Filter::Node(node_id)).await)
}

#[get("/v0/nodes/{node_id}/pools/{pool_id}")]
async fn get_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_pool(Filter::NodePool(node_id, pool_id)).await,
    )
}

#[put("/v0/nodes/{node_id}/pools/{pool_id}")]
async fn put_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
    create: web::Json<CreatePoolBody>,
) -> impl Responder {
    let create = create.into_inner().bus_request(node_id, pool_id);
    RestRespond::result(MessageBus::create_pool(create).await)
}

#[delete("/v0/nodes/{node_id}/pools/{pool_id}")]
async fn del_node_pool(
    web::Path((node_id, pool_id)): web::Path<(NodeId, PoolId)>,
) -> impl Responder {
    destroy_pool(Filter::NodePool(node_id, pool_id)).await
}
#[delete("/v0/pools/{pool_id}")]
async fn del_pool(web::Path(pool_id): web::Path<PoolId>) -> impl Responder {
    destroy_pool(Filter::Pool(pool_id)).await
}

async fn destroy_pool(filter: Filter) -> impl Responder {
    let destroy = match filter.clone() {
        Filter::NodePool(node_id, pool_id) => DestroyPool {
            node: node_id,
            id: pool_id,
        },
        Filter::Pool(pool_id) => {
            let node_id = match MessageBus::get_pool(filter).await {
                Ok(pool) => pool.node,
                Err(error) => return (RestError::from(error)).into(),
            };
            DestroyPool {
                node: node_id,
                id: pool_id,
            }
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::destroy_pool(destroy).await)
}
