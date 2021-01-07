use super::*;

struct Factory {}
impl HttpServiceFactory for Factory {
    fn register(self, config: &mut AppService) {
        get_nexuses.register(config);
        get_nexus.register(config);
        get_node_nexuses.register(config);
        get_node_nexus.register(config);
        put_node_nexus.register(config);
        del_node_nexus.register(config);
        del_nexus.register(config);
        put_node_nexus_share.register(config);
        del_node_nexus_share.register(config);
    }
}
pub(crate) fn factory() -> impl HttpServiceFactory {
    Factory {}
}

#[get("/v0/nexuses")]
async fn get_nexuses() -> impl Responder {
    RestRespond::result(MessageBus::get_nexuses(Filter::None).await)
}
#[get("/v0/nexuses/{nexus_id}")]
async fn get_nexus(web::Path(nexus_id): web::Path<NexusId>) -> impl Responder {
    RestRespond::result(MessageBus::get_nexuses(Filter::Nexus(nexus_id)).await)
}

#[get("/v0/nodes/{id}/nexuses")]
async fn get_node_nexuses(
    web::Path(node_id): web::Path<NodeId>,
) -> impl Responder {
    RestRespond::result(MessageBus::get_nexuses(Filter::Node(node_id)).await)
}
#[get("/v0/nodes/{node_id}/nexuses/{nexus_id}")]
async fn get_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_nexus(Filter::NodeNexus(node_id, nexus_id)).await,
    )
}

#[put("/v0/nodes/{node_id}/nexuses/{nexus_id}")]
async fn put_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
    create: web::Json<CreateNexusBody>,
) -> impl Responder {
    let create = create.into_inner().bus_request(node_id, nexus_id);
    RestRespond::result(MessageBus::create_nexus(create).await)
}

#[delete("/v0/nodes/{node_id}/nexuses/{nexus_id}")]
async fn del_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> impl Responder {
    destroy_nexus(Filter::NodeNexus(node_id, nexus_id)).await
}
#[delete("/v0/nexuses/{nexus_id}")]
async fn del_nexus(web::Path(nexus_id): web::Path<NexusId>) -> impl Responder {
    destroy_nexus(Filter::Nexus(nexus_id)).await
}

#[put("/v0/nodes/{node_id}/nexuses/{nexus_id}/share/{protocol}")]
async fn put_node_nexus_share(
    web::Path((node_id, nexus_id, protocol)): web::Path<(
        NodeId,
        NexusId,
        Protocol,
    )>,
) -> impl Responder {
    let share = ShareNexus {
        node: node_id,
        uuid: nexus_id,
        key: None,
        protocol,
    };
    RestRespond::result(MessageBus::share_nexus(share).await)
}

#[delete("/v0/nodes/{node_id}/nexuses/{nexus_id}/share")]
async fn del_node_nexus_share(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> impl Responder {
    let unshare = UnshareNexus {
        node: node_id,
        uuid: nexus_id,
    };
    RestRespond::result(MessageBus::unshare_nexus(unshare).await)
}

async fn destroy_nexus(filter: Filter) -> impl Responder {
    let destroy = match filter.clone() {
        Filter::NodeNexus(node_id, nexus_id) => DestroyNexus {
            node: node_id,
            uuid: nexus_id,
        },
        Filter::Nexus(nexus_id) => {
            let node_id = match MessageBus::get_nexus(filter).await {
                Ok(nexus) => nexus.node,
                Err(error) => return (RestError::from(error)).into(),
            };
            DestroyNexus {
                node: node_id,
                uuid: nexus_id,
            }
        }
        _ => return (RestError::from(BusError::NotFound)).into(),
    };

    RestRespond::result(MessageBus::destroy_nexus(destroy).await)
}
