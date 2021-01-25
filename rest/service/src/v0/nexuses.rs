use super::*;

pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_nexuses)
        .service(get_nexus)
        .service(get_node_nexuses)
        .service(get_node_nexus)
        .service(put_node_nexus)
        .service(del_node_nexus)
        .service(del_nexus)
        .service(put_node_nexus_share)
        .service(del_node_nexus_share);
}

#[get("/v0/nexuses", tags(Nexuses))]
async fn get_nexuses() -> Result<Json<Vec<Nexus>>, RestError> {
    RestRespond::result(MessageBus::get_nexuses(Filter::None).await)
}
#[get("/v0/nexuses/{nexus_id}", tags(Nexuses))]
async fn get_nexus(
    web::Path(nexus_id): web::Path<NexusId>,
) -> Result<Json<Nexus>, RestError> {
    RestRespond::result(MessageBus::get_nexus(Filter::Nexus(nexus_id)).await)
}

#[get("/v0/nodes/{id}/nexuses", tags(Nexuses))]
async fn get_node_nexuses(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<Json<Vec<Nexus>>, RestError> {
    RestRespond::result(MessageBus::get_nexuses(Filter::Node(node_id)).await)
}
#[get("/v0/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn get_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<Json<Nexus>, RestError> {
    RestRespond::result(
        MessageBus::get_nexus(Filter::NodeNexus(node_id, nexus_id)).await,
    )
}

#[put("/v0/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn put_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
    create: web::Json<CreateNexusBody>,
) -> Result<Json<Nexus>, RestError> {
    let create = create.into_inner().bus_request(node_id, nexus_id);
    RestRespond::result(MessageBus::create_nexus(create).await)
}

#[delete("/v0/nodes/{node_id}/nexuses/{nexus_id}", tags(Nexuses))]
async fn del_node_nexus(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<Json<()>, RestError> {
    destroy_nexus(Filter::NodeNexus(node_id, nexus_id)).await
}
#[delete("/v0/nexuses/{nexus_id}", tags(Nexuses))]
async fn del_nexus(
    web::Path(nexus_id): web::Path<NexusId>,
) -> Result<Json<()>, RestError> {
    destroy_nexus(Filter::Nexus(nexus_id)).await
}

#[put(
    "/v0/nodes/{node_id}/nexuses/{nexus_id}/share/{protocol}",
    tags(Nexuses)
)]
async fn put_node_nexus_share(
    web::Path((node_id, nexus_id, protocol)): web::Path<(
        NodeId,
        NexusId,
        Protocol,
    )>,
) -> Result<Json<String>, RestError> {
    let share = ShareNexus {
        node: node_id,
        uuid: nexus_id,
        key: None,
        protocol,
    };
    RestRespond::result(MessageBus::share_nexus(share).await)
}

#[delete("/v0/nodes/{node_id}/nexuses/{nexus_id}/share", tags(Nexuses))]
async fn del_node_nexus_share(
    web::Path((node_id, nexus_id)): web::Path<(NodeId, NexusId)>,
) -> Result<Json<()>, RestError> {
    let unshare = UnshareNexus {
        node: node_id,
        uuid: nexus_id,
    };
    RestRespond::result(MessageBus::unshare_nexus(unshare).await)
}

async fn destroy_nexus(filter: Filter) -> Result<Json<()>, RestError> {
    let destroy = match filter.clone() {
        Filter::NodeNexus(node_id, nexus_id) => DestroyNexus {
            node: node_id,
            uuid: nexus_id,
        },
        Filter::Nexus(nexus_id) => {
            let node_id = match MessageBus::get_nexus(filter).await {
                Ok(nexus) => nexus.node,
                Err(error) => return Err(RestError::from(error)),
            };
            DestroyNexus {
                node: node_id,
                uuid: nexus_id,
            }
        }
        _ => return Err(RestError::from(BusError::NotFound)),
    };

    RestRespond::result(MessageBus::destroy_nexus(destroy).await)
}
