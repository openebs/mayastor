use super::*;

struct Factory {}
impl HttpServiceFactory for Factory {
    fn register(self, config: &mut AppService) {
        get_volumes.register(config);
        get_volume.register(config);
        get_node_volumes.register(config);
        get_node_volume.register(config);
        put_volume.register(config);
        del_volume.register(config);
    }
}
pub(crate) fn factory() -> impl HttpServiceFactory {
    Factory {}
}

#[get("/v0/volumes")]
async fn get_volumes() -> impl Responder {
    RestRespond::result(MessageBus::get_volumes(Filter::None).await)
}

#[get("/v0/volumes/{volume_id}")]
async fn get_volume(
    web::Path(volume_id): web::Path<VolumeId>,
) -> impl Responder {
    RestRespond::result(MessageBus::get_volume(Filter::Volume(volume_id)).await)
}

#[get("/v0/nodes/{node_id}/volumes")]
async fn get_node_volumes(
    web::Path(node_id): web::Path<NodeId>,
) -> impl Responder {
    RestRespond::result(MessageBus::get_volumes(Filter::Node(node_id)).await)
}
#[get("/v0/nodes/{node_id}/volumes/{volume_id}")]
async fn get_node_volume(
    web::Path((node_id, volume_id)): web::Path<(NodeId, VolumeId)>,
) -> impl Responder {
    RestRespond::result(
        MessageBus::get_volume(Filter::NodeVolume(node_id, volume_id)).await,
    )
}

#[put("/v0/volumes/{volume_id}")]
async fn put_volume(
    web::Path(volume_id): web::Path<VolumeId>,
    create: web::Json<CreateVolumeBody>,
) -> impl Responder {
    let create = create.into_inner().bus_request(volume_id);
    RestRespond::result(MessageBus::create_volume(create).await)
}

#[delete("/v0/volumes/{volume_id}")]
async fn del_volume(
    web::Path(volume_id): web::Path<VolumeId>,
) -> impl Responder {
    let request = DestroyVolume {
        uuid: volume_id,
    };
    RestRespond::result(MessageBus::delete_volume(request).await)
}
