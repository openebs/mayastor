use super::*;

pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_volumes)
        .service(get_volume)
        .service(get_node_volumes)
        .service(get_node_volume)
        .service(put_volume)
        .service(del_volume);
}

#[get("/v0/volumes", tags(Volumes))]
async fn get_volumes() -> Result<Json<Vec<Volume>>, RestError> {
    RestRespond::result(MessageBus::get_volumes(Filter::None).await)
}

#[get("/v0/volumes/{volume_id}", tags(Volumes))]
async fn get_volume(
    web::Path(volume_id): web::Path<VolumeId>,
) -> Result<Json<Volume>, RestError> {
    RestRespond::result(MessageBus::get_volume(Filter::Volume(volume_id)).await)
}

#[get("/v0/nodes/{node_id}/volumes", tags(Volumes))]
async fn get_node_volumes(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<Json<Vec<Volume>>, RestError> {
    RestRespond::result(MessageBus::get_volumes(Filter::Node(node_id)).await)
}
#[get("/v0/nodes/{node_id}/volumes/{volume_id}", tags(Volumes))]
async fn get_node_volume(
    web::Path((node_id, volume_id)): web::Path<(NodeId, VolumeId)>,
) -> Result<Json<Volume>, RestError> {
    RestRespond::result(
        MessageBus::get_volume(Filter::NodeVolume(node_id, volume_id)).await,
    )
}

#[put("/v0/volumes/{volume_id}", tags(Volumes))]
async fn put_volume(
    web::Path(volume_id): web::Path<VolumeId>,
    create: web::Json<CreateVolumeBody>,
) -> Result<Json<Volume>, RestError> {
    let create = create.into_inner().bus_request(volume_id);
    RestRespond::result(MessageBus::create_volume(create).await)
}

#[delete("/v0/volumes/{volume_id}", tags(Volumes))]
async fn del_volume(
    web::Path(volume_id): web::Path<VolumeId>,
) -> Result<Json<()>, RestError> {
    let request = DestroyVolume {
        uuid: volume_id,
    };
    RestRespond::result(MessageBus::delete_volume(request).await)
}
