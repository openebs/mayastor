use super::*;
use mbus_api::v0::GetBlockDevices;

pub(super) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_block_devices);
}

// Get block devices takes a query parameter 'all' which is used to determine
// whether to return all found devices or only those that are usable.
// Omitting the query parameter will result in all block devices being shown.
//
// # Examples
// Get only usable block devices with query parameter:
//      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices?all=false" \
//      -H  "accept: application/json"
//
// Get all block devices with query parameter:
//      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices?all=true" \
//      -H  "accept: application/json" -k
//
// Get all block devices without query parameter:
//      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices" \
//      -H  "accept: application/json" -k
//
#[get("/v0", "/nodes/{node}/block_devices", tags(BlockDevices))]
async fn get_block_devices(
    web::Query(info): web::Query<GetBlockDeviceQueryParams>,
    web::Path(node): web::Path<NodeId>,
) -> Result<Json<Vec<BlockDevice>>, RestError> {
    RestRespond::result(Ok(MessageBus::get_block_devices(GetBlockDevices {
        node,
        all: info.all.unwrap_or(true),
    })
    .await?
    .into_inner()))
}
