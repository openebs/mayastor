//! Provides a REST interface to interact with JSON gRPC methods.
//! These methods are typically used to control SPDK directly.

use super::*;

/// Configure the functions that this service supports.
pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(json_grpc_call);
}

// A PUT request is required so that method parameters can be passed in the
// body.
//
// # Example
// To create a malloc bdev:
// ```
//  curl -X PUT "https://localhost:8080/v0/nodes/mayastor/jsongrpc/bdev_malloc_create" \
//  -H "accept: application/json" -H "Content-Type: application/json" \
//  -d '{"block_size": 512, "num_blocks": 64, "name": "Malloc0"}'
// ```
#[put("/v0", "/nodes/{node}/jsongrpc/{method}", tags(JsonGrpc))]
async fn json_grpc_call(
    web::Path((node, method)): web::Path<(NodeId, JsonGrpcMethod)>,
    body: web::Json<JsonGeneric>,
) -> Result<web::Json<JsonGeneric>, RestError> {
    RestRespond::result(
        MessageBus::json_grpc_call(JsonGrpcRequest {
            node,
            method,
            params: body.into_inner().to_string().into(),
        })
        .await,
    )
    .map(|x| web::Json(JsonGeneric::from(x.into_inner())))
}
