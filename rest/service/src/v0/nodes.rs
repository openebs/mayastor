use super::*;

pub(crate) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_nodes).service(get_node);
}

#[get("/v0/nodes", tags(Nodes))]
async fn get_nodes() -> Result<web::Json<Vec<Node>>, RestError> {
    RestRespond::result(MessageBus::get_nodes().await)
}
#[get("/v0/nodes/{id}", tags(Nodes))]
async fn get_node(
    web::Path(node_id): web::Path<NodeId>,
) -> Result<web::Json<Node>, RestError> {
    RestRespond::result(MessageBus::get_node(&node_id).await)
}
