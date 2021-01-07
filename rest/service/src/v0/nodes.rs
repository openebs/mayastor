use super::*;

struct Factory {}
impl HttpServiceFactory for Factory {
    fn register(self, config: &mut AppService) {
        get_node.register(config);
        get_nodes.register(config);
    }
}
pub(crate) fn factory() -> impl HttpServiceFactory {
    Factory {}
}

#[get("/v0/nodes")]
async fn get_nodes() -> impl Responder {
    RestRespond::result(MessageBus::get_nodes().await)
}
#[get("/v0/nodes/{id}")]
async fn get_node(web::Path(node_id): web::Path<NodeId>) -> impl Responder {
    RestRespond::result(MessageBus::get_node(&node_id).await)
}
