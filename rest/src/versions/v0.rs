use super::super::ActixRestClient;
use async_trait::async_trait;
use std::string::ToString;
use strum_macros::{self, Display};

/// Node from the node service
pub type Node = mbus_api::v0::Node;
/// Vector of Nodes from the node service
pub type Nodes = mbus_api::v0::Nodes;

/// RestClient interface
#[async_trait(?Send)]
pub trait RestClient {
    /// Get all the known nodes
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>>;
}

#[derive(Display, Debug)]
enum RestURNs {
    #[strum(serialize = "nodes")]
    GetNodes(Nodes),
}

macro_rules! get {
    ($S:ident, $T:ident) => {
        $S.get(
            format!("/v0/{}", RestURNs::$T(Default::default()).to_string()),
            RestURNs::$T,
        )
    };
}

#[async_trait(?Send)]
impl RestClient for ActixRestClient {
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>> {
        let nodes = get!(self, GetNodes).await?;
        Ok(nodes.0)
    }
}

impl ActixRestClient {
    /// Get RestClient v0
    pub fn v0(&self) -> impl RestClient {
        self.clone()
    }
}
