use super::super::ActixRestClient;
use actix_web::{body::Body, http::StatusCode, HttpResponse, ResponseError};
use async_trait::async_trait;
use mbus_api::{
    message_bus::{v0, v0::BusError},
    ErrorChain,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    string::ToString,
};
use strum_macros::{self, Display};

/// Node from the node service
pub type Node = v0::Node;
/// Vector of Nodes from the node service
pub type Nodes = v0::Nodes;
/// Pool from the node service
pub type Pool = v0::Pool;
/// Vector of Pools from the node service
pub type Pools = v0::Pools;
/// Replica
pub type Replica = v0::Replica;
/// Vector of Replicas from the node service
pub type Replicas = v0::Replicas;
/// Replica protocol
pub type Protocol = v0::Protocol;
/// Create Pool request
pub type CreatePool = v0::CreatePool;
/// Create Replica request
pub type CreateReplica = v0::CreateReplica;
/// Replica Destroy
pub type DestroyReplica = v0::DestroyReplica;
/// Pool Destroy
pub type DestroyPool = v0::DestroyPool;
/// Create Replica Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateReplicaBody {
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
}
/// Create Pool Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreatePoolBody {
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<String>,
}
impl From<CreatePool> for CreatePoolBody {
    fn from(create: CreatePool) -> Self {
        CreatePoolBody {
            disks: create.disks,
        }
    }
}
impl CreatePoolBody {
    /// convert into message bus type
    pub fn bus_request(
        &self,
        node_id: String,
        pool_id: String,
    ) -> v0::CreatePool {
        v0::CreatePool {
            node: node_id,
            name: pool_id,
            disks: self.disks.clone(),
        }
    }
}
impl From<CreateReplica> for CreateReplicaBody {
    fn from(create: CreateReplica) -> Self {
        CreateReplicaBody {
            size: create.size,
            thin: create.thin,
            share: create.share,
        }
    }
}
impl CreateReplicaBody {
    /// convert into message bus type
    pub fn bus_request(
        &self,
        node_id: String,
        pool_id: String,
        uuid: String,
    ) -> v0::CreateReplica {
        v0::CreateReplica {
            node: node_id,
            uuid,
            pool: pool_id,
            size: self.size,
            thin: self.thin,
            share: self.share.clone(),
        }
    }
}
/// Filter Nodes, Pools, Replicas
pub type Filter = v0::Filter;

/// RestClient interface
#[async_trait(?Send)]
pub trait RestClient {
    /// Get all the known nodes
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>>;
    /// Get all the known pools
    async fn get_pools(&self, filter: Filter) -> anyhow::Result<Vec<Pool>>;
    /// Get all the known replicas
    async fn get_replicas(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Replica>>;
    /// Create new pool with arguments
    async fn create_pool(&self, args: CreatePool) -> anyhow::Result<Pool>;
    /// Create new replica with arguments
    async fn create_replica(
        &self,
        args: CreateReplica,
    ) -> anyhow::Result<Replica>;
    /// Destroy pool with arguments
    async fn destroy_pool(&self, args: DestroyPool) -> anyhow::Result<()>;
    /// Destroy replica with arguments
    async fn destroy_replica(&self, args: DestroyReplica)
        -> anyhow::Result<()>;
}

#[derive(Display, Debug)]
#[allow(clippy::enum_variant_names)]
enum RestURNs {
    #[strum(serialize = "nodes")]
    GetNodes(Nodes),
    #[strum(serialize = "pools")]
    GetPools(Pools),
    #[strum(serialize = "replicas")]
    GetReplicas(Replicas),
    /* does not work as expect as format! only takes literals...
     * #[strum(serialize = "nodes/{}/pools/{}")]
     * PutPool(Pool), */
}

macro_rules! get_all {
    ($S:ident, $T:ident) => {
        $S.get(
            format!("/v0/{}", RestURNs::$T(Default::default()).to_string()),
            RestURNs::$T,
        )
    };
}
macro_rules! get_filter {
    ($S:ident, $F:ident, $T:ident) => {
        $S.get(
            format!(
                "/v0/{}",
                get_filtered_urn($F, &RestURNs::$T(Default::default()))?
            ),
            RestURNs::$T,
        )
    };
}

fn get_filtered_urn(filter: Filter, r: &RestURNs) -> anyhow::Result<String> {
    let urn = match r {
        RestURNs::GetNodes(_) => match filter {
            Filter::None => "nodes".to_string(),
            Filter::Node(id) => format!("nodes/{}", id),
            _ => return Err(anyhow::Error::msg("Invalid filter for Nodes")),
        },
        RestURNs::GetPools(_) => match filter {
            Filter::None => "pools".to_string(),
            Filter::Node(id) => format!("nodes/{}/pools", id),
            Filter::Pool(id) => format!("pools/{}", id),
            Filter::NodePool(n, p) => format!("nodes/{}/pools/{}", n, p),
            _ => return Err(anyhow::Error::msg("Invalid filter for pools")),
        },
        RestURNs::GetReplicas(_) => match filter {
            Filter::None => "replicas".to_string(),
            Filter::Node(id) => format!("nodes/{}/replicas", id),
            Filter::Pool(id) => format!("pools/{}/replicas", id),
            Filter::Replica(id) => format!("replicas/{}", id),
            Filter::NodePool(n, p) => {
                format!("nodes/{}/pools/{}/replicas", n, p)
            }
            Filter::NodeReplica(n, r) => format!("nodes/{}/replicas/{}", n, r),
            Filter::NodePoolReplica(n, p, r) => {
                format!("nodes/{}/pools/{}/replicas/{}", n, p, r)
            }
            Filter::PoolReplica(p, r) => format!("pools/{}/replicas/{}", p, r),
        },
    };

    Ok(urn)
}

#[async_trait(?Send)]
impl RestClient for ActixRestClient {
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>> {
        let nodes = get_all!(self, GetNodes).await?;
        Ok(nodes.into_inner())
    }

    async fn get_pools(&self, filter: Filter) -> anyhow::Result<Vec<Pool>> {
        let pools = get_filter!(self, filter, GetPools).await?;
        Ok(pools.into_inner())
    }

    async fn get_replicas(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Replica>> {
        let replicas = get_filter!(self, filter, GetReplicas).await?;
        Ok(replicas.into_inner())
    }

    async fn create_pool(&self, args: CreatePool) -> anyhow::Result<Pool> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.name);
        let pool = self.put(urn, CreatePoolBody::from(args)).await?;
        Ok(pool)
    }

    async fn create_replica(
        &self,
        args: CreateReplica,
    ) -> anyhow::Result<Replica> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        let replica = self.put(urn, CreateReplicaBody::from(args)).await?;
        Ok(replica)
    }

    async fn destroy_pool(&self, args: DestroyPool) -> anyhow::Result<()> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.name);
        self.del(urn).await?;
        Ok(())
    }

    async fn destroy_replica(
        &self,
        args: DestroyReplica,
    ) -> anyhow::Result<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }
}

impl Into<Body> for CreatePoolBody {
    fn into(self) -> Body {
        Body::from(serde_json::to_value(self).unwrap())
    }
}
impl Into<Body> for CreateReplicaBody {
    fn into(self) -> Body {
        Body::from(serde_json::to_value(self).unwrap())
    }
}

impl ActixRestClient {
    /// Get RestClient v0
    pub fn v0(&self) -> impl RestClient {
        self.clone()
    }
}

/// Rest Error
#[derive(Debug)]
pub struct RestError {
    kind: BusError,
    message: String,
}

impl RestError {
    // todo: response type convention
    fn get_resp_error(&self) -> HttpResponse {
        match &self.kind {
            BusError::NotFound => HttpResponse::NoContent().json(()),
            BusError::NotUnique => {
                let error = serde_json::json!({"error": self.kind.as_ref(), "message": self.message });
                tracing::error!("Got error: {}", error);
                HttpResponse::InternalServerError().json(error)
            }
            BusError::MessageBusError {
                source,
            } => {
                let error = serde_json::json!({"error": source.as_ref(), "message": source.full_string() });
                tracing::error!("Got error: {}", error);
                HttpResponse::InternalServerError().json(error)
            }
        }
    }
}
// used by the trait ResponseError only when the default error_response trait
// method is used.
impl Display for RestError {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
impl ResponseError for RestError {
    fn status_code(&self) -> StatusCode {
        self.get_resp_error().status()
    }
    fn error_response(&self) -> HttpResponse {
        self.get_resp_error()
    }
}
impl From<BusError> for RestError {
    fn from(kind: BusError) -> Self {
        Self {
            message: kind.to_string(),
            kind,
        }
    }
}
impl Into<HttpResponse> for RestError {
    fn into(self) -> HttpResponse {
        self.get_resp_error()
    }
}

/// Respond using a message bus response Result<Response,BusError>
/// In case of success the Response is sent via the body of a HttpResponse with
/// StatusCode OK.
/// Otherwise, the RestError is returned, also as a HttpResponse/ResponseError.
#[derive(Debug)]
pub struct RestRespond<T>(Result<T, RestError>);

// used by the trait ResponseError only when the default error_response trait
// method is used.
impl<T> Display for RestRespond<T> {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
impl<T: Serialize> RestRespond<T> {
    /// Respond with a Result<T, BusError>
    pub fn result(from: Result<T, BusError>) -> HttpResponse {
        let resp: Self = from.into();
        resp.into()
    }
    /// Respond T with success
    pub fn ok(object: T) -> Result<HttpResponse, RestError> {
        Ok(HttpResponse::Ok().json(object))
    }
}
impl<T> Into<RestRespond<T>> for Result<T, BusError> {
    fn into(self) -> RestRespond<T> {
        RestRespond(self.map_err(RestError::from))
    }
}
impl<T: Serialize> Into<HttpResponse> for RestRespond<T> {
    fn into(self) -> HttpResponse {
        match self.0 {
            Ok(resp) => HttpResponse::Ok().json(resp),
            Err(error) => error.into(),
        }
    }
}
