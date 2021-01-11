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
/// Vector of Pools from the pool service
pub type Pools = v0::Pools;
/// Replica
pub type Replica = v0::Replica;
/// Vector of Replicas from the pool service
pub type Replicas = v0::Replicas;
/// Replica protocol
pub type Protocol = v0::Protocol;
/// Create Pool request
pub type CreatePool = v0::CreatePool;
/// Create Replica request
pub type CreateReplica = v0::CreateReplica;
/// Replica Destroy
pub type DestroyReplica = v0::DestroyReplica;
/// Replica Share
pub type ShareReplica = v0::ShareReplica;
/// Replica Unshare
pub type UnshareReplica = v0::UnshareReplica;
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
        node_id: NodeId,
        pool_id: PoolId,
    ) -> v0::CreatePool {
        v0::CreatePool {
            node: node_id,
            id: pool_id,
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
        node_id: NodeId,
        pool_id: PoolId,
        uuid: ReplicaId,
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
/// Filter Nodes, Pools, Replicas, Nexuses
pub type Filter = v0::Filter;
/// Nexus from the volume service
pub type Nexus = v0::Nexus;
/// Vector of Nexuses from the volume service
pub type Nexuses = v0::Nexuses;
/// State of the nexus
pub type NexusState = v0::NexusState;
/// State of the nexus
pub type VolumeState = v0::VolumeState;
/// Child of the nexus
pub type Child = v0::Child;
/// State of the child
pub type ChildState = v0::ChildState;
/// Nexus Create
pub type CreateNexus = v0::CreateNexus;
/// Nexus Destroy
pub type DestroyNexus = v0::DestroyNexus;
/// Nexus Share
pub type ShareNexus = v0::ShareNexus;
/// Nexus Unshare
pub type UnshareNexus = v0::UnshareNexus;

/// Create Nexus Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateNexusBody {
    /// size of the device in bytes
    pub size: u64,
    /// replica can be iscsi and nvmf remote targets or a local spdk bdev
    /// (i.e. bdev:///name-of-the-bdev).
    ///
    /// uris to the targets we connect to
    pub children: Vec<ChildUri>,
}
impl From<CreateNexus> for CreateNexusBody {
    fn from(create: CreateNexus) -> Self {
        CreateNexusBody {
            size: create.size,
            children: create.children,
        }
    }
}
impl CreateNexusBody {
    /// convert into message bus type
    pub fn bus_request(
        &self,
        node_id: NodeId,
        nexus_id: NexusId,
    ) -> v0::CreateNexus {
        v0::CreateNexus {
            node: node_id,
            uuid: nexus_id,
            size: self.size,
            children: self.children.clone(),
        }
    }
}
/// Remove Nexus Child
pub type RemoveNexusChild = v0::RemoveNexusChild;
/// Add Nexus Child
pub type AddNexusChild = v0::AddNexusChild;
/// Volume
pub type Volume = v0::Volume;
/// Volumes
pub type Volumes = v0::Volumes;
/// Create Volume
pub type CreateVolume = v0::CreateVolume;
/// Destroy Volume
pub type DestroyVolume = v0::DestroyVolume;
/// Id of a mayastor node
pub type NodeId = v0::NodeId;
/// Id of a mayastor pool
pub type PoolId = v0::PoolId;
/// UUID of a mayastor pool replica
pub type ReplicaId = v0::ReplicaId;
/// UUID of a mayastor nexus
pub type NexusId = v0::NexusId;
/// URI of a mayastor nexus child
pub type ChildUri = v0::ChildUri;
/// UUID of a mayastor volume
pub type VolumeId = v0::VolumeId;

/// Create Volume Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateVolumeBody {
    /// size of the volume in bytes
    pub size: u64,
    /// number of children nexuses (ANA)
    pub nexuses: u64,
    /// number of replicas per nexus
    pub replicas: u64,
    /// only these nodes can be used for the replicas
    #[serde(default)]
    pub allowed_nodes: Vec<NodeId>,
    /// preferred nodes for the replicas
    #[serde(default)]
    pub preferred_nodes: Vec<NodeId>,
    /// preferred nodes for the nexuses
    #[serde(default)]
    pub preferred_nexus_nodes: Vec<NodeId>,
}
impl From<CreateVolume> for CreateVolumeBody {
    fn from(create: CreateVolume) -> Self {
        CreateVolumeBody {
            size: create.size,
            nexuses: create.nexuses,
            replicas: create.replicas,
            preferred_nodes: create.preferred_nodes,
            allowed_nodes: create.allowed_nodes,
            preferred_nexus_nodes: create.preferred_nexus_nodes,
        }
    }
}
impl CreateVolumeBody {
    /// convert into message bus type
    pub fn bus_request(&self, volume_id: VolumeId) -> CreateVolume {
        CreateVolume {
            uuid: volume_id,
            size: self.size,
            nexuses: self.nexuses,
            replicas: self.replicas,
            allowed_nodes: self.allowed_nodes.clone(),
            preferred_nodes: self.preferred_nodes.clone(),
            preferred_nexus_nodes: self.preferred_nexus_nodes.clone(),
        }
    }
}

/// RestClient interface
#[async_trait(?Send)]
pub trait RestClient {
    /// Get all the known nodes
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>>;
    /// Get all the known pools
    async fn get_pools(&self, filter: Filter) -> anyhow::Result<Vec<Pool>>;
    /// Create new pool with arguments
    async fn create_pool(&self, args: CreatePool) -> anyhow::Result<Pool>;
    /// Destroy pool with arguments
    async fn destroy_pool(&self, args: DestroyPool) -> anyhow::Result<()>;
    /// Get all the known replicas
    async fn get_replicas(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Replica>>;
    /// Create new replica with arguments
    async fn create_replica(
        &self,
        args: CreateReplica,
    ) -> anyhow::Result<Replica>;
    /// Destroy replica with arguments
    async fn destroy_replica(&self, args: DestroyReplica)
        -> anyhow::Result<()>;
    /// Share replica with arguments
    async fn share_replica(&self, args: ShareReplica)
        -> anyhow::Result<String>;
    /// Unshare replica with arguments
    async fn unshare_replica(&self, args: UnshareReplica)
        -> anyhow::Result<()>;
    /// Get all the known nexuses
    async fn get_nexuses(&self, filter: Filter) -> anyhow::Result<Vec<Nexus>>;
    /// Create new nexus with arguments
    async fn create_nexus(&self, args: CreateNexus) -> anyhow::Result<Nexus>;
    /// Destroy nexus with arguments
    async fn destroy_nexus(&self, args: DestroyNexus) -> anyhow::Result<()>;
    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> anyhow::Result<Nexus>;
    /// Unshare nexus
    async fn unshare_nexus(&self, args: UnshareNexus) -> anyhow::Result<()>;
    /// Remove nexus child
    async fn remove_nexus_child(
        &self,
        args: RemoveNexusChild,
    ) -> anyhow::Result<()>;
    /// Add nexus child
    async fn add_nexus_child(
        &self,
        args: AddNexusChild,
    ) -> anyhow::Result<Child>;
    /// Get all children by filter
    async fn get_nexus_children(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Child>>;
    /// Get all volumes by filter
    async fn get_volumes(&self, filter: Filter) -> anyhow::Result<Vec<Volume>>;
    /// Create volume
    async fn create_volume(&self, args: CreateVolume)
        -> anyhow::Result<Volume>;
    /// Destroy volume
    async fn destroy_volume(&self, args: DestroyVolume) -> anyhow::Result<()>;
}

#[derive(Display, Debug)]
#[allow(clippy::enum_variant_names)]
enum RestURNs {
    #[strum(serialize = "nodes")]
    GetNodes(Node),
    #[strum(serialize = "pools")]
    GetPools(Pool),
    #[strum(serialize = "replicas")]
    GetReplicas(Replica),
    #[strum(serialize = "nexuses")]
    GetNexuses(Nexus),
    #[strum(serialize = "children")]
    GetChildren(Child),
    #[strum(serialize = "volumes")]
    GetVolumes(Volume),
    /* does not work as expect as format! only takes literals...
     * #[strum(serialize = "nodes/{}/pools/{}")]
     * PutPool(Pool), */
}

macro_rules! get_all {
    ($S:ident, $T:ident) => {
        $S.get_vec(format!(
            "/v0/{}",
            RestURNs::$T(Default::default()).to_string()
        ))
    };
}
macro_rules! get_filter {
    ($S:ident, $F:ident, $T:ident) => {
        $S.get_vec(format!(
            "/v0/{}",
            get_filtered_urn($F, &RestURNs::$T(Default::default()))?
        ))
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
            _ => return Err(anyhow::Error::msg("Invalid filter for replicas")),
        },
        RestURNs::GetNexuses(_) => match filter {
            Filter::None => "nexuses".to_string(),
            Filter::Node(n) => format!("nodes/{}/nexuses", n),
            Filter::NodeNexus(n, x) => format!("nodes/{}/nexuses/{}", n, x),
            Filter::Nexus(x) => format!("nexuses/{}", x),
            _ => return Err(anyhow::Error::msg("Invalid filter for nexuses")),
        },
        RestURNs::GetChildren(_) => match filter {
            Filter::NodeNexus(n, x) => {
                format!("nodes/{}/nexuses/{}/children", n, x)
            }
            Filter::Nexus(x) => format!("nexuses/{}/children", x),
            _ => return Err(anyhow::Error::msg("Invalid filter for nexuses")),
        },
        RestURNs::GetVolumes(_) => match filter {
            Filter::None => "volumes".to_string(),
            Filter::Node(n) => format!("nodes/{}/volumes", n),
            Filter::Volume(x) => format!("volumes/{}", x),
            _ => return Err(anyhow::Error::msg("Invalid filter for volumes")),
        },
    };

    Ok(urn)
}

#[async_trait(?Send)]
impl RestClient for ActixRestClient {
    async fn get_nodes(&self) -> anyhow::Result<Vec<Node>> {
        let nodes = get_all!(self, GetNodes).await?;
        Ok(nodes)
    }

    async fn get_pools(&self, filter: Filter) -> anyhow::Result<Vec<Pool>> {
        let pools = get_filter!(self, filter, GetPools).await?;
        Ok(pools)
    }

    async fn create_pool(&self, args: CreatePool) -> anyhow::Result<Pool> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        let pool = self.put(urn, CreatePoolBody::from(args)).await?;
        Ok(pool)
    }

    async fn destroy_pool(&self, args: DestroyPool) -> anyhow::Result<()> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        self.del(urn).await?;
        Ok(())
    }

    async fn get_replicas(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Replica>> {
        let replicas = get_filter!(self, filter, GetReplicas).await?;
        Ok(replicas)
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

    /// Share replica with arguments
    async fn share_replica(
        &self,
        args: ShareReplica,
    ) -> anyhow::Result<String> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}/share/{}",
            &args.node,
            &args.pool,
            &args.uuid,
            args.protocol.to_string()
        );
        let share = self.put(urn, Body::Empty).await?;
        Ok(share)
    }
    /// Unshare replica with arguments
    async fn unshare_replica(
        &self,
        args: UnshareReplica,
    ) -> anyhow::Result<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}/share",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }

    async fn get_nexuses(&self, filter: Filter) -> anyhow::Result<Vec<Nexus>> {
        let nexuses = get_filter!(self, filter, GetNexuses).await?;
        Ok(nexuses)
    }

    async fn get_nexus_children(
        &self,
        filter: Filter,
    ) -> anyhow::Result<Vec<Child>> {
        let children = get_filter!(self, filter, GetChildren).await?;
        Ok(children)
    }

    async fn create_nexus(&self, args: CreateNexus) -> anyhow::Result<Nexus> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        let replica = self.put(urn, CreateNexusBody::from(args)).await?;
        Ok(replica)
    }

    async fn destroy_nexus(&self, args: DestroyNexus) -> anyhow::Result<()> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> anyhow::Result<Nexus> {
        let urn = format!(
            "/v0/nodes/{}/nexuses/{}/share/{}",
            &args.node,
            &args.uuid,
            args.protocol.to_string()
        );
        let nexus = self.put(urn, Body::Empty).await?;
        Ok(nexus)
    }

    /// Unshare nexus
    async fn unshare_nexus(&self, args: UnshareNexus) -> anyhow::Result<()> {
        let urn =
            format!("/v0/nodes/{}/nexuses/{}/share", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn remove_nexus_child(
        &self,
        args: RemoveNexusChild,
    ) -> anyhow::Result<()> {
        let urn = match url::Url::parse(args.uri.as_str()) {
            Ok(uri) => {
                // remove initial '/'
                uri.path()[1 ..].to_string()
            }
            _ => args.uri.to_string(),
        };
        self.del(urn).await?;
        Ok(())
    }
    async fn add_nexus_child(
        &self,
        args: AddNexusChild,
    ) -> anyhow::Result<Child> {
        let urn = format!(
            "/v0/nodes/{}/nexuses/{}/children/{}",
            &args.node, &args.nexus, &args.uri
        );
        let replica = self.put(urn, Body::Empty).await?;
        Ok(replica)
    }

    async fn get_volumes(&self, filter: Filter) -> anyhow::Result<Vec<Volume>> {
        let volumes = get_filter!(self, filter, GetVolumes).await?;
        Ok(volumes)
    }

    async fn create_volume(
        &self,
        args: CreateVolume,
    ) -> anyhow::Result<Volume> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        let volume = self.put(urn, CreateVolumeBody::from(args)).await?;
        Ok(volume)
    }

    async fn destroy_volume(&self, args: DestroyVolume) -> anyhow::Result<()> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
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
impl Into<Body> for CreateNexusBody {
    fn into(self) -> Body {
        Body::from(serde_json::to_value(self).unwrap())
    }
}
impl Into<Body> for CreateVolumeBody {
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
