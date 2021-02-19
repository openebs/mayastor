#![allow(clippy::field_reassign_with_default)]
use super::super::ActixRestClient;
use crate::{ClientError, ClientResult, JsonGeneric};
use actix_web::{
    body::Body,
    http::StatusCode,
    web::Json,
    HttpResponse,
    ResponseError,
};
use async_trait::async_trait;
pub use mbus_api::message_bus::v0::*;
use paperclip::actix::Apiv2Schema;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    string::ToString,
};
use strum_macros::{self, Display};

/// Create Replica Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
pub struct CreateReplicaBody {
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
}
/// Create Pool Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
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

/// Create Nexus Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
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

/// Create Volume Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
pub struct CreateVolumeBody {
    /// size of the volume in bytes
    pub size: u64,
    /// number of children nexuses (ANA)
    pub nexuses: u64,
    /// number of replicas per nexus
    pub replicas: u64,
    /// only these nodes can be used for the replicas
    #[serde(default)]
    pub allowed_nodes: Option<Vec<NodeId>>,
    /// preferred nodes for the replicas
    #[serde(default)]
    pub preferred_nodes: Option<Vec<NodeId>>,
    /// preferred nodes for the nexuses
    #[serde(default)]
    pub preferred_nexus_nodes: Option<Vec<NodeId>>,
}
impl From<CreateVolume> for CreateVolumeBody {
    fn from(create: CreateVolume) -> Self {
        CreateVolumeBody {
            size: create.size,
            nexuses: create.nexuses,
            replicas: create.replicas,
            preferred_nodes: create.preferred_nodes.into(),
            allowed_nodes: create.allowed_nodes.into(),
            preferred_nexus_nodes: create.preferred_nexus_nodes.into(),
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
            allowed_nodes: self.allowed_nodes.clone().unwrap_or_default(),
            preferred_nodes: self.preferred_nodes.clone().unwrap_or_default(),
            preferred_nexus_nodes: self
                .preferred_nexus_nodes
                .clone()
                .unwrap_or_default(),
        }
    }
}

/// Contains the query parameters that can be passed when calling
/// get_block_devices
#[derive(Deserialize, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockDeviceQueryParams {
    /// specifies whether to list all devices or only usable ones
    pub all: Option<bool>,
}

/// RestClient interface
#[async_trait(?Send)]
pub trait RestClient {
    /// Get all the known nodes
    async fn get_nodes(&self) -> ClientResult<Vec<Node>>;
    /// Get all the known pools
    async fn get_pools(&self, filter: Filter) -> ClientResult<Vec<Pool>>;
    /// Create new pool with arguments
    async fn create_pool(&self, args: CreatePool) -> ClientResult<Pool>;
    /// Destroy pool with arguments
    async fn destroy_pool(&self, args: DestroyPool) -> ClientResult<()>;
    /// Get all the known replicas
    async fn get_replicas(&self, filter: Filter) -> ClientResult<Vec<Replica>>;
    /// Create new replica with arguments
    async fn create_replica(
        &self,
        args: CreateReplica,
    ) -> ClientResult<Replica>;
    /// Destroy replica with arguments
    async fn destroy_replica(&self, args: DestroyReplica) -> ClientResult<()>;
    /// Share replica with arguments
    async fn share_replica(&self, args: ShareReplica) -> ClientResult<String>;
    /// Unshare replica with arguments
    async fn unshare_replica(&self, args: UnshareReplica) -> ClientResult<()>;
    /// Get all the known nexuses
    async fn get_nexuses(&self, filter: Filter) -> ClientResult<Vec<Nexus>>;
    /// Create new nexus with arguments
    async fn create_nexus(&self, args: CreateNexus) -> ClientResult<Nexus>;
    /// Destroy nexus with arguments
    async fn destroy_nexus(&self, args: DestroyNexus) -> ClientResult<()>;
    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> ClientResult<String>;
    /// Unshare nexus
    async fn unshare_nexus(&self, args: UnshareNexus) -> ClientResult<()>;
    /// Remove nexus child
    async fn remove_nexus_child(
        &self,
        args: RemoveNexusChild,
    ) -> ClientResult<()>;
    /// Add nexus child
    async fn add_nexus_child(&self, args: AddNexusChild)
        -> ClientResult<Child>;
    /// Get all children by filter
    async fn get_nexus_children(
        &self,
        filter: Filter,
    ) -> ClientResult<Vec<Child>>;
    /// Get all volumes by filter
    async fn get_volumes(&self, filter: Filter) -> ClientResult<Vec<Volume>>;
    /// Create volume
    async fn create_volume(&self, args: CreateVolume) -> ClientResult<Volume>;
    /// Destroy volume
    async fn destroy_volume(&self, args: DestroyVolume) -> ClientResult<()>;
    /// Generic JSON gRPC call
    async fn json_grpc(
        &self,
        args: JsonGrpcRequest,
    ) -> ClientResult<JsonGeneric>;
    /// Get block devices
    async fn get_block_devices(
        &self,
        args: GetBlockDevices,
    ) -> ClientResult<Vec<BlockDevice>>;
}

#[derive(Display, Debug)]
#[allow(clippy::enum_variant_names)]
enum RestUrns {
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
            RestUrns::$T(Default::default()).to_string()
        ))
    };
}
macro_rules! get_filter {
    ($S:ident, $F:ident, $T:ident) => {
        $S.get_vec(format!(
            "/v0/{}",
            get_filtered_urn($F, &RestUrns::$T(Default::default()))?
        ))
    };
}

fn get_filtered_urn(filter: Filter, r: &RestUrns) -> ClientResult<String> {
    let urn = match r {
        RestUrns::GetNodes(_) => match filter {
            Filter::None => "nodes".to_string(),
            Filter::Node(id) => format!("nodes/{}", id),
            _ => return Err(ClientError::filter("Invalid filter for Nodes")),
        },
        RestUrns::GetPools(_) => match filter {
            Filter::None => "pools".to_string(),
            Filter::Node(id) => format!("nodes/{}/pools", id),
            Filter::Pool(id) => format!("pools/{}", id),
            Filter::NodePool(n, p) => format!("nodes/{}/pools/{}", n, p),
            _ => return Err(ClientError::filter("Invalid filter for pools")),
        },
        RestUrns::GetReplicas(_) => match filter {
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
            _ => {
                return Err(ClientError::filter("Invalid filter for replicas"))
            }
        },
        RestUrns::GetNexuses(_) => match filter {
            Filter::None => "nexuses".to_string(),
            Filter::Node(n) => format!("nodes/{}/nexuses", n),
            Filter::NodeNexus(n, x) => format!("nodes/{}/nexuses/{}", n, x),
            Filter::Nexus(x) => format!("nexuses/{}", x),
            _ => return Err(ClientError::filter("Invalid filter for nexuses")),
        },
        RestUrns::GetChildren(_) => match filter {
            Filter::NodeNexus(n, x) => {
                format!("nodes/{}/nexuses/{}/children", n, x)
            }
            Filter::Nexus(x) => format!("nexuses/{}/children", x),
            _ => return Err(ClientError::filter("Invalid filter for nexuses")),
        },
        RestUrns::GetVolumes(_) => match filter {
            Filter::None => "volumes".to_string(),
            Filter::Node(n) => format!("nodes/{}/volumes", n),
            Filter::Volume(x) => format!("volumes/{}", x),
            _ => return Err(ClientError::filter("Invalid filter for volumes")),
        },
    };

    Ok(urn)
}

#[async_trait(?Send)]
impl RestClient for ActixRestClient {
    async fn get_nodes(&self) -> ClientResult<Vec<Node>> {
        let nodes = get_all!(self, GetNodes).await?;
        Ok(nodes)
    }

    async fn get_pools(&self, filter: Filter) -> ClientResult<Vec<Pool>> {
        let pools = get_filter!(self, filter, GetPools).await?;
        Ok(pools)
    }

    async fn create_pool(&self, args: CreatePool) -> ClientResult<Pool> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        let pool = self.put(urn, CreatePoolBody::from(args)).await?;
        Ok(pool)
    }

    async fn destroy_pool(&self, args: DestroyPool) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        self.del(urn).await?;
        Ok(())
    }

    async fn get_replicas(&self, filter: Filter) -> ClientResult<Vec<Replica>> {
        let replicas = get_filter!(self, filter, GetReplicas).await?;
        Ok(replicas)
    }

    async fn create_replica(
        &self,
        args: CreateReplica,
    ) -> ClientResult<Replica> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        let replica = self.put(urn, CreateReplicaBody::from(args)).await?;
        Ok(replica)
    }

    async fn destroy_replica(&self, args: DestroyReplica) -> ClientResult<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }

    /// Share replica with arguments
    async fn share_replica(&self, args: ShareReplica) -> ClientResult<String> {
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
    async fn unshare_replica(&self, args: UnshareReplica) -> ClientResult<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}/share",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }

    async fn get_nexuses(&self, filter: Filter) -> ClientResult<Vec<Nexus>> {
        let nexuses = get_filter!(self, filter, GetNexuses).await?;
        Ok(nexuses)
    }

    async fn create_nexus(&self, args: CreateNexus) -> ClientResult<Nexus> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        let replica = self.put(urn, CreateNexusBody::from(args)).await?;
        Ok(replica)
    }

    async fn destroy_nexus(&self, args: DestroyNexus) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> ClientResult<String> {
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
    async fn unshare_nexus(&self, args: UnshareNexus) -> ClientResult<()> {
        let urn =
            format!("/v0/nodes/{}/nexuses/{}/share", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn remove_nexus_child(
        &self,
        args: RemoveNexusChild,
    ) -> ClientResult<()> {
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
    ) -> ClientResult<Child> {
        let urn = format!(
            "/v0/nodes/{}/nexuses/{}/children/{}",
            &args.node, &args.nexus, &args.uri
        );
        let replica = self.put(urn, Body::Empty).await?;
        Ok(replica)
    }
    async fn get_nexus_children(
        &self,
        filter: Filter,
    ) -> ClientResult<Vec<Child>> {
        let children = get_filter!(self, filter, GetChildren).await?;
        Ok(children)
    }

    async fn get_volumes(&self, filter: Filter) -> ClientResult<Vec<Volume>> {
        let volumes = get_filter!(self, filter, GetVolumes).await?;
        Ok(volumes)
    }

    async fn create_volume(&self, args: CreateVolume) -> ClientResult<Volume> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        let volume = self.put(urn, CreateVolumeBody::from(args)).await?;
        Ok(volume)
    }

    async fn destroy_volume(&self, args: DestroyVolume) -> ClientResult<()> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn json_grpc(
        &self,
        args: JsonGrpcRequest,
    ) -> ClientResult<JsonGeneric> {
        let urn = format!("/v0/nodes/{}/jsongrpc/{}", args.node, args.method);
        self.put(urn, Body::from(args.params.to_string())).await
    }

    async fn get_block_devices(
        &self,
        args: GetBlockDevices,
    ) -> ClientResult<Vec<BlockDevice>> {
        let urn =
            format!("/v0/nodes/{}/block_devices?all={}", args.node, args.all);
        self.get_vec(urn).await
    }
}

impl From<CreatePoolBody> for Body {
    fn from(src: CreatePoolBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateReplicaBody> for Body {
    fn from(src: CreateReplicaBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateNexusBody> for Body {
    fn from(src: CreateNexusBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateVolumeBody> for Body {
    fn from(src: CreateVolumeBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
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

#[cfg(not(feature = "nightly"))]
impl paperclip::v2::schema::Apiv2Errors for RestError {}

impl RestError {
    // todo: response type convention
    fn get_resp_error(&self) -> HttpResponse {
        match &self.kind {
            BusError::NotFound => HttpResponse::NotFound().json(()),
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
impl From<RestError> for HttpResponse {
    fn from(src: RestError) -> Self {
        src.get_resp_error()
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
    pub fn result(from: Result<T, BusError>) -> Result<Json<T>, RestError> {
        match from {
            Ok(v) => Ok(Json::<T>(v)),
            Err(e) => Err(e.into()),
        }
    }
    /// Respond T with success
    pub fn ok(object: T) -> Result<Json<T>, RestError> {
        Ok(Json(object))
    }
}
impl<T> From<Result<T, BusError>> for RestRespond<T> {
    fn from(src: Result<T, BusError>) -> Self {
        RestRespond(src.map_err(RestError::from))
    }
}
impl<T: Serialize> From<RestRespond<T>> for HttpResponse {
    fn from(src: RestRespond<T>) -> Self {
        match src.0 {
            Ok(resp) => HttpResponse::Ok().json(resp),
            Err(error) => error.into(),
        }
    }
}
