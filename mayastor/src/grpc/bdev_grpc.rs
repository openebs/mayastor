use tonic::{Request, Response, Status};
use tracing::instrument;

use rpc::mayastor::{
    bdev_rpc_server::BdevRpc,
    Bdev as RpcBdev,
    BdevShareReply,
    BdevShareRequest,
    BdevUri,
    Bdevs,
    CreateReply,
    Null,
};

use crate::{
    core::{Bdev, Reactors, Share},
    grpc::GrpcResult,
    nexus_uri::{bdev_create, NexusBdevError},
};

impl From<NexusBdevError> for tonic::Status {
    fn from(e: NexusBdevError) -> Self {
        match e {
            NexusBdevError::UrlParseError {
                ..
            } => Status::invalid_argument(e.to_string()),
            NexusBdevError::UriSchemeUnsupported {
                ..
            } => Status::invalid_argument(e.to_string()),
            NexusBdevError::UriInvalid {
                ..
            } => Status::invalid_argument(e.to_string()),
            e => Status::internal(e.to_string()),
        }
    }
}

impl From<Bdev> for RpcBdev {
    fn from(b: Bdev) -> Self {
        Self {
            name: b.name(),
            uuid: b.uuid_as_string(),
            num_blocks: b.num_blocks(),
            blk_size: b.block_len(),
            claimed: b.is_claimed(),
            claimed_by: b.claimed_by().unwrap_or_else(|| "Orphaned".into()),
            aliases: b.aliases().join(","),
        }
    }
}

#[derive(Debug)]
pub struct BdevSvc {}

#[tonic::async_trait]
impl BdevRpc for BdevSvc {
    #[instrument(level = "debug", err)]
    async fn list(&self, _request: Request<Null>) -> GrpcResult<Bdevs> {
        let mut list: Vec<RpcBdev> = Vec::new();
        if let Some(bdev) = Bdev::bdev_first() {
            bdev.into_iter().for_each(|bdev| list.push(bdev.into()))
        }

        Ok(Response::new(Bdevs {
            bdevs: list,
        }))
    }

    #[instrument(level = "debug", err)]
    async fn create(
        &self,
        request: Request<BdevUri>,
    ) -> Result<Response<CreateReply>, Status> {
        let uri = request.into_inner().uri;
        let bdev = locally! { async move { bdev_create(&uri).await } };

        Ok(Response::new(CreateReply {
            name: bdev,
        }))
    }

    #[instrument(level = "debug", err)]
    async fn share(
        &self,
        request: Request<BdevShareRequest>,
    ) -> GrpcResult<BdevShareReply> {
        let r = request.into_inner();
        let name = r.name;
        let proto = r.proto;

        if Bdev::lookup_by_name(&name).is_none() {
            return Err(Status::not_found(name));
        }

        if proto != "iscsi" && proto != "nvmf" {
            return Err(Status::invalid_argument(proto));
        }

        match proto.as_str() {
            "nvmf" => Reactors::master().spawn_local(async move {
                let bdev = Bdev::lookup_by_name(&name).unwrap();
                bdev.share_nvmf()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            }),

            "iscsi" => Reactors::master().spawn_local(async move {
                let bdev = Bdev::lookup_by_name(&name).unwrap();
                bdev.share_iscsi()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            }),

            _ => unreachable!(),
        }
        .await
        .unwrap()
        .map(|share| {
            Response::new(BdevShareReply {
                uri: share,
            })
        })
    }

    #[instrument(level = "debug", err)]
    async fn unshare(&self, request: Request<CreateReply>) -> GrpcResult<Null> {
        let name = request.into_inner().name;
        let hdl = Reactors::master().spawn_local(async move {
            let bdev = Bdev::lookup_by_name(&name).unwrap();
            let _ = bdev
                .unshare()
                .await
                .map_err(|e| Status::internal(e.to_string()));
        });

        hdl.await.unwrap();

        Ok(Response::new(Null {}))
    }
}
