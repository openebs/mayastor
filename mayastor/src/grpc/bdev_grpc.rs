use tonic::{Request, Response, Status};
use tracing::instrument;

use std::convert::TryFrom;
use url::Url;

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
    core::{Bdev, Mthread, Reactors, Share},
    grpc::{rpc_submit, sync_config, GrpcResult},
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
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
            product_name: b.product_name(),
            share_uri: b.share_uri().unwrap_or_else(|| "".into()),
            uri: Url::try_from(b).map_or("".into(), |u| u.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct BdevSvc;

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

        let rx = rpc_submit(async move { bdev_create(&uri).await })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(|e| Status::from(e))
            .map(|name| {
                Ok(Response::new(CreateReply {
                    name,
                }))
            })?
    }

    #[instrument(level = "debug", err)]
    async fn destroy(&self, request: Request<BdevUri>) -> GrpcResult<Null> {
        let uri = request.into_inner().uri;

        let rx = rpc_submit(async move { bdev_destroy(&uri).await })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(|e| Status::from(e))
            .map(|_| Ok(Response::new(Null {})))?
    }

    #[instrument(level = "debug", err)]
    async fn share(
        &self,
        request: Request<BdevShareRequest>,
    ) -> Result<Response<BdevShareReply>, Status> {
        let r = request.into_inner();
        let name = r.name;
        let proto = r.proto;

        if Bdev::lookup_by_name(&name).is_none() {
            return Err(Status::not_found(name));
        }

        if proto != "iscsi" && proto != "nvmf" {
            return Err(Status::invalid_argument(proto));
        }
        let bdev_name = name.clone();
        let rx = match proto.as_str() {
            "nvmf" => rpc_submit(async move {
                let bdev = Bdev::lookup_by_name(&bdev_name).unwrap();
                bdev.share_nvmf(None)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            })?,

            "iscsi" => rpc_submit(async move {
                let bdev = Bdev::lookup_by_name(&bdev_name).unwrap();
                bdev.share_iscsi()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            })?,

            _ => unreachable!(),
        };

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map(|share| {
                let bdev = Bdev::lookup_by_name(&name).unwrap();
                Ok(Response::new(BdevShareReply {
                    uri: bdev.share_uri().unwrap_or(share),
                }))
            })?
    }

    #[instrument(level = "debug", err)]
    async fn unshare(&self, request: Request<CreateReply>) -> GrpcResult<Null> {
        sync_config(async {
            let name = request.into_inner().name;
            let hdl = Reactors::master().spawn_local(async move {
                let bdev = Bdev::lookup_by_name(&name).unwrap();
                let _ = bdev
                    .unshare()
                    .await
                    .map_err(|e| Status::internal(e.to_string()));
            });

            hdl.await;
            Ok(Response::new(Null {}))
        })
        .await
    }
}
