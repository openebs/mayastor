use tonic::{Request, Response, Status};
use tracing::instrument;

use std::{convert::TryFrom, pin::Pin};
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
    core::{CoreError, Share, UntypedBdev},
    grpc::{rpc_submit, GrpcResult},
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
};

impl From<UntypedBdev> for RpcBdev {
    fn from(b: UntypedBdev) -> Self {
        Self {
            name: b.name().to_string(),
            uuid: b.uuid_as_string(),
            num_blocks: b.num_blocks(),
            blk_size: b.block_len(),
            claimed: b.is_claimed(),
            claimed_by: b.claimed_by().unwrap_or_else(|| "Orphaned".into()),
            aliases: b.aliases().join(","),
            product_name: b.product_name().to_string(),
            share_uri: b.share_uri().unwrap_or_else(|| "".into()),
            uri: Url::try_from(b).map_or("".into(), |u| u.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct BdevSvc {}

impl BdevSvc {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BdevSvc {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl BdevRpc for BdevSvc {
    #[instrument(level = "debug", err)]
    async fn list(&self, _request: Request<Null>) -> GrpcResult<Bdevs> {
        let rx = rpc_submit::<_, _, NexusBdevError>(async {
            let mut list: Vec<RpcBdev> = Vec::new();
            if let Some(bdev) = UntypedBdev::bdev_first() {
                bdev.into_iter().for_each(|bdev| list.push(bdev.into()))
            }

            Ok(Bdevs {
                bdevs: list,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
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
            .map_err(Status::from)
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
            .map_err(Status::from)
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

        if UntypedBdev::lookup_by_name(&name).is_none() {
            return Err(Status::not_found(name));
        }

        if proto != "nvmf" {
            return Err(Status::invalid_argument(proto));
        }
        let bdev_name = name.clone();
        let rx = match proto.as_str() {
            "nvmf" => rpc_submit::<_, String, CoreError>(async move {
                let mut bdev = UntypedBdev::lookup_by_name(&bdev_name).unwrap();
                let share = Pin::new(&mut bdev).share_nvmf(None).await?;
                let bdev = UntypedBdev::lookup_by_name(&name).unwrap();
                Ok(bdev.share_uri().unwrap_or(share))
            }),

            _ => unreachable!(),
        }?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(|e| Status::internal(e.to_string()))
            .map(|uri| {
                Ok(Response::new(BdevShareReply {
                    uri,
                }))
            })?
    }

    #[instrument(level = "debug", err)]
    async fn unshare(&self, request: Request<CreateReply>) -> GrpcResult<Null> {
        let rx = rpc_submit::<_, _, CoreError>(async {
            let name = request.into_inner().name;
            if let Some(mut bdev) = UntypedBdev::lookup_by_name(&name) {
                let _ = Pin::new(&mut bdev).unshare().await?;
            }
            Ok(Null {})
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }
}
