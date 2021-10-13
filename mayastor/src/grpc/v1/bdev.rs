use crate::{
    core::{Bdev, CoreError, Protocol, Share},
    grpc::{rpc_submit, GrpcResult},
    nexus_uri::{bdev_create, bdev_destroy, NexusBdevError},
};
use rpc::mayastor::v1::bdev::{
    BdevRequest,
    BdevResponse,
    BdevRpc,
    BdevShareRequest,
    BdevShareResponse,
    BdevUnshareRequest,
    Kind,
    ListBdevResponse,
    NullableString,
};
use std::convert::TryFrom;
use tonic::{Request, Response, Status};
use url::Url;

impl From<Bdev> for BdevResponse {
    fn from(b: crate::core::Bdev) -> Self {
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

/// RPC service for spdk bdev operations
#[derive(Debug)]
pub struct BdevService {}

impl BdevService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BdevService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl BdevRpc for BdevService {
    #[tracing::instrument(skip(self))]
    async fn list(
        &self,
        request: Request<NullableString>,
    ) -> GrpcResult<ListBdevResponse> {
        let rx = rpc_submit::<_, _, NexusBdevError>(async {
            let mut bdevs = Vec::new();
            let name = request.into_inner().kind;

            if let Some(Kind::Value(name)) = name {
                if let Some(bdev) = Bdev::lookup_by_name(&name) {
                    bdevs.push(bdev.into());
                }
            } else {
                Bdev::bdev_first()
                    .into_iter()
                    .for_each(|bdev| bdevs.push(bdev.into()));
            }

            Ok(ListBdevResponse {
                bdevs,
            })
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    #[tracing::instrument(skip(self))]
    async fn create(
        &self,
        request: Request<BdevRequest>,
    ) -> Result<Response<BdevResponse>, Status> {
        let uri = request.into_inner().uri;

        let rx = rpc_submit(async move {
            let name = bdev_create(&uri).await?;

            if let Some(bdev) = Bdev::lookup_by_name(&name) {
                Ok(Response::new(bdev.into()))
            } else {
                Err(NexusBdevError::BdevNotFound {
                    name,
                })
            }
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
    }

    #[tracing::instrument(skip(self))]
    async fn destroy(&self, request: Request<BdevRequest>) -> GrpcResult<()> {
        let uri = request.into_inner().uri;

        let rx = rpc_submit(async move { bdev_destroy(&uri).await })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(|_| Ok(Response::new(())))?
    }

    #[tracing::instrument(skip(self))]
    async fn share(
        &self,
        request: Request<BdevShareRequest>,
    ) -> Result<Response<BdevShareResponse>, Status> {
        let r = request.into_inner();
        let name = r.name;
        let protocol = r.protocol;

        if Bdev::lookup_by_name(&name).is_none() {
            return Err(Status::not_found(name));
        }

        let bdev_name = name.clone();
        let rx = match Protocol::try_from(protocol) {
            Ok(Protocol::Nvmf) => {
                rpc_submit::<_, String, CoreError>(async move {
                    let bdev = Bdev::lookup_by_name(&bdev_name).unwrap();
                    let share = bdev.share_nvmf(None).await?;
                    let bdev = Bdev::lookup_by_name(&name).unwrap();
                    Ok(bdev.share_uri().unwrap_or(share))
                })
            }

            Ok(Protocol::Iscsi) => {
                rpc_submit::<_, String, CoreError>(async move {
                    let bdev = Bdev::lookup_by_name(&bdev_name).unwrap();
                    let share = bdev.share_iscsi().await?;
                    let bdev = Bdev::lookup_by_name(&name).unwrap();
                    Ok(bdev.share_uri().unwrap_or(share))
                })
            }

            Err(_) => {
                return Err(Status::invalid_argument(protocol.to_string()))
            }

            _ => unreachable!(),
        }?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(|e| Status::internal(e.to_string()))
            .map(|uri| {
                Ok(Response::new(BdevShareResponse {
                    uri,
                }))
            })?
    }

    #[tracing::instrument(skip(self))]
    async fn unshare(
        &self,
        request: Request<BdevUnshareRequest>,
    ) -> GrpcResult<()> {
        let rx = rpc_submit::<_, _, CoreError>(async {
            let name = request.into_inner().name;
            if let Some(bdev) = Bdev::lookup_by_name(&name) {
                let _ = bdev.unshare().await?;
            }
            Ok(())
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }
}
