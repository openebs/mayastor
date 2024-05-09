use crate::{
    core::{ProtectedSubsystems, ResourceLockManager, Share},
    grpc::{
        acquire_subsystem_lock,
        rpc_submit,
        rpc_submit_ext,
        v1::pool::{PoolGrpc, PoolIdProbe, PoolSvcRpc},
        GrpcResult,
    },
    lvs::{BsError, Lvs, LvsError},
    pool_backend::{PoolArgs, PoolBackend},
};
use io_engine_api::v1::pool::*;
use std::{convert::TryFrom, fmt::Debug};
use tonic::{Request, Response, Status};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

/// An LVS Pool Service.
#[derive(Debug, Clone)]
pub(crate) struct PoolService {}

impl PoolService {
    /// Create a new `Self`.
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// Probe the LVS Pool service for a pool.
    pub(crate) async fn probe(
        probe: PoolIdProbe,
    ) -> Result<bool, tonic::Status> {
        let rx = rpc_submit_ext(async move {
            match probe {
                PoolIdProbe::Uuid(uuid) => Lvs::lookup_by_uuid(&uuid).is_some(),
                PoolIdProbe::UuidOrName(id) => {
                    Lvs::lookup_by_uuid(&id).is_some()
                        || Lvs::lookup(&id).is_some()
                }
                PoolIdProbe::NameUuid {
                    name,
                    uuid,
                } => match uuid {
                    Some(uuid) => match Lvs::lookup_by_uuid(&uuid) {
                        Some(pool) if pool.name() == name => true,
                        Some(_) => false,
                        None => false,
                    },
                    None => Lvs::lookup(&name).is_some(),
                },
            }
        })?;

        rx.await.map_err(|_| Status::cancelled("cancelled"))
    }

    pub(crate) async fn list_svc_pools(
        &self,
        args: &ListPoolOptions,
    ) -> Result<Vec<Pool>, tonic::Status> {
        let args = args.clone();

        let rx = rpc_submit::<_, _, LvsError>(async move {
            let mut pools = vec![];
            if let Some(name) = &args.name {
                if let Some(lvs) = Lvs::lookup(name) {
                    pools.push(lvs.into());
                }
            } else if let Some(uuid) = &args.uuid {
                if let Some(lvs) = Lvs::lookup_by_uuid(uuid) {
                    pools.push(lvs.into());
                }
            } else {
                pools.extend(Lvs::iter().map(|lvs| lvs.into()));
            }

            Ok(pools)
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
    }
}

async fn find_pool(
    name: &str,
    uuid: &Option<String>,
) -> Result<Lvs, tonic::Status> {
    let Some(pool) = Lvs::lookup(name) else {
        return Err(LvsError::PoolNotFound {
            source: BsError::LvsNotFound {},
            msg: format!("Pool {name} was not found"),
        }
        .into());
    };
    let pool_uuid = pool.uuid();
    if uuid.is_some() && uuid.as_ref() != Some(&pool_uuid) {
        return Err(LvsError::Invalid {
            source: BsError::LvsIdMismatch {},
            msg: format!(
                "invalid uuid {uuid:?}, found pool with uuid {pool_uuid}"
            ),
        }
        .into());
    }
    Ok(pool)
}

#[async_trait::async_trait]
impl PoolSvcRpc for PoolService {
    fn kind(&self) -> PoolBackend {
        PoolBackend::Lvs
    }
}

#[tonic::async_trait]
impl PoolRpc for PoolService {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        let args = PoolArgs::try_from(request.into_inner())?;
        crate::lvs_run!(async move {
            let pool_subsystem = ResourceLockManager::get_instance()
                .get_subsystem(ProtectedSubsystems::POOL);
            let _lock_guard =
                acquire_subsystem_lock(pool_subsystem, Some(&args.name))
                    .await?;
            let pool = Lvs::create_or_import(args).await?;
            Ok(Pool::from(pool))
        })
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();

        crate::lvs_run!(async move {
            let pool = find_pool(&args.name, &args.uuid).await?;
            PoolGrpc::new(pool).destroy().await
        })
    }

    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();

        crate::lvs_run!(async move {
            let pool = find_pool(&args.name, &args.uuid).await?;
            PoolGrpc::new(pool).export().await
        })
    }

    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        let args = PoolArgs::try_from(request.into_inner())?;

        crate::lvs_run!(async move {
            let pool = Lvs::import_from_args(args).await?;
            Ok(Pool::from(pool))
        })
    }

    async fn list_pools(
        &self,
        _request: Request<ListPoolOptions>,
    ) -> GrpcResult<ListPoolsResponse> {
        unimplemented!("Request is not cloneable, so we have to use another fn")
    }
}

impl From<Lvs> for Pool {
    fn from(l: Lvs) -> Self {
        Self {
            uuid: l.uuid(),
            name: l.name().into(),
            disks: vec![l
                .base_bdev()
                .bdev_uri_str()
                .unwrap_or_else(|| "".into())],
            state: PoolState::PoolOnline.into(),
            capacity: l.capacity(),
            used: l.used(),
            committed: l.committed(),
            pooltype: PoolType::Lvs as i32,
            cluster_size: l.blob_cluster_size() as u32,
        }
    }
}
