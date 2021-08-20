use crate::{
    core::{ProtectedSubsystems, ResourceLockManager, Share},
    grpc::{
        acquire_subsystem_lock,
        lvm_enabled,
        rpc_submit,
        rpc_submit_ext,
        v1::pool::PoolProbe,
        GrpcResult,
    },
    lvs::{Error as LvsError, Lvs},
    pool_backend::PoolArgs,
};
use io_engine_api::v1::pool::*;
use nix::errno::Errno;
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
    pub(crate) async fn probe(probe: PoolProbe) -> Result<bool, tonic::Status> {
        let rx = rpc_submit_ext(async move {
            match probe {
                PoolProbe::Uuid(uuid) => Lvs::lookup_by_uuid(&uuid).is_some(),
                PoolProbe::UuidOrName(id) => {
                    Lvs::lookup_by_uuid(&id).is_some()
                        || Lvs::lookup(&id).is_some()
                }
                PoolProbe::NameUuid {
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

#[tonic::async_trait]
impl PoolRpc for PoolService {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        let args = PoolArgs::try_from(request.into_inner())?;
        let rx = rpc_submit::<_, _, LvsError>(async move {
            let pool_subsystem = ResourceLockManager::get_instance()
                .get_subsystem(ProtectedSubsystems::POOL);
            let _lock_guard =
                acquire_subsystem_lock(pool_subsystem, Some(&args.name))
                    .await
                    .map_err(|_| LvsError::ResourceLockFailed {
                        msg: format!(
                            "resource {}, for disk pool {:?}",
                            &args.name, &args.disks,
                        ),
                    })?;
            let pool = Lvs::create_or_import(args).await?;
            Ok(Pool::from(pool))
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, LvsError>(async move {
            let Some(pool) = Lvs::lookup(&args.name) else {
                return Err(LvsError::PoolNotFound {
                    source: Errno::ENOMEDIUM,
                    msg: format!(
                        "Destroy failed as pool {} was not found",
                        args.name,
                    ),
                });
            };
            if args.uuid.is_some() && args.uuid != Some(pool.uuid()) {
                return Err(LvsError::Invalid {
                    source: Errno::EINVAL,
                    msg: format!(
                        "invalid uuid {}, found pool with uuid {}",
                        args.uuid.unwrap(),
                        pool.uuid(),
                    ),
                });
            }
            pool.destroy().await
        })?;
        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        let args = request.into_inner();
        let rx = rpc_submit::<_, _, LvsError>(async move {
            if let Some(pool) = Lvs::lookup(&args.name) {
                if args.uuid.is_some() && args.uuid != Some(pool.uuid()) {
                    return Err(LvsError::Invalid {
                        source: Errno::EINVAL,
                        msg: format!(
                            "invalid uuid {}, found pool with uuid {}",
                            args.uuid.unwrap(),
                            pool.uuid(),
                        ),
                    });
                }
                pool.export().await?;
            } else {
                return Err(LvsError::Invalid {
                    source: Errno::EINVAL,
                    msg: format!("pool {} not found", args.name),
                });
            }
            Ok(())
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        lvm_enabled()?;
        let args = PoolArgs::try_from(request.into_inner())?;

        let rx = rpc_submit::<_, _, LvsError>(async move {
            let pool = Lvs::import_from_args(args).await?;
            Ok(Pool::from(pool))
        })?;

        rx.await
            .map_err(|_| Status::cancelled("cancelled"))?
            .map_err(Status::from)
            .map(Response::new)
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
