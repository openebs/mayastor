use crate::{
    core::{MayastorFeatures, ProtectedSubsystems, ResourceLockManager},
    grpc::{
        acquire_subsystem_lock,
        lvm_enabled,
        v1::pool::PoolProbe,
        GrpcResult,
    },
    lvm::{CmnQueryArgs, Error as LvmError, VolumeGroup},
    lvs::Lvs,
    pool_backend::PoolArgs,
};
use io_engine_api::v1::pool::*;
use std::{convert::TryFrom, fmt::Debug};
use tonic::{Request, Response, Status};

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

/// An LVM Pool Service.
#[derive(Debug, Clone)]
pub(crate) struct PoolService {}

impl PoolService {
    /// Create a new `Self`.
    pub(crate) fn new() -> Self {
        Self {}
    }
    /// Probe the LVM Pool service for a pool.
    pub(crate) async fn probe(
        probe: &PoolProbe,
    ) -> Result<bool, tonic::Status> {
        if !MayastorFeatures::get_features().lvm() {
            return Ok(false);
        }

        let query = match probe {
            PoolProbe::Uuid(uuid) => CmnQueryArgs::ours().uuid(uuid),
            PoolProbe::UuidOrName(uuid) => CmnQueryArgs::ours().uuid(uuid),
            PoolProbe::NameUuid {
                name,
                uuid,
            } => CmnQueryArgs::ours().named(name).uuid_opt(uuid),
        };
        match VolumeGroup::lookup(query).await {
            Ok(_) => Ok(true),
            Err(LvmError::NotFound {
                ..
            }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) async fn list_svc_pools(
        &self,
        args: &ListPoolOptions,
    ) -> Result<Vec<Pool>, tonic::Status> {
        if !MayastorFeatures::get_features().lvm() {
            return Ok(vec![]);
        }

        let pools = VolumeGroup::list(
            &CmnQueryArgs::ours()
                .named_opt(&args.name)
                .uuid_opt(&args.uuid),
        )
        .await?;
        Ok(pools.into_iter().map(Into::into).collect())
    }
}

#[tonic::async_trait]
impl PoolRpc for PoolService {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> GrpcResult<Pool> {
        lvm_enabled()?;

        let args = PoolArgs::try_from(request.into_inner())?;

        let pool_subsystem = ResourceLockManager::get_instance()
            .get_subsystem(ProtectedSubsystems::POOL);
        let _lock_guard =
            acquire_subsystem_lock(pool_subsystem, Some(&args.name)).await?;

        // bail if an lvs pool already exists with the same name
        if let Some(_pool) = Lvs::lookup(args.name.as_str()) {
            return Err(Status::invalid_argument(
                "lvs pool with the same name already exists",
            ));
        };
        // check if the disks are used by existing lvs pool
        if Lvs::iter()
            .map(|l| l.base_bdev().name().to_string())
            .any(|d| args.disks.contains(&d))
        {
            return Err(Status::invalid_argument(
                "an lvs pool already uses the disk",
            ));
        };
        VolumeGroup::create(args)
            .await
            .map_err(Status::from)
            .map(Pool::from)
            .map(Response::new)
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> GrpcResult<()> {
        lvm_enabled()?;

        let args = request.into_inner();
        let pool = VolumeGroup::lookup(
            CmnQueryArgs::ours().named(&args.name).uuid_opt(&args.uuid),
        )
        .await?;
        pool.destroy()
            .await
            .map_err(Status::from)
            .map(Response::new)
    }

    async fn export_pool(
        &self,
        request: Request<ExportPoolRequest>,
    ) -> GrpcResult<()> {
        lvm_enabled()?;

        let args = request.into_inner();
        let mut pool = VolumeGroup::lookup(
            CmnQueryArgs::ours().named(&args.name).uuid_opt(&args.uuid),
        )
        .await?;
        pool.export().await?;
        return Ok(Response::new(()));
    }

    async fn import_pool(
        &self,
        request: Request<ImportPoolRequest>,
    ) -> GrpcResult<Pool> {
        lvm_enabled()?;

        let args = PoolArgs::try_from(request.into_inner())?;

        // bail if an lvs pool already exists with the same name
        if let Some(_pool) = Lvs::lookup(args.name.as_str()) {
            return Err(Status::invalid_argument(
                "lvs pool with the same name already exists",
            ));
        };
        // check if the disks are used by existing lvs pool
        if Lvs::iter()
            .map(|l| l.base_bdev().name().to_string())
            .any(|d| args.disks.contains(&d))
        {
            return Err(Status::invalid_argument(
                "an lvs pool already uses the disk",
            ));
        };
        VolumeGroup::import(args)
            .await
            .map_err(Status::from)
            .map(Pool::from)
            .map(Response::new)
    }

    async fn list_pools(
        &self,
        _request: Request<ListPoolOptions>,
    ) -> GrpcResult<ListPoolsResponse> {
        unimplemented!("Request is not cloneable, so we have to use another fn")
    }
}

impl From<LvmError> for Status {
    fn from(e: LvmError) -> Self {
        match e {
            LvmError::InvalidPoolType {
                ..
            }
            | LvmError::VgUuidSet {
                ..
            }
            | LvmError::DisksMismatch {
                ..
            } => Status::invalid_argument(e.to_string()),
            LvmError::NotFound {
                ..
            }
            | LvmError::LvNotFound {
                ..
            } => Status::not_found(e.to_string()),
            LvmError::NoSpace {
                ..
            } => Status::resource_exhausted(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}
impl From<VolumeGroup> for Pool {
    fn from(v: VolumeGroup) -> Self {
        Self {
            uuid: v.uuid().to_string(),
            name: v.name().into(),
            disks: v.disks(),
            state: PoolState::PoolOnline.into(),
            capacity: v.capacity(),
            used: v.used(),
            pooltype: PoolType::Lvm as i32,
            committed: v.committed(),
            cluster_size: v.cluster_size() as u32,
        }
    }
}
