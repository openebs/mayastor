//!
//! RPC methods as they are implemented for MOAC.

use crate::{
    core::{Bdev, CoreError, Protocol, Share},
    grpc::{rpc_call, GrpcResult},
    lvs::{Error as LvsError, Error, Lvol, Lvs},
    nexus_uri::NexusBdevError,
};
use nix::errno::Errno;
use rpc::mayastor::{
    CreatePoolRequest,
    CreateReplicaRequest,
    DestroyPoolRequest,
    DestroyReplicaRequest,
    ListPoolsReply,
    ListReplicasReply,
    Null,
    Pool,
    PoolState,
    Replica,
    ShareReplicaReply,
    ShareReplicaRequest,
};
use std::convert::TryFrom;
use tonic::{Response, Status};
use tracing::instrument;

impl From<LvsError> for Status {
    fn from(e: LvsError) -> Self {
        match e {
            Error::Import {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::Create {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::Invalid {
                ..
            } => Status::invalid_argument(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl From<Protocol> for i32 {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::None => 0,
            Protocol::Nvmf => 1,
            Protocol::Iscsi => 2,
        }
    }
}

impl From<Lvs> for Pool {
    fn from(l: Lvs) -> Self {
        Self {
            name: l.name().into(),
            disks: vec![l.base_bdev().bdev_uri().unwrap_or_else(|| "".into())],
            state: PoolState::PoolOnline.into(),
            capacity: l.capacity(),
            used: l.used(),
        }
    }
}

impl From<Lvol> for Replica {
    fn from(l: Lvol) -> Self {
        Self {
            uuid: l.name(),
            pool: l.pool(),
            thin: l.is_thin(),
            size: l.size(),
            share: if l.shared().is_none() {
                Protocol::None.into()
            } else {
                l.shared().unwrap().into()
            },
            uri: l
                .share_uri()
                .unwrap_or_else(|| format!("bdev:///{}", l.name())),
        }
    }
}
/// create a pool to that can be used to provision replicas.
///
/// This method should be idempotent if the pool exists. To validate
/// this we check the name of the pool and base_bdevs
#[instrument(level = "debug", err)]
pub async fn create(args: CreatePoolRequest) -> GrpcResult<Pool> {
    rpc_call(Lvs::create_or_import(args))
}

/// Destroy a pool; this method name is somewhat misleading. It does not
/// destroy the pool rather it exports the pool. If the pool does not
/// exist; it returns OK.
#[instrument(level = "debug", err)]
pub async fn destroy(args: DestroyPoolRequest) -> GrpcResult<Null> {
    if let Some(pool) = Lvs::lookup(&args.name) {
        rpc_call(pool.destroy())
    } else {
        Ok(Response::new(Null {}))
    }
}

/// list all the pools found within this instance
pub fn list() -> GrpcResult<ListPoolsReply> {
    Ok(Response::new(ListPoolsReply {
        pools: Lvs::iter().map(|l| l.into()).collect::<Vec<Pool>>(),
    }))
}

/// create a replica on the given pool returns an Error if the lvol already
/// exists
#[instrument(level = "debug", err)]
pub async fn create_replica(args: CreateReplicaRequest) -> GrpcResult<Replica> {
    if Lvs::lookup(&args.pool).is_none() {
        return Err(Status::not_found(args.pool));
    }

    if let Some(b) = Bdev::lookup_by_name(&args.uuid) {
        let lvol = Lvol::try_from(b)?;
        return Ok(Response::new(Replica::from(lvol)));
    }

    rpc_call(async move {
        let p = Lvs::lookup(&args.pool).unwrap();
        match p.create_lvol(&args.uuid, args.size, false).await {
            Ok(lvol) => match lvol.share_nvmf().await {
                Ok(s) => {
                    debug!("created and shared {} as {}", lvol, s);
                    Ok(lvol)
                }
                Err(e) => {
                    debug!(
                        "failed to share created lvol {}: {} .. destroying",
                        lvol,
                        e.to_string()
                    );
                    let _ = lvol.destroy().await;
                    Err(e)
                }
            },
            Err(e) => Err(e),
        }
    })
}

/// destroy the replica on the given pool, returning OK if the replica was
/// not found
#[instrument(level = "debug", err)]
pub async fn destroy_replica(args: DestroyReplicaRequest) -> GrpcResult<Null> {
    rpc_call(async move {
        match Bdev::lookup_by_name(&args.uuid) {
            Some(b) => {
                let lvol = Lvol::try_from(b)?;
                lvol.destroy().await.map(|_r| Null {})
            }
            None => Ok(Null {}),
        }
    })
}

/// list all the replicas
#[instrument(level = "debug", err)]
pub fn list_replicas() -> GrpcResult<ListReplicasReply> {
    let mut replicas = Vec::new();
    if let Some(bdev) = Bdev::bdev_first() {
        replicas = bdev
            .into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Replica::from(Lvol::try_from(b).unwrap()))
            .collect::<Vec<_>>();
    }

    Ok(Response::new(ListReplicasReply {
        replicas,
    }))
}

/// shares the replica over nvmf -- replicas are always shared over nvmf if
/// already shared returns OK.
///
/// There is no unshare RPC in mayastor_svc
#[instrument(level = "debug", err)]
pub async fn share_replica(
    args: ShareReplicaRequest,
) -> GrpcResult<ShareReplicaReply> {
    rpc_call(async move {
        if let Some(b) = Bdev::lookup_by_name(&args.uuid) {
            let lvol = Lvol::try_from(b)?;

            if lvol.shared().is_some() && args.share == 1 {
                return Ok(ShareReplicaReply {
                    uri: lvol.share_uri().unwrap(),
                });
            }
            match args.share {
                0 => lvol.unshare().await.map(|_| ShareReplicaReply {
                    uri: format!("bdev:///{}", lvol.name()),
                }),

                1 => lvol.share_nvmf().await.map(|_| ShareReplicaReply {
                    uri: lvol.share_uri().unwrap(),
                }),
                _ => Err(LvsError::LvolShare {
                    source: CoreError::NotSupported {
                        source: Errno::ENOSYS,
                    },
                    msg: "protocol not supported for lvols".to_string(),
                }),
            }
        } else {
            Err(LvsError::InvalidBdev {
                source: NexusBdevError::BdevNotFound {
                    name: args.uuid,
                },
                msg: "no such lvol to share".to_string(),
            })
        }
    })
}
