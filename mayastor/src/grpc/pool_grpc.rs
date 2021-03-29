//!
//! RPC methods as they are implemented for MOAC.

use std::convert::TryFrom;

use nix::errno::Errno;
use tonic::{Response, Status};
use tracing::instrument;

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
    ReplicaStats,
    ShareReplicaReply,
    ShareReplicaRequest,
    StatReplicasReply,
    Stats,
};

use crate::{
    core::{Bdev, BlockDeviceIoStats, CoreError, Protocol, Share},
    grpc::{rpc_call, GrpcResult},
    lvs::{Error as LvsError, Error, Lvol, Lvs},
    nexus_uri::NexusBdevError,
};

impl From<LvsError> for Status {
    fn from(e: LvsError) -> Self {
        match e {
            Error::Import {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::Create {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::Destroy {
                source, ..
            } => source.into(),
            Error::Invalid {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidBdev {
                source, ..
            } => source.into(),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl From<Protocol> for i32 {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::Off => 0,
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

impl From<BlockDeviceIoStats> for Stats {
    fn from(b: BlockDeviceIoStats) -> Self {
        Self {
            num_read_ops: b.num_read_ops,
            num_write_ops: b.num_write_ops,
            bytes_read: b.bytes_read,
            bytes_written: b.bytes_written,
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
            share: l.shared().unwrap().into(),
            uri: l.share_uri().unwrap(),
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

/// Destroy a pool; and deletes all lvols
/// If the pool does not exist; it returns OK.
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

/// create a replica on the given pool returns an OK if the lvol already
/// exist. If replica fails to share, it will be destroyed prior to returning
/// an error.
#[instrument(level = "debug", err)]
pub async fn create_replica(args: CreateReplicaRequest) -> GrpcResult<Replica> {
    if Lvs::lookup(&args.pool).is_none() {
        return Err(Status::not_found(args.pool));
    }

    if let Some(b) = Bdev::lookup_by_name(&args.uuid) {
        let lvol = Lvol::try_from(b)?;
        return Ok(Response::new(Replica::from(lvol)));
    }

    if !matches!(
        Protocol::try_from(args.share)?,
        Protocol::Off | Protocol::Nvmf
    ) {
        return Err(Status::invalid_argument(format!(
            "invalid protocol {}",
            args.share
        )));
    }

    rpc_call(async move {
        let p = Lvs::lookup(&args.pool).unwrap();
        match p.create_lvol(&args.uuid, args.size, false).await {
            Ok(lvol) if Protocol::try_from(args.share)? == Protocol::Nvmf => {
                match lvol.share_nvmf(None).await {
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
                }
            }
            Ok(lvol) => {
                debug!("created lvol {}", lvol);
                Ok(lvol)
            }
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

            // if we are already shared return OK
            if lvol.shared() == Some(Protocol::try_from(args.share)?) {
                return Ok(ShareReplicaReply {
                    uri: lvol.share_uri().unwrap(),
                });
            }
            match Protocol::try_from(args.share)? {
                Protocol::Off => {
                    lvol.unshare().await.map(|_| ShareReplicaReply {
                        uri: format!("bdev:///{}", lvol.name()),
                    })
                }

                Protocol::Nvmf => {
                    lvol.share_nvmf(None).await.map(|_| ShareReplicaReply {
                        uri: lvol.share_uri().unwrap(),
                    })
                }
                Protocol::Iscsi => Err(LvsError::LvolShare {
                    source: CoreError::NotSupported {
                        source: Errno::ENOSYS,
                    },
                    name: args.uuid,
                }),
            }
        } else {
            Err(LvsError::InvalidBdev {
                source: NexusBdevError::BdevNotFound {
                    name: args.uuid.clone(),
                },
                name: args.uuid,
            })
        }
    })
}

/// get the stats of replica's (lvol's only)
#[instrument(level = "debug", err)]
pub async fn stat_replica() -> GrpcResult<StatReplicasReply> {
    rpc_call::<_, _, LvsError, _>(async {
        let mut lvols = Vec::new();
        if let Some(bdev) = Bdev::bdev_first() {
            bdev.into_iter()
                .filter(|b| b.driver() == "lvol")
                .for_each(|b| lvols.push(Lvol::try_from(b).unwrap()))
        }

        let mut replicas = Vec::new();
        for l in lvols {
            let stats = l.as_bdev().stats().await;
            if stats.is_err() {
                error!("failed to get stats for lvol: {}", l);
            }

            replicas.push(ReplicaStats {
                uuid: l.name(),
                pool: l.pool(),
                stats: stats.ok().map(Stats::from),
            });
        }

        Ok(StatReplicasReply {
            replicas,
        })
    })
}
