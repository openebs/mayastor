//! The high-level replica json-rpc methods.
//!
//! Replica is a logical data volume exported over nvmf (in SPDK terminology
//! an lvol). Here we define methods for easy management of replicas.

use std::ffi::{c_void, CStr, CString};

use futures::{
    channel::oneshot,
    future::{self, FutureExt},
};
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};

use rpc::mayastor::{
    CreateReplicaReply,
    CreateReplicaRequest,
    DestroyReplicaRequest,
    ListReplicasReply,
    Replica as ReplicaJson,
    ReplicaStats,
    ShareProtocolReplica,
    ShareReplicaReply,
    ShareReplicaRequest,
    StatReplicasReply,
    Stats,
};
use spdk_sys::{
    spdk_lvol,
    vbdev_lvol_create,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
    LVOL_CLEAR_WITH_UNMAP,
    LVOL_CLEAR_WITH_WRITE_ZEROES,
    SPDK_BDEV_IO_TYPE_UNMAP,
};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, errno_result_from_i32, ErrnoResult},
    jsonrpc::{jsonrpc_register, Code, RpcErrorCode},
    pool::Pool,
    target,
};

/// These are high-level context errors one for each rpc method.
#[derive(Debug, Snafu)]
pub enum RpcError {
    #[snafu(display("Failed to create replica {}", uuid))]
    CreateReplica { source: Error, uuid: String },
    #[snafu(display("Failed to destroy replica {}", uuid))]
    DestroyReplica { source: Error, uuid: String },
    #[snafu(display("Failed to (un)share replica {}", uuid))]
    ShareReplica { source: Error, uuid: String },
}

impl RpcErrorCode for RpcError {
    fn rpc_error_code(&self) -> Code {
        match self {
            RpcError::CreateReplica {
                source, ..
            } => source.rpc_error_code(),
            RpcError::DestroyReplica {
                source, ..
            } => source.rpc_error_code(),
            RpcError::ShareReplica {
                source, ..
            } => source.rpc_error_code(),
        }
    }
}

impl From<RpcError> for tonic::Status {
    fn from(e: RpcError) -> Self {
        match e {
            RpcError::CreateReplica {
                source, ..
            } => Self::from(source),
            RpcError::DestroyReplica {
                source, ..
            } => Self::from(source),
            RpcError::ShareReplica {
                source, ..
            } => Self::from(source),
        }
    }
}

// Replica errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The pool \"{}\" does not exist", pool))]
    PoolNotFound { pool: String },
    #[snafu(display("Replica already exists"))]
    ReplicaExists {},
    #[snafu(display("Invalid parameters"))]
    InvalidParams {},
    #[snafu(display("Failed to create lvol"))]
    CreateLvol { source: Errno },
    #[snafu(display("Failed to destroy lvol"))]
    DestroyLvol { source: Errno },
    #[snafu(display("Replica has been already shared"))]
    ReplicaShared {},
    #[snafu(display("share nvmf"))]
    ShareNvmf { source: target::nvmf::Error },
    #[snafu(display("share iscsi"))]
    ShareIscsi { source: target::iscsi::Error },
    #[snafu(display("unshare nvmf"))]
    UnshareNvmf { source: target::nvmf::Error },
    #[snafu(display("unshare iscsi"))]
    UnshareIscsi { source: target::iscsi::Error },
    #[snafu(display("Invalid share protocol {} in request", protocol))]
    InvalidProtocol { protocol: i32 },
    #[snafu(display("Replica does not exist"))]
    ReplicaNotFound {},
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        match self {
            Error::PoolNotFound {
                ..
            } => Code::NotFound,
            Error::ReplicaNotFound {
                ..
            } => Code::NotFound,
            Error::ReplicaExists {
                ..
            } => Code::AlreadyExists,
            Error::InvalidParams {
                ..
            } => Code::InvalidParams,
            Error::CreateLvol {
                ..
            } => Code::InvalidParams,
            Error::InvalidProtocol {
                ..
            } => Code::InvalidParams,
            Error::ShareNvmf {
                source, ..
            } => source.rpc_error_code(),
            Error::ShareIscsi {
                source, ..
            } => source.rpc_error_code(),
            Error::UnshareNvmf {
                source, ..
            } => source.rpc_error_code(),
            Error::UnshareIscsi {
                source, ..
            } => source.rpc_error_code(),
            _ => Code::InternalError,
        }
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::PoolNotFound {
                ..
            } => Self::not_found(e.to_string()),
            Error::ReplicaExists {
                ..
            } => Self::already_exists(e.to_string()),
            Error::InvalidParams {
                ..
            } => Self::invalid_argument(e.to_string()),
            Error::CreateLvol {
                ..
            } => Self::invalid_argument(e.to_string()),
            Error::DestroyLvol {
                ..
            } => Self::internal(e.to_string()),
            Error::ReplicaShared {
                ..
            } => Self::internal(e.to_string()),
            Error::ShareNvmf {
                ..
            } => Self::internal(e.to_string()),
            Error::ShareIscsi {
                ..
            } => Self::internal(e.to_string()),
            Error::UnshareNvmf {
                ..
            } => Self::internal(e.to_string()),
            Error::UnshareIscsi {
                ..
            } => Self::internal(e.to_string()),
            Error::InvalidProtocol {
                ..
            } => Self::invalid_argument(e.to_string()),
            Error::ReplicaNotFound {
                ..
            } => Self::not_found(e.to_string()),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Structure representing a replica which is basically SPDK lvol.
///
/// Note about safety: The structure wraps raw C pointer from SPDK.
/// It is safe to use only in synchronous context. If you keep Replica for
/// longer than that then something else can run on reactor_0 inbetween
/// which may destroy the replica and invalidate the pointer!
pub struct Replica {
    lvol_ptr: *mut spdk_lvol,
}

/// Types of remote access storage protocols and IDs for sharing replicas.
pub enum ShareType {
    Nvmf,
    Iscsi,
}

/// Detect share protocol (if any) for replica with given uuid and share ID
/// string.
fn detect_share(uuid: &str) -> Option<(ShareType, String)> {
    // first try nvmf and then try iscsi
    match target::nvmf::get_uri(uuid) {
        Some(uri) => Some((ShareType::Nvmf, uri)),
        None => match target::iscsi::get_uri(target::Side::Replica, uuid) {
            Some(uri) => Some((ShareType::Iscsi, uri)),
            None => None,
        },
    }
}

impl Replica {
    /// Create replica on storage pool.
    pub async fn create(
        uuid: &str,
        pool: &str,
        size: u64,
        thin: bool,
    ) -> Result<Self> {
        let pool = match Pool::lookup(pool) {
            Some(p) => p,
            None => {
                return Err(Error::PoolNotFound {
                    pool: pool.to_owned(),
                })
            }
        };
        let clear_method = if pool
            .get_base_bdev()
            .io_type_supported(SPDK_BDEV_IO_TYPE_UNMAP)
        {
            LVOL_CLEAR_WITH_UNMAP
        } else {
            LVOL_CLEAR_WITH_WRITE_ZEROES
        };

        if Self::lookup(uuid).is_some() {
            return Err(Error::ReplicaExists {});
        }
        let c_uuid = CString::new(uuid).unwrap();
        let (sender, receiver) =
            oneshot::channel::<ErrnoResult<*mut spdk_lvol>>();
        let rc = unsafe {
            vbdev_lvol_create(
                pool.as_ptr(),
                c_uuid.as_ptr(),
                size,
                thin,
                clear_method,
                Some(Self::replica_done_cb),
                cb_arg(sender),
            )
        };
        if rc != 0 {
            // XXX sender is leaked
            return Err(Error::InvalidParams {});
        }

        let lvol_ptr = receiver
            .await
            .expect("Cancellation is not supported")
            .context(CreateLvol {})?;

        info!("Created replica {} on pool {}", uuid, pool.get_name());
        Ok(Self {
            lvol_ptr,
        })
    }

    /// Lookup replica by uuid (=name).
    pub fn lookup(uuid: &str) -> Option<Self> {
        match Bdev::lookup_by_name(uuid) {
            Some(bdev) => {
                let lvol = unsafe { vbdev_lvol_get_from_bdev(bdev.as_ptr()) };
                if lvol.is_null() {
                    None
                } else {
                    Some(Self {
                        lvol_ptr: lvol,
                    })
                }
            }
            None => None,
        }
    }

    /// Destroy replica. Consumes the "self" so after calling this method self
    /// can't be used anymore. If the replica is shared, it is unshared before
    /// the destruction.
    //
    // TODO: Error value should contain self so that it can be used when
    // destroy fails.
    pub async fn destroy(self) -> Result<()> {
        self.unshare().await?;

        let uuid = self.get_uuid();
        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
        unsafe {
            vbdev_lvol_destroy(
                self.lvol_ptr,
                Some(done_errno_cb),
                cb_arg(sender),
            );
        }

        receiver
            .await
            .expect("Cancellation is not supported")
            .context(DestroyLvol {})?;

        info!("Destroyed replica {}", uuid);
        Ok(())
    }

    /// Expose replica over supported remote access storage protocols (nvmf
    /// and iscsi).
    pub async fn share(&self, kind: ShareType) -> Result<()> {
        let uuid = self.get_uuid().to_owned();
        if detect_share(&uuid).is_some() {
            return Err(Error::ReplicaShared {});
        }

        let bdev = unsafe { Bdev::from((*self.lvol_ptr).bdev) };

        match kind {
            ShareType::Nvmf => target::nvmf::share(&uuid, &bdev)
                .await
                .context(ShareNvmf {})?,
            ShareType::Iscsi => {
                target::iscsi::share(&uuid, &bdev, target::Side::Replica)
                    .context(ShareIscsi {})?
            }
        }
        Ok(())
    }

    /// The opposite of share. It is not an error to call unshare on a replica
    /// which is not shared.
    pub async fn unshare(&self) -> Result<()> {
        let uuid = self.get_uuid().to_owned();
        if let Some((share_type, _)) = detect_share(&uuid) {
            match share_type {
                ShareType::Nvmf => target::nvmf::unshare(&uuid)
                    .await
                    .context(UnshareNvmf {})?,
                ShareType::Iscsi => target::iscsi::unshare(&uuid)
                    .await
                    .context(UnshareIscsi {})?,
            }
        };
        Ok(())
    }

    /// Return either a type of share and a string identifying the share
    /// (nqn for nvmf and iqn for iscsi) or none if the replica is not
    /// shared.
    pub fn get_share_type(&self) -> Option<ShareType> {
        detect_share(self.get_uuid()).map(|val| val.0)
    }

    /// Return storage URI understood & used by nexus to access the replica.
    pub fn get_share_uri(&self) -> String {
        match detect_share(self.get_uuid()) {
            Some((_, share_uri)) => share_uri,
            None => format!("bdev:///{}", self.get_uuid()),
        }
    }

    /// Get size of the replica in bytes.
    pub fn get_size(&self) -> u64 {
        let bdev: Bdev = unsafe { (*self.lvol_ptr).bdev.into() };
        u64::from(bdev.block_len()) * bdev.num_blocks()
    }

    /// Get name of the pool which replica belongs to.
    pub fn get_pool_name(&self) -> &str {
        unsafe {
            let lvs = &*(*self.lvol_ptr).lvol_store;
            CStr::from_ptr(&lvs.name as *const i8).to_str().unwrap()
        }
    }

    /// Get uuid (= name) of the replica.
    pub fn get_uuid(&self) -> &str {
        unsafe {
            CStr::from_ptr(&(*self.lvol_ptr).name as *const i8)
                .to_str()
                .unwrap()
        }
    }

    /// Return if replica has been thin provisioned.
    pub fn is_thin(&self) -> bool {
        unsafe { (*self.lvol_ptr).thin_provision }
    }

    /// Return raw pointer to lvol (C struct spdk_lvol).
    pub fn as_ptr(&self) -> *mut spdk_lvol {
        self.lvol_ptr
    }

    /// Callback called from SPDK for replica create method.
    extern "C" fn replica_done_cb(
        sender_ptr: *mut c_void,
        lvol_ptr: *mut spdk_lvol,
        errno: i32,
    ) {
        let sender = unsafe {
            Box::from_raw(
                sender_ptr as *mut oneshot::Sender<ErrnoResult<*mut spdk_lvol>>,
            )
        };
        sender
            .send(errno_result_from_i32(lvol_ptr, errno))
            .expect("Receiver is gone");
    }
}

/// Iterator over replicas
#[derive(Default)]
pub struct ReplicaIter {
    /// Last bdev examined by the iterator during the call to next()
    bdev: Option<Bdev>,
}

impl ReplicaIter {
    pub fn new() -> ReplicaIter {
        ReplicaIter {
            bdev: None,
        }
    }
}

// XXX: shut this not simply be a bdev iterator with .filter()?

impl Iterator for ReplicaIter {
    type Item = Replica;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_bdev = match &mut self.bdev {
                Some(bdev) => {
                    let ptr =
                        unsafe { spdk_sys::spdk_bdev_next(bdev.as_ptr()) };
                    if !ptr.is_null() {
                        Some(Bdev::from(ptr))
                    } else {
                        None
                    }
                }
                None => Bdev::bdev_first(),
            };

            let bdev = match maybe_bdev {
                Some(bdev) => bdev,
                None => return None,
            };

            // Skip all other bdevs which are not lvols (i.e. aio)
            let lvol = unsafe { vbdev_lvol_get_from_bdev(bdev.as_ptr()) };
            if !lvol.is_null() {
                let mut aliases = bdev.aliases();
                // each lvol has a first alias of form "pool/lvol-name"
                if !aliases.is_empty() {
                    let alias = aliases.remove(0);
                    let parts: Vec<&str> = alias.split('/').collect();

                    if parts.len() == 2 && bdev.name() == parts[1] {
                        let replica = Replica {
                            lvol_ptr: lvol,
                        };

                        if replica.get_pool_name() == parts[0] {
                            // we found a replica
                            self.bdev = Some(bdev);
                            return Some(replica);
                        }
                    }
                }
            }
            self.bdev = Some(bdev);
        }
    }
}

pub(crate) async fn create_replica(
    args: CreateReplicaRequest,
) -> Result<CreateReplicaReply, RpcError> {
    let want_share = match ShareProtocolReplica::from_i32(args.share) {
        Some(val) => val,
        None => Err(Error::InvalidProtocol {
            protocol: args.share,
        })
        .context(CreateReplica {
            uuid: args.uuid.clone(),
        })?,
    };
    // Should we ignore EEXIST error?
    let replica = Replica::create(&args.uuid, &args.pool, args.size, args.thin)
        .await
        .context(CreateReplica {
            uuid: args.uuid.clone(),
        })?;

    // TODO: destroy replica if the share operation fails
    match want_share {
        ShareProtocolReplica::ReplicaNvmf => replica
            .share(ShareType::Nvmf)
            .await
            .context(CreateReplica {
                uuid: args.uuid.clone(),
            })?,
        ShareProtocolReplica::ReplicaIscsi => replica
            .share(ShareType::Iscsi)
            .await
            .context(CreateReplica {
                uuid: args.uuid.clone(),
            })?,
        ShareProtocolReplica::ReplicaNone => (),
    }
    Ok(CreateReplicaReply {
        uri: replica.get_share_uri(),
    })
}

pub(crate) async fn destroy_replica(
    args: DestroyReplicaRequest,
) -> Result<(), RpcError> {
    match Replica::lookup(&args.uuid) {
        Some(replica) => replica.destroy().await.context(DestroyReplica {
            uuid: args.uuid,
        }),
        None => Err(Error::ReplicaNotFound {}).context(DestroyReplica {
            uuid: args.uuid,
        }),
    }
}

pub(crate) fn list_replicas() -> ListReplicasReply {
    ListReplicasReply {
        replicas: ReplicaIter::new()
            .map(|r| ReplicaJson {
                uuid: r.get_uuid().to_owned(),
                pool: r.get_pool_name().to_owned(),
                size: r.get_size(),
                thin: r.is_thin(),
                share: match r.get_share_type() {
                    Some(share_type) => match share_type {
                        ShareType::Iscsi => {
                            ShareProtocolReplica::ReplicaIscsi as i32
                        }
                        ShareType::Nvmf => {
                            ShareProtocolReplica::ReplicaNvmf as i32
                        }
                    },
                    None => ShareProtocolReplica::ReplicaNone as i32,
                },
                uri: r.get_share_uri(),
            })
            .collect::<Vec<ReplicaJson>>(),
    }
}

pub(crate) async fn stat_replicas() -> Result<StatReplicasReply, RpcError> {
    let mut stats = Vec::new();

    // XXX is it safe to hold bdev pointer in iterator across context
    // switch!?
    for r in ReplicaIter::new() {
        let lvol = r.as_ptr();
        let uuid = r.get_uuid().to_owned();
        let pool = r.get_pool_name().to_owned();
        let bdev: Bdev = unsafe { (*lvol).bdev.into() };

        // cancellation point here
        let st = bdev.stats().await;

        match st {
            Ok(st) => {
                stats.push(ReplicaStats {
                    uuid,
                    pool,
                    stats: Some(Stats {
                        num_read_ops: st.num_read_ops,
                        num_write_ops: st.num_write_ops,
                        bytes_read: st.bytes_read,
                        bytes_written: st.bytes_written,
                    }),
                });
            }
            Err(errno) => {
                warn!(
                    "Failed to get stats for {} (errno={})",
                    bdev.name(),
                    errno
                );
            }
        }
    }
    Ok(StatReplicasReply {
        replicas: stats,
    })
}

pub(crate) async fn share_replica(
    args: ShareReplicaRequest,
) -> Result<ShareReplicaReply, RpcError> {
    let want_share = match ShareProtocolReplica::from_i32(args.share) {
        Some(val) => val,
        None => Err(Error::InvalidProtocol {
            protocol: args.share,
        })
        .context(ShareReplica {
            uuid: args.uuid.clone(),
        })?,
    };
    let replica = match Replica::lookup(&args.uuid) {
        Some(replica) => replica,
        None => Err(Error::ReplicaNotFound {}).context(ShareReplica {
            uuid: args.uuid.clone(),
        })?,
    };
    // first unshare the replica if there is a protocol change
    let unshare = match replica.get_share_type() {
        Some(share_type) => match share_type {
            ShareType::Iscsi => {
                want_share != ShareProtocolReplica::ReplicaIscsi
            }
            ShareType::Nvmf => want_share != ShareProtocolReplica::ReplicaNvmf,
        },
        None => false,
    };
    if unshare {
        replica.unshare().await.context(ShareReplica {
            uuid: args.uuid.clone(),
        })?;
    }
    // share the replica if it is not shared, and we want it to be
    // shared
    if replica.get_share_type().is_none() {
        match want_share {
            ShareProtocolReplica::ReplicaIscsi => replica
                .share(ShareType::Iscsi)
                .await
                .context(ShareReplica {
                    uuid: args.uuid.clone(),
                })?,
            ShareProtocolReplica::ReplicaNvmf => {
                replica.share(ShareType::Nvmf).await.context(ShareReplica {
                    uuid: args.uuid.clone(),
                })?
            }
            ShareProtocolReplica::ReplicaNone => (),
        }
    }
    Ok(ShareReplicaReply {
        uri: replica.get_share_uri(),
    })
}

/// Register replica json-rpc methods.
pub fn register_replica_methods() {
    jsonrpc_register::<_, _, _, RpcError>(
        "create_replica",
        |args: CreateReplicaRequest| create_replica(args).boxed_local(),
    );

    jsonrpc_register::<_, _, _, RpcError>(
        "destroy_replica",
        |args: DestroyReplicaRequest| destroy_replica(args).boxed_local(),
    );

    jsonrpc_register::<(), _, _, RpcError>("list_replicas", |_| {
        future::ok(list_replicas()).boxed_local()
    });

    jsonrpc_register::<(), _, _, RpcError>("stat_replicas", |_| {
        stat_replicas().boxed_local()
    });

    jsonrpc_register::<_, _, _, RpcError>(
        "share_replica",
        |args: ShareReplicaRequest| {
            async move { share_replica(args).await }.boxed_local()
        },
    );
}
