use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

/// In-memory structure containing pool runtime information.
pub static POOL_INFO: Lazy<RwLock<HashMap<String, RwLock<PoolInfo>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// An operation which is currently being performed on a pool.
/// This is not an error: a caller which finds one in progress has nothing to do but wait for it
/// to complete, rather than issue a competing request which could only be aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolOperation {
    /// The pool is being created.
    Create,
    /// The pool is being imported.
    Import,
    /// The pool is being destroyed.
    Destroy,
    /// The pool is being grown.
    Grow,
    /// The pool is being exported.
    Export,
    /// The pool's errors are being cleared.
    ClearErrors,
    /// A replica is being created on the pool.
    ReplicaCreate,
    /// A replica is being destroyed on the pool.
    ReplicaDestroy,
    /// A snapshot is being destroyed on the pool.
    SnapshotDestroy,
    /// A replica on the pool is being shared.
    ReplicaShare,
    /// A replica on the pool is being unshared.
    ReplicaUnshare,
}

/// Pool specfic information stored in-memory.
#[derive(Default, Debug, Clone)]
pub struct PoolInfo {
    /// Set when Pools io is under stall.
    pub io_stalled: bool,
    /// Contains list of timestamps when pool resumed from stall state.
    pub transition_timestamps: VecDeque<Instant>,
}

impl PoolInfo {
    /// Retains only transition timestamps that are within the stall_transition_window.
    pub fn update_transition_timestamp(&mut self, stall_transition_window: Duration) {
        self.transition_timestamps
            .retain(|ts| ts.elapsed() < stall_transition_window);
    }
    /// Get the [`PoolInfo`] for the given pool, if present.
    pub fn get(id: &str) -> Option<parking_lot::MappedRwLockReadGuard<'static, RwLock<PoolInfo>>> {
        pool_info(id)
    }
    /// Clears the transition timestamps for the pool.
    pub fn clear(&mut self) {
        self.transition_timestamps.clear();
    }
}

/// Returns write lock of the hashmap.
pub fn pool_info_write() -> RwLockWriteGuard<'static, HashMap<String, RwLock<PoolInfo>>> {
    POOL_INFO.write()
}

/// Returns read lock of the hashmap.
pub fn pool_info_read() -> RwLockReadGuard<'static, HashMap<String, RwLock<PoolInfo>>> {
    POOL_INFO.read()
}

fn pool_info(id: &str) -> Option<parking_lot::MappedRwLockReadGuard<'static, RwLock<PoolInfo>>> {
    let read = POOL_INFO.read();

    parking_lot::RwLockReadGuard::try_map(read, |info| info.get(id)).ok()
}

/// Operations currently in progress, by pool name.
/// Kept apart from [`POOL_INFO`] on purpose: that records the pools which exist, whereas an
/// operation may well be running against a pool which does not exist yet - a create or an
/// import - or which never will, should it fail. An entry here means an operation is running
/// and nothing else, so it needs no notion of the pool's own lifetime.
static POOL_OPS: Lazy<RwLock<HashMap<String, PoolOperation>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Records an operation as being in progress on a pool for as long as it is held.
/// # NOTE
/// At most one operation exists per pool, since this is only ever taken whilst the pool's
/// exclusive resource lock is held, see the io-engine's `acquire_pool_lock`.
pub struct PoolOperationGuard {
    /// The pool being operated on.
    pool: String,
}

impl PoolOperationGuard {
    /// Record `operation` as in progress on `pool` until the returned guard is dropped.
    pub fn new(pool: String, operation: PoolOperation) -> Self {
        POOL_OPS.write().insert(pool.clone(), operation);
        Self { pool }
    }
}

impl Drop for PoolOperationGuard {
    fn drop(&mut self) {
        POOL_OPS.write().remove(&self.pool);
    }
}

/// The operation currently in progress on the given pool, if any.
pub fn pool_operation(pool: &str) -> Option<PoolOperation> {
    POOL_OPS.read().get(pool).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The operation is reported whilst the guard lives, and only whilst it lives.
    #[test]
    fn the_operation_is_reported_whilst_it_runs() {
        let pool = "the_operation_is_reported_whilst_it_runs";

        let guard = PoolOperationGuard::new(pool.to_string(), PoolOperation::Import);
        assert_eq!(pool_operation(pool), Some(PoolOperation::Import));

        drop(guard);
        assert_eq!(pool_operation(pool), None);
    }

    /// Operating on a pool leaves nothing behind, whether or not the pool ever came to exist,
    /// and whatever the backend: this map only ever says what is running.
    #[test]
    fn nothing_is_left_behind() {
        let pool = "nothing_is_left_behind";

        for operation in [PoolOperation::Create, PoolOperation::Destroy] {
            drop(PoolOperationGuard::new(pool.to_string(), operation));
            assert_eq!(pool_operation(pool), None);
        }

        assert!(!pool_info_read().contains_key(pool), "POOL_INFO untouched");
    }
}
