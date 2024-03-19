use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::Duration,
};

use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Common IO engine resource subsystems.
pub struct ProtectedSubsystems;
impl ProtectedSubsystems {
    pub const NEXUS: &'static str = "nexus";
    pub const REPLICA: &'static str = "replica";
    pub const POOL: &'static str = "pool";
}

/// Configuration parameters for initialization of the Lock manager.
#[derive(Debug, Default)]
pub struct ResourceLockManagerConfig {
    /// Configs for subsystems: denote id and maximum amount of lockable
    /// resources.
    subsystems: Vec<(String, usize)>,
}

impl ResourceLockManagerConfig {
    /// Add resource subsystem to the config.
    /// Panics if another subsystem with the same id already exists.
    pub fn with_subsystem<T: AsRef<str>>(
        mut self,
        id: T,
        num_objects: usize,
    ) -> Self {
        let ids = id.as_ref();

        if self.subsystems.iter().any(|(i, _)| ids.eq(i)) {
            panic!("Subsystem {} already exists", ids);
        }

        self.subsystems.push((ids.to_owned(), num_objects));
        self
    }
}

/// Resource subsystem that holds locks for all resources withing this system.
#[derive(Clone)]
pub struct ResourceSubsystem {
    id: String,
    object_locks: Vec<Arc<RwLock<LockStats>>>,
    subsystem_lock: Arc<RwLock<LockStats>>,
}

impl ResourceSubsystem {
    /// Create a new resource subsystem with target id and maximum number of
    /// objects.
    fn new(id: String, num_objects: usize) -> Self {
        let object_locks =
            std::iter::repeat_with(|| RwLock::new(LockStats::default()).into())
                .take(num_objects)
                .collect::<Vec<_>>();

        Self {
            id,
            object_locks,
            subsystem_lock: RwLock::new(LockStats::default()).into(),
        }
    }

    /// Acquire the subsystem write lock.
    pub async fn write_lock_subsystem(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        acquire_write_lock(&self.subsystem_lock, wait_timeout).await
    }

    /// Write lock subsystem resource by its ID and obtain a lock guard.
    pub async fn write_lock_resource<T: AsRef<str>>(
        &self,
        id: T,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        // Calculate hash of the object to get the mutex index.
        let mut hasher = DefaultHasher::new();
        id.as_ref().hash(&mut hasher);
        let mutex_id = hasher.finish() as usize % self.object_locks.len();
        acquire_write_lock(&self.object_locks[mutex_id], wait_timeout).await
    }

    /// Acquire the subsystem read lock.
    pub async fn read_lock_subsystem(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        acquire_read_lock(&self.subsystem_lock, wait_timeout).await
    }

    /// Read lock subsystem resource by its ID and obtain a lock guard.
    pub async fn read_lock_resource<T: AsRef<str>>(
        &self,
        id: T,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        // Calculate hash of the object to get the mutex index.
        let mut hasher = DefaultHasher::new();
        id.as_ref().hash(&mut hasher);
        let mutex_id = hasher.finish() as usize % self.object_locks.len();
        acquire_read_lock(&self.object_locks[mutex_id], wait_timeout).await
    }
}

/// Structure that holds per-lock statistics.
#[derive(Debug, Default)]
pub struct LockStats {
    num_acquires: usize,
}

/// Lock manager which is used for protecting access to sensitive resources.
/// The following hierarchical levels of resource protection are supported:
/// 1) Global - lock manager exposes one single lock which can be used as
/// the global lock to control access at the topmost level.
/// 2) Subsystem - Subsystems group resources of the same type (examples are:
/// "nexus", "pool", etc). Every subsystem exposes the global, per-subsystem
/// lock to control resource access at the subsystem level.
/// Example: create/delete nexus operations must be globally serialized,
/// which can be achieved by locking the "nexus" subsystem.
/// 3) Resource - control access at per-object level.
/// Example: control access to a nexus instance whilst modifying nexus state.
pub struct ResourceLockManager {
    /// All known resource subsystems with locks.
    subsystems: Vec<ResourceSubsystem>,
    /// Global resource lock,
    mgr_lock: Arc<RwLock<LockStats>>,
}

/// Resource guard for read and write which automatically released once dropped.
pub enum ResourceLockGuard<'a> {
    Read(RwLockReadGuard<'a, LockStats>),
    Write(RwLockWriteGuard<'a, LockStats>),
}

/// Global instance of the resource lock manager.
static LOCK_MANAGER: OnceCell<ResourceLockManager> = OnceCell::new();

/// Helper function to aquire read lock.
async fn acquire_read_lock(
    lock: &Arc<RwLock<LockStats>>,
    wait_timeout: Option<Duration>,
) -> Option<ResourceLockGuard<'_>> {
    let lock_guard = if let Some(d) = wait_timeout {
        match tokio::time::timeout(d, lock.read()).await {
            Err(_) => return None,
            Ok(g) => g,
        }
    } else {
        // No timeout, wait for the lock indefinitely.
        lock.read().await
    };

    Some(ResourceLockGuard::Read(lock_guard))
}

/// Helper function to aquire write lock.
async fn acquire_write_lock(
    lock: &RwLock<LockStats>,
    wait_timeout: Option<Duration>,
) -> Option<ResourceLockGuard<'_>> {
    let mut lock_guard = if let Some(d) = wait_timeout {
        match tokio::time::timeout(d, lock.write()).await {
            Err(_) => return None,
            Ok(g) => g,
        }
    } else {
        // No timeout, wait for the lock indefinitely.
        lock.write().await
    };
    lock_guard.num_acquires += 1;

    Some(ResourceLockGuard::Write(lock_guard))
}

impl ResourceLockManager {
    /// Initialize instance of the lock manager. This function must be called
    /// prior to using the lock manager API.
    pub fn initialize(cfg: ResourceLockManagerConfig) {
        LOCK_MANAGER.get_or_init(|| {
            let subsystems = cfg
                .subsystems
                .iter()
                .map(|(id, n)| ResourceSubsystem::new(id.to_owned(), *n))
                .collect::<Vec<_>>();

            ResourceLockManager {
                subsystems,
                mgr_lock: RwLock::new(LockStats::default()).into(),
            }
        });
    }

    /// Acquire the global Lock manager write lock.
    pub async fn write_lock(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        acquire_write_lock(&self.mgr_lock, wait_timeout).await
    }

    /// Acquire the global Lock manager read lock.
    pub async fn read_lock(
        &self,
        wait_timeout: Option<Duration>,
    ) -> Option<ResourceLockGuard<'_>> {
        acquire_read_lock(&self.mgr_lock, wait_timeout).await
    }

    /// Get resource subsystem by its id.
    pub fn get_subsystem<T: AsRef<str>>(&self, id: T) -> &ResourceSubsystem {
        let ids = id.as_ref();

        for s in &self.subsystems {
            if s.id.eq(ids) {
                return s;
            }
        }

        panic!("Resource subsystem {} doesn't exist", id.as_ref());
    }

    /// Get global instance of the lock manager. Panics if Lock manager is not
    /// initialized.
    pub fn get_instance() -> &'static ResourceLockManager {
        LOCK_MANAGER.get().expect("Lock Manager is not initialized")
    }
}
