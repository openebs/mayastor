use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

/// In-memory structure containing pool runtime information.
pub static POOL_INFO: Lazy<RwLock<HashMap<String, RwLock<PoolInfo>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Pool specfic information stored in-memory.
#[derive(Default, Debug, Clone)]
pub struct PoolInfo {
    /// Set when Pools io is under stall.
    pub io_stalled: bool,
    /// Contains list of timestamps when pool went into stall state.
    pub transition_timestamps: VecDeque<Instant>,
}

impl PoolInfo {
    /// Retains only transition timestamps that are within the stall_transition_window.
    pub fn update_transition_timestamp(&mut self, stall_transition_window: Duration) {
        self.transition_timestamps
            .retain(|ts| ts.elapsed() < stall_transition_window);
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
