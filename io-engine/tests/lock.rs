use io_engine::core::lock::{ResourceLockManager, ResourceLockManagerConfig};
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    sync::oneshot,
    time::{sleep, Duration},
};

#[derive(Copy, Clone)]
enum LockLevel {
    Global,
    Subsystem,
    Resource,
}

const TEST_SUBSYSTEM: &str = "items";
const TEST_RESOURCE: &str = "item1";

fn get_lock_manager() -> &'static ResourceLockManager {
    let cfg =
        ResourceLockManagerConfig::default().with_subsystem(TEST_SUBSYSTEM, 8);
    ResourceLockManager::initialize(cfg);
    ResourceLockManager::get_instance()
}

// Helper function to test all possible lock levels from 2 tasks
// that try to simultaneously acquire the same locks.
async fn test_lock_level(level: LockLevel) {
    static STEP_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

    let (tx, rx) = oneshot::channel();

    STEP_COUNT.store(0, Ordering::Relaxed);

    // Task that owns the lock.
    let h1 = tokio::spawn(async move {
        let lock_mgr = get_lock_manager();

        // Step 1: acquire lock.
        let guard = match level {
            LockLevel::Global => lock_mgr.lock(None, false).await,
            LockLevel::Subsystem => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock(None, false)
                    .await
            }
            LockLevel::Resource => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock_resource(TEST_RESOURCE, None, false)
                    .await
            }
        };
        assert!(guard.is_some(), "Failed to acquire the lock");

        if guard.is_some() {
            let try_lock_guard = match level {
                LockLevel::Global => lock_mgr.lock(None, true).await,
                LockLevel::Subsystem => {
                    lock_mgr
                        .get_subsystem(TEST_SUBSYSTEM)
                        .lock(None, true)
                        .await
                }
                LockLevel::Resource => {
                    lock_mgr
                        .get_subsystem(TEST_SUBSYSTEM)
                        .lock_resource(TEST_RESOURCE, None, true)
                        .await
                }
            };
            assert!(try_lock_guard.is_none(), "Double Lock acquired");
        }

        // Notify that the lock is acquired.
        tx.send(0).expect("Failed to notify the peer ");

        // Sleep to let the other part of test execute.
        // Note: the lock is held so the other task must wait.
        sleep(Duration::from_millis(500)).await;

        // Check that the counter remains unchanged since we're still holding
        // the lock.
        assert_eq!(
            STEP_COUNT.load(Ordering::Relaxed),
            0,
            "Protected resource accessed by the other task"
        );

        // Lock will be automatically released here when lock guard leaves the
        // scope.
    });

    // Task that triggers lock contention.
    let h2 = tokio::spawn(async move {
        let lock_mgr = get_lock_manager();

        // Wait till the lock is acquired.
        rx.await
            .expect("Failed to receive notification that the lock is acquired");

        // Try to grab the lock - we must wait, since the lock is already
        // acquired.
        let guard = match level {
            LockLevel::Global => lock_mgr.lock(None, false).await,
            LockLevel::Subsystem => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock(None, false)
                    .await
            }
            LockLevel::Resource => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock_resource(TEST_RESOURCE, None, false)
                    .await
            }
        };
        assert!(guard.is_some(), "Failed to acquire the lock");

        // Adjust the counter to denote that lock is acquired.
        STEP_COUNT.fetch_add(1, Ordering::SeqCst);
    });

    h1.await.expect("Test task panicked");
    h2.await.expect("Test task panicked");
}

// Helper function to test all possible lock levels from 2 tasks
// that try to simultaneously acquire the same locks.
async fn test_lock_timed_level(level: LockLevel) {
    let (tx, rx) = oneshot::channel();

    // Task that owns the lock.
    let h1 = tokio::spawn(async move {
        let lock_mgr = get_lock_manager();

        // Step 1: acquire lock.
        let guard = match level {
            LockLevel::Global => lock_mgr.lock(None, false).await,
            LockLevel::Subsystem => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock(None, false)
                    .await
            }
            LockLevel::Resource => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock_resource(TEST_RESOURCE, None, false)
                    .await
            }
        };
        assert!(guard.is_some(), "Failed to acquire the lock");

        // Notify that the lock is acquired.
        tx.send(0).expect("Failed to notify the peer ");

        // Sleep to let the other part of test execute.
        // Note: the lock is held so the other task must timeout.
        sleep(Duration::from_secs(2)).await;
    });

    // Task that tries to acquire the lock with a timeout lesser
    // than the time the lock is held.
    let h2 = tokio::spawn(async move {
        let lock_mgr = get_lock_manager();

        // Wait till the lock is acquired.
        rx.await
            .expect("Failed to receive notification that the lock is acquired");

        let duration = Some(std::time::Duration::from_secs(1));
        // Try to grab the lock - we must wait, since the lock is already
        // acquired.
        let guard = match level {
            LockLevel::Global => lock_mgr.lock(duration, false).await,
            LockLevel::Subsystem => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock(duration, false)
                    .await
            }
            LockLevel::Resource => {
                lock_mgr
                    .get_subsystem(TEST_SUBSYSTEM)
                    .lock_resource(TEST_RESOURCE, duration, false)
                    .await
            }
        };
        assert!(
            guard.is_none(),
            "Successfully acquired the lock within timeout"
        );
    });

    h1.await.expect("Test task panicked");
    h2.await.expect("Test task panicked");
}

#[tokio::test]
async fn test_lock_global() {
    test_lock_level(LockLevel::Global).await
}

#[tokio::test]
async fn test_lock_subsystem() {
    test_lock_level(LockLevel::Subsystem).await
}

#[tokio::test]
async fn test_lock_resource() {
    test_lock_level(LockLevel::Resource).await
}

#[tokio::test]
async fn test_lock_timed_global() {
    test_lock_timed_level(LockLevel::Global).await
}

#[tokio::test]
async fn test_lock_timed_subsystem() {
    test_lock_timed_level(LockLevel::Subsystem).await
}

#[tokio::test]
async fn test_lock_timed_resource() {
    test_lock_timed_level(LockLevel::Resource).await
}
