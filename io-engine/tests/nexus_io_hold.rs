//! Nexus I/O pause holds: holders, renewal, release and lease expiry.
use io_engine::{
    bdev::nexus::{
        nexus_create, nexus_lookup, nexus_lookup_mut, Error, NexusPauseState, NvmeAnaState,
    },
    core::{MayastorCliArgs, Protocol},
};
use once_cell::sync::OnceCell;
use std::time::Duration;

pub mod common;
use common::MayastorTest;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();
static NEXUS_UUID: &str = "4a5a8b72-0f4e-4e6a-9a37-6a1d3c8f2b10";

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn nexus_pause_state(name: &str) -> Option<NexusPauseState> {
    nexus_lookup(name).unwrap().io_subsystem_state()
}

const LONG: Duration = Duration::from_secs(60);

#[tokio::test]
async fn nexus_io_hold() {
    let ms = get_ms();
    let nexus = format!("nexus-{NEXUS_UUID}");

    let name = nexus.clone();
    ms.spawn(async move {
        let children = vec!["malloc:///d0?size_mb=64".into()];
        nexus_create(&name, 32 * 1024 * 1024, Some(NEXUS_UUID), &children)
            .await
            .unwrap();

        // an unshared nexus cannot be held paused
        let result = nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("a", LONG, false, None)
            .await;
        assert!(matches!(result, Err(Error::NotSharedNvmf { .. })));

        nexus_lookup_mut(&name)
            .unwrap()
            .share(Protocol::Nvmf, None)
            .await
            .unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));

        // a holder and a non-zero lease are required
        let result = nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("", LONG, false, None)
            .await;
        assert!(matches!(result, Err(Error::InvalidArguments { .. })));
        let result = nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("a", Duration::ZERO, false, None)
            .await;
        assert!(matches!(result, Err(Error::InvalidArguments { .. })));
        // a lease is bounded, so that it cannot overflow its expiry time
        let result = nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("a", Duration::MAX, false, None)
            .await;
        assert!(matches!(result, Err(Error::InvalidArguments { .. })));
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));

        // holding again with the same holder renews it, it doesn't add another pause
        let nx = || nexus_lookup_mut(&name).unwrap();
        nx().hold_io_pause("a", LONG, false, None).await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
        nx().hold_io_pause("a", LONG, false, None).await.unwrap();
        nx().release_io_hold("a").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));

        // the I/O stays paused until every holder releases its hold
        nx().hold_io_pause("a", LONG, false, None).await.unwrap();
        nx().hold_io_pause("b", LONG, false, None).await.unwrap();
        nx().release_io_hold("a").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
        // releasing a holder twice, or one which never held, is a no-op
        nx().release_io_hold("a").await.unwrap();
        nx().release_io_hold("c").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));

        // the ANA state cannot change underneath a held pause
        let result = nx().set_ana_state(NvmeAnaState::OptimizedState).await;
        assert!(matches!(result, Err(Error::OperationNotAllowed { .. })));

        nx().release_io_hold("b").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));

        // a pause taken internally (eg: snapshot) nests with a held pause
        nx().hold_io_pause("a", LONG, false, None).await.unwrap();
        nx().pause().await.unwrap();
        nx().resume().await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
        nx().release_io_hold("a").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));
    })
    .await;

    // a hold is released once its lease expires
    let name = nexus.clone();
    ms.spawn(async move {
        nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("x", Duration::from_secs(1), false, None)
            .await
            .unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let name = nexus.clone();
    ms.spawn(async move {
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));
        // an expired hold cannot be renewed: a late renewal must not pause the I/O again,
        // otherwise the gap in between would go unnoticed
        let result = nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("x", LONG, true, None)
            .await;
        assert!(
            matches!(result, Err(Error::IoHoldNotFound { .. })),
            "{:?}",
            result
        );
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));

        // renew-only renews a hold which is still held, and a drain timeout which isn't hit
        // doesn't affect the hold
        let nx = || nexus_lookup_mut(&name).unwrap();
        nx().hold_io_pause("z", LONG, false, Some(LONG))
            .await
            .unwrap();
        nx().hold_io_pause("z", LONG, true, None).await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
        nx().release_io_hold("z").await.unwrap();
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));
    })
    .await;

    // renewing a hold extends its lease
    let name = nexus.clone();
    ms.spawn(async move {
        nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("y", Duration::from_secs(2), false, None)
            .await
            .unwrap();
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let name = nexus.clone();
    ms.spawn(async move {
        nexus_lookup_mut(&name)
            .unwrap()
            .hold_io_pause("y", Duration::from_secs(4), false, None)
            .await
            .unwrap();
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let name = nexus.clone();
    ms.spawn(async move {
        // past the original lease, within the renewed one
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let name = nexus.clone();
    ms.spawn(async move {
        assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Unpaused));
    })
    .await;
}
