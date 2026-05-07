use once_cell::sync::OnceCell;
use std::time::Duration;

use io_engine::{
    bdev::{device_create, device_destroy},
    core::MayastorCliArgs,
    rebuild::RebuildState,
};

pub mod common;
use common::compose::MayastorTest;
use io_engine::{
    core::{LogicalVolume, ReadOptions},
    lvs::{Lvol, LvsLvol},
    rebuild::{RebuildJobOptions, SnapshotRebuildJob},
    sleep::mayastor_sleep,
};
use io_engine_tests::pool::{PoolBuilderLocal, PoolLocal, PoolOps};

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            ..Default::default()
        })
    })
}

async fn create_replica(pool: &PoolLocal, uuid: &str) -> Result<Lvol, String> {
    pool.create_repl(uuid, SIZE_MB * 1024 * 1024, Some(uuid), true, None)
        .await
        .map_err(|error| error.to_string())
}
async fn destroy_replica(replica: Lvol) -> Result<(), String> {
    replica
        .destroy_replica()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

const BLOCK_SIZE: u64 = 512;
fn mb_to_blocks(mb: u64) -> u64 {
    (mb * 1024 * 1024) / BLOCK_SIZE
}
const SIZE_MB: u64 = 32;
const POOL_SZ_MB: u64 = SIZE_MB * 3;

#[tokio::test]
async fn malloc_to_malloc() {
    let ms = get_ms();

    ms.spawn(async move {
        let src_uri = format!("malloc:///d?size_mb={SIZE_MB}");
        let dst_uri = format!("malloc:///d2?size_mb={SIZE_MB}");

        device_create(&src_uri).await.unwrap();
        device_create(&dst_uri).await.unwrap();

        let job = SnapshotRebuildJob::builder()
            .with_snapshot_uri(&src_uri)
            .with_replica_uri(&dst_uri)
            .build()
            .await
            .unwrap()
            .store()
            .unwrap();
        println!("job: {job:?}");

        let name = job.name();
        let chan = job.start().await.unwrap();
        {
            assert!(SnapshotRebuildJob::lookup(name).is_ok());
            assert!(SnapshotRebuildJob::lookup(&dst_uri).is_err());
        }

        let state = chan.await.unwrap();
        // todo: use completion channel with stats rather than just state?
        let stats = job.stats().await;

        device_destroy(&src_uri).await.unwrap();
        device_destroy(&dst_uri).await.unwrap();
        job.destroy();

        assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
        assert_eq!(stats.blocks_transferred, mb_to_blocks(SIZE_MB));
    })
    .await;
}

#[tokio::test]
async fn malloc_to_replica() {
    let ms = get_ms();

    ms.spawn(async move {
        let src_uri = format!("malloc:///d?size_mb={SIZE_MB}");

        let pool = PoolBuilderLocal::malloc("md", POOL_SZ_MB).await.unwrap();
        let replica = create_replica(&pool, "3be1219f-682b-4672-b88b-8b9d07e8104a")
            .await
            .unwrap();

        let job = SnapshotRebuildJob::builder()
            .with_replica_uuid(&replica.uuid())
            .with_snapshot_uri(src_uri)
            .build()
            .await
            .unwrap()
            .store()
            .unwrap();
        println!("job: {job:?}");

        let chan = job.start().await.unwrap();
        assert!(SnapshotRebuildJob::lookup(&replica.uuid()).is_ok());

        let state = chan.await.unwrap();
        let stats = job.stats().await;

        destroy_replica(replica).await.unwrap();
        job.destroy();

        assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
        assert_eq!(stats.blocks_transferred, mb_to_blocks(SIZE_MB));
    })
    .await;
}

#[tokio::test]
async fn replica_to_rebuild_full() {
    let ms = get_ms();

    ms.spawn(async move {
        let pool = PoolBuilderLocal::malloc("md", POOL_SZ_MB).await.unwrap();
        let replica_src = create_replica(&pool, "2be1219f-682b-4672-b88b-8b9d07e8104a")
            .await
            .unwrap();
        let replica_dst = create_replica(&pool, "3be1219f-682b-4672-b88b-8b9d07e8104a")
            .await
            .unwrap();

        let job = SnapshotRebuildJob::builder()
            .with_option(RebuildJobOptions::default().with_read_opts(ReadOptions::None))
            .with_replica_uuid(&replica_dst.uuid())
            .with_snapshot_uri(replica_src.bdev_share_uri().unwrap())
            .build()
            .await
            .unwrap()
            .store()
            .unwrap();
        println!("job: {job:?}");

        let chan = job.start().await.unwrap();
        assert!(SnapshotRebuildJob::lookup(&replica_dst.uuid()).is_ok());

        let state = chan.await.unwrap();
        let stats = job.stats().await;

        destroy_replica(replica_src).await.unwrap();
        destroy_replica(replica_dst).await.unwrap();
        job.destroy();

        assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
        assert_eq!(stats.blocks_transferred, mb_to_blocks(SIZE_MB));
    })
    .await;
}

#[tokio::test]
async fn replica_to_rebuild_partial() {
    let ms = get_ms();

    ms.spawn(async move {
        let pool = PoolBuilderLocal::malloc("md", POOL_SZ_MB).await.unwrap();
        let replica_src = create_replica(&pool, "2be1219f-682b-4672-b88b-8b9d07e8104a")
            .await
            .unwrap();
        let replica_dst = create_replica(&pool, "3be1219f-682b-4672-b88b-8b9d07e8104a")
            .await
            .unwrap();

        let job = SnapshotRebuildJob::builder()
            .with_replica_uuid(&replica_dst.uuid())
            .with_snapshot_uri(replica_src.bdev_share_uri().unwrap())
            .build()
            .await
            .unwrap()
            .store()
            .unwrap();
        println!("job: {job:?}");

        let chan = job.start().await.unwrap();
        assert!(SnapshotRebuildJob::lookup(&replica_dst.uuid()).is_ok());

        let state = chan.await.unwrap();
        let stats = job.stats().await;

        destroy_replica(replica_src).await.unwrap();
        destroy_replica(replica_dst).await.unwrap();
        job.destroy();

        assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
        // 8MiB which are write-zeroes at src replica creation
        assert_eq!(stats.blocks_transferred, mb_to_blocks(8));
        mayastor_sleep(Duration::from_millis(1)).await.unwrap();
    })
    .await;
}
