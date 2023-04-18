pub mod common;

use once_cell::sync::OnceCell;

use common::compose::MayastorTest;

use common::compose::{
    rpc::v1::{
        bdev::ListBdevOptions,
        pool::CreatePoolRequest,
        replica::CreateReplicaRequest,
        GrpcConnect,
    },
    Builder,
    ComposeTest,
};

use io_engine::{
    bdev::{device_create, device_open},
    core::{MayastorCliArgs, SnapshotParams},
    subsys::{Config, NvmeBdevOpts},
};

use std::str;
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

/// Get the global Mayastor test suite instance.
fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn replica_name() -> String {
    "volume1".to_string()
}

fn replica_uuid() -> String {
    "65acdaac-14c4-41d8-a55e-d03bfd7185a4".to_string()
}

fn pool_uuid() -> String {
    "6e3c062c-293b-46e6-8ab3-ff13c1643437".to_string()
}

/// Launch a containerized I/O agent with a shared volume on it.
async fn launch_instance() -> (ComposeTest, String) {
    common::composer_init();

    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 2_000_000,
            keep_alive_timeout_ms: 5_000,
            transport_retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_dbg("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let mut ms1 = conn.grpc_handle("ms1").await.unwrap();

    ms1.pool
        .create_pool(CreatePoolRequest {
            name: "pool1".to_string(),
            uuid: Some(pool_uuid()),
            pooltype: 0,
            disks: vec!["malloc:///disk0?size_mb=32".into()],
        })
        .await
        .unwrap();

    ms1.replica
        .create_replica(CreateReplicaRequest {
            name: replica_name(),
            uuid: replica_uuid(),
            pooluuid: pool_uuid(),
            size: 8 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let bdev_url = format!(
        "nvmf://{}:8420/nqn.2019-05.io.openebs:{}",
        ms1.endpoint.ip(),
        replica_name(),
    );

    (test, bdev_url)
}

#[tokio::test]
async fn test_replica_handle_snapshot() {
    let ms = get_ms();
    let (test, url) = launch_instance().await;
    let conn = GrpcConnect::new(&test);
    static SNAP_NAME: &str = "snap21";

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    // Make sure no snapshot exists on the remote node prior testing.
    let bdevs = ms1
        .bdev
        .list(ListBdevOptions {
            name: Some(SNAP_NAME.to_string()),
        })
        .await
        .expect("Snapshot is not created")
        .into_inner();

    assert_eq!(
        bdevs.bdevs.len(),
        0,
        "Snapshot already exists on remote volume"
    );

    ms.spawn(async move {
        let device_name = device_create(&url).await.unwrap();
        let descr = device_open(&device_name, false)
            .expect("Can't open remote lvol device");
        let handle = descr.into_handle().unwrap();

        let entity_id = String::from("e1");
        let parent_id = String::from("p1");
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from(SNAP_NAME);

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        handle
            .create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create snapshot");
    })
    .await;

    // Make sure snapshot exists on the remote node.
    let bdevs = ms1
        .bdev
        .list(ListBdevOptions {
            name: Some(SNAP_NAME.to_string()),
        })
        .await
        .expect("Snapshot is not created")
        .into_inner();

    assert_eq!(
        bdevs.bdevs.len(),
        1,
        "Snapshot is not created on remote volume"
    );
}
