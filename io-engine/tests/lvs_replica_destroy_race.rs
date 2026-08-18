pub mod common;

use common::compose::{
    rpc::v1::{
        pool::{CreatePoolRequest, ListPoolOptions},
        replica::{destroy_replica_request, CreateReplicaRequest, DestroyReplicaRequest},
        GrpcConnect,
    },
    Binary, Builder,
};

/// Reproducer for concurrency issues between stats and replica destroy
/// as seen in openebs/mayastor#2055.
///
/// Root cause of the race (all on the single-threaded reactor, inside SPDK):
///  1. `DestroyReplica` calls `vbdev_lvol_destroy`, which first
///     `spdk_bdev_unregister`s the lvol bdev. The bdev destruct closes the
///     lvol and `lvol_close_blob_cb` sets `lvol->blob = NULL`.
///  2. The lvol is NOT unlinked from the `lvs->lvols` list until later, in
///     `lvol_delete_blob_cb`, after `spdk_bs_delete_blob` completes.
///  3. Between those two SPDK callbacks the lvol is still enumerable via
///     `Lvs::lvols()` but its blob is already null.
///
/// A concurrent `ListPools` gRPC (as issued by `metrics-exporter-io-engine`)
/// builds the `Pool` message, which computes `Lvs::committed()` ->
/// `Lvol::committed()` -> `blob_checked()`, dereferencing the null blob during
/// that window and aborting the whole data-plane process.
#[tokio::test]
async fn lvs_replica_destroy_committed_race() {
    // Number of create/destroy iterations. The null-blob window is only a few
    // reactor iterations wide, so we churn enough times to overlap with the
    // concurrent `ListPools` stream.
    const ITERATIONS: usize = 500;
    const POOL_NAME: &str = "pool-2055";
    const POOL_UUID: &str = "40baf8b5-6256-4f29-b073-61ebf67d20a0";

    common::composer_init();
    let test = Builder::new()
        .name("lvs_replica_destroy_race")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    // Two independent gRPC handles to the *same* io-engine, so that the
    // `DestroyReplica` and `ListPools` calls are processed concurrently by the
    // engine (mirroring the control-plane and the metrics-exporter).
    let mut churn_hdl = conn.grpc_handle("ms").await.unwrap();
    let mut stats_hdl = conn.grpc_handle("ms").await.unwrap();

    // Create the pool used for the churn.
    churn_hdl
        .pool
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.into(),
            uuid: Some(POOL_UUID.into()),
            pooltype: 0,
            disks: vec!["malloc:///disk0?size_mb=128".into()],
            cluster_size: None,
            md_args: None,
            encryption: None,
        })
        .await
        .unwrap();

    // Task #1: the metrics-exporter path. Repeatedly list pools, which builds
    // the `Pool` message and computes `committed()` over every lvol. Runs until
    // the churn signals completion on the channel.
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stats_task = async move {
        loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }
            // On unfixed code this returns a transport error once the engine
            // aborts with `assertion failed: !blob.is_null()`.
            stats_hdl
                .pool
                .list_pools(ListPoolOptions {
                    name: None,
                    pooltype: None,
                    uuid: None,
                })
                .await
                .expect("list_pools failed - the io-engine likely panicked (see #2055)");
        }
    };

    // Task #2: the control-plane path. Create and destroy thin replicas so that
    // each destruction overlaps with the concurrent `ListPools` enumeration.
    let churn_task = async move {
        for i in 0..ITERATIONS {
            let uuid = format!("00000000-0000-0000-0000-{i:012x}");
            let name = format!("replica-{i}");

            churn_hdl
                .replica
                .create_replica(CreateReplicaRequest {
                    name: name.clone(),
                    uuid: uuid.clone(),
                    pooluuid: POOL_UUID.into(),
                    size: 16 * 1024 * 1024,
                    thin: true,
                    share: 0,
                    ..Default::default()
                })
                .await
                .expect("create_replica failed - the io-engine likely panicked (see #2055)");

            churn_hdl
                .replica
                .destroy_replica(DestroyReplicaRequest {
                    uuid: uuid.clone(),
                    pool: Some(destroy_replica_request::Pool::PoolUuid(POOL_UUID.into())),
                })
                .await
                .expect("destroy_replica failed - the io-engine likely panicked (see #2055)");
        }
        let _ = stop_tx.send(());
    };

    // Drive both concurrently. If the race triggers, the engine aborts and one
    // of the `.expect(..)`s above fails the test.
    tokio::join!(stats_task, churn_task);
}
