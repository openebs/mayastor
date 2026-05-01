use common::MayastorTest;
use io_engine::{
    core::{logical_volume::LogicalVolume, MayastorCliArgs, NvmeCliArgs, NvmfShareProps},
    lvs::{LvolSnapshotOps, Lvs},
    pool_backend::{PoolArgs, PoolBackend, ReplicaArgs},
};
use io_engine_api::v1::replica::ListReplicaOptions;
use once_cell::sync::OnceCell;

pub mod common;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    std::env::set_var("RUST_LOG", "error");
    let ms = MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x3".into(),
            // log_components: vec!["all".into()],
            nvme: NvmeCliArgs {
                max_namespaces: 8192,
            },
            ..Default::default()
        })
    });
    ms.start_grpc();
    ms.start_device_monitor();
    ms
}

#[tokio::test]
async fn lvol_list() {
    let ms = ms();

    let pool_size = "4GiB";
    let repl_size = 4 * 1024 * 1024;

    use io_engine::pool_backend::PoolMetadataArgs;
    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![format!("malloc:///m?size={pool_size}")],
        backend: PoolBackend::Lvs,
        md_args: Some(PoolMetadataArgs {
            max_expansion: Some("300GiB".into()),
        }),
        ..Default::default()
    };

    ms.spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args).await.unwrap();

        for i in 1..8000 {
            let name = format!("replica-{i}");
            let opts = ReplicaArgs::new(name, repl_size)
                .wipe_super(false)
                .thin(true);
            let _repl = lvs_pool.create_lvol_with_opts(opts).await.unwrap();
        }
    })
    .await;

    // this could vary depending on the system where we're running, but this is large enough that
    // it should run on weaker systems as well as low enough to ensure we're testing the fix.
    let max_dur = std::time::Duration::from_millis(300);

    // 1. this list is "quicker" as there's no nvmf subsystems
    let list_tm = std::time::Instant::now();
    ms.spawn(async move {
        for lvs_pool in Lvs::iter() {
            for lvol in lvs_pool.lvols() {
                let _replica: io_engine_api::v1::replica::Replica = lvol.into();
            }
        }
    })
    .await;
    let no_uri_elapsed = list_tm.elapsed();
    println!("Lvol List: {no_uri_elapsed:?}");
    assert!(no_uri_elapsed <= max_dur, "Listing replicas took too long");

    // 2. we share all replicas, so each replica now must search its subsystems
    ms.spawn(async move {
        for lvs_pool in Lvs::iter() {
            for mut lvol in lvs_pool.lvols() {
                use io_engine::replica_backend::ReplicaOps;
                lvol.share_nvmf(NvmfShareProps::new()).await.unwrap();
            }
        }
    })
    .await;

    // 3. we have to iter but also convert each lvol to a Replica so we can exercise the subsystem listing
    let list_tm = std::time::Instant::now();
    ms.spawn(async move {
        let list_tm = std::time::Instant::now();
        for lvs_pool in Lvs::iter() {
            for lvol in lvs_pool.lvols() {
                let _replica: io_engine_api::v1::replica::Replica = lvol.into();
            }
        }
        println!("Lvol List time: {:?}", list_tm.elapsed());
    })
    .await;
    let uri_elapsed = list_tm.elapsed();
    println!("Lvol List: {uri_elapsed:?}");
    assert!(uri_elapsed <= max_dur, "Listing replicas took too long");

    use io_engine_api::v1::replica::ReplicaRpcClient;
    let mut h = ReplicaRpcClient::connect("http://localhost:10124")
        .await
        .unwrap();

    let list_tm = std::time::Instant::now();
    h.list_replicas(ListReplicaOptions::default())
        .await
        .unwrap();
    let grpc_elapsed = list_tm.elapsed();
    println!("gRPC Lvol List: {grpc_elapsed:?}");
    assert!(
        // adds some extra buffer for gRPC
        grpc_elapsed <= (max_dur + std::time::Duration::from_millis(100)),
        "Listing replicas took too long"
    );

    ms.spawn(async move {
        for lvs_pool in Lvs::iter() {
            lvs_pool.destroy().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn lvol_snap_list() {
    let ms = ms();

    let pool_size = "4GiB";
    let repl_size = 4 * 1024 * 1024;

    use io_engine::pool_backend::PoolMetadataArgs;
    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![format!("malloc:///m?size={pool_size}")],
        backend: PoolBackend::Lvs,
        md_args: Some(PoolMetadataArgs {
            max_expansion: Some("300GiB".into()),
        }),
        ..Default::default()
    };

    ms.spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args).await.unwrap();

        for i in 1..=1024 {
            let name = format!("replica-{i}");
            let opts = ReplicaArgs::new(name, repl_size)
                .wipe_super(false)
                .thin(true);
            let repl = lvs_pool.create_lvol_with_opts(opts).await.unwrap();
            if i > 10 {
                continue;
            }
            use io_engine::core::SnapshotParams;
            for j in 1..=256 {
                let snapshot = repl
                    .create_snapshot(SnapshotParams {
                        entity_id: Some(format!("e-{i}-{j}")),
                        parent_id: Some(repl.uuid()),
                        txn_id: Some(format!("txn-{i}-{j}")),
                        snap_name: Some(format!("snap-{i}-{j}")),
                        snapshot_uuid: Some(uuid::Uuid::new_v4().to_string()),
                        create_time: Some(chrono::Utc::now().to_string()),
                        discarded_snapshot: false,
                    })
                    .await
                    .unwrap();
                let n = uuid::Uuid::new_v4().to_string();
                let _clone = snapshot
                    .create_clone(io_engine::core::CloneParams {
                        clone_name: Some(n.clone()),
                        clone_uuid: Some(n.clone()),
                        source_uuid: Some(snapshot.uuid()),
                        clone_create_time: Some(chrono::Utc::now().to_string()),
                    })
                    .await
                    .unwrap();
            }
        }
    })
    .await;

    // this could vary depending on the system where we're running, but this is large enough that
    // it should run on weaker systems as well as low enough to ensure we're testing the fix.
    let max_dur = std::time::Duration::from_secs(3);

    let list_tm = std::time::Instant::now();
    ms.spawn(async move {
        use io_engine::lvs::Lvol;
        for snap in Lvol::list_all_snapshots(None) {
            assert_eq!(snap.info().num_clones, 1);
        }
    })
    .await;
    let elapsed = list_tm.elapsed();
    println!("Snapshot List: {elapsed:?}");
    assert!(elapsed <= max_dur, "Listing snapshots took too long");

    ms.spawn(async move {
        for lvs_pool in Lvs::iter() {
            lvs_pool.destroy().await.unwrap();
        }
    })
    .await;
}
