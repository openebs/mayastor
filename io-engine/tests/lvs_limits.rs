pub mod common;

use io_engine::{
    core::{LvolSnapshotOps, MayastorCliArgs},
    lvs::{BsError, Lvs, LvsError},
    pool_backend::PoolArgs,
    replica_backend::ReplicaOps,
};

use io_engine_tests::MayastorTest;

use once_cell::sync::OnceCell;

const DISK_SIZE: u64 = 10000;
const REPL_SIZE: u64 = 16;
const DISK_NAME: &str = "/tmp/disk0.img";
const BDEV_NAME: &str = "aio:///tmp/disk0.img?blk_size=512";
const POOL_NAME: &str = "pool_0";
const POOL_UUID: &str = "40baf8b5-6256-4f29-b073-61ebf67d9b91";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            log_format: Some("nodate,nohost,compact".parse().unwrap()),
            reactor_mask: "0x3".into(),
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lvs_metadata_limit() {
    const REPL_CNT: u64 = 100;
    const SNAP_CNT: u64 = 100;

    common::composer_init();

    common::delete_file(&[DISK_NAME.to_string()]);
    common::truncate_file_bytes(DISK_NAME, DISK_SIZE * 1024 * 1024);

    let ms = get_ms();

    ms.spawn(async {
        let lvs_args = PoolArgs {
            name: POOL_NAME.to_string(),
            disks: vec![BDEV_NAME.to_string()],
            uuid: Some(POOL_UUID.to_string()),
            cluster_size: None,
            md_args: None,
            backend: Default::default(),
        };

        // Create LVS.
        let lvs = Lvs::create_or_import(lvs_args.clone()).await.unwrap();

        // Create replicas.
        for i in 0 .. REPL_CNT {
            let repl_name = format!("r_{i}");
            let repl_uuid = format!("45c23e54-dc86-45f6-b55b-e44d05f1{i:04}");

            let lvol = match lvs
                .create_lvol(
                    &repl_name,
                    REPL_SIZE * 1024 * 1024,
                    Some(&repl_uuid),
                    true,
                    None,
                )
                .await
            {
                Ok(lvol) => lvol,
                Err(err) => {
                    match err {
                        LvsError::RepCreate {
                            source, ..
                        } => {
                            assert!(matches!(
                                source,
                                BsError::OutOfMetadata {}
                            ));
                            break;
                        }
                        _ => {
                            panic!("Unexpected error {:?}", err);
                        }
                    };
                }
            };

            // Create snapshots for each replicas.
            for j in 0 .. SNAP_CNT {
                let snap_name = format!("r_{i}_snap_{j}");
                let eid = format!("e_{i}_{j}");
                let txn_id = format!("t_{i}_{j}");
                let snap_uuid =
                    format!("55c23e54-dc89-45f6-b55b-e44d{i:04}{j:04}");

                let snap_config = lvol
                    .prepare_snap_config(&snap_name, &eid, &txn_id, &snap_uuid)
                    .unwrap();

                if let Err(err) = lvol.create_snapshot(snap_config).await {
                    match err {
                        LvsError::SnapshotCreate {
                            source, ..
                        } => {
                            assert!(matches!(
                                source,
                                BsError::OutOfMetadata {}
                            ));
                            break;
                        }
                        _ => {
                            panic!("Unexpected error {:?}", err);
                        }
                    }
                }
            }

            println!(
                "Replica #{i} {repl_name} '{repl_uuid}' \
                and its {SNAP_CNT} snapshots has been created"
            );
        }
    })
    .await;
}
