pub mod common;

use io_engine::{
    core::{logical_volume::LogicalVolume, LvolSnapshotOps, MayastorCliArgs},
    lvs::Lvs,
    pool_backend::PoolArgs,
    replica_backend::ReplicaOps,
};

use io_engine_tests::MayastorTest;

use once_cell::sync::OnceCell;
use std::{collections::HashSet, time::Instant};

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
async fn lvs_import_many_volume() {
    const REPL_CNT: u64 = 100;
    const SNAP_CNT: u64 = 10;

    common::composer_init();

    common::delete_file(&[DISK_NAME.to_string()]);
    common::truncate_file_bytes(DISK_NAME, DISK_SIZE * 1024 * 1024);

    let ms = get_ms();

    ms.spawn(async {
        // Set of UUIDs of successfully created volumes (replicas and
        // snapshots).
        let mut created: HashSet<String> = HashSet::new();

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

            let lvol = lvs
                .create_lvol(
                    &repl_name,
                    REPL_SIZE * 1024 * 1024,
                    Some(&repl_uuid),
                    true,
                    None,
                )
                .await
                .unwrap();

            created.insert(repl_name.clone());

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

                lvol.create_snapshot(snap_config).await.unwrap();
                created.insert(snap_name.clone());
            }

            println!(
                "Replica #{i} {repl_name} '{repl_uuid}' \
                and its {SNAP_CNT} snapshots has been created"
            );
        }

        println!(
            "{REPL_CNT} replicas x {SNAP_CNT} snapshots = {t} \
            volumes have been created",
            t = REPL_CNT * SNAP_CNT
        );

        // Export the LVS.
        println!("Exporting ...");
        let t = Instant::now();
        lvs.export().await.unwrap();
        println!("Exported in {d} ms", d = t.elapsed().as_millis());

        // Import the LVS.
        println!("Importing ...");
        let t = Instant::now();
        let lvs = Lvs::create_or_import(lvs_args).await.unwrap();
        println!("Imported in {d} ms", d = t.elapsed().as_millis());

        // Check that all volumes were properly imported.
        let mut imported = HashSet::new();
        lvs.lvols().unwrap().for_each(|v| {
            imported.insert(v.name());
        });

        let diff = created.difference(&imported).collect::<Vec<_>>();

        assert!(
            diff.is_empty(),
            "Some volumes were not properly imported: {:?}",
            diff
        );

        let diff = imported.difference(&created).collect::<Vec<_>>();

        assert!(
            diff.is_empty(),
            "Some additional volumes were wrongly imported: {:?}",
            diff
        );
    })
    .await;
}
