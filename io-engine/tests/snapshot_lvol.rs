pub mod common;

use once_cell::sync::OnceCell;

use common::compose::MayastorTest;

use io_engine::{
    bdev::device_open,
    core::{
        LogicalVolume,
        MayastorCliArgs,
        SnapshotParams,
        SnapshotXattrs,
        UntypedBdev,
    },
    lvs::{Lvol, Lvs},
    pool_backend::PoolArgs,
};

use chrono::Utc;
use io_engine::core::{
    snapshot::VolumeSnapshotDescriptor,
    SnapshotDescriptor,
    SnapshotOps,
};
use log::info;
use std::{convert::TryFrom, str};
use uuid::Uuid;
static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

/// Get the global Mayastor test suite instance.
fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

/// Must be called only in Mayastor context !s
async fn create_test_pool(pool_name: &str, disk: String) -> Lvs {
    Lvs::create_or_import(PoolArgs {
        name: pool_name.to_string(),
        disks: vec![disk],
        uuid: None,
    })
    .await
    .expect("Failed to create test pool");

    Lvs::lookup(pool_name).expect("Failed to lookup test pool")
}

async fn find_snapshot_device(name: &String) -> Option<Lvol> {
    let bdev = UntypedBdev::bdev_first().expect("Failed to enumerate devices");

    let mut devices = bdev
        .into_iter()
        .filter(|b| b.driver() == "lvol" && b.name() == name)
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .collect::<Vec<Lvol>>();

    assert!(
        devices.len() <= 1,
        "Found more than one snapshot with name '{}'",
        name
    );

    match devices.len() {
        0 => None,
        _ => Some(devices.remove(0)),
    }
}

async fn check_snapshot(params: SnapshotParams) {
    let attrs = [
        (SnapshotXattrs::TxId, params.txn_id().unwrap()),
        (SnapshotXattrs::EntityId, params.entity_id().unwrap()),
        (SnapshotXattrs::ParentId, params.parent_id().unwrap()),
        (
            SnapshotXattrs::SnapshotUuid,
            params.snapshot_uuid().unwrap(),
        ),
    ];

    // Locate snapshot device.
    let lvol = find_snapshot_device(&params.name().unwrap())
        .await
        .expect("Can't find target snapshot device");

    for (attr_name, attr_value) in attrs {
        let v = Lvol::get_blob_xattr(&lvol, &attr_name)
            .expect("Failed to get snapshot attribute");
        assert_eq!(v, attr_value, "Snapshot attr doesn't match");
    }
}

fn check_snapshot_descriptor(
    params: &SnapshotParams,
    descr: &VolumeSnapshotDescriptor,
) {
    let snap_params = descr.snapshot_params();

    assert_eq!(
        params.name().unwrap(),
        snap_params
            .name()
            .expect("Snapshot descriptor has no snapshot name"),
        "Snapshot name doesn't match"
    );

    assert_eq!(
        params.parent_id().unwrap(),
        snap_params
            .parent_id()
            .expect("Snapshot descriptor has no parent ID"),
        "Snapshot parent ID doesn't match"
    );

    assert_eq!(
        params.entity_id().unwrap(),
        snap_params
            .entity_id()
            .expect("Snapshot descriptor has no entity ID"),
        "Snapshot entity ID doesn't match"
    );

    assert_eq!(
        params.snapshot_uuid().unwrap(),
        snap_params
            .snapshot_uuid()
            .expect("Snapshot descriptor has no snapshot UUID"),
        "Snapshot UUID doesn't match"
    );

    assert_eq!(
        params.txn_id().unwrap(),
        snap_params
            .txn_id()
            .expect("Snapshot descriptor has no txn ID"),
        "Snapshot txn ID doesn't match"
    );
    assert_eq!(
        params.create_time().unwrap(),
        snap_params
            .create_time()
            .expect("Snapshot descriptor has no snapshot createtime"),
        "Snapshot CreateTime doesn't match"
    );
}

#[tokio::test]
async fn test_lvol_bdev_snapshot() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool =
            create_test_pool("pool1", "malloc:///disk0?size_mb=64".to_string())
                .await;
        let lvol = pool
            .create_lvol(
                "lvol1",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot via lvol object.
        let entity_id = String::from("e1");
        let parent_id = String::from("p1");
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("snap11");
        let snap_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name.clone()),
            Some(snap_uuid.clone()),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Check blob attributes for snapshot.
        check_snapshot(snapshot_params).await;

        // Check the device UUID mathches requested snapshot UUID.
        let lvol = find_snapshot_device(&snap_name)
            .await
            .expect("Can't find target snapshot device");
        assert_eq!(snap_uuid, lvol.uuid(), "Snapshot UUID doesn't match");
    })
    .await;
}

#[tokio::test]
async fn test_lvol_handle_snapshot() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool =
            create_test_pool("pool2", "malloc:///disk1?size_mb=64".to_string())
                .await;

        pool.create_lvol(
            "lvol2",
            32 * 1024 * 1024,
            Some(&Uuid::new_v4().to_string()),
            false,
        )
        .await
        .expect("Failed to create test lvol");

        // Create a snapshot using device handle directly.
        let descr =
            device_open("lvol2", false).expect("Failed to open volume device");
        let handle = descr
            .into_handle()
            .expect("Failed to get I/O handle for volume device");

        let entity_id = String::from("e1");
        let parent_id = String::from("p1");
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("snap21");
        let snap_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snap_uuid),
            Some(Utc::now().to_string()),
        );

        handle
            .create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create snapshot");

        check_snapshot(snapshot_params).await;
    })
    .await;
}

#[tokio::test]
async fn test_lvol_list_snapshot() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool =
            create_test_pool("pool3", "malloc:///disk3?size_mb=64".to_string())
                .await;
        let lvol = pool
            .create_lvol(
                "lvol3",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("e13");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("snap13");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("e14");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("snap14");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = lvol.list_snapshot_by_source_uuid();
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(2, snapshot_list.len(), "Snapshot Count not matched!!");
    })
    .await;
}

#[tokio::test]
async fn test_list_all_snapshots() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool4",
            "malloc:///disk4?size_mb=128".to_string(),
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol4",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol4_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol4_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol4_e2");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol4_snap2");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // create another lvol and snapshots
        let lvol = pool
            .create_lvol(
                "lvol5",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol5_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol5_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol5_e2");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol5_snap2");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_snapshots();
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(4, snapshot_list.len(), "Snapshot Count not matched!!");
    })
    .await;
}

#[tokio::test]
async fn test_list_pool_snapshots() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool =
            create_test_pool("pool6", "malloc:///disk6?size_mb=32".to_string())
                .await;

        let lvol = pool
            .create_lvol(
                "volume6",
                16 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
            )
            .await
            .expect("Failed to create test lvol");

        // Create the first snapshot.
        let entity_id = String::from("lvol6_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol6_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params1 = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params1.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create the second snapshot.
        let entity_id2 = String::from("lvol6_e2");
        let parent_id2 = lvol.uuid();
        let txn_id2 = Uuid::new_v4().to_string();
        let snap_name2 = String::from("lvol6_snap2");
        let snapshot_uuid2 = Uuid::new_v4().to_string();

        let snapshot_params2 = SnapshotParams::new(
            Some(entity_id2.clone()),
            Some(parent_id2.clone()),
            Some(txn_id2.clone()),
            Some(snap_name2.clone()),
            Some(snapshot_uuid2.clone()),
            Some(Utc::now().to_string()),
        );

        lvol.create_snapshot(snapshot_params2.clone())
            .await
            .expect("Failed to create a snapshot");

        // Check that snapshots are properly reported via pool snapshot
        // iterator.
        let snapshots = pool
            .snapshots()
            .expect("Can't get snapshot iterator for lvol")
            .collect::<Vec<_>>();

        assert_eq!(snapshots.len(), 2, "Not all snapshots are listed");

        let n = snapshots[0]
            .snapshot_params()
            .name()
            .expect("Can't get snapshot name");
        let idxs: [usize; 2] = if n == snap_name2 { [1, 0] } else { [0, 1] };

        // Check that snapshots match their initial parameters.
        check_snapshot_descriptor(&snapshot_params1, &snapshots[idxs[0]]);
        check_snapshot_descriptor(&snapshot_params2, &snapshots[idxs[1]]);

        pool.export().await.expect("Failed to export the pool");
    })
    .await;
}
