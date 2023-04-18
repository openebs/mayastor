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
    ffihelper::IntoCString,
    lvs::{Lvol, Lvs},
    pool_backend::PoolArgs,
};

use io_engine::lvs::LvsLvol;
use std::convert::TryFrom;

use io_engine::core::{SnapshotDescriptor, SnapshotOps};
use log::info;
use spdk_rs::libspdk::spdk_blob_get_xattr_value;
use std::{ffi::c_void, str};
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
    ];

    // Locate snapshot device.
    let lvol = find_snapshot_device(&params.name().unwrap())
        .await
        .expect("Can't find target snapshot device");

    let blob = lvol.bs_iter_first();

    for (attr_name, attr_value) in attrs {
        unsafe {
            let mut val = std::ptr::null();
            let mut size: u64 = 0;
            let attr_id = attr_name.name().to_string().into_cstring();

            let r = spdk_blob_get_xattr_value(
                blob,
                attr_id.as_ptr(),
                &mut val as *mut *const c_void,
                &mut size as *mut u64,
            );

            assert_eq!(
                r,
                0,
                "No attribute {} exists in snapshot",
                attr_name.name()
            );

            let s = String::from_raw_parts(
                val as *mut u8,
                size as usize,
                size as usize,
            );

            assert_eq!(s, attr_value, "Snapshot attr doesn't match");
        }
    }
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

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        check_snapshot(snapshot_params).await;
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

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("e14");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("snap14");

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = lvol.list_snapshot();
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

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol4_e2");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol4_snap2");

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
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

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol5_e2");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol5_snap2");

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
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
