pub mod common;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    snapshot::ReplicaSnapshotBuilder,
};
use io_engine_tests::{
    file_io::DataSize,
    nvmf::test_write_to_nvmf,
    replica::validate_replicas,
    snapshot::SnapshotCloneBuilder,
};

use once_cell::sync::OnceCell;

use common::{bdev_io, compose::MayastorTest};

use io_engine::{
    bdev::{device_create, device_open},
    core::{
        CloneParams,
        CloneXattrs,
        LogicalVolume,
        MayastorCliArgs,
        SnapshotParams,
        SnapshotXattrs,
        UntypedBdev,
    },
    lvs::{Lvol, Lvs, LvsLvol},
    pool_backend::PoolArgs,
};

use chrono::Utc;
use io_engine::{
    core::{ISnapshotDescriptor, LvolSnapshotOps},
    lvs::LvolSnapshotDescriptor,
    pool_backend::PoolBackend,
};
use log::info;
use std::{convert::TryFrom, str};
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

static POOL_DISK_NAME: &str = "/tmp/disk1.img";
static POOL_DEVICE_NAME: &str = "aio:///tmp/disk1.img";
static LVOL_SIZE: u64 = 24 * 1024 * 1024;
/// Get the global Mayastor test suite instance.
fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

/// Must be called only in Mayastor context !s
async fn create_test_pool(
    pool_name: &str,
    disk: String,
    cluster_size: Option<u32>,
) -> Lvs {
    Lvs::create_or_import(PoolArgs {
        name: pool_name.to_string(),
        disks: vec![disk],
        uuid: None,
        cluster_size,
        md_args: None,
        backend: PoolBackend::Lvs,
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
        let v = Lvol::get_blob_xattr(lvol.blob_checked(), attr_name.name())
            .expect("Failed to get snapshot attribute");
        assert_eq!(v, attr_value, "Snapshot attr doesn't match");
    }
}

async fn check_clone(clone_lvol: Lvol, params: CloneParams) {
    let attrs = [
        (CloneXattrs::SourceUuid, params.source_uuid().unwrap()),
        (
            CloneXattrs::CloneCreateTime,
            params.clone_create_time().unwrap(),
        ),
        (CloneXattrs::CloneUuid, params.clone_uuid().unwrap()),
    ];
    for (attr_name, attr_value) in attrs {
        let v =
            Lvol::get_blob_xattr(clone_lvol.blob_checked(), attr_name.name())
                .expect("Failed to get clone attribute");
        assert_eq!(v, attr_value, "clone attr doesn't match");
    }
}

async fn clean_snapshots(snapshot_list: Vec<LvolSnapshotDescriptor>) {
    for snapshot in snapshot_list {
        let snap_lvol = UntypedBdev::lookup_by_uuid_str(
            &snapshot
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        snap_lvol
            .destroy()
            .await
            .expect("Failed to destroy Snapshot");
    }
}

async fn test_lvol_alloc_after_snapshot(index: u32, thin: bool) {
    let ms = get_ms();

    let pool_name = format!("pool_{index}");
    let disk = format!("malloc:///disk{index}?size_mb=64");
    let lvol_name = format!("lvol_{index}");

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(&pool_name, disk, None).await;
        let cluster_size = pool.blob_cluster_size();
        let lvol = pool
            .create_lvol(
                &lvol_name,
                LVOL_SIZE,
                Some(&Uuid::new_v4().to_string()),
                thin,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1
        let entity_id = format!("{lvol_name}_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = format!("{lvol_name}_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );
        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");
        // Original Lvol Size to be 0 after snapshot is created.
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Volume still has some space allocated after taking a snapshot"
        );
        // Write some data to original lvol.
        bdev_io::write_some(&lvol_name, 0, 16, 0xccu8)
            .await
            .expect("Failed to write data to volume");

        assert_eq!(
            lvol.usage().allocated_bytes,
            cluster_size,
            "Volume still has some space allocated after taking a snapshot"
        );

        // Create a snapshot-2 after io done to lvol.
        let entity_id = format!("{lvol_name}_e2");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = format!("{lvol_name}_snap2");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );
        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        // Original Lvol Size to be 0 after snapshot is created.
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Volume still has some space allocated after taking a snapshot"
        );
    })
    .await;
}

fn check_snapshot_descriptor(
    params: &SnapshotParams,
    descr: &LvolSnapshotDescriptor,
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
        let pool = create_test_pool(
            "pool1",
            "malloc:///disk0?size_mb=64".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol1",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
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
            false,
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
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        clean_snapshots(snapshot_list).await;
    })
    .await;
}

#[tokio::test]
async fn test_lvol_handle_snapshot() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool2",
            "malloc:///disk1?size_mb=64".to_string(),
            None,
        )
        .await;

        pool.create_lvol(
            "lvol2",
            32 * 1024 * 1024,
            Some(&Uuid::new_v4().to_string()),
            false,
            None,
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
            false,
        );

        handle
            .create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create snapshot");

        check_snapshot(snapshot_params).await;
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        clean_snapshots(snapshot_list).await;
    })
    .await;
}

#[tokio::test]
async fn test_lvol_list_snapshot() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool3",
            "malloc:///disk3?size_mb=64".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol3",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
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
            false,
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
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let mut snapshot_list = lvol.list_snapshot_by_source_uuid();
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(2, snapshot_list.len(), "Snapshot Count not matched!!");
        lvol.destroy()
            .await
            .expect("Failed to destroy the original replica");
        let snap_lvol_1 = snapshot_list.remove(0).snapshot;
        let snap_lvol_2 = snapshot_list.remove(0).snapshot;
        snap_lvol_1
            .destroy_snapshot()
            .await
            .expect("Failed to destroy first snapshot");
        assert!(
            snap_lvol_2.is_snapshot(),
            "It is a snapshot, wrongly recognized as normal replica"
        );
        snap_lvol_2
            .destroy_snapshot()
            .await
            .expect("Failed to destroy last snapshot");
    })
    .await;
}

#[tokio::test]
async fn test_list_all_lvol_snapshots() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool4",
            "malloc:///disk4?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol4",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
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
            false,
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
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_lvol_snapshots(Some(&lvol));
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(2, snapshot_list.len(), "Snapshot Count not matched!!");

        // create another lvol and snapshots
        let lvol = pool
            .create_lvol(
                "lvol5",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");
        let snapshot_list = Lvol::list_all_lvol_snapshots(Some(&lvol));
        assert_eq!(0, snapshot_list.len(), "Snapshot Count not matched!!");

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
            false,
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
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(4, snapshot_list.len(), "Snapshot Count not matched!!");
        clean_snapshots(snapshot_list).await;
    })
    .await;
}

#[tokio::test]
async fn test_list_pool_snapshots() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool6",
            "malloc:///disk6?size_mb=32".to_string(),
            None,
        )
        .await;

        let lvol = pool
            .create_lvol(
                "volume6",
                16 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
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
            false,
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
            false,
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
        clean_snapshots(snapshots).await;
        pool.export().await.expect("Failed to export the pool");
    })
    .await;
}

#[tokio::test]
async fn test_list_all_lvol_snapshots_with_replica_destroy() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool7",
            "malloc:///disk7?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol7",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol7_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol7_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        lvol.destroy().await.expect("Failed to destroy replica");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        info!("Total number of snapshots: {}", snapshot_list.len());
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");
        clean_snapshots(snapshot_list).await;
    })
    .await;
}
#[tokio::test]
async fn test_snapshot_referenced_size() {
    let ms = get_ms();
    const LVOL_NAME: &str = "lvol8";

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool8",
            "malloc:///disk8?size_mb=64".to_string(),
            Some(1024 * 1024),
        )
        .await;

        let cluster_size = pool.blob_cluster_size();
        assert_eq!(cluster_size, 1024 * 1024, "Create cluster size doesn't match with blob cluster size");
        let lvol = pool
            .create_lvol(
                LVOL_NAME,
                LVOL_SIZE,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Thick-provisioned volume, all blob clusters must be pre-allocated.
        assert_eq!(
            lvol.usage().allocated_bytes,
            LVOL_SIZE,
            "Wiped superbock is not properly accounted in volume allocated bytes"
        );

        /* Scenario 1: create a snapshot for a volume without any data written:
         * snapshot size must be equal to the initial volume size and current
         * size of the volume must be zero.
         * Note: initially volume is thick-provisioned, so snapshot shall own
         * all volume's data.
         */
        let snap1_name = "lvol8_snapshot1".to_string();
        let mut snapshot_params = SnapshotParams::new(
            Some("e1".to_string()),
            Some("p1".to_string()),
            Some(Uuid::new_v4().to_string()),
            Some(snap1_name.clone()),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");

        // Make sure snapshot fully owns initial volume data.
        let snapshot_list = pool.snapshots().expect("Failed to enumerate poool snapshots").collect::<Vec<_>>();
        assert_eq!(snapshot_list.len(), 1, "No first snapshot found");
        assert_eq!(
            snapshot_list[0].snapshot_size,
            LVOL_SIZE,
            "Snapshot size doesn't properly reflect wiped superblock"
        );

        let snap_lvol = find_snapshot_device(&snap1_name)
            .await
            .expect("Can't lookup snapshot lvol");
        assert_eq!(
            snap_lvol.usage().allocated_bytes,
            LVOL_SIZE,
            "Snapshot size doesn't properly reflect wiped superblock"
        );

        // Make sure volume has no allocated space after snapshot is taken.
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Volume still has some space allocated after taking a snapshot"
        );

        /* Scenario 2: write some data to volume at 2nd cluster, take the second snapshot
         * and make sure snapshot size reflects the amount of data written (aligned by
         * the size of the blobstore cluster).
         * Note: volume is now a thin-provisioned volume, so the volume stores only incremental
         * differences from its underlying snapshot.
         */
        bdev_io::write_some(LVOL_NAME, 2 * cluster_size, 16, 0xaau8)
            .await
            .expect("Failed to write data to volume");

        bdev_io::write_some(LVOL_NAME, 3 * cluster_size, 16, 0xbbu8)
            .await
            .expect("Failed to write data to volume");

        // Make sure volume has exactly one allocated cluster even if a smaller amount of bytes was written.
        assert_eq!(
            lvol.usage().allocated_bytes,
            2 * cluster_size,
            "Volume still has some space allocated after taking a snapshot"
        );

        let snap2_name = "lvol8_snapshot2".to_string();
        snapshot_params.set_name(snap2_name.clone());
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the second snapshot for test volume");

        let snapshot_list = pool.snapshots().expect("Failed to enumerate poool snapshots").collect::<Vec<_>>();
        assert_eq!(snapshot_list.len(), 2, "Not all snapshots found");
        let snap_lvol = snapshot_list.iter().find(|s| {
            s.snapshot_params().name().expect("Snapshot has no name") == snap2_name
        })
        .expect("No second snapshot found");

        // Volume size should be zero.
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Volume still has some space allocated after taking a snapshot"
        );

        // Make sure snapshot owns newly written volume data.
        assert_eq!(
            snap_lvol.snapshot_size,
            2 * cluster_size,
            "Snapshot size doesn't properly reflect new volume data"
        );

        let snap_lvol = find_snapshot_device(&snap2_name)
            .await
            .expect("Can't lookup snapshot lvol");
        assert_eq!(
            snap_lvol.usage().allocated_bytes,
            2 * cluster_size,
            "Snapshot size doesn't properly reflect wiped superblock"
        );

        // Write some data to the volume and make sure volume accounts only
        // new incremental storage difference (1 cluster).
        bdev_io::write_some(LVOL_NAME, 0, 16, 0xccu8)
            .await
            .expect("Failed to write data to volume");

        assert_eq!(
            lvol.usage().allocated_bytes,
            cluster_size,
            "Volume still has some space allocated after taking a snapshot"
        );

        // Make sure snapshots allocated space hasn't changed.
        let snap_lvol = find_snapshot_device(&snap2_name)
            .await
            .expect("Can't lookup snapshot lvol");
        assert_eq!(
            snap_lvol.usage().allocated_bytes,
            2 * cluster_size,
            "Snapshot size doesn't properly reflect wiped superblock"
        );

        // If snapshot is created from clone, the allocated bytes for these
        // snapshot will not carry size calculation from parent snapshot of clone.

        let clone_name = String::from("lvol8_snap2_clone_1");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snap_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone1 = snap_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");
        check_clone(clone1.clone(), clone_param).await;

        bdev_io::write_some("lvol8_snap2_clone_1", 0, 16, 0xccu8)
            .await
            .expect("Failed to write data to volume");

        let clone_1_snapshot1 = "lvol8_clone_1_snapshot1".to_string();
        snapshot_params.set_name(clone_1_snapshot1.clone());
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());
        snapshot_params.set_parent_id(clone1.uuid());
        clone1.clone().create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the second snapshot for test volume");
        let clone_snap_lvol = find_snapshot_device(&clone_1_snapshot1)
        .await
        .expect("Can't lookup snapshot lvol");
        assert_eq!(
            clone_snap_lvol.usage().allocated_bytes_snapshot_from_clone.unwrap_or_default(),
            0,
            "Clone Snapshot allocated size should not include snapshot created from the original replica before clone"
        );
        let mut total_clone_snapshot_alloc = clone_snap_lvol.usage().allocated_bytes;
        bdev_io::write_some("lvol8_snap2_clone_1", 0, 16, 0xccu8)
            .await
            .expect("Failed to write data to volume");

        let clone_1_snapshot2 = "lvol8_clone_1_snapshot2".to_string();
        snapshot_params.set_name(clone_1_snapshot2.clone());
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());
        snapshot_params.set_parent_id(clone1.uuid());
        clone1.clone().create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the second snapshot for test volume");
        let clone_snap_lvol = find_snapshot_device(&clone_1_snapshot2)
            .await
            .expect("Can't lookup snapshot lvol");
        total_clone_snapshot_alloc += clone_snap_lvol.usage().allocated_bytes;
        assert_eq!(
            clone_snap_lvol.usage().allocated_bytes_snapshot_from_clone.unwrap_or_default(),
            cluster_size,
            "Clone Snapshot allocated size should not include snapshot created from the original replica before clone"
        );
        assert_eq!(
            total_clone_snapshot_alloc,
            clone1.usage().allocated_bytes_snapshot_from_clone.unwrap_or_default(),
            "Clone Snapshot allocated size should not include snapshot created from the original replica before clone"
        );
    })
    .await;
}
#[tokio::test]
async fn test_snapshot_clone() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool9",
            "malloc:///disk5?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol9",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a snapshot-1 via lvol object.
        let entity_id = String::from("lvol9_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol9_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");
        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        let clone_name = String::from("lvol9_snap1_clone_1");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone1 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");
        check_clone(clone1, clone_param).await;

        let clone_name = String::from("lvol9_snap1_clone_2");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone2 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");
        check_clone(clone2, clone_param).await;
        info!(
            "Total number of Clones: {:?}",
            snapshot_lvol.list_clones_by_snapshot_uuid().len()
        );
        let clones = snapshot_lvol.list_clones_by_snapshot_uuid();

        assert_eq!(clones.len(), 2, "Number of Clones Doesn't match");
        for clone in &clones {
            assert!(
                clone.is_snapshot_clone().is_some(),
                "Wrongly judge as not a clone"
            );
        }
        assert!(lvol.is_snapshot_clone().is_none(), "Wrongly judge as clone");
        assert!(
            snapshot_lvol.is_snapshot_clone().is_none(),
            "Wrongly judge as clone"
        );
        for clone in clones {
            clone.destroy().await.expect("destroy clone failed");
        }
        clean_snapshots(Lvol::list_all_lvol_snapshots(None)).await;
    })
    .await;
}

#[tokio::test]
async fn test_snapshot_volume_provisioning_mode() {
    let ms = get_ms();
    const LVOL_NAME: &str = "lvol10";

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool10",
            "malloc:///disk10?size_mb=64".to_string(),
            None,
        )
        .await;

        let lvol = pool
            .create_lvol(
                LVOL_NAME,
                LVOL_SIZE,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        let snap1_name = "lvol10_snapshot1".to_string();
        let snapshot_params = SnapshotParams::new(
            Some("e1".to_string()),
            Some("p1".to_string()),
            Some(Uuid::new_v4().to_string()),
            Some(snap1_name.clone()),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
            false,
        );

        // Volume must be reported as thick-provisioned before taking a snapshot.
        assert!(!lvol.is_thin(), "Volume is reported as thin-provisioned before taking a snapshot");

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");

        // Volume must be reported as thin provisioned after taking a snapshot.
        assert!(lvol.is_thin(), "Volume is not reported as thin-provisioned after taking a snapshot");
    })
    .await;
}
#[tokio::test]
async fn test_thin_provision_lvol_alloc_after_snapshot() {
    const IDX: u32 = 11;
    test_lvol_alloc_after_snapshot(IDX, true).await;
}

#[tokio::test]
async fn test_thick_provision_lvol_alloc_after_snapshot() {
    const IDX: u32 = 12;
    test_lvol_alloc_after_snapshot(IDX, false).await;
}

#[tokio::test]
async fn test_snapshot_attr() {
    let ms = get_ms();

    common::delete_file(&[POOL_DISK_NAME.into()]);
    common::truncate_file(POOL_DISK_NAME, 128 * 1024);

    ms.spawn(async move {
        // Create a pool and lvol.
        let mut pool =
            create_test_pool("pool20", POOL_DEVICE_NAME.into(), None).await;
        let lvol = pool
            .create_lvol(
                "lvol20",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a test snapshot.
        let entity_id = String::from("lvol20_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol20_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let mut snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");
        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();

        // Set snapshot attribute.
        let snap_attr_name = String::from("my.attr.name");
        let snap_attr_value = String::from("top_secret");

        snapshot_lvol
            .set_blob_attr(
                snap_attr_name.clone(),
                snap_attr_value.clone(),
                true,
            )
            .await
            .expect("Failed to set snapshot attribute");

        // Check attribute.
        let v =
            Lvol::get_blob_xattr(snapshot_lvol.blob_checked(), &snap_attr_name)
                .expect("Failed to get snapshot attribute");
        assert_eq!(v, snap_attr_value, "Snapshot attribute doesn't match");

        // Export pool, then reimport it again and check the attribute again.
        pool.export().await.expect("Failed to export test pool");

        // Make sure no snapshots exist after pool is exported.
        assert_eq!(
            Lvol::list_all_lvol_snapshots(None).len(),
            0,
            "Snapshots still exist after pool was exported"
        );

        // Recreate the pool device, as pool export destroys it.
        device_create(POOL_DEVICE_NAME).await.unwrap();

        pool = Lvs::import("pool20", POOL_DEVICE_NAME)
            .await
            .expect("Failed to import pool");

        snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(
            1,
            snapshot_list.len(),
            "No snapshots found after pool imported"
        );

        let imported_snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();

        // Get attribute from imported snapshot and check.
        let v = Lvol::get_blob_xattr(
            imported_snapshot_lvol.blob_checked(),
            &snap_attr_name,
        )
        .expect("Failed to get snapshot attribute");
        assert_eq!(v, snap_attr_value, "Snapshot attribute doesn't match");
        clean_snapshots(snapshot_list).await;
        pool.destroy().await.expect("Failed to destroy test pool");
    })
    .await;
}
#[tokio::test]
async fn test_delete_snapshot_with_valid_clone() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool13",
            "malloc:///disk13?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol13",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None
            )
            .await
            .expect("Failed to create test lvol");

        // Create a test snapshot.
        let entity_id = String::from("lvol13_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol13_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");

        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();

        let clone_name = String::from("lvol13_snap1_clone_1");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone1 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");

        let clone_name = String::from("lvol13_snap1_clone_2");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone2 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");

        snapshot_lvol.destroy_snapshot().await.ok();
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        assert!(
            snapshot_lvol.is_discarded_snapshot(),
            "Snapshot discardedSnapshotFlag not set properly"
        );
        clone1.destroy_replica().await.expect("Clone1 Destroy Failed");
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(
            1,
            snapshot_list.len(),
            "Snapshot should not be deleted as part single clone deletion"
        );
        clone2.destroy_replica().await.expect("Clone2 Destroy Failed");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(
            0,
            snapshot_list.len(),
            "Snapshot marked as deleted not deleted after last linked clone is destroyed"
        );
    })
    .await;
}

#[tokio::test]
async fn test_delete_snapshot_with_valid_clone_fail_1() {
    let ms = get_ms();

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool14",
            "malloc:///disk14?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                "lvol14",
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                false,
                None,
            )
            .await
            .expect("Failed to create test lvol");

        // Create a test snapshot.
        let entity_id = String::from("lvol14_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol14_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create a snapshot");

        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");

        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();

        let clone_name = String::from("lvol14_snap1_clone_1");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone1 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");

        snapshot_lvol.destroy_snapshot().await.ok();
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        assert!(
            snapshot_lvol.is_discarded_snapshot(),
            "Snapshot discardedSnapshotFlag not set properly"
        );
        clone1.destroy().await.expect("Clone1 Destroy Failed");
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(
            1,
            snapshot_list.len(),
            "Snapshot should not be destroyed, if fault happened after clone deletion"
        );
        Lvol::destroy_pending_discarded_snapshot().await;
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(
            0,
            snapshot_list.len(),
            "After clone destroy failure retry, snapshot is not destroyed"
        );
    })
    .await;
}

#[tokio::test]
async fn test_snapshot_verify_restore_data() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
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

    let ms = conn.grpc_handle_shared("ms").await.unwrap();

    const POOL_SIZE: u64 = 200;
    const REPL_SIZE: u64 = 40;

    let mut pool = PoolBuilder::new(ms.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);
    let mut repl_1 = ReplicaBuilder::new(ms.clone())
        .with_pool(&pool)
        .with_name("repl1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);
    // Create pool.
    pool.create().await.unwrap();
    // Create replica.
    repl_1.create().await.unwrap();
    // Share replica.
    repl_1.share().await.unwrap();
    // Write some data to replica.
    test_write_to_nvmf(
        &repl_1.nvmf_location(),
        DataSize::from_bytes(0),
        30,
        DataSize::from_mb(1),
    )
    .await
    .unwrap();
    // Create snapshot for the replica.
    let mut snap_1 = ReplicaSnapshotBuilder::new(ms.clone())
        .with_replica_uuid(repl_1.uuid().as_str())
        .with_snapshot_uuid()
        .with_snapshot_name("snap1")
        .with_entity_id("snap1_e1")
        .with_txn_id("snap1-t1");
    snap_1.create_replica_snapshot().await.unwrap();

    // Create a clone from the replica.
    let mut clone_1 = SnapshotCloneBuilder::new(ms.clone())
        .with_snapshot_uuid(snap_1.snapshot_uuid().as_str())
        .with_clone_name("clone1")
        .with_clone_uuid(Uuid::new_v4().to_string().as_str());
    clone_1.create_snapshot_clone().await.unwrap();

    // Create restore object.
    let mut restore_1 = ReplicaBuilder::new(ms.clone())
        .with_uuid(clone_1.clone_uuid().as_str())
        .with_name(clone_1.clone_name().as_str());

    restore_1.share().await.unwrap();

    // Check the original replica and restore clone is identical.
    validate_replicas(&vec![repl_1.clone(), restore_1.clone()]).await;
}

#[tokio::test]
async fn test_snapshot_parent_usage_post_snapshot_destroy() {
    let ms = get_ms();
    const LVOL_NAME: &str = "lvol16";

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool16",
            "malloc:///disk16?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                LVOL_NAME,
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                true,
                None,
            )
            .await
            .expect("Failed to create test lvol");
        let cluster_size = pool.blob_cluster_size();
        // Create a test snapshot.
        let entity_id = String::from("lvol16_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol16_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        bdev_io::write_some(LVOL_NAME, 2 * cluster_size, 16, 0xaau8)
            .await
            .expect("Failed to write data to volume");
        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");

        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Source Lvol size should be 0, after snapshot created from it"
        );

        bdev_io::write_some(LVOL_NAME, 3 * cluster_size, 16, 0xbbu8)
            .await
            .expect("Failed to write data to volume");
        bdev_io::write_some(LVOL_NAME, 4 * cluster_size, 16, 0xccu8)
            .await
            .expect("Failed to write data to volume");
        snapshot_lvol
            .destroy()
            .await
            .expect("Destroy snapshot failed");
        assert_eq!(
            lvol.usage().allocated_bytes,
            5 * cluster_size,
            "Source Lvol size should be restored after snapshot destroy"
        );
    })
    .await;
}

#[tokio::test]
async fn test_clone_snapshot_usage_post_clone_destroy() {
    let ms = get_ms();
    const LVOL_NAME: &str = "lvol17";

    ms.spawn(async move {
        // Create a pool and lvol.
        let pool = create_test_pool(
            "pool17",
            "malloc:///disk17?size_mb=128".to_string(),
            None,
        )
        .await;
        let lvol = pool
            .create_lvol(
                LVOL_NAME,
                32 * 1024 * 1024,
                Some(&Uuid::new_v4().to_string()),
                true,
                None,
            )
            .await
            .expect("Failed to create test lvol");
        let cluster_size = pool.blob_cluster_size();
        // Create a test snapshot.
        let entity_id = String::from("lvol17_e1");
        let parent_id = lvol.uuid();
        let txn_id = Uuid::new_v4().to_string();
        let snap_name = String::from("lvol17_snap1");
        let snapshot_uuid = Uuid::new_v4().to_string();

        let mut snapshot_params = SnapshotParams::new(
            Some(entity_id),
            Some(parent_id),
            Some(txn_id),
            Some(snap_name),
            Some(snapshot_uuid),
            Some(Utc::now().to_string()),
            false,
        );

        bdev_io::write_some(LVOL_NAME, 2 * cluster_size, 16, 0xaau8)
            .await
            .expect("Failed to write data to volume");
        lvol.create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");
        let snapshot_list = Lvol::list_all_lvol_snapshots(None);
        assert_eq!(1, snapshot_list.len(), "Snapshot Count not matched!!");

        let snapshot_lvol = UntypedBdev::lookup_by_uuid_str(
            snapshot_list
                .get(0)
                .unwrap()
                .snapshot_params()
                .snapshot_uuid()
                .unwrap_or_default()
                .as_str(),
        )
        .map(|b| Lvol::try_from(b).expect("Can't create Lvol from device"))
        .unwrap();
        assert_eq!(
            lvol.usage().allocated_bytes,
            0,
            "Source Lvol size should be 0, after snapshot created from it"
        );

        let clone_name = String::from("lvol17_snap1_clone_1");
        let clone_uuid = Uuid::new_v4().to_string();
        let source_uuid = snapshot_lvol.uuid();

        let clone_param = CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        );
        let clone1 = snapshot_lvol
            .create_clone(clone_param.clone())
            .await
            .expect("Failed to create a clone");
        bdev_io::write_some("lvol17_snap1_clone_1", 0, 16, 0xbbu8)
            .await
            .expect("Failed to write data to volume");
        snapshot_params.set_parent_id(clone1.uuid());
        snapshot_params.set_entity_id(String::from("lvol17_clone1_e1"));
        snapshot_params.set_name(String::from("lvol17_clone_1_snap1"));
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());
        snapshot_params.set_txn_id(Uuid::new_v4().to_string());

        clone1
            .create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");
        snapshot_params.set_parent_id(clone1.uuid());
        snapshot_params.set_entity_id(String::from("lvol17_clone1_e2"));
        snapshot_params.set_name(String::from("lvol17_clone_1_snap2"));
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());
        snapshot_params.set_txn_id(Uuid::new_v4().to_string());
        bdev_io::write_some(
            "lvol17_snap1_clone_1",
            3 * cluster_size,
            16,
            0xbbu8,
        )
        .await
        .expect("Failed to write data to volume");
        clone1
            .create_snapshot(snapshot_params.clone())
            .await
            .expect("Failed to create the first snapshot for test volume");
        let snapshots = clone1.list_snapshot_by_source_uuid();
        let mut clone_snapshot =
            snapshots.iter().map(|v| v.snapshot()).collect::<Vec<_>>();
        lvol.destroy()
            .await
            .expect("Original replica destroy failed");
        clone1
            .destroy_replica()
            .await
            .expect("Destroy Clone failed");
        assert_eq!(
            clone_snapshot.len(),
            2,
            "Number of Clone Snapshot not matched"
        );
        assert_eq!(
            clone_snapshot.remove(0).usage().allocated_bytes,
            cluster_size,
            "Clone1 snap1 cache is not cleared"
        );
        assert_eq!(
            clone_snapshot.remove(0).usage().allocated_bytes,
            cluster_size,
            "Clone1 snap2 cache is not cleared"
        );
        clean_snapshots(Lvol::list_all_lvol_snapshots(None)).await;
    })
    .await;
}
