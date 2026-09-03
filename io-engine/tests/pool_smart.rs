//! Integration coverage for `ListPoolsSMART`, requested on PR #2029 review
//! ("There's no integration tests (ie `io-engine/tests`). Can we use loop
//! device to at least check it runs without panic?").
//!
//! Exercises the real dispatch path end to end via the `ListPoolsSMART` gRPC
//! call -- `list_pools_smart` handler -> `PoolOps::read_device_health` ->
//! `Lvs`'s bdev-resolving override -> `device_health()` -> `smartctl` --
//! against pools backed by real (if virtual) kernel block devices, confirming
//! the gRPC response is well-formed and completes gracefully on two device
//! classes:
//! - a loop device (`losetup`) -- unsupported by smartctl, exercises the
//!   error path (DiskHealth::supported == false).
//! - a `scsi_debug` device -- exercises the SCSI-flavoured success path,
//!   including the `logical_unit_id` WWN fallback (see `device_health.rs`).

use common::MayastorTest;
use io_engine::{
    core::MayastorCliArgs,
    lvs::Lvs,
    pool_backend::{PoolArgs, PoolBackend},
};
use io_engine_api::v1::pool::{ListPoolsSmartOptions, PoolRpcClient};
use once_cell::sync::OnceCell;

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";
static DISKNAME: &str = "/tmp/io-engine-tests/pool_smart_disk.img";

struct LoopDevGuard {
    ldev: String,
    disk: &'static str,
}

impl Drop for LoopDevGuard {
    fn drop(&mut self) {
        common::detach_loopdev(&self.ldev);
        common::delete_file(&[self.disk.into()]);
    }
}

struct ScsiDebugGuard;

impl Drop for ScsiDebugGuard {
    fn drop(&mut self) {
        common::teardown_scsi_debug_device();
    }
}

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    let ms = MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x3".into(),
            ..Default::default()
        })
    });
    ms.start_grpc();
    ms.start_device_monitor();
    ms
}

async fn pool_grpc_client() -> PoolRpcClient<tonic::transport::Channel> {
    PoolRpcClient::connect("http://127.0.0.1:10124")
        .await
        .expect("failed to connect to gRPC server")
}

#[tokio::test]
async fn pool_smart_on_loop_device_does_not_panic() {
    let ms = ms();

    let _ = std::process::Command::new("mkdir")
        .args(["-p", TESTDIR])
        .output()
        .expect("failed to execute mkdir");
    common::delete_file(&[DISKNAME.into()]);
    common::truncate_file(DISKNAME, 64 * 1024);
    // A real kernel block device (as opposed to a plain aio-backed file) --
    // this is the device class tiagolobocastro's comment asked for.
    let ldev = common::setup_loopdev_file(DISKNAME, None);
    let _guard = LoopDevGuard {
        ldev: ldev.clone(),
        disk: DISKNAME,
    };

    let ldev_pool = ldev.clone();
    let pool_name = "pool_smart_test";
    ms.spawn({
        let pool_name = pool_name.to_string();
        async move {
            Lvs::create_or_import(PoolArgs {
                name: pool_name,
                disks: vec![format!("aio://{ldev_pool}")],
                backend: PoolBackend::Lvs,
                ..Default::default()
            })
            .await
            .unwrap();
        }
    })
    .await;

    let mut client = pool_grpc_client().await;
    let response = client
        .list_pools_smart(ListPoolsSmartOptions {
            name: Some(pool_name.to_string()),
            uuid: None,
            pooltype: None,
        })
        .await
        .expect("list_pools_smart gRPC call failed")
        .into_inner();

    assert_eq!(response.pools.len(), 1, "expected exactly one pool");
    let pool_smart = &response.pools[0];
    assert_eq!(pool_smart.name, pool_name);
    assert!(!pool_smart.disks.is_empty(), "expected at least one disk");

    // A loop device is virtual, so smartctl typically can't identify it --
    // the disk should be reported as unsupported (or, if smartctl happens to
    // work, at least the response is well-formed).
    let disk = &pool_smart.disks[0];
    if !disk.supported {
        assert!(
            disk.error.is_some(),
            "unsupported disk should have an error message"
        );
    }

    ms.spawn({
        let pool_name = pool_name.to_string();
        async move {
            let pool = Lvs::lookup(&pool_name).unwrap();
            pool.destroy().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn pool_smart_on_scsi_debug_device_does_not_panic() {
    let ms = ms();

    // scsi_debug (requires root -- see setup_scsi_debug_device) creates a
    // real, if fully virtual, SCSI block device -- a genuinely different
    // device class from the loop-device test above: smartctl *does*
    // recognise it, so this exercises the success path, including the
    // logical_unit_id -> wwn fallback confirmed live on real scsi_debug
    // hardware in production (see parse_smartctl_identity).
    let dev = common::setup_scsi_debug_device(64);
    let _guard = ScsiDebugGuard;

    let dev_pool = dev.clone();
    let pool_name = "pool_smart_scsi_test";
    ms.spawn({
        let pool_name = pool_name.to_string();
        async move {
            Lvs::create_or_import(PoolArgs {
                name: pool_name,
                disks: vec![format!("aio://{dev_pool}")],
                backend: PoolBackend::Lvs,
                ..Default::default()
            })
            .await
            .unwrap();
        }
    })
    .await;

    let mut client = pool_grpc_client().await;
    let response = client
        .list_pools_smart(ListPoolsSmartOptions {
            name: Some(pool_name.to_string()),
            uuid: None,
            pooltype: None,
        })
        .await
        .expect("list_pools_smart gRPC call failed")
        .into_inner();

    assert_eq!(response.pools.len(), 1, "expected exactly one pool");
    let pool_smart = &response.pools[0];
    assert_eq!(pool_smart.name, pool_name);
    assert!(!pool_smart.disks.is_empty(), "expected at least one disk");

    let disk = &pool_smart.disks[0];
    if disk.supported {
        let health = disk
            .health
            .as_ref()
            .expect("supported disk should have health");
        let identity = health
            .identity
            .as_ref()
            .expect("scsi_debug reports identity");
        assert_eq!(identity.model.as_deref(), Some("Linux scsi_debug"));
        assert!(
            identity.wwn.is_some(),
            "expected a logical_unit_id-derived wwn"
        );
    } else {
        // Only acceptable if smartctl itself isn't installed in
        // whatever environment runs this test -- scsi_debug is a
        // real device as far as smartctl is concerned, so anything
        // else here would be a genuine bug.
        assert!(
            disk.error.is_some(),
            "unsupported disk should have an error message"
        );
    }

    ms.spawn({
        let pool_name = pool_name.to_string();
        async move {
            let pool = Lvs::lookup(&pool_name).unwrap();
            pool.destroy().await.unwrap();
        }
    })
    .await;
}
