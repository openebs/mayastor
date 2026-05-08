use common::MayastorTest;
use futures::channel::oneshot;
use io_engine::{
    bdev::crypto::{Cipher, EncryptionKey},
    bdev_api::bdev_create,
    core::{
        logical_volume::LogicalVolume, CoreError, MayastorCliArgs, PoolCliArgs, Protocol, Reactors,
        Share, ToErrno, UnshareProps, UntypedBdev,
    },
    grpc::v1::pool::pool_to_proto,
    lvm::dm_setup::DmState,
    lvs::{Lvs, LvsLvol, PropName, PropValue},
    pool_backend::{PoolArgs, PoolBackend, PoolOps, ReplicaArgs},
    subsys::NvmfSubsystem,
};
use io_engine_api::v1::pool::{
    Pool, PoolAlert, PoolAlertStatus, PoolAlerts, PoolErrors, PoolState,
};
use once_cell::sync::OnceCell;
use std::{pin::Pin, time::Duration};

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";
static DISKNAME1: &str = "/tmp/io-engine-tests/disk1.img";
static DISKNAME2: &str = "/tmp/io-engine-tests/disk2.img";
static DISKNAME3: &str = "/tmp/io-engine-tests/disk3.img";
static DISK_CRYPTO: &str = "/tmp/io-engine-tests/crypto_disk.img";
static XTS_KEY: &str = "2b7e151628aed2a6abf7158809cf4f3c";
static XTS_KEY2: &str = "2b7e151628aed2a6abf7158809cf4f3d";
const IO_ERROR_THRESHOLD: u64 = 5;
const IO_STALL_TRANSITION_WINDOW: Duration = Duration::from_secs(6);
const IO_STALL_DEADLINE: Duration = Duration::from_secs(1);
const IO_STALL_TRANSITION_THRESHOLD: u64 = 3;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    let ms = MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x3".into(),
            pool: PoolCliArgs {
                io_error_threshold: IO_ERROR_THRESHOLD,
                io_stall_transition_threshold: IO_STALL_TRANSITION_THRESHOLD,
                io_stall_transition_window: IO_STALL_TRANSITION_WINDOW.into(),
                io_stall_deadline: IO_STALL_DEADLINE.into(),
            },
            // log_components: vec!["all".into()],
            ..Default::default()
        })
    });
    ms.start_grpc();
    ms.start_device_monitor();
    ms
}

#[tokio::test]
async fn lvs_pool_test() {
    // Create directory for placing test disk files
    // todo: Create this from some common place and use for all other tests too.
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    common::delete_file(&[
        DISKNAME1.into(),
        DISKNAME2.into(),
        DISKNAME3.into(),
        DISK_CRYPTO.into(),
    ]);
    common::truncate_file(DISKNAME1, 128 * 1024);
    common::truncate_file(DISKNAME2, 128 * 1024);
    common::truncate_file(DISKNAME3, 128 * 1024);
    common::truncate_file(DISK_CRYPTO, 128 * 1024);

    //setup disk3 via loop device using a sector size of 4096.
    let ldev = common::setup_loopdev_file(DISKNAME3, Some(4096));

    let ms = ms();

    // should fail to import a pool that does not exist on disk
    ms.spawn(async {
        assert!(Lvs::import("tpool", format!("aio://{DISKNAME1}").as_str())
            .await
            .is_err())
    })
    .await;

    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![format!("aio://{DISKNAME1}")],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    // should succeed to create a pool we can not import
    ms.spawn({
        let pool_args = pool_args.clone();
        async {
            Lvs::create_or_import(pool_args).await.unwrap();
        }
    })
    .await;

    // should fail to create the pool again, notice that we use
    // create directly here to ensure that if we
    // have an idempotent snafu, we dont crash and
    // burn
    ms.spawn(async { assert!(Lvs::create_from_args_inner(pool_args).await.is_err()) })
        .await;

    // should fail to import the pool that is already imported
    // similar to above, we use the import directly
    ms.spawn(async {
        assert!(Lvs::import("tpool", format!("aio://{DISKNAME1}").as_str())
            .await
            .is_err())
    })
    .await;

    // should be able to find our new LVS
    ms.spawn(async {
        assert_eq!(Lvs::iter().count(), 1);
        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.name(), "tpool");
        assert_eq!(pool.used(), 0);
        dbg!(pool.uuid());
        assert_eq!(pool.base_bdev_().name(), DISKNAME1);
    })
    .await;

    // export the pool keeping the bdev alive and then
    // import the pool and validate the uuid

    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        let uuid = pool.uuid();
        pool.export().await.unwrap();

        // import and export implicitly destroy the base_bdev, for
        // testing import and create we
        // sometimes create the base_bdev manually
        bdev_create(format!("aio://{DISKNAME1}").as_str())
            .await
            .unwrap();

        assert!(Lvs::import("tpool", format!("aio://{DISKNAME1}").as_str())
            .await
            .is_ok());

        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.uuid(), uuid);
    })
    .await;

    // destroy the pool, a import should now fail, creating a new
    // pool should not having a matching UUID of the
    // old pool
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        let uuid = pool.uuid();
        pool.destroy().await.unwrap();

        bdev_create(format!("aio://{DISKNAME1}").as_str())
            .await
            .unwrap();
        assert!(Lvs::import("tpool", format!("aio://{DISKNAME1}").as_str())
            .await
            .is_err());

        assert_eq!(Lvs::iter().count(), 0);
        assert!(Lvs::create_from_args_inner(PoolArgs {
            name: "tpool".to_string(),
            disks: vec![format!("aio://{DISKNAME1}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .is_ok());

        let pool = Lvs::lookup("tpool").unwrap();
        assert_ne!(uuid, pool.uuid());
        assert_eq!(Lvs::iter().count(), 1);
    })
    .await;

    // create 10 lvol on this pool
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        for i in 0..10 {
            pool.create_lvol(&format!("vol-{i}"), 8 * 1024 * 1024, None, true, None)
                .await
                .unwrap();
        }

        assert_eq!(pool.lvols().count(), 10);
    })
    .await;

    // create a second pool and ensure it filters correctly
    ms.spawn(async {
        let pool2 = Lvs::create_or_import(PoolArgs {
            name: "tpool2".to_string(),
            disks: vec!["malloc:///malloc0?size_mb=64".to_string()],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();

        for i in 0..5 {
            pool2
                .create_lvol(
                    &format!("pool2-vol-{i}"),
                    8 * 1024 * 1024,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();
        }

        assert_eq!(pool2.lvols().count(), 5);

        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.lvols().count(), 10);
    })
    .await;

    // export the first pool and import it again, all replica's
    // should be present, destroy  all of them by name to
    // ensure they are all there

    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        pool.export().await.unwrap();
        let pool = Lvs::create_or_import(PoolArgs {
            name: "tpool".to_string(),
            disks: vec![format!("aio://{DISKNAME1}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(pool.lvols().count(), 10);

        let df = pool.lvols().map(|r| r.destroy()).collect::<Vec<_>>();
        assert_eq!(df.len(), 10);
        futures::future::join_all(df).await;
    })
    .await;

    // share all the replica's on the pool tpool2
    ms.spawn(async {
        let pool2 = Lvs::lookup("tpool2").unwrap();
        for mut l in pool2.lvols() {
            Pin::new(&mut l).share_nvmf(None).await.unwrap();
        }
    })
    .await;

    // destroy the pool and verify that all nvmf shares are removed
    ms.spawn(async {
        let p = Lvs::lookup("tpool2").unwrap();
        p.destroy().await.unwrap();
        assert_eq!(
            NvmfSubsystem::first().unwrap().into_iter().count(),
            1 // only the discovery system remains
        )
    })
    .await;

    // test setting the share property that is stored on disk
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        let mut lvol = pool
            .create_lvol("vol-1", 1024 * 1024 * 8, None, false, None)
            .await
            .unwrap();

        {
            let mut lvol = Pin::new(&mut lvol);

            lvol.as_mut().set(PropValue::Shared(true)).await.unwrap();
            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(true)
            );

            lvol.as_mut().set(PropValue::Shared(false)).await.unwrap();
            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(false)
            );

            // sharing should set the property on disk

            lvol.as_mut().share_nvmf(None).await.unwrap();

            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(true)
            );

            lvol.as_mut().unshare(None).await.unwrap();

            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(false)
            );

            // sharing without persisting

            lvol.as_mut().share_nvmf(None).await.unwrap();

            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(true)
            );

            lvol.as_mut()
                .unshare(Some(UnshareProps::new(false)))
                .await
                .unwrap();

            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(true)
            );
        }

        lvol.destroy().await.unwrap();
    })
    .await;

    // create 10 shares, 1 unshared lvol and export the pool
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();

        for i in 0..10 {
            pool.create_lvol(&format!("vol-{i}"), 8 * 1024 * 1024, None, true, None)
                .await
                .unwrap();
        }

        for mut l in pool.lvols() {
            let l = Pin::new(&mut l);
            l.share_nvmf(None).await.unwrap();
        }

        pool.create_lvol("notshared", 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();

        pool.export().await.unwrap();
    })
    .await;

    // import the pool all shares should be there, but also validate
    // the share that not shared to be -- not shared
    ms.spawn(async {
        bdev_create(format!("aio://{DISKNAME1}").as_str())
            .await
            .unwrap();
        let pool = Lvs::import("tpool", format!("aio://{DISKNAME1}").as_str())
            .await
            .unwrap();

        for l in pool.lvols() {
            if l.name() == "notshared" {
                assert_eq!(l.shared().unwrap(), Protocol::Off);
            } else {
                assert_eq!(l.shared().unwrap(), Protocol::Nvmf);
            }
        }

        assert_eq!(NvmfSubsystem::first().unwrap().into_iter().count(), 1 + 10);
    })
    .await;

    // lastly destroy the pool, import/create it again, no shares
    // should be present
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        pool.destroy().await.unwrap();
        assert_eq!(NvmfSubsystem::first().unwrap().into_iter().count(), 1);

        let pool = Lvs::create_or_import(PoolArgs {
            name: "tpool".into(),
            disks: vec![format!("aio://{DISKNAME1}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(NvmfSubsystem::first().unwrap().into_iter().count(), 1);

        assert_eq!(pool.lvols().count(), 0);
        pool.export().await.unwrap();
    })
    .await;

    let pool_dev_aio = ldev.clone();
    // should succeed to create an aio bdev pool on a loop blockdev of 4096
    // bytes sector size.
    ms.spawn(async move {
        Lvs::create_or_import(PoolArgs {
            name: "tpool_4k_aio".into(),
            disks: vec![format!("aio://{pool_dev_aio}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();
    })
    .await;

    // should be able to find our new LVS created on loopdev, and subsequently
    // destroy it.
    ms.spawn(async {
        let pool = Lvs::lookup("tpool_4k_aio").unwrap();
        assert_eq!(pool.name(), "tpool_4k_aio");
        assert_eq!(pool.used(), 0);
        dbg!(pool.uuid());
        pool.destroy().await.unwrap();
    })
    .await;

    let pool_dev_uring = ldev.clone();
    // should succeed to create an uring pool on a loop blockdev of 4096 bytes
    // sector size.
    ms.spawn(async move {
        Lvs::create_or_import(PoolArgs {
            name: "tpool_4k_uring".into(),
            disks: vec![format!("uring://{pool_dev_uring}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();
    })
    .await;

    // should be able to find our new LVS created on loopdev, and subsequently
    // destroy it.
    ms.spawn(async {
        let pool = Lvs::lookup("tpool_4k_uring").unwrap();
        assert_eq!(pool.name(), "tpool_4k_uring");
        assert_eq!(pool.used(), 0);
        dbg!(pool.uuid());
        pool.destroy().await.unwrap();
    })
    .await;

    // validate the expected state of mayastor
    ms.spawn(async {
        // no shares left except for the discovery controller

        assert_eq!(NvmfSubsystem::first().unwrap().into_iter().count(), 1);

        // all pools destroyed
        assert_eq!(Lvs::iter().count(), 0);

        // no bdevs left

        assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);

        // importing a pool with the wrong name should fail
        Lvs::create_or_import(PoolArgs {
            name: "jpool".into(),
            disks: vec![format!("aio://{DISKNAME1}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .err()
        .unwrap();
    })
    .await;

    common::delete_file(&[DISKNAME1.into()]);

    // if not specified, default driver scheme should be AIO
    ms.spawn(async {
        let pool = Lvs::create_or_import(PoolArgs {
            name: "tpool2".into(),
            disks: vec![format!("aio://{DISKNAME2}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(pool.base_bdev_().driver(), "aio");
    })
    .await;

    common::delete_file(&[DISKNAME2.into()]);
    common::detach_loopdev(ldev.as_str());
    common::delete_file(&[DISKNAME3.into()]);

    // Create an encrypted pool
    ms.spawn(async {
        let pool = Lvs::create_or_import(PoolArgs {
            name: "enc_pool".into(),
            disks: vec![format!("aio://{DISK_CRYPTO}")],
            enc_key: Some(EncryptionKey {
                cipher: Cipher::AesXts,
                key_name: "test_key".into(),
                key: XTS_KEY.into(),
                key_len: 128,
                key2: Some(XTS_KEY2.into()),
                key2_len: Some(128),
            }),
            crypto_vbdev_name: Some("crypto_enc_pool".into()),
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .unwrap();
        let pool_base_bdev = pool.base_bdev_();
        assert_eq!(pool_base_bdev.driver(), "crypto");
        let underlying_bdev = pool_base_bdev.crypto_base_bdev().unwrap();
        // we internally use diskname as aio bdev name.
        assert_eq!(underlying_bdev.name(), DISK_CRYPTO);

        // create some replicas on encrypted pool
        let pool = Lvs::lookup("enc_pool").unwrap();
        for i in 0..5 {
            pool.create_lvol(&format!("encvol-{i}"), 8 * 1024 * 1024, None, true, None)
                .await
                .unwrap();
        }
        assert_eq!(pool.lvols().count(), 5);
        let dest = pool.lvols().map(|r| r.destroy()).collect::<Vec<_>>();
        assert_eq!(dest.len(), 5);
        futures::future::join_all(dest).await;
        pool.destroy().await.unwrap();
        common::delete_file(&[DISK_CRYPTO.into()]);
    })
    .await;
}

#[tokio::test]
async fn lvs_errors() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 128 * 1024);

    const VG_NAME: &str = "vg-1";
    const LV_NAME: &str = "lvol1";

    struct TestGuard {
        loop_dev: Option<String>,
    }
    impl Drop for TestGuard {
        fn drop(&mut self) {
            if let Some(loop_dev) = self.loop_dev.take() {
                let script = r#"
                    export LVM_SUPPRESS_FD_WARNINGS=1
                    vgremove -f $2
                    pvremove -f $3
                "#;
                let args = vec![DISKNAME1.into(), VG_NAME.into(), loop_dev.clone()];
                run_script::run_script!(script, args, run_script::ScriptOptions::new()).ok();
                common::detach_loopdev(&loop_dev);
            }
        }
    }
    let mut guard = TestGuard { loop_dev: None };

    //setup disk1 via loop device using a sector size of 4096.
    let ldev = common::setup_loopdev_file(DISKNAME1, Some(4096));
    guard.loop_dev = Some(ldev.clone());

    let vg_pool = io_engine::lvm::VolumeGroup::create(PoolArgs {
        name: VG_NAME.into(),
        disks: vec![ldev.clone()],
        no_spdk: true,
        ..Default::default()
    })
    .await
    .unwrap();

    let glvol = vg_pool
        .create_lvol(ReplicaArgs {
            name: LV_NAME.into(),
            uuid: LV_NAME.into(),
            size: 64 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();
    let gpool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![format!("aio://{}", glvol.path())],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };
    let pool_args = gpool_args.clone();
    let mut lvol = glvol.clone();

    let ms: &MayastorTest<'_> = ms();
    ms.spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args.clone()).await.unwrap();

        // We bork the device, making it return -EIO for all I/O
        let table = lvol.bork().await.unwrap();

        for i in 1..=IO_ERROR_THRESHOLD {
            let error = lvs_pool
                .create_lvol("fail", 8 * 1024 * 1024, None, true, None)
                .await
                .expect_err("error due to -EIO");
            println!("error: {error}");
            assert_eq!(error.to_errno(), nix::Error::EIO);

            if i < IO_ERROR_THRESHOLD {
                let (pool, errors, alerts) = pool_info(&lvs_pool).await;

                assert_eq!(pool.state(), PoolState::PoolOnline);
                assert_eq!(errors.io_error_count, i);
                assert_eq!(alerts.attention, vec![PoolAlert::IoError as i32]);
                assert_eq!(alerts.status(), PoolAlertStatus::Attention);
            }
        }

        let (pool, errors, alerts) = pool_info(&lvs_pool).await;
        assert_eq!(pool.state(), PoolState::PoolSuspected);
        assert_eq!(errors.io_error_count, IO_ERROR_THRESHOLD);
        assert_eq!(alerts.warning, vec![PoolAlert::IoErrorExc as i32]);
        assert_eq!(alerts.status(), PoolAlertStatus::Warning);

        lvol.unbork(table).await.unwrap();

        let repl = lvs_pool
            .create_lvol("ok", 8 * 1024 * 1024, None, true, None)
            .await
            .expect("now we can create it");

        println!("repl: {}", repl.uuid());

        lvs_pool.reset_errors().await.unwrap();

        let (pool, errors, alerts) = pool_info(&lvs_pool).await;
        assert_eq!(pool.state(), PoolState::PoolOnline);
        assert_eq!(errors.io_error_count, 0);
        assert_eq!(
            alerts.notice.len()
                + alerts.attention.len()
                + alerts.warning.len()
                + alerts.critical.len(),
            0
        );
        assert_eq!(alerts.status(), PoolAlertStatus::Healthy);

        lvs_pool.destroy().await.unwrap();
        lvol
    })
    .await;

    let pool_args = gpool_args;
    let mut lvol = glvol;
    ms.spawn(async move {
        // We bork the device, making it return -EIO for all I/O
        let table = lvol.bork().await.unwrap();

        let result = Lvs::create_or_import(pool_args.clone())
            .await
            .expect_err("EIO");
        assert_eq!(result.to_errno(), nix::Error::EIO, "{result:#?}");

        let result = Lvs::import_from_args(pool_args.clone())
            .await
            .expect_err("EIO");
        assert_eq!(result.to_errno(), nix::Error::EIO, "{result:#?}");

        let backend = UntypedBdev::lookup_by_name(lvol.path());
        assert!(backend.is_none(), "Disk Bdev should have been cleaned up");

        lvol.unbork(table).await.unwrap();

        let lvs = Lvs::create_or_import(pool_args.clone()).await.unwrap();

        let table = lvol.bork().await.unwrap();

        let result = lvs.export().await.expect_err("EIO");
        assert_eq!(result.to_errno(), nix::Error::EIO, "{result:#?}");

        lvol.unbork(table).await.unwrap();

        let lvs = Lvs::create_or_import(pool_args.clone()).await.unwrap();

        let table = lvol.bork().await.unwrap();

        let result = lvs.destroy().await.expect_err("EIO");
        assert_eq!(result.to_errno(), nix::Error::EIO, "{result:#?}");

        lvol.unbork(table).await.unwrap();
    })
    .await;

    vg_pool.purge().await.unwrap();
}

#[tokio::test]
async fn lvs_stall() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 128 * 1024);

    const VG_NAME: &str = "vg-1";

    struct TestGuard {
        loop_dev: Option<String>,
    }
    impl Drop for TestGuard {
        fn drop(&mut self) {
            if let Some(loop_dev) = self.loop_dev.take() {
                let script = r#"
                    export LVM_SUPPRESS_FD_WARNINGS=1
                    dmsetup resume $2/lvol1
                    dmsetup resume $2/lvol2
                    lvremove -f vg-1/lvol1
                    lvremove -f vg-1/lvol2
                    vgremove -f -y $2
                    pvremove -f $3
                "#;
                let args = vec![DISKNAME2.into(), VG_NAME.into(), loop_dev.clone()];
                run_script::run_script!(script, args, run_script::ScriptOptions::new()).ok();
                common::detach_loopdev(&loop_dev);
            }
        }
    }
    let mut guard = TestGuard { loop_dev: None };

    //setup disk1 via loop device using a sector size of 4096.
    let ldev = common::setup_loopdev_file(DISKNAME1, Some(4096));
    guard.loop_dev = Some(ldev.clone());

    let vg_pool = io_engine::lvm::VolumeGroup::create(PoolArgs {
        name: VG_NAME.into(),
        disks: vec![ldev.clone()],
        no_spdk: true,
        ..Default::default()
    })
    .await
    .unwrap();

    let vg_cln = vg_pool.clone();
    ms().spawn_detached(async move {
        stall_test(vg_cln, StallBdev::Aio).await;
    });
    stall_test(vg_pool, StallBdev::Uring).await;
}

enum StallBdev {
    Aio,
    Uring,
}
impl StallBdev {
    fn lv_name(&self) -> &'static str {
        match self {
            StallBdev::Aio => "lvm-lv-aio",
            StallBdev::Uring => "lvm-lv-uring",
        }
    }
    const fn pool_name(&self) -> &'static str {
        match self {
            StallBdev::Aio => "lvs-aio",
            StallBdev::Uring => "lvs-uring",
        }
    }
    const fn repl_name(&self) -> &'static str {
        match self {
            StallBdev::Aio => "lvs-lv-aio",
            StallBdev::Uring => "lvs-lv-uring",
        }
    }
    fn disk_uri(&self, path: &str) -> String {
        match self {
            StallBdev::Aio => format!("aio://{path}"),
            StallBdev::Uring => format!("uring://{path}"),
        }
    }
}

async fn stall_test(vg: io_engine::lvm::VolumeGroup, bdev: StallBdev) {
    let mut lvol = vg
        .create_lvol(ReplicaArgs {
            name: bdev.lv_name().into(),
            uuid: bdev.lv_name().into(),
            size: 32 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();

    let pool_n: &'static str = bdev.pool_name();
    let repl: &'static str = bdev.repl_name();
    let pool_args = PoolArgs {
        name: pool_n.into(),
        disks: vec![bdev.disk_uri(lvol.path())],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    let ms = ms();
    ms.spawn(async move {
        let lvs = Lvs::create_or_import(pool_args).await.unwrap();
        let _repl = lvs
            .create_lvol(repl, 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();
    })
    .await;

    async fn sleep(duration: Duration) {
        _ = io_engine::sleep::mayastor_sleep(duration).await;
    }

    for i in 1..(IO_STALL_TRANSITION_THRESHOLD + 1) {
        let state = lvol.dm_suspend().await.unwrap();

        assert_eq!(state, DmState::Suspended);

        // Will write to all these reactor cores
        let cores = 2;
        let mut io_completions = vec![];
        for core in 0..cores {
            write_reactor(repl, core, io_completions.as_mut());
        }

        sleep(IO_STALL_DEADLINE).await;

        // staggard write I/O
        write_reactor(repl, cores - 1, io_completions.as_mut());

        sleep(IO_STALL_DEADLINE).await;

        let (pool, _errors, alerts) = ms
            .spawn(async move { pool_info(&Lvs::lookup(pool_n).unwrap()).await })
            .await;

        assert_eq!(pool.state(), PoolState::PoolSuspected);
        assert_eq!(alerts.critical, vec![PoolAlert::IoStalled as i32]);
        assert_eq!(alerts.status(), PoolAlertStatus::Critical);

        let state = lvol.dm_resume().await.unwrap();
        assert_eq!(state, DmState::Active);

        // Backend device is active, I/Os should now complete inline here...
        let result = ms
            .spawn(async move { common::bdev_io::write_some(repl, 8192, 1, 0xbb).await })
            .await;
        assert!(result.is_ok(), "I/O: {:?}", result);
        // Original stalled I/Os should also have completed!
        for (i, r) in io_completions.into_iter().enumerate() {
            let result = r.await.unwrap();
            assert!(result.is_ok(), "I/O[{i}] => {:?}", result);
        }

        if 1 < i && i < IO_STALL_TRANSITION_THRESHOLD {
            let (pool, errors, alerts) = ms
                .spawn(async move { pool_info(&Lvs::lookup(pool_n).unwrap()).await })
                .await;

            assert_eq!(pool.state(), PoolState::PoolOnline);
            assert_eq!(errors.io_stall_transition_count, i);
            assert_eq!(
                alerts.attention,
                vec![PoolAlert::IoStallIntermittent as i32]
            );
            assert_eq!(alerts.status(), PoolAlertStatus::Attention);
        }
    }

    ms.spawn(async move {
        let lvs = Lvs::lookup(pool_n).unwrap();
        let (pool, errors, alerts) = pool_info(&lvs).await;
        assert_eq!(pool.state(), PoolState::PoolSuspected);
        assert_eq!(
            errors.io_stall_transition_count,
            IO_STALL_TRANSITION_THRESHOLD
        );
        assert_eq!(
            alerts.warning,
            vec![PoolAlert::IoStallIntermittentExc as i32]
        );
        assert_eq!(alerts.status(), PoolAlertStatus::Warning);
    })
    .await;

    tracing::info!("Waiting for stall transition window of {IO_STALL_TRANSITION_WINDOW:?}");
    sleep(IO_STALL_TRANSITION_WINDOW).await;

    ms.spawn(async move {
        let lvs = Lvs::lookup(pool_n).unwrap();
        let (pool, errors, _alerts) = pool_info(&lvs).await;
        assert_eq!(pool.state(), PoolState::PoolOnline);
        assert_eq!(errors.io_stall_transition_count, 0);
    })
    .await;

    ms.spawn(async move {
        let lvs = Lvs::lookup(pool_n).unwrap();
        lvs.destroy().await.unwrap();
    })
    .await;
}

fn write_reactor(
    repl: &'static str,
    core: u64,
    io_completions: &mut Vec<oneshot::Receiver<Result<(), CoreError>>>,
) {
    let reactor = Reactors::get_by_core(core as u32).unwrap();
    let (s, r) = oneshot::channel();
    reactor.send_future(async move {
        tracing::info!("Writing I/O to {repl} on reactor #{core}");
        let res = common::bdev_io::write_some(repl, core * 4096, 1, 0xaa).await;
        tracing::info!("Completed I/O to {repl} on reactor #{core} => {res:?}");
        s.send(res).unwrap();
    });
    io_completions.push(r);
}

async fn pool_info(pool: &dyn PoolOps) -> (Pool, PoolErrors, PoolAlerts) {
    let pool = pool_to_proto(pool).await;
    let errors = pool.errors.clone().unwrap();
    let alerts = errors.alerts.clone().unwrap_or_default();
    (pool, errors, alerts)
}

#[tokio::test]
async fn lvs_hot_remove() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 128 * 1024);

    let Some(guard) = TestHotRmGuard::new(DISKNAME1) else {
        return;
    };

    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![guard.ublk_dev.clone()],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    ms().spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args.clone()).await.unwrap();

        // NOTE: There's currently an issue when we destroy a replica manually and at the same time the hot-removal
        // is happening. In this case looks like the check for no lvols is done before the callbacks for this replica
        // destroy attempt completes, and so we end up with a stuck lvs which needs to be retried again.
        // To make matters worse, the bdev is removing, so it's not returned by `vbdev_get_lvs_bdev_by_lvs` leaving
        // us with no base bdev, and no way to determine if we need tear down of the bdevs behind the base...
        // Creating this dud will allow it's closure to trigger proper lvs unload...
        let _dud = lvs_pool
            .create_lvol("dud", 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();
        let mut _dud2 = lvs_pool
            .create_lvol("dud2", 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();
        Pin::new(&mut _dud2).share_nvmf(None).await.unwrap();

        let repl = lvs_pool
            .create_lvol("ok", 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();

        // We bork the device, leading to hot-removal
        drop(guard);

        let error = lvs_pool
            .create_lvol("fail", 8 * 1024 * 1024, None, true, None)
            .await
            .expect_err("-EIO");
        println!("repl: {error:#?}");
        assert_eq!(error.to_errno(), nix::Error::EIO);

        let error = repl.destroy().await.expect_err("-EIO");
        assert_eq!(error.to_errno(), nix::Error::EIO);

        lvs_pool.destroy().await.ok();

        assert_eq!(Lvs::iter_all().count(), 1);
        assert_eq!(Lvs::iter().count(), 0);

        let error = Lvs::create_or_import(pool_args.clone())
            .await
            .expect_err("removing");
        assert_eq!(error.to_errno(), nix::Error::EINPROGRESS);

        for _ in 0..10 {
            if Lvs::iter_all().count() == 0 {
                break;
            }
            io_engine::sleep::mayastor_sleep(std::time::Duration::from_millis(100))
                .await
                .unwrap();
        }

        assert_eq!(Lvs::iter_all().count(), 0);
        assert_eq!(Lvs::iter().count(), 0);

        let mut pool_args = pool_args;
        pool_args.disks = vec![format!("aio://{DISKNAME1}?blk_size=4096")];
        let lvs = Lvs::create_or_import(pool_args).await.expect("removed");

        let _repl = lvs
            .create_lvol("new", 8 * 1024 * 1024, None, true, None)
            .await
            .unwrap();

        lvs.destroy().await.unwrap();

        common::delete_file(&[DISKNAME1.into()]);
    })
    .await;
}

#[tokio::test]
async fn lvs_hot_detach_and_reattach() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    let mk_guard = || TestHotRmGuard::new(DISKNAME1);
    let Some(guard) = mk_guard() else {
        return;
    };
    hot_detach_retach(guard, true, true).await;
    hot_detach_retach(mk_guard().unwrap(), true, false).await;
    hot_detach_retach(mk_guard().unwrap(), false, true).await;
    hot_detach_retach(mk_guard().unwrap(), false, false).await;
}

struct TestHotRmGuard {
    ublk_n: u64,
    ublk_dev: String,
}
impl Drop for TestHotRmGuard {
    fn drop(&mut self) {
        let script = r#"
            if ! ublk del -n $1 --async; then
                echo "Ublk delete async not supported..."
                ublk del -n $1 &
                PID=$!
                sleep 1
                kill $PID || kill -9 $PID
            fi
        "#;
        let args = vec![self.ublk_n.to_string()];
        let out = run_script::run_script!(script, args, run_script::ScriptOptions::new()).unwrap();
        if out.0 != 0 {
            eprintln!("TestGuard=>{out:#?}");
        }
    }
}
impl TestHotRmGuard {
    fn new(disk: &str) -> Option<Self> {
        common::delete_file(&[disk.into()]);
        common::truncate_file(disk, 128 * 1024);

        let script = r#"
        set -euo pipefail
        modprobe ublk_drv
        o=$(ublk add -t loop -f $1)
        echo $o | head -n 1 | awk '{print $3}' | tr -d ':'
    "#;
        let args = vec![disk.into()];
        let result =
            run_script::run_script!(script, args, run_script::ScriptOptions::new()).unwrap();
        if result.0 == 1 && result.2.contains("ublk_drv not found") {
            match std::env::var("CI")
                .unwrap_or_default()
                .to_lowercase()
                .as_str()
            {
                "1" | "true" => {
                    panic!("UBLK kernel module not found in CI environment!!");
                }
                _ => {
                    eprint!(" skipped because UBLK kernel module not found");
                    return None;
                }
            }
        }
        assert_eq!(result.0, 0, "Failed to setup ublk device: {result:#?}");

        let ublk_n: u64 = result.1.trim_end().parse().unwrap();
        let ublk_dev = format!("/dev/ublkb{ublk_n}");

        Some(TestHotRmGuard { ublk_n, ublk_dev })
    }
}

async fn hot_detach_retach(guard: TestHotRmGuard, share: bool, io: bool) {
    println!("\n\n\nhot_detach_retach with {share}/{io}\n\n\n");

    ms().spawn(async move {
        hot_detach_retach_(guard, share, io).await;
    })
    .await;
}

async fn hot_detach_retach_(guard: TestHotRmGuard, share: bool, io: bool) {
    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![guard.ublk_dev.clone()],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    let lvs_pool = Lvs::create_or_import(pool_args.clone()).await.unwrap();

    // NOTE: There's currently an issue when we destroy a replica manually and at the same time the hot-removal
    // is happening. In this case looks like the check for no lvols is done before the callbacks for this replica
    // destroy attempt completes, and so we end up with a stuck lvs which needs to be retried again.
    // To make matters worse, the bdev is removing, so it's not returned by `vbdev_get_lvs_bdev_by_lvs` leaving
    // us with no base bdev, and no way to determine if we need tear down of the bdevs behind the base...
    // Creating this dud will allow it's closure to trigger proper lvs unload...
    let _dud = lvs_pool
        .create_lvol("dud", 8 * 1024 * 1024, None, true, None)
        .await
        .unwrap();
    let mut dud2 = lvs_pool
        .create_lvol("dud2", 8 * 1024 * 1024, None, true, None)
        .await
        .unwrap();
    if share {
        Pin::new(&mut dud2).share_nvmf(None).await.unwrap();
    }
    let uri = dud2.share_uri().unwrap();

    if io {
        common::bdev_io::write_some("dud2", 0, 2, 0xaa)
            .await
            .unwrap();
    }

    // we create a nexus with a handle open for the nexus via nvmf
    let ch = vec!["malloc:///d?size=100MiB&blk_size=4096".into(), uri.clone()];
    io_engine::bdev::nexus::nexus_create("nx", 1024 * 1024, None, &ch)
        .await
        .unwrap();

    // We bork the device, leading to hot-removal
    drop(guard);

    let error = lvs_pool.grow().await.expect_err("msg");
    assert_eq!(error.to_errno(), nix::Error::EIO);

    assert_eq!(Lvs::iter_all().count(), 1);

    for _ in 0..500 {
        if Lvs::iter_all().count() == 0 {
            break;
        }
        io_engine::sleep::mayastor_sleep(std::time::Duration::from_millis(10))
            .await
            .unwrap();
    }

    assert_eq!(Lvs::iter_all().count(), 0);
    assert_eq!(Lvs::iter().count(), 0);

    {
        let nx = io_engine::bdev::nexus::nexus_lookup_mut("nx").unwrap();

        tracing::error!("{:#?}", nx.children());
        let nx = nx.into_grpc().await;
        tracing::error!("{nx:#?}");
    }

    let mut pool_args = pool_args;
    pool_args.disks = vec![format!("aio://{DISKNAME1}?blk_size=4096")];

    let lvs = Lvs::import_from_args(pool_args.clone())
        .await
        .expect("re-attach");

    let start = std::time::Instant::now();
    loop {
        let nexus = io_engine::bdev::nexus::nexus_lookup_mut("nx").unwrap();
        let nexus = nexus.into_grpc().await;
        let child = nexus.children.iter().find(|c| c.uri == uri).unwrap();

        if child.state() == io_engine_api::v1::nexus::ChildState::Faulted {
            tracing::error!("{nexus:#?}");
            break;
        }

        if start.elapsed().as_secs() > 2 {
            tracing::error!("{nexus:#?}");
            panic!("Child not in correct state");
        }

        io_engine::sleep::mayastor_sleep(std::time::Duration::from_millis(10))
            .await
            .unwrap();
    }

    let start = std::time::Instant::now();
    loop {
        let mut nexus = io_engine::bdev::nexus::nexus_lookup_mut("nx").unwrap();
        let result = nexus.as_mut().online_child(&uri).await;
        let nexus = nexus.into_grpc().await;

        if result.is_ok() {
            tracing::error!("{nexus:#?}");
            break;
        }

        if start.elapsed().as_secs() > 1 {
            panic!("Child not in correct state: {:#?}", nexus);
        }

        io_engine::sleep::mayastor_sleep(std::time::Duration::from_millis(10))
            .await
            .unwrap();
    }

    let nexus = io_engine::bdev::nexus::nexus_lookup_mut("nx").unwrap();
    nexus.destroy().await.unwrap();
    lvs.destroy().await.unwrap();
}
