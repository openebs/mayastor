use common::MayastorTest;
use io_engine::{
    bdev::crypto::{Cipher, EncryptionKey},
    bdev_api::bdev_create,
    core::{
        logical_volume::LogicalVolume, MayastorCliArgs, NvmeCliArgs, NvmfShareProps, PoolCliArgs,
        Protocol, Share, ToErrno, UntypedBdev,
    },
    grpc::v1::pool::pool_to_proto,
    lvs::{Lvs, LvsLvol, PropName, PropValue},
    pool_backend::{PoolArgs, PoolBackend, PoolOps, ReplicaArgs},
    subsys::NvmfSubsystem,
};
use io_engine_api::v1::{
    pool::{Pool, PoolAlert, PoolAlertStatus, PoolAlerts, PoolErrors, PoolState},
    replica::ListReplicaOptions,
};
use once_cell::sync::OnceCell;
use std::pin::Pin;

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";
static DISKNAME1: &str = "/tmp/io-engine-tests/disk1.img";
static DISKNAME2: &str = "/tmp/io-engine-tests/disk2.img";
static DISKNAME3: &str = "/tmp/io-engine-tests/disk3.img";
static DISK_CRYPTO: &str = "/tmp/io-engine-tests/crypto_disk.img";
static XTS_KEY: &str = "2b7e151628aed2a6abf7158809cf4f3c";
static XTS_KEY2: &str = "2b7e151628aed2a6abf7158809cf4f3d";
const IO_ERROR_THRESHOLD: u64 = 5;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x3".into(),
            pool: PoolCliArgs {
                io_error_threshold: IO_ERROR_THRESHOLD,
                ..Default::default()
            },
            nvme: NvmeCliArgs {
                max_namespaces: 8192,
            },
            ..Default::default()
        })
    })
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
        assert_eq!(pool.base_bdev().name(), DISKNAME1);
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

        assert_eq!(pool.lvols().unwrap().count(), 10);
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

        assert_eq!(pool2.lvols().unwrap().count(), 5);

        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.lvols().unwrap().count(), 10);
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

        assert_eq!(pool.lvols().unwrap().count(), 10);

        let df = pool
            .lvols()
            .unwrap()
            .map(|r| r.destroy())
            .collect::<Vec<_>>();
        assert_eq!(df.len(), 10);
        futures::future::join_all(df).await;
    })
    .await;

    // share all the replica's on the pool tpool2
    ms.spawn(async {
        let pool2 = Lvs::lookup("tpool2").unwrap();
        for mut l in pool2.lvols().unwrap() {
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

            lvol.as_mut().unshare().await.unwrap();

            assert_eq!(
                lvol.get(PropName::Shared).await.unwrap(),
                PropValue::Shared(false)
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

        for mut l in pool.lvols().unwrap() {
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

        for l in pool.lvols().unwrap() {
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

        assert_eq!(pool.lvols().unwrap().count(), 0);
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
        assert_eq!(pool.base_bdev().driver(), "aio");
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
        let pool_base_bdev = pool.base_bdev();
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
        assert_eq!(pool.lvols().unwrap().count(), 5);
        let dest = pool
            .lvols()
            .unwrap()
            .map(|r| r.destroy())
            .collect::<Vec<_>>();
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

    let mut lvol = vg_pool
        .create_lvol(ReplicaArgs {
            name: LV_NAME.into(),
            uuid: LV_NAME.into(),
            size: 64 * 1024 * 1024,
            ..Default::default()
        })
        .await
        .unwrap();
    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![format!("aio://{}", lvol.path())],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    let ms = ms();
    ms.spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args).await.unwrap();

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
    })
    .await;

    vg_pool.purge().await.unwrap();
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

    let script = r#"
        set -euo pipefail
        modprobe ublk_drv
        o=$(ublk add -t loop -f $1)
        echo $o | head -n 1 | awk '{print $3}' | tr -d ':'
    "#;
    let args = vec![DISKNAME1.into()];
    let result = run_script::run_script!(script, args, run_script::ScriptOptions::new()).unwrap();
    if result.0 == 1 && result.2.contains("ublk_drv not found") {
        eprint!(" skipped because UBLK kernel module not found, not");
        return;
    }
    assert_eq!(result.0, 0, "Failed to setup ublk device: {result:#?}");

    let ublk_n: u64 = result.1.trim_end().parse().unwrap();
    let ublk_dev = format!("/dev/ublkb{ublk_n}");

    struct TestGuard {
        ublk_n: u64,
    }
    impl Drop for TestGuard {
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
            let out =
                run_script::run_script!(script, args, run_script::ScriptOptions::new()).unwrap();
            if out.0 != 0 {
                eprintln!("TestGuard=>{out:#?}");
            }
            common::delete_file(&[DISKNAME1.into()]);
        }
    }
    let guard = TestGuard { ublk_n };

    let pool_args = PoolArgs {
        name: "tpool".into(),
        disks: vec![ublk_dev],
        backend: PoolBackend::Lvs,
        ..Default::default()
    };

    ms().start_grpc();

    ms().spawn(async move {
        let lvs_pool = Lvs::create_or_import(pool_args).await.unwrap();

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
    })
    .await;
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

    ms.start_grpc();
    ms.start_device_monitor();

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
            for lvol in lvs_pool.lvols().unwrap() {
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
            for mut lvol in lvs_pool.lvols().unwrap() {
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
            for lvol in lvs_pool.lvols().unwrap() {
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
