use common::MayastorTest;
use io_engine::{
    bdev_api::bdev_create,
    core::{
        logical_volume::LogicalVolume,
        MayastorCliArgs,
        Protocol,
        Share,
        UntypedBdev,
    },
    lvs::{Lvs, LvsLvol, PropName, PropValue},
    pool_backend::{PoolArgs, PoolBackend},
    subsys::NvmfSubsystem,
};
use std::pin::Pin;

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";
static DISKNAME1: &str = "/tmp/io-engine-tests/disk1.img";
static DISKNAME2: &str = "/tmp/io-engine-tests/disk2.img";
static DISKNAME3: &str = "/tmp/io-engine-tests/disk3.img";

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
    ]);
    common::truncate_file(DISKNAME1, 128 * 1024);
    common::truncate_file(DISKNAME2, 128 * 1024);
    common::truncate_file(DISKNAME3, 128 * 1024);

    //setup disk3 via loop device using a sector size of 4096.
    let ldev = common::setup_loopdev_file(DISKNAME3, Some(4096));

    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };
    let ms = MayastorTest::new(args);

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
        uuid: None,
        cluster_size: None,
        md_args: None,
        backend: PoolBackend::Lvs,
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
    ms.spawn(async {
        assert!(Lvs::create_from_args_inner(pool_args).await.is_err())
    })
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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
        for i in 0 .. 10 {
            pool.create_lvol(
                &format!("vol-{i}"),
                8 * 1024 * 1024,
                None,
                true,
                None,
            )
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
        })
        .await
        .unwrap();

        for i in 0 .. 5 {
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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

        for i in 0 .. 10 {
            pool.create_lvol(
                &format!("vol-{i}"),
                8 * 1024 * 1024,
                None,
                true,
                None,
            )
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
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
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
        })
        .await
        .unwrap();
        assert_eq!(pool.base_bdev().driver(), "aio");
    })
    .await;

    common::delete_file(&[DISKNAME2.into()]);
    common::detach_loopdev(ldev.as_str());
    common::delete_file(&[DISKNAME3.into()]);
}
