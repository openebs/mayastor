use common::MayastorTest;
use io_engine::{
    core::{MayastorCliArgs, Protocol, Share, UntypedBdev},
    lvs::{Lvs, PropName, PropValue},
    nexus_uri::bdev_create,
    pool::PoolArgs,
    subsys::NvmfSubsystem,
};
use std::pin::Pin;

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static DISKNAME2: &str = "/tmp/disk2.img";

#[tokio::test]
async fn lvs_pool_test() {
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 128 * 1024);
    common::truncate_file(DISKNAME2, 128 * 1024);
    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };
    let ms = MayastorTest::new(args);

    // should fail to import a pool that does not exist on disk
    ms.spawn(async {
        assert!(Lvs::import("tpool", "aio:///tmp/disk1.img").await.is_err())
    })
    .await;

    // should succeed to create a pool we can not import
    ms.spawn(async {
        Lvs::create_or_import(PoolArgs {
            name: "tpool".into(),
            disks: vec!["aio:///tmp/disk1.img".into()],
            uuid: None,
        })
        .await
        .unwrap();
    })
    .await;

    // should fail to create the pool again, notice that we use
    // create directly here to ensure that if we
    // have an idempotent snafu, we dont crash and
    // burn
    ms.spawn(async {
        assert!(Lvs::create("tpool", "aio:///tmp/disk1.img", None)
            .await
            .is_err())
    })
    .await;

    // should fail to import the pool that is already imported
    // similar to above, we use the import directly
    ms.spawn(async {
        assert!(Lvs::import("tpool", "aio:///tmp/disk1.img").await.is_err())
    })
    .await;

    // should be able to find our new LVS
    ms.spawn(async {
        assert_eq!(Lvs::iter().count(), 1);
        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.name(), "tpool");
        assert_eq!(pool.used(), 0);
        dbg!(pool.uuid());
        assert_eq!(pool.base_bdev().name(), "/tmp/disk1.img");
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
        bdev_create("aio:///tmp/disk1.img").await.unwrap();

        assert!(Lvs::import("tpool", "aio:///tmp/disk1.img").await.is_ok());

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

        bdev_create("aio:///tmp/disk1.img").await.unwrap();
        assert!(Lvs::import("tpool", "aio:///tmp/disk1.img").await.is_err());

        assert_eq!(Lvs::iter().count(), 0);
        assert!(Lvs::create("tpool", "aio:///tmp/disk1.img", None)
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
                &format!("vol-{}", i),
                8 * 1024 * 1024,
                None,
                true,
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
        })
        .await
        .unwrap();

        for i in 0 .. 5 {
            pool2
                .create_lvol(
                    &format!("pool2-vol-{}", i),
                    8 * 1024 * 1024,
                    None,
                    false,
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
            disks: vec!["aio:///tmp/disk1.img".to_string()],
            uuid: None,
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
            .create_lvol("vol-1", 1024 * 1024 * 8, None, false)
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
                &format!("vol-{}", i),
                8 * 1024 * 1024,
                None,
                true,
            )
            .await
            .unwrap();
        }

        for mut l in pool.lvols().unwrap() {
            let l = Pin::new(&mut l);
            l.share_nvmf(None).await.unwrap();
        }

        pool.create_lvol("notshared", 8 * 1024 * 1024, None, true)
            .await
            .unwrap();

        pool.export().await.unwrap();
    })
    .await;

    // import the pool all shares should be there, but also validate
    // the share that not shared to be -- not shared
    ms.spawn(async {
        bdev_create("aio:///tmp/disk1.img").await.unwrap();
        let pool = Lvs::import("tpool", "aio:///tmp/disk1.img").await.unwrap();

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
            disks: vec!["aio:///tmp/disk1.img".into()],
            uuid: None,
        })
        .await
        .unwrap();

        assert_eq!(NvmfSubsystem::first().unwrap().into_iter().count(), 1);

        assert_eq!(pool.lvols().unwrap().count(), 0);
        pool.export().await.unwrap();
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
            disks: vec!["aio:///tmp/disk1.img".into()],
            uuid: None,
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
            disks: vec!["/tmp/disk2.img".into()],
            uuid: None,
        })
        .await
        .unwrap();
        assert_eq!(pool.base_bdev().driver(), "aio");
    })
    .await;

    common::delete_file(&[DISKNAME2.into()]);
}
