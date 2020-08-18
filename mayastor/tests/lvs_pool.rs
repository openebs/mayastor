use std::panic::catch_unwind;

use mayastor::{
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Protocol,
        Reactor,
        Share,
    },
    lvs::{Lvs, PropName, PropValue},
    nexus_uri::bdev_create,
    subsys::NvmfSubsystem,
};
use rpc::mayastor::CreatePoolRequest;

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";

#[test]
fn lvs_pool_test() {
    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();

    let result = catch_unwind(|| {
        MayastorEnvironment::new(args)
            .start(|| {
                // should fail to import a pool that does not exist on disk
                Reactor::block_on(async {
                    assert_eq!(
                        Lvs::import("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_err(),
                        true
                    )
                });

                // should succeed to create a pool we can not import
                Reactor::block_on(async {
                    Lvs::create_or_import(CreatePoolRequest {
                        name: "tpool".into(),
                        disks: vec!["aio:///tmp/disk1.img".into()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();
                });

                // returns OK when the pool is already there and we create
                // it again
                Reactor::block_on(async {
                    assert_eq!(
                        Lvs::create_or_import(CreatePoolRequest {
                            name: "tpool".into(),
                            disks: vec!["aio:///tmp/disk1.img".into()],
                            block_size: 0,
                            io_if: 0,
                        })
                        .await
                        .is_ok(),
                        true
                    )
                });

                // should fail to create the pool again, notice that we use
                // create directly here to ensure that if we
                // have an idempotent snafu, we dont crash and
                // burn
                Reactor::block_on(async {
                    assert_eq!(
                        Lvs::create("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_err(),
                        true
                    )
                });

                // should fail to import the pool that is already imported
                // similar to above, we use the import directly
                Reactor::block_on(async {
                    assert_eq!(
                        Lvs::import("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_err(),
                        true
                    )
                });

                // should be able to find our new LVS
                Reactor::block_on(async {
                    assert_eq!(Lvs::iter().count(), 1);
                    let pool = Lvs::lookup("tpool").unwrap();
                    assert_eq!(pool.name(), "tpool");
                    assert_eq!(pool.used(), 0);
                    dbg!(pool.uuid());
                    assert_eq!(pool.base_bdev().name(), "/tmp/disk1.img");
                });

                // export the pool keeping the bdev alive and then
                // import the pool and validate the uuid

                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    let uuid = pool.uuid();
                    pool.export().await.unwrap();

                    // import and export implicitly destroy the base_bdev, for
                    // testing import and create we
                    // sometimes create the base_bdev manually
                    bdev_create("aio:///tmp/disk1.img").await.unwrap();

                    assert_eq!(
                        Lvs::import("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_ok(),
                        true
                    );

                    let pool = Lvs::lookup("tpool").unwrap();
                    assert_eq!(pool.uuid(), uuid);
                });

                // destroy the pool, a import should now fail, creating a new
                // pool should not having a matching UUID of the
                // old pool
                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    let uuid = pool.uuid();
                    pool.destroy().await.unwrap();

                    bdev_create("aio:///tmp/disk1.img").await.unwrap();
                    assert_eq!(
                        Lvs::import("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_err(),
                        true
                    );

                    assert_eq!(Lvs::iter().count(), 0);
                    assert_eq!(
                        Lvs::create("tpool", "aio:///tmp/disk1.img")
                            .await
                            .is_ok(),
                        true
                    );

                    let pool = Lvs::lookup("tpool").unwrap();
                    assert_ne!(uuid, pool.uuid());
                    assert_eq!(Lvs::iter().count(), 1);
                });

                // create 10 lvol on this pool
                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    for i in 0 .. 10 {
                        pool.create_lvol(&format!("vol-{}", i), 4 * 1024, true)
                            .await
                            .unwrap();
                    }

                    assert_eq!(pool.lvols().unwrap().count(), 10);
                });

                // create a second pool and ensure it filters correctly
                Reactor::block_on(async {
                    let pool2 = Lvs::create_or_import(CreatePoolRequest {
                        name: "tpool2".to_string(),
                        disks: vec!["malloc:///malloc0?size_mb=64".to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();

                    for i in 0 .. 5 {
                        pool2
                            .create_lvol(
                                &format!("pool2-vol-{}", i),
                                4 * 1024,
                                false,
                            )
                            .await
                            .unwrap();
                    }

                    assert_eq!(pool2.lvols().unwrap().count(), 5);

                    let pool = Lvs::lookup("tpool").unwrap();
                    assert_eq!(pool.lvols().unwrap().count(), 10);
                });

                // export the first pool and import it again, all replica's
                // should be present, destroy  all of them by name to
                // ensure they are all there

                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    pool.export().await.unwrap();
                    let pool = Lvs::create_or_import(CreatePoolRequest {
                        name: "tpool".to_string(),
                        disks: vec!["aio:///tmp/disk1.img".to_string()],
                        block_size: 0,
                        io_if: 0,
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
                });

                // share all the replica's on the pool tpool2
                Reactor::block_on(async {
                    let pool2 = Lvs::lookup("tpool2").unwrap();
                    for l in pool2.lvols().unwrap() {
                        l.share_nvmf().await.unwrap();
                    }
                });

                // destroy the pool and verify that all nvmf shares are removed
                Reactor::block_on(async {
                    let p = Lvs::lookup("tpool2").unwrap();
                    p.destroy().await.unwrap();
                    assert_eq!(
                        NvmfSubsystem::first().unwrap().into_iter().count(),
                        1 // only the discovery system remains
                    )
                });

                // test setting the share property that is stored on disk
                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    let lvol = pool
                        .create_lvol("vol-1", 1024 * 4, false)
                        .await
                        .unwrap();

                    lvol.set(PropValue::Shared(true)).await.unwrap();
                    assert_eq!(
                        lvol.get(PropName::Shared).await.unwrap(),
                        PropValue::Shared(true)
                    );

                    lvol.set(PropValue::Shared(false)).await.unwrap();
                    assert_eq!(
                        lvol.get(PropName::Shared).await.unwrap(),
                        PropValue::Shared(false)
                    );

                    // sharing should set the property on disk

                    lvol.share_nvmf().await.unwrap();

                    assert_eq!(
                        lvol.get(PropName::Shared).await.unwrap(),
                        PropValue::Shared(true)
                    );

                    lvol.unshare().await.unwrap();

                    assert_eq!(
                        lvol.get(PropName::Shared).await.unwrap(),
                        PropValue::Shared(false)
                    );

                    lvol.destroy().await.unwrap();
                });

                // create 10 shares, 1 unshared lvol and export the pool
                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();

                    for i in 0 .. 10 {
                        pool.create_lvol(&format!("vol-{}", i), 4 * 1024, true)
                            .await
                            .unwrap();
                    }

                    for l in pool.lvols().unwrap() {
                        l.share_nvmf().await.unwrap();
                    }

                    pool.create_lvol("notshared", 4 * 1024, true)
                        .await
                        .unwrap();

                    pool.export().await.unwrap();
                });

                // import the pool all shares should be there, but also validate
                // the share that not shared to be -- not shared
                Reactor::block_on(async {
                    bdev_create("aio:///tmp/disk1.img").await.unwrap();
                    let pool = Lvs::import("tpool", "aio:///tmp/disk1.img")
                        .await
                        .unwrap();

                    for l in pool.lvols().unwrap() {
                        if l.name() == "notshared" {
                            assert_eq!(l.shared(), None);
                        } else {
                            assert_eq!(l.shared().unwrap(), Protocol::Nvmf);
                        }
                    }

                    assert_eq!(
                        NvmfSubsystem::first().unwrap().into_iter().count(),
                        1 + 10
                    );
                });

                // lastly destroy the pool, import/create it again, no shares
                // should be present
                Reactor::block_on(async {
                    let pool = Lvs::lookup("tpool").unwrap();
                    pool.destroy().await.unwrap();
                    assert_eq!(
                        NvmfSubsystem::first().unwrap().into_iter().count(),
                        1
                    );

                    let pool = Lvs::create_or_import(CreatePoolRequest {
                        name: "tpool".into(),
                        disks: vec!["aio:///tmp/disk1.img".into()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();

                    assert_eq!(
                        NvmfSubsystem::first().unwrap().into_iter().count(),
                        1
                    );

                    assert_eq!(pool.lvols().unwrap().count(), 0);
                    pool.destroy().await.unwrap();
                });

                // validate the expected state of mayastor
                Reactor::block_on(async {
                    // no shares left except for the discovery controller

                    assert_eq!(
                        NvmfSubsystem::first().unwrap().into_iter().count(),
                        1
                    );

                    // all pools destroyed
                    assert_eq!(Lvs::iter().count(), 0);

                    // no bdevs left

                    assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
                });

                mayastor_env_stop(0);
            })
            .unwrap();
    });

    common::delete_file(&[DISKNAME1.into()]);
    result.unwrap();
}
