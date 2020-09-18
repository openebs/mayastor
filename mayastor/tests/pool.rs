use std::panic::catch_unwind;

use mayastor::{
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        Share,
    },
    pool::{create_pool, Pool, PoolsIter},
};
use rpc::mayastor::CreatePoolRequest;

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";

#[test]
fn create_pool_legacy() {
    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();

    let result = catch_unwind(|| {
        MayastorEnvironment::new(args)
            .start(|| {
                // create a pool with legacy device names
                Reactor::block_on(async {
                    create_pool(CreatePoolRequest {
                        name: "legacy".into(),
                        disks: vec![DISKNAME1.to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();
                });

                // create a pool using uri's
                Reactor::block_on(async {
                    create_pool(CreatePoolRequest {
                        name: "uri".into(),
                        disks: vec!["malloc:///malloc0?size_mb=64".to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();
                });

                // should succeed to create the same pool with the same name and
                // with the same bdev (idempotent)

                Reactor::block_on(async {
                    let pool = create_pool(CreatePoolRequest {
                        name: "uri".into(),
                        disks: vec!["malloc:///malloc0?size_mb=64".to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await;

                    assert_eq!(pool.is_ok(), true);
                });

                // should fail to create the pool with same name and different
                // bdev
                Reactor::block_on(async {
                    let pool = create_pool(CreatePoolRequest {
                        name: "uri".into(),
                        disks: vec!["malloc:///malloc1?size_mb=64".to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await;
                    assert_eq!(pool.is_err(), true)
                });

                // validate some properties from the pool(s)
                Reactor::block_on(async {
                    let pool = Pool::lookup("uri").unwrap();
                    assert_eq!(pool.get_name(), "uri");
                    let bdev = pool.get_base_bdev();
                    assert_eq!(bdev.name(), "malloc0");
                    assert_eq!(
                        bdev.bdev_uri().unwrap(),
                        format!(
                            "malloc:///malloc0?size_mb=64&uuid={}",
                            bdev.uuid_as_string()
                        )
                    );
                });

                // destroy the pool
                Reactor::block_on(async {
                    let pool = Pool::lookup("uri").unwrap();
                    pool.destroy().await.unwrap();
                });

                // destroy the legacy pool
                Reactor::block_on(async {
                    let pool = Pool::lookup("legacy").unwrap();
                    pool.destroy().await.unwrap();
                });

                // create the pools again
                Reactor::block_on(async {
                    create_pool(CreatePoolRequest {
                        name: "uri".into(),
                        disks: vec!["malloc:///malloc0?size_mb=64".to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();

                    create_pool(CreatePoolRequest {
                        name: "legacy".into(),
                        disks: vec![DISKNAME1.to_string()],
                        block_size: 0,
                        io_if: 0,
                    })
                    .await
                    .unwrap();
                });

                // validate they are there again and then destroy them
                Reactor::block_on(async {
                    // Note: destroying the pools as you iterate over
                    // them gives undefined behaviour currently (18/09/2020).
                    // So collect() the pools into a vec first and then
                    // iterate over that.
                    let pools: Vec<Pool> = PoolsIter::new().collect();
                    assert_eq!(pools.len(), 2);
                    for p in pools {
                        p.destroy().await.unwrap();
                    }
                });

                mayastor_env_stop(0);
            })
            .unwrap();
    });

    common::delete_file(&[DISKNAME1.into()]);
    result.unwrap();
}
