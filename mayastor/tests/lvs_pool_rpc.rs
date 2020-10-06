use std::panic::catch_unwind;

use mayastor::{
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
    grpc::pool_grpc,
};
use rpc::mayastor::{
    CreatePoolRequest,
    CreateReplicaRequest,
    DestroyPoolRequest,
    DestroyReplicaRequest,
    ShareReplicaRequest,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";

#[test]
fn lvs_pool_rpc() {
    // testing basic rpc methods

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();
    let _r = catch_unwind(|| {
        MayastorEnvironment::new(args)
            .start(|| {
                Reactor::block_on(async {
                    // create a pool
                    pool_grpc::create(CreatePoolRequest {
                        name: "tpool".to_string(),
                        disks: vec!["aio:///tmp/disk1.img".into()],
                    })
                    .await
                    .unwrap();

                    // should succeed
                    pool_grpc::create(CreatePoolRequest {
                        name: "tpool".to_string(),
                        disks: vec!["aio:///tmp/disk1.img".into()],
                    })
                    .await
                    .unwrap();

                    //list the pool
                    let list = pool_grpc::list().unwrap();
                    assert_eq!(list.into_inner().pools.len(), 1);

                    // create replica not shared
                    pool_grpc::create_replica(CreateReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                        pool: "tpool".to_string(),
                        size: 4 * 1024,
                        thin: false,
                        share: 0,
                    })
                    .await
                    .unwrap();

                    // should succeed
                    pool_grpc::create_replica(CreateReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                        pool: "tpool".to_string(),
                        size: 4 * 1024,
                        thin: false,
                        share: 0,
                    })
                    .await
                    .unwrap();

                    // share replica
                    pool_grpc::share_replica(ShareReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                        share: 1,
                    })
                    .await
                    .unwrap();

                    // share again, should succeed
                    pool_grpc::share_replica(ShareReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                        share: 1,
                    })
                    .await
                    .unwrap();

                    // assert we are shared
                    assert_eq!(
                        pool_grpc::list_replicas()
                            .unwrap()
                            .into_inner()
                            .replicas[0]
                            .uri
                            .contains("nvmf://"),
                        true
                    );

                    // unshare it
                    pool_grpc::share_replica(ShareReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                        share: 0,
                    })
                    .await
                    .unwrap();

                    // assert we are not shared
                    assert_eq!(
                        pool_grpc::list_replicas()
                            .unwrap()
                            .into_inner()
                            .replicas[0]
                            .uri
                            .contains("bdev://"),
                        true
                    );

                    // destroy the replica
                    pool_grpc::destroy_replica(DestroyReplicaRequest {
                        uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47"
                            .to_string(),
                    })
                    .await
                    .unwrap();

                    // destroy the pool
                    pool_grpc::destroy(DestroyPoolRequest {
                        name: "tpool".to_string(),
                    })
                    .await
                    .unwrap();
                })
                .unwrap();
                mayastor_env_stop(0);
            })
            .unwrap();
    });

    common::delete_file(&[DISKNAME1.into()]);
    _r.unwrap();
}
