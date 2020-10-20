use rpc::mayastor::{
    CreatePoolRequest,
    CreateReplicaRequest,
    DestroyPoolRequest,
    DestroyReplicaRequest,
    Null,
    ShareReplicaRequest,
};

pub mod common;
use common::Builder;
static DISKNAME1: &str = "/tmp/disk1.img";

#[tokio::test]
async fn lvs_pool_rpc() {
    let test = Builder::new()
        .name("lvs-pool-grpc")
        .with_clean(true)
        .network("10.1.0.0/16")
        .add_container("ms1")
        .build()
        .await
        .unwrap();

    // testing basic rpc methods
    let mut handles = test.grpc_handles().await.unwrap();
    let gdl = handles.get_mut(0).unwrap();

    // create a pool
    gdl.mayastor
        .create_pool(CreatePoolRequest {
            name: "tpool".to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    gdl.mayastor
        .create_pool(CreatePoolRequest {
            name: "tpool".to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();
    //list the pool
    let list = gdl.mayastor.list_pools(Null {}).await.unwrap();

    assert_eq!(list.into_inner().pools.len(), 1);

    // create replica not shared
    gdl.mayastor
        .create_replica(CreateReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
            pool: "tpool".to_string(),
            size: 4 * 1024,
            thin: false,
            share: 0,
        })
        .await
        .unwrap();

    // should succeed
    gdl.mayastor
        .create_replica(CreateReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
            pool: "tpool".to_string(),
            size: 4 * 1024,
            thin: false,
            share: 0,
        })
        .await
        .unwrap();

    // share replica
    gdl.mayastor
        .share_replica(ShareReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
            share: 1,
        })
        .await
        .unwrap();

    // share again, should succeed
    gdl.mayastor
        .share_replica(ShareReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
            share: 1,
        })
        .await
        .unwrap();

    // assert we are shared
    assert_eq!(
        gdl.mayastor
            .list_replicas(Null {})
            .await
            .unwrap()
            .into_inner()
            .replicas[0]
            .uri
            .contains("nvmf://"),
        true
    );

    // unshare it
    gdl.mayastor
        .share_replica(ShareReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
            share: 0,
        })
        .await
        .unwrap();

    // assert we are not shared
    assert_eq!(
        gdl.mayastor
            .list_replicas(Null {})
            .await
            .unwrap()
            .into_inner()
            .replicas[0]
            .uri
            .contains("bdev://"),
        true
    );

    // destroy the replica
    gdl.mayastor
        .destroy_replica(DestroyReplicaRequest {
            uuid: "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string(),
        })
        .await
        .unwrap();

    // destroy the pool
    gdl.mayastor
        .destroy_pool(DestroyPoolRequest {
            name: "tpool".to_string(),
        })
        .await
        .unwrap();

    test.logs("ms1").await.unwrap();
    common::delete_file(&[DISKNAME1.into()]);
}
