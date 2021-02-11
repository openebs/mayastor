#![feature(allow_fail)]

pub mod common;
use common::*;

#[actix_rt::test]
async fn create_pool_malloc() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();
    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();
}

#[actix_rt::test]
async fn create_pool_with_missing_disk() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["/dev/c/3po".into()],
        })
        .await
        .expect_err("Device should not exist");
}

#[actix_rt::test]
async fn create_pool_with_existing_disk() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop-new".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .expect_err("Disk should be used by another pool");

    cluster
        .rest_v0()
        .destroy_pool(v0::DestroyPool {
            node: "mayastor".into(),
            id: "pooloop".into(),
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop-new".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .expect("Should now be able to create the new pool");
}

#[actix_rt::test]
async fn create_pool_idempotent() {
    let cluster = ClusterBuilder::builder().build().await.unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();
}

/// FIXME: CAS-710
#[actix_rt::test]
#[allow_fail]
async fn create_pool_idempotent_same_disk_different_query() {
    let cluster = ClusterBuilder::builder()
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=100&blk_size=512".into()],
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///disk?size_mb=200&blk_size=4096".into()],
        })
        .await
        .expect_err("Different query not allowed!");
}

#[actix_rt::test]
async fn create_pool_idempotent_different_nvmf_host() {
    let cluster = ClusterBuilder::builder()
        .with_options(|opts| opts.with_mayastors(3))
        .build()
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor-1".into(),
            id: "pooloop-1".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();

    let replica1 = cluster
        .rest_v0()
        .create_replica(v0::CreateReplica {
            node: "mayastor-1".into(),
            uuid: "0aa4a830-a971-4e96-a97c-15c39dd8f162".into(),
            pool: "pooloop-1".into(),
            size: 10 * 1024 * 1024,
            thin: true,
            share: v0::Protocol::Nvmf,
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor-2".into(),
            id: "pooloop-2".into(),
            disks: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();

    let replica2 = cluster
        .rest_v0()
        .create_replica(v0::CreateReplica {
            node: "mayastor-2".into(),
            uuid: "0aa4a830-a971-4e96-a97c-15c39dd8f162".into(),
            pool: "pooloop-2".into(),
            size: 10 * 1024 * 1024,
            thin: true,
            share: v0::Protocol::Nvmf,
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor-3".into(),
            id: "pooloop".into(),
            disks: vec![replica1.uri.clone()],
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor-3".into(),
            id: "pooloop".into(),
            disks: vec![replica1.uri],
        })
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_pool(v0::CreatePool {
            node: "mayastor-3".into(),
            id: "pooloop".into(),
            disks: vec![replica2.uri],
        })
        .await
        .expect_err("Different host!");
}
