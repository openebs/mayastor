#![feature(allow_fail)]

pub mod common;
use common::*;

// FIXME: CAS-721
#[actix_rt::test]
#[allow_fail]
async fn create_replica() {
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();

    let replica = v0::CreateReplica {
        node: cluster.node(0),
        uuid: Default::default(),
        pool: cluster.pool(0, 0),
        size: 5 * 1024 * 1024,
        thin: true,
        share: v0::Protocol::Off,
    };
    let created_replica = cluster
        .rest_v0()
        .create_replica(replica.clone())
        .await
        .unwrap();
    assert_eq!(created_replica.node, replica.node);
    assert_eq!(created_replica.uuid, replica.uuid);
    assert_eq!(created_replica.pool, replica.pool);

    // todo: why is this not the same?
    // assert_eq!(created_replica.size, replica.size);
    // fixme: replicas are always created without thin provisioning
    assert_eq!(created_replica.thin, replica.thin);
    assert_eq!(created_replica.share, replica.share);
}

#[actix_rt::test]
async fn create_replica_protocols() {
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        .build()
        .await
        .unwrap();

    let protocols = vec![
        Err(v0::Protocol::Nbd),
        Err(v0::Protocol::Iscsi),
        Ok(v0::Protocol::Nvmf),
        Ok(v0::Protocol::Off),
    ];

    for test in protocols {
        let protocol = result_either!(&test);
        test_result(
            &test,
            cluster.rest_v0().create_replica(v0::CreateReplica {
                node: cluster.node(0),
                uuid: v0::ReplicaId::new(),
                pool: cluster.pool(0, 0),
                size: 5 * 1024 * 1024,
                thin: true,
                share: protocol.clone(),
            }),
        )
        .await
        .unwrap();
    }
}

// FIXME: CAS-731
#[actix_rt::test]
#[allow_fail]
async fn create_replica_idempotent_different_sizes() {
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        // don't log whilst we have the allow_fail
        .compose_build(|c| c.with_logs(false))
        .await
        .unwrap();

    let uuid = v0::ReplicaId::new();
    let size = 5 * 1024 * 1024;
    let replica = cluster
        .rest_v0()
        .create_replica(v0::CreateReplica {
            node: cluster.node(0),
            uuid: uuid.clone(),
            pool: cluster.pool(0, 0),
            size,
            thin: false,
            share: v0::Protocol::Off,
        })
        .await
        .unwrap();
    assert_eq!(&replica.uuid, &uuid);

    cluster
        .rest_v0()
        .create_replica(v0::CreateReplica {
            node: cluster.node(0),
            uuid: uuid.clone(),
            pool: cluster.pool(0, 0),
            size,
            thin: replica.thin,
            share: v0::Protocol::Off,
        })
        .await
        .unwrap();

    let sizes = vec![Ok(size), Err(size / 2), Err(size * 2)];
    for test in sizes {
        let size = result_either!(test);
        test_result(
            &test,
            cluster.rest_v0().create_replica(v0::CreateReplica {
                node: cluster.node(0),
                uuid: v0::ReplicaId::new(),
                pool: cluster.pool(0, 0),
                size,
                thin: replica.thin,
                share: v0::Protocol::Off,
            }),
        )
        .await
        .unwrap();
    }
}
