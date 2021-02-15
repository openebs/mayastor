#![feature(allow_fail)]

pub mod common;
use common::*;

#[actix_rt::test]
async fn create_nexus() {
    let cluster = ClusterBuilder::builder()
        .with_pools(1)
        .with_replicas(2, 5 * 1024 * 1024, v0::Protocol::Off)
        .build()
        .await
        .unwrap();

    cluster
        .rest_v0()
        .create_nexus(v0::CreateNexus {
            node: cluster.node(0),
            uuid: v0::NexusId::new(),
            size: 10 * 1024 * 1024,
            children: vec!["malloc:///disk?size_mb=100".into()],
        })
        .await
        .unwrap();
}
