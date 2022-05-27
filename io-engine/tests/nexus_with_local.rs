pub mod common;
use common::compose::Builder;
use composer::{Binary, RpcHandle};

use rpc::mayastor::{
    AddChildNexusRequest,
    BdevUri,
    CreateNexusRequest,
    CreatePoolRequest,
    CreateReplicaRequest,
    Null,
    RemoveChildNexusRequest,
};

fn uuid() -> String {
    "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string()
}

fn pool() -> String {
    "tpool".to_string()
}

async fn create_replicas(h: &mut RpcHandle) {
    h.mayastor
        .create_pool(CreatePoolRequest {
            name: pool(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    h.mayastor
        .create_replica(CreateReplicaRequest {
            uuid: uuid(),
            pool: pool(),
            size: 8 * 1024 * 1024,
            thin: false,
            share: 1,
        })
        .await
        .unwrap();
}

async fn check_aliases(h: &mut RpcHandle, present: bool) {
    let bdevs = h.bdev.list(Null {}).await.unwrap().into_inner();
    assert_eq!(
        bdevs
            .bdevs
            .into_iter()
            .any(|b| b.aliases.contains("bdev:///")),
        present
    );
}

async fn create_nexus(h: &mut RpcHandle, children: Vec<String>) {
    h.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: uuid(),
            size: 4 * 1024 * 1024,
            children,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn nexus_with_local() {
    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .with_default_tracing()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms2",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut ms1 = test.grpc_handle("ms1").await.unwrap();
    let mut ms2 = test.grpc_handle("ms2").await.unwrap();
    // create and share a bdev on each container
    create_replicas(&mut ms1).await;
    create_replicas(&mut ms2).await;

    let children = vec![
        format!("bdev:///{}", uuid()),
        format!(
            "nvmf://{}/nqn.2019-05.io.openebs:{}",
            ms2.endpoint.ip(),
            uuid()
        ),
    ];

    create_nexus(&mut ms1, children).await;
    check_aliases(&mut ms1, true).await;

    ms1.mayastor
        .remove_child_nexus(RemoveChildNexusRequest {
            uri: format!("bdev:///{}", uuid()),
            uuid: uuid(),
        })
        .await
        .unwrap();

    check_aliases(&mut ms1, false).await;

    ms1.mayastor
        .add_child_nexus(AddChildNexusRequest {
            uri: format!("bdev:///{}", uuid()),
            uuid: uuid(),
            norebuild: false,
        })
        .await
        .unwrap();

    ms1.mayastor
        .add_child_nexus(AddChildNexusRequest {
            uri: format!("bdev:///{}", uuid()),
            uuid: uuid(),
            norebuild: false,
        })
        .await
        .expect_err("Should fail to add the same child again");

    check_aliases(&mut ms1, true).await;
    ms1.bdev
        .destroy(BdevUri {
            uri: format!("bdev:///{}", uuid()),
        })
        .await
        .unwrap();
}
