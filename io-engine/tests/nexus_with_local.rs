pub mod common;

use io_engine::constants::NVME_NQN_PREFIX;

use common::compose::{
    rpc::v1::{
        bdev::{DestroyBdevRequest, ListBdevOptions},
        nexus::{
            AddChildNexusRequest,
            CreateNexusRequest,
            RemoveChildNexusRequest,
        },
        pool::CreatePoolRequest,
        replica::CreateReplicaRequest,
        GrpcConnect,
        RpcHandle,
    },
    Binary,
    Builder,
};

fn nexus_uuid() -> String {
    "cdc2a7db-3ac3-403a-af80-7fadc1581c47".to_string()
}

fn nexus_name() -> String {
    "nexus0".to_string()
}

fn repl_uuid() -> String {
    "65acdaac-14c4-41d8-a55e-d03bfd7185a4".to_string()
}

fn repl_name() -> String {
    "repl0".to_string()
}

fn pool_uuid() -> String {
    "6e3c062c-293b-46e6-8ab3-ff13c1643437".to_string()
}

fn pool_name() -> String {
    "tpool".to_string()
}

async fn create_replicas(h: &mut RpcHandle) {
    h.pool
        .create_pool(CreatePoolRequest {
            name: pool_name(),
            uuid: Some(pool_uuid()),
            pooltype: 0,
            disks: vec!["malloc:///disk0?size_mb=64".into()],
            cluster_size: None,
            md_args: None,
        })
        .await
        .unwrap();

    h.replica
        .create_replica(CreateReplicaRequest {
            name: repl_name(),
            uuid: repl_uuid(),
            pooluuid: pool_uuid(),
            size: 8 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn check_aliases(h: &mut RpcHandle, present: bool) {
    let bdevs = h
        .bdev
        .list(ListBdevOptions {
            name: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        bdevs
            .bdevs
            .into_iter()
            .any(|b| b.aliases.contains("bdev:///")),
        present
    );
}

async fn create_nexus(h: &mut RpcHandle, children: Vec<String>) {
    h.nexus
        .create_nexus(CreateNexusRequest {
            name: nexus_name(),
            uuid: nexus_uuid(),
            size: 4 * 1024 * 1024,
            min_cntl_id: 1,
            max_cntl_id: 1,
            resv_key: 1,
            preempt_key: 0,
            children,
            nexus_info_key: nexus_name(),
            resv_type: None,
            preempt_policy: 0,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn nexus_with_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
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

    let conn = GrpcConnect::new(&test);

    let mut ms1 = conn.grpc_handle("ms1").await.unwrap();
    let mut ms2 = conn.grpc_handle("ms2").await.unwrap();

    // create and share a bdev on each container
    create_replicas(&mut ms1).await;
    create_replicas(&mut ms2).await;

    let child0 = format!("bdev:///{}", repl_name());
    let child1 = format!(
        "nvmf://{}/{NVME_NQN_PREFIX}:{}",
        ms2.endpoint.ip(),
        repl_name()
    );

    create_nexus(&mut ms1, vec![child0.clone(), child1.clone()]).await;
    check_aliases(&mut ms1, true).await;

    ms1.nexus
        .remove_child_nexus(RemoveChildNexusRequest {
            uri: child0.clone(),
            uuid: nexus_uuid(),
        })
        .await
        .unwrap();

    check_aliases(&mut ms1, false).await;

    ms1.nexus
        .add_child_nexus(AddChildNexusRequest {
            uri: child0.clone(),
            uuid: nexus_uuid(),
            norebuild: false,
        })
        .await
        .unwrap();

    ms1.nexus
        .add_child_nexus(AddChildNexusRequest {
            uri: child0.clone(),
            uuid: nexus_uuid(),
            norebuild: false,
        })
        .await
        .expect_err("Should fail to add the same child again");

    check_aliases(&mut ms1, true).await;
    ms1.bdev
        .destroy(DestroyBdevRequest {
            uri: child0.clone(),
        })
        .await
        .unwrap();
}
