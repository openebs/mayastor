use composer::RpcHandle;
use rpc::mayastor::{
    Bdev,
    CreateNexusRequest,
    CreatePoolRequest,
    CreateReplicaRequest,
    Null,
    Replica,
    ShareProtocolReplica,
    ShareReplicaRequest,
};
use std::str::FromStr;
use tracing::info;

pub mod common;
use common::compose::Builder;

const DISKSIZE_KB: u64 = 96 * 1024;
const VOLUME_SIZE_MB: u64 = (DISKSIZE_KB / 1024) / 2;
const VOLUME_SIZE_B: u64 = VOLUME_SIZE_MB * 1024 * 1024;
const VOLUME_UUID: &str = "cb9e1a5c-7af8-44a7-b3ae-05390be75d83";

// pool name for mayastor from handle_index
fn pool_name(handle_index: usize) -> String {
    format!("pool{}", handle_index)
}

// tests that both local and remote replicas have a unique identifier within
// their share uri as a query parameter which can be used to uniquely identify a
// replica since the replica UUID is effectively the replica name which is used
// by MOAC as a unique volume identifier
#[tokio::test]
async fn replica_uri() {
    let test = Builder::new()
        .name("replica_uri")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .add_container("ms2")
        .with_clean(true)
        .with_default_tracing()
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

    for (i, hdl) in hdls.iter_mut().enumerate() {
        // create a pool on each node
        hdl.mayastor
            .create_pool(CreatePoolRequest {
                name: pool_name(i),
                disks: vec![format!(
                    "malloc:///disk0?size_mb={}",
                    DISKSIZE_KB / 1024
                )],
            })
            .await
            .unwrap();
    }

    // create replica, shared over nvmf
    let replica_nvmf = hdls[1]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: VOLUME_UUID.to_string(),
            pool: pool_name(1),
            size: VOLUME_SIZE_B,
            thin: false,
            share: ShareProtocolReplica::ReplicaNvmf as i32,
        })
        .await
        .unwrap()
        .into_inner();

    info!("Replica: {:?}", replica_nvmf);
    check_replica_uri(&mut hdls[1], &replica_nvmf).await;

    let replica_loopback = hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: VOLUME_UUID.to_string(),
            pool: pool_name(0),
            size: VOLUME_SIZE_B,
            thin: false,
            share: ShareProtocolReplica::ReplicaNone as i32,
        })
        .await
        .unwrap()
        .into_inner();

    info!("Replica: {:?}", replica_loopback);
    check_replica_uri(&mut hdls[0], &replica_loopback).await;

    // share it and make sure the reply URI contains the uuid
    let replica_uri = hdls[0]
        .mayastor
        .share_replica(ShareReplicaRequest {
            uuid: VOLUME_UUID.to_string(),
            share: ShareProtocolReplica::ReplicaNvmf as i32,
        })
        .await;
    info!("Replica: {:?}", replica_uri);
    assert!(replica_uri.unwrap().into_inner().uri.contains("uuid="));

    // unshare it and make sure the reply URI contains the uuid
    let replica_uri = hdls[0]
        .mayastor
        .share_replica(ShareReplicaRequest {
            uuid: VOLUME_UUID.to_string(),
            share: ShareProtocolReplica::ReplicaNone as i32,
        })
        .await;
    info!("Replica: {:?}", replica_uri);
    assert!(replica_uri.unwrap().into_inner().uri.contains("uuid="));

    // sanity check creating a nexus with the unique uuid's within the URI
    hdls[0]
        .mayastor
        .create_nexus(CreateNexusRequest {
            uuid: VOLUME_UUID.to_string(),
            size: VOLUME_SIZE_B,
            children: [replica_loopback.uri, replica_nvmf.uri].to_vec(),
        })
        .await
        .unwrap();
}

async fn get_bdev(handle: &mut RpcHandle) -> Bdev {
    let bdevs = handle.bdev.list(Null {}).await.unwrap().into_inner().bdevs;
    bdevs
        .iter()
        .find(|b| b.name == VOLUME_UUID)
        .expect("Should find our replica as a bdev")
        .clone()
}

async fn check_replica_uri(handle: &mut RpcHandle, replica: &Replica) {
    let bdev = get_bdev(handle).await;
    let replica_url = url::Url::from_str(&replica.uri).unwrap();
    assert_eq!(
        // expect to see the replica UUID as a query parameter: uuid=xxxxx
        replica_url.query().unwrap().replace("uuid=", ""),
        bdev.uuid
    );
    // different to the volume uuid, it should be unique per replica
    assert_ne!(VOLUME_UUID, bdev.uuid);
}
