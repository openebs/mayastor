use crate::common::fio_run_verify;
use common::compose::Builder;
use composer::{Binary, ComposeTest, ContainerSpec, RpcHandle};
use etcd_client::Client;
use rpc::{
    mayastor::{
        BdevShareRequest,
        BdevUri,
        Child,
        ChildState,
        CreateNexusRequest,
        CreateReply,
        DestroyNexusRequest,
        Nexus,
        NexusState,
        Null,
        PublishNexusRequest,
        ShareProtocolNexus,
    },
    persistence::{
        ChildInfo,
        ChildState as PersistentChildState,
        NexusInfo,
        Reason as PersistentReason,
    },
};
use std::{convert::TryFrom, thread::sleep, time::Duration};
use url::Url;

pub mod common;

static ETCD_ENDPOINT: &str = "0.0.0.0:2379";

/// This test checks that when an unexpected restart occurs, all persisted info
/// remains unchanged. In particular, the clean shutdown variable must be false.
#[tokio::test]
async fn persist_unexpected_restart() {
    let test = start_infrastructure("persist_unexpected_restart").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Create bdevs and share over nvmf.
    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Create a nexus.
    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    create_nexus(ms1, nexus_uuid, vec![child1.clone(), child2.clone()]).await;

    // Retrieve the nexus info from the store.

    let mut etcd = Client::connect([ETCD_ENDPOINT], None).await.unwrap();
    let response = etcd.get(nexus_uuid, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: NexusInfo = serde_json::from_slice(value).unwrap();

    // Check the persisted nexus info is correct.

    assert_eq!(nexus_info.clean_shutdown, false);

    let child = child_info(&nexus_info, &uuid(&child1));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    let child = child_info(&nexus_info, &uuid(&child2));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    // Restart the container where the nexus lives.
    test.restart("ms1")
        .await
        .expect("Failed to restart container.");

    let response = etcd.get(nexus_uuid, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: NexusInfo = serde_json::from_slice(value).unwrap();

    // Check the persisted nexus info remains unchanged.

    assert_eq!(nexus_info.clean_shutdown, false);

    let child = child_info(&nexus_info, &uuid(&child1));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    let child = child_info(&nexus_info, &uuid(&child2));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);
}

/// This test checks that, when a nexus is destroyed successfully the "clean
/// shutdown" variable is persisted to the store correctly.
#[tokio::test]
async fn persist_clean_shutdown() {
    let test = start_infrastructure("persist_clean_shutdown").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Create bdevs and share over nvmf.
    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Create a nexus.
    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    create_nexus(ms1, nexus_uuid, vec![child1.clone(), child2.clone()]).await;

    // Retrieve the nexus info from the store.

    let mut etcd = Client::connect([ETCD_ENDPOINT], None).await.unwrap();
    let response = etcd.get(nexus_uuid, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: NexusInfo = serde_json::from_slice(value).unwrap();

    // Check the persisted nexus info is correct.

    assert_eq!(nexus_info.clean_shutdown, false);

    let child = child_info(&nexus_info, &uuid(&child1));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    let child = child_info(&nexus_info, &uuid(&child2));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    // Destroy the nexus
    ms1.mayastor
        .destroy_nexus(DestroyNexusRequest {
            uuid: nexus_uuid.to_string(),
        })
        .await
        .expect("Failed to destroy nexus");

    let response = etcd.get(nexus_uuid, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: NexusInfo = serde_json::from_slice(value).unwrap();

    // Check the persisted nexus info is correct.

    assert_eq!(nexus_info.clean_shutdown, true);

    let child = child_info(&nexus_info, &uuid(&child1));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    let child = child_info(&nexus_info, &uuid(&child2));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);
}

/// This test checks that the state of a child is successfully updated in the
/// persistent store when there is an I/O failure.
#[tokio::test]
async fn persist_io_failure() {
    let test = start_infrastructure("persist_io_failure").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Create bdevs and share over nvmf.
    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Create and publish a nexus.
    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    create_nexus(ms1, nexus_uuid, vec![child1.clone(), child2.clone()]).await;
    let nexus_uri = publish_nexus(ms1, nexus_uuid).await;

    // Unshare one of the children.
    ms3.bdev
        .unshare(CreateReply {
            name: "disk0".to_string(),
        })
        .await
        .expect("Failed to unshare");

    // Create and connect NVMF target.
    let target = nvmeadm::NvmeTarget::try_from(nexus_uri.clone()).unwrap();
    let devices = target.connect().unwrap();
    let fio_hdl = tokio::spawn(async move {
        fio_run_verify(&devices[0].path.to_string()).unwrap()
    });

    fio_hdl.await.unwrap();

    // Disconnect NVMF target
    target.disconnect().unwrap();

    // Reshare bdev to prevent infinite nvmf retries.
    ms3.bdev
        .share(BdevShareRequest {
            name: "disk0".to_string(),
            proto: "nvmf".to_string(),
        })
        .await
        .expect("Failed to share");

    assert_eq!(
        get_nexus_state(ms1, &nexus_uuid).await.unwrap(),
        NexusState::NexusDegraded as i32
    );
    assert_eq!(
        get_child(ms1, &nexus_uuid, &child1).await.state,
        ChildState::ChildOnline as i32
    );
    assert_eq!(
        get_child(ms1, &nexus_uuid, &child2).await.state,
        ChildState::ChildFaulted as i32
    );

    // Use etcd-client to check the persisted entry.

    let mut etcd = Client::connect([ETCD_ENDPOINT], None).await.unwrap();
    let response = etcd.get(nexus_uuid, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let nexus_info: NexusInfo = serde_json::from_slice(value).unwrap();
    assert_eq!(nexus_info.clean_shutdown, false);

    // Expect child1 to be healthy.
    let child = child_info(&nexus_info, &uuid(&child1));
    assert_eq!(child.state, PersistentChildState::Open as i32);
    assert_eq!(child.reason, PersistentReason::Unknown as i32);

    // Expect child2 to be faulted due to an I/O error.
    let child = child_info(&nexus_info, &uuid(&child2));
    assert_eq!(child.state, PersistentChildState::Faulted as i32);
    assert_eq!(child.reason, PersistentReason::IoError as i32);
}

/// This test checks the behaviour when a connection to the persistent store is
/// faulty.
#[tokio::test]
async fn persistent_store_connection() {
    let test = start_infrastructure("persistent_store_connection").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    // Pause the etcd container.
    test.pause("etcd")
        .await
        .expect("Failed to pause the etcd container");

    // Create bdevs and share over nvmf.
    let child1 = create_and_share_bdevs(ms2).await;
    let child2 = create_and_share_bdevs(ms3).await;

    // Attempt to create a nexus.
    // This operation requires the persistent store to be updated. Because etcd
    // is currently unavailable, the operation is expected to timeout.
    let nexus_uuid = "8272e9d3-3738-4e33-b8c3-769d8eed5771";
    tokio::time::timeout(
        Duration::from_secs(3),
        create_nexus(ms1, nexus_uuid, vec![child1.clone(), child2.clone()]),
    )
    .await
    .expect_err("Create nexus should have timed out.");

    // Unpause the etcd container.
    test.thaw("etcd")
        .await
        .expect("Failed to unpause the etcd container.");

    // Allow some time for the connection to etcd to be re-established.
    sleep(Duration::from_secs(5));

    // Once etcd becomes available again the previously timed out operation
    // should complete. Therefore check the nexus has been created.
    assert!(get_nexus(ms1, nexus_uuid).await.is_some());
}

/// Start the containers for the tests.
async fn start_infrastructure(test_name: &str) -> ComposeTest {
    let etcd_endpoint = format!("http://etcd.{}:2379", test_name);
    let test = Builder::new()
        .name(test_name)
        .add_container_spec(
            ContainerSpec::from_binary(
                "etcd",
                Binary::from_nix("etcd").with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .add_container_bin(
            "ms1",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .add_container_bin(
            "ms2",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .add_container_bin(
            "ms3",
            Binary::from_dbg("mayastor").with_args(vec!["-p", &etcd_endpoint]),
        )
        .build()
        .await
        .unwrap();
    test
}

/// Creates and publishes a nexus.
/// Returns the share uri of the nexus.
async fn create_nexus(hdl: &mut RpcHandle, uuid: &str, children: Vec<String>) {
    hdl.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: uuid.to_string(),
            size: 20 * 1024 * 1024,
            children,
        })
        .await
        .expect("Failed to create nexus.");
}

/// Publish a nexus with the given UUID over NVMf.
async fn publish_nexus(hdl: &mut RpcHandle, uuid: &str) -> String {
    hdl.mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: uuid.to_string(),
            key: "".to_string(),
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .expect("Failed to publish nexus")
        .into_inner()
        .device_uri
}

/// Creates and shares a bdev over NVMf and returns the share uri.
async fn create_and_share_bdevs(hdl: &mut RpcHandle) -> String {
    hdl.bdev
        .create(BdevUri {
            uri: "malloc:///disk0?size_mb=100".into(),
        })
        .await
        .unwrap();
    let reply = hdl
        .bdev
        .share(BdevShareRequest {
            name: "disk0".into(),
            proto: "nvmf".into(),
        })
        .await
        .unwrap();
    reply.into_inner().uri
}

/// Returns the nexus with the given uuid.
async fn get_nexus(hdl: &mut RpcHandle, uuid: &str) -> Option<Nexus> {
    let nexus_list = hdl
        .mayastor
        .list_nexus(Null {})
        .await
        .unwrap()
        .into_inner()
        .nexus_list;
    let n = nexus_list
        .iter()
        .filter(|n| n.uuid == uuid)
        .collect::<Vec<_>>();
    if n.is_empty() {
        return None;
    }
    Some(n[0].clone())
}

/// Returns the state of the nexus with the given uuid.
async fn get_nexus_state(hdl: &mut RpcHandle, uuid: &str) -> Option<i32> {
    let list = hdl
        .mayastor
        .list_nexus(Null {})
        .await
        .unwrap()
        .into_inner()
        .nexus_list;
    for nexus in list {
        if nexus.uuid == uuid {
            return Some(nexus.state);
        }
    }
    None
}

/// Returns a child with the given URI.
async fn get_child(
    hdl: &mut RpcHandle,
    nexus_uuid: &str,
    child_uri: &str,
) -> Child {
    let n = get_nexus(hdl, nexus_uuid)
        .await
        .expect("Failed to get nexus");
    let c = n
        .children
        .iter()
        .filter(|c| c.uri == child_uri)
        .collect::<Vec<_>>();
    assert_eq!(c.len(), 1);
    c[0].clone()
}

/// Return the child info of the child with the given UUID.
fn child_info(nexus: &NexusInfo, uuid: &str) -> ChildInfo {
    for child in &nexus.children {
        if child.uuid == uuid {
            return child.clone();
        }
    }
    panic!("Child info not found for {}", uuid);
}
/// Extract UUID from uri.
pub(crate) fn uuid(uri: &str) -> String {
    let url = Url::parse(uri).expect("Failed to parse uri");
    for pair in url.query_pairs() {
        if pair.0 == "uuid" {
            return pair.1.to_string();
        }
    }
    panic!("URI does not contain a uuid query parameter.");
}
