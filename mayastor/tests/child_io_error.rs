use composer::{Builder, RpcHandle};
use crossbeam::channel::{unbounded, Receiver};
use rpc::mayastor::{
    BdevShareRequest,
    BdevUri,
    ChildState,
    CreateNexusRequest,
    CreateReply,
    DestroyNexusRequest,
    Nexus,
    NexusState,
    Null,
    PublishNexusRequest,
    ShareProtocolNexus,
};
use std::{convert::TryFrom, time::Duration};

pub mod common;

/// Test the states of the nexus and children when an I/O error occurs.
/// A child with a failed I/O is expected to be faulted.
#[tokio::test]
async fn child_io_error() {
    let test = Builder::new()
        .name("child_io_error")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .add_container("ms2")
        .add_container("ms3")
        .with_clean(true)
        .with_prune(true)
        .build()
        .await
        .unwrap();

    let nexus_hdl = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms2_share_uri = bdev_create_and_share(ms2).await;
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();
    let ms3_share_uri = bdev_create_and_share(ms3).await;

    const NEXUS_UUID: &str = "00000000-0000-0000-0000-000000000001";
    const NEXUS_SIZE: u64 = 50 * 1024 * 1024; // 50MiB

    // Create a nexus and run fio against it.
    let nexus_uri = nexus_create_and_publish(
        nexus_hdl,
        NEXUS_UUID.into(),
        NEXUS_SIZE,
        vec![ms2_share_uri.clone(), ms3_share_uri.clone()],
    )
    .await;
    let nexus_tgt = nvmf_connect(nexus_uri.clone());
    let fio_receiver = run_fio(nexus_tgt, NEXUS_SIZE);
    // Let fio run for a bit.
    std::thread::sleep(Duration::from_secs(2));

    // Cause an I/O error by unsharing a child then wait for fio to complete.
    bdev_unshare(ms3).await;
    let fio_result = fio_receiver.recv().unwrap();
    assert_eq!(fio_result, 0, "Failed to run fio_verify_size");

    // Check the state of the nexus and children.
    assert_eq!(
        get_nexus_state(nexus_hdl, &NEXUS_UUID).await,
        NexusState::NexusDegraded as i32
    );
    assert_eq!(
        get_child_state(nexus_hdl, &NEXUS_UUID, &ms2_share_uri).await,
        ChildState::ChildOnline as i32
    );
    assert_eq!(
        get_child_state(nexus_hdl, &NEXUS_UUID, &ms3_share_uri).await,
        ChildState::ChildFaulted as i32
    );

    // Teardown.
    nvmf_disconnect(nexus_uri);
    nexus_hdl
        .mayastor
        .destroy_nexus(DestroyNexusRequest {
            uuid: NEXUS_UUID.into(),
        })
        .await
        .expect("Failed to destroy nexus");
}

/// Create and publish a nexus with the given uuid and size.
/// The nexus is published over NVMf and the nexus uri is returned.
async fn nexus_create_and_publish(
    hdl: &mut RpcHandle,
    uuid: String,
    size: u64,
    children: Vec<String>,
) -> String {
    hdl.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: uuid.clone(),
            size,
            children,
        })
        .await
        .unwrap();
    hdl.mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: uuid.clone(),
            key: "".into(),
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .unwrap()
        .into_inner()
        .device_uri
}

/// Create and share a bdev over NVMf.
async fn bdev_create_and_share(hdl: &mut RpcHandle) -> String {
    const DISK_NAME: &str = "disk0";
    hdl.bdev
        .create(BdevUri {
            uri: format!("malloc:///{}?size_mb=100", DISK_NAME),
        })
        .await
        .unwrap();
    hdl.bdev
        .share(BdevShareRequest {
            name: DISK_NAME.into(),
            proto: "nvmf".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .uri
}

/// Unshare a bdev.
async fn bdev_unshare(hdl: &mut RpcHandle) {
    hdl.bdev
        .unshare(CreateReply {
            name: "disk0".to_string(),
        })
        .await
        .unwrap();
}

/// Connect to a NVMf target and return the device name.
fn nvmf_connect(uri: String) -> String {
    let target = nvmeadm::NvmeTarget::try_from(uri).unwrap();
    let devices = target.connect().unwrap();
    devices[0].path.to_string()
}

// Disconnect from a NVMf target.
fn nvmf_disconnect(uri: String) {
    let target = nvmeadm::NvmeTarget::try_from(uri).unwrap();
    target.disconnect().unwrap();
}

/// Return the state of the nexus with the given uuid.
async fn get_nexus_state(hdl: &mut RpcHandle, uuid: &str) -> i32 {
    get_nexus(hdl, uuid).await.state
}

/// Return the nexus with the given uuid.
async fn get_nexus(hdl: &mut RpcHandle, uuid: &str) -> Nexus {
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
    assert_eq!(n.len(), 1);
    n[0].clone()
}

/// Return the state of a child.
async fn get_child_state(
    hdl: &mut RpcHandle,
    nexus_uuid: &str,
    child_uri: &str,
) -> i32 {
    let n = get_nexus(hdl, nexus_uuid).await;
    let c = n
        .children
        .iter()
        .filter(|c| c.uri == child_uri)
        .collect::<Vec<_>>();
    assert_eq!(c.len(), 1);
    c[0].state
}

/// Run fio in a spawned thread and return a receiver channel which is signalled
/// when fio completes.
fn run_fio(target: String, target_size: u64) -> Receiver<i32> {
    let (s, r) = unbounded::<i32>();
    std::thread::spawn(move || {
        if let Err(e) = s.send(common::fio_verify_size(&target, target_size)) {
            tracing::error!("Failed to send fio complete with error {}", e);
        }
    });
    r
}
