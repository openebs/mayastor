use composer::{Builder, ComposeTest, RpcHandle};

use rpc::mayastor::{
    AddChildNexusRequest,
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
    PauseRebuildRequest,
    PublishNexusRequest,
    RebuildProgressRequest,
    RebuildStateRequest,
    RemoveChildNexusRequest,
    ResumeRebuildRequest,
    ShareProtocolNexus,
    StartRebuildRequest,
    StopRebuildRequest,
};

use std::time::Duration;

use crossbeam::channel::unbounded;
use spdk_sys::SPDK_BDEV_LARGE_BUF_MAX_SIZE;
use std::convert::TryFrom;

pub mod common;

const NEXUS_UUID: &str = "00000000-0000-0000-0000-000000000001";
const NEXUS_SIZE: u64 = 50 * 1024 * 1024; // 50MiB

/// Test that a child added to a nexus can be successfully rebuild.
#[tokio::test]
async fn rebuild_basic() {
    let test = start_infrastructure("rebuild_basic").await;
    let (mut ms1, _, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Check a rebuild is started for a newly added child.
    add_child(nexus_hdl, child, true).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "running",
        Duration::from_secs(1),
    )
    .await
    .unwrap());

    // Check nexus is healthy after rebuild completion.
    assert!(
        wait_for_rebuild_completion(nexus_hdl, child, Duration::from_secs(20))
            .await
    );
    check_nexus_state(nexus_hdl, NexusState::NexusOnline).await;
}

/// Test the "norebuild" flag when adding a child.
#[tokio::test]
async fn rebuild_add_flag() {
    let test = start_infrastructure("rebuild_add_flag").await;
    let (mut ms1, _, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Add child but don't rebuild.
    add_child(nexus_hdl, child, false).await;
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
    check_nexus_state(nexus_hdl, NexusState::NexusDegraded).await;

    // Start rebuild.
    start_rebuild(nexus_hdl, child).await.unwrap();
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "running",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
}

/// Test the rebuild progress gets updated.
#[tokio::test]
async fn rebuild_progress() {
    let test = start_infrastructure("rebuild_progress").await;
    let (mut ms1, _, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Start a rebuild and give it some time to run.
    add_child(nexus_hdl, child, true).await;
    std::thread::sleep(Duration::from_millis(100));

    // Pause rebuild and get current progress.
    pause_rebuild(nexus_hdl, child).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "paused",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
    let progress1 = get_rebuild_progress(nexus_hdl, child).await;

    // Resume rebuild and give it some time to run.
    resume_rebuild(nexus_hdl, child).await.unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Pause rebuild and check for further progress.
    pause_rebuild(nexus_hdl, child).await;
    let progress2 = get_rebuild_progress(nexus_hdl, child).await;
    assert!(progress2 > progress1);
}

/// Test cases where a rebuild should not be started.
#[tokio::test]
async fn rebuild_not_required() {
    let test = start_infrastructure("rebuild_not_required").await;
    let (mut ms1, ms2, ms3) = setup_test(&test, 2).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Attempt to rebuild a healthy child.
    start_rebuild(nexus_hdl, child)
        .await
        .expect_err("Shouldn't rebuild");
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);

    // Remove one of the healthy children.
    remove_child(nexus_hdl, child).await;

    // Can't rebuild a single child which is healthy.
    let last_child = &get_share_uri(&ms2);
    start_rebuild(nexus_hdl, last_child)
        .await
        .expect_err("Shouldn't rebuild");
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
}

/// Test removing the source of a rebuild.
#[tokio::test]
async fn rebuild_src_removal() {
    let test = start_infrastructure("rebuild_src_removal").await;
    let (mut ms1, ms2, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Pause rebuild for added child.
    add_child(nexus_hdl, child, true).await;
    pause_rebuild(nexus_hdl, child).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "paused",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
    check_nexus_state(nexus_hdl, NexusState::NexusDegraded).await;

    // Remove the rebuild source.
    let src_child = &get_share_uri(&ms2);
    remove_child(nexus_hdl, src_child).await;
    // Give a little time for the rebuild to fail.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
    // Nexus must be faulted because it doesn't have any healthy children.
    check_nexus_state(nexus_hdl, NexusState::NexusFaulted).await;
}

/// Test removing the destination of a rebuild.
#[tokio::test]
async fn rebuild_dst_removal() {
    let test = start_infrastructure("rebuild_dst_removal").await;
    let (mut ms1, _, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Pause rebuild for added child.
    add_child(nexus_hdl, child, true).await;
    pause_rebuild(nexus_hdl, child).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "paused",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
    check_nexus_state(nexus_hdl, NexusState::NexusDegraded).await;

    // Remove the child that is being rebuilt.
    remove_child(nexus_hdl, child).await;
    // Give a little time for the rebuild to fail.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
    // Nexus must be online because it has a single healthy child.
    check_nexus_state(nexus_hdl, NexusState::NexusOnline).await;
}

/// Test faulting the source of a rebuild.
#[tokio::test]
async fn rebuild_fault_src() {
    let test = start_infrastructure("rebuild_fault_src").await;
    let (mut ms1, mut ms2, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Check a rebuild is started for the added child.
    add_child(nexus_hdl, child, true).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "running",
        Duration::from_millis(500),
    )
    .await
    .unwrap());

    // Fault the rebuild source by unsharing the bdev.
    bdev_unshare(&mut ms2).await;

    // The rebuild failed so the destination should be faulted.
    assert!(
        wait_for_child_state(
            nexus_hdl,
            child,
            ChildState::ChildFaulted,
            Duration::from_millis(500),
        )
        .await
    );
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
}

/// Test faulting the destination of a rebuild.
#[tokio::test]
async fn rebuild_fault_dst() {
    let test = start_infrastructure("rebuild_fault_dst").await;
    let (mut ms1, _, mut ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;
    let child = &get_share_uri(&ms3);

    // Check a rebuild is started for the added child.
    add_child(nexus_hdl, child, true).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child,
        "running",
        Duration::from_millis(500),
    )
    .await
    .unwrap());

    // Fault the rebuild destination by unsharing the bdev.
    bdev_unshare(&mut ms3).await;

    // Check the state of the destination child.
    // Give a sufficiently high timeout time as unsharing an NVMf bdev can take
    // some time to propagate up as an error from the rebuild job.
    assert!(
        wait_for_child_state(
            nexus_hdl,
            child,
            ChildState::ChildFaulted,
            Duration::from_secs(20),
        )
        .await
    );
    check_nexus_state(nexus_hdl, NexusState::NexusDegraded).await;
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);
}

/// Test rebuild with different sizes of source and destination children.
#[tokio::test]
async fn rebuild_sizes() {
    struct TestCase {
        child1_size: u64,
        child2_size: u64,
        child3_size: u64,
    }

    // Test cases where the child sizes include space for the metadata.

    const META_SIZE_MB: u64 = 5;
    let default_size: u64 = 50 + META_SIZE_MB;

    let mut test_cases = vec![];
    // Children with same size.
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size,
        child3_size: default_size,
    });
    // 2nd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size * 2,
        child3_size: default_size,
    });
    // 3rd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size,
        child3_size: default_size * 2,
    });
    // 2nd and 3rd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size * 2,
        child3_size: default_size * 2,
    });

    // Test cases where the metadata size is not included. This will result in
    // the nexus size being smaller than requested in order to accommodate the
    // metadata on the children.

    let default_size: u64 = 50;

    // Children with same size.
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size,
        child3_size: default_size,
    });
    // 2nd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size * 2,
        child3_size: default_size,
    });
    // 3rd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size,
        child3_size: default_size * 2,
    });
    // 2nd and 3rd child larger
    test_cases.push(TestCase {
        child1_size: default_size,
        child2_size: default_size * 2,
        child3_size: default_size * 2,
    });

    let test = start_infrastructure("rebuild_sizes").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();
    let nexus_hdl = ms1;

    // Run the tests.
    for test in test_cases {
        let child1 =
            bdev_create_and_share(ms2, Some(test.child1_size), None).await;
        let child2 =
            bdev_create_and_share(ms3, Some(test.child2_size), None).await;
        let local_child =
            format!("malloc:///disk0?size_mb={}", test.child3_size.to_string());

        // Create a nexus with 2 remote children.
        create_nexus(nexus_hdl, vec![child1.clone(), child2.clone()]).await;

        // Add the local child and wait for rebuild.
        add_child(nexus_hdl, &local_child, true).await;
        assert!(
            wait_for_rebuild_completion(
                nexus_hdl,
                &local_child,
                Duration::from_secs(2),
            )
            .await
        );

        // Teardown
        destroy_nexus(nexus_hdl).await;
        bdev_unshare(ms2).await;
        bdev_destroy(ms2, "malloc:///disk0".into()).await;
        bdev_unshare(ms3).await;
        bdev_destroy(ms3, "malloc:///disk0".into()).await;
    }
}

/// Tests the rebuild with different nexus sizes.
#[tokio::test]
async fn rebuild_segment_sizes() {
    let test = start_infrastructure("rebuild_segment_sizes").await;
    let ms1 = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();
    let nexus_hdl = ms1;

    const SEGMENT_SIZE: u64 = SPDK_BDEV_LARGE_BUF_MAX_SIZE as u64;
    let test_cases = vec![
        // multiple of SEGMENT_SIZE
        SEGMENT_SIZE * 10,
        // not multiple of SEGMENT_SIZE
        (SEGMENT_SIZE * 10) + 512,
    ];

    // Run the tests.
    for test_case in test_cases.iter() {
        let child1 = bdev_create_and_share(ms2, None, None).await;
        let child2 = bdev_create_and_share(ms3, None, None).await;

        let nexus_size = *test_case;
        nexus_hdl
            .mayastor
            .create_nexus(CreateNexusRequest {
                uuid: NEXUS_UUID.into(),
                size: nexus_size,
                children: vec![child1],
            })
            .await
            .unwrap();

        // Wait for rebuild to complete.
        add_child(nexus_hdl, &child2, true).await;
        assert!(
            wait_for_rebuild_completion(
                nexus_hdl,
                &child2,
                Duration::from_secs(5)
            )
            .await
        );

        // Teardown
        destroy_nexus(nexus_hdl).await;
        bdev_unshare(ms2).await;
        bdev_destroy(ms2, "malloc:///disk0".into()).await;
        bdev_unshare(ms3).await;
        bdev_destroy(ms3, "malloc:///disk0".into()).await;
    }
}

/// Test the various rebuild operations.
#[tokio::test]
async fn rebuild_operations() {
    let test = start_infrastructure("rebuild_operations").await;
    let (mut ms1, ms2, ms3) = setup_test(&test, 1).await;
    let nexus_hdl = &mut ms1;

    // Rebuilding a healthy child should do nothing.
    let child1 = &get_share_uri(&ms2);
    resume_rebuild(nexus_hdl, child1)
        .await
        .expect_err("Should be nothing to rebuild");
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);

    // Start a rebuild.
    let child2 = &get_share_uri(&ms3);
    add_child(nexus_hdl, child2, true).await;
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);

    // Resuming a running rebuild should do nothing.
    resume_rebuild(nexus_hdl, child2).await.unwrap();
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);

    // Pause a running rebuild.
    pause_rebuild(nexus_hdl, child2).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child2,
        "paused",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);

    // Pause the paused rebuild.
    pause_rebuild(nexus_hdl, child2).await;
    assert!(wait_for_rebuild_state(
        nexus_hdl,
        child2,
        "paused",
        Duration::from_secs(1),
    )
    .await
    .unwrap());
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);

    // Start another rebuild for the same child.
    start_rebuild(nexus_hdl, child2)
        .await
        .expect_err("Should already be rebuilding child");
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 1);

    // Stop rebuild - this will cause the rebuild job to be removed
    stop_rebuild(nexus_hdl, child2).await;

    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    let mut number = u32::MAX;
    let mut retries = 5;
    loop {
        ticker.tick().await;
        if get_num_rebuilds(nexus_hdl).await == 0 {
            number = 0;
            break;
        }

        retries -= 1;
        if retries == 0 {
            break;
        }
    }

    if number != 0 {
        panic!("retries failed");
    }
}

/// Test multiple rebuilds running at the same time.
#[tokio::test]
async fn rebuild_multiple() {
    let child_names = vec!["ms1", "ms2", "ms3", "ms4", "ms5"];
    let test = Builder::new()
        .name("rebuild_multiple")
        .network("10.1.0.0/16")
        .add_container(child_names[0])
        .add_container(child_names[1])
        .add_container(child_names[2])
        .add_container(child_names[3])
        .add_container(child_names[4])
        .with_clean(true)
        .with_prune(true)
        .build()
        .await
        .unwrap();

    #[derive(Clone)]
    struct Child {
        hdl: RpcHandle,
        share_uri: String,
    }

    let mut children = vec![];
    for name in child_names {
        let share_uri = bdev_create_and_share(
            &mut test.grpc_handle(name).await.unwrap(),
            None,
            None,
        )
        .await;
        children.push(Child {
            hdl: test.grpc_handle(name).await.unwrap(),
            share_uri,
        });
    }

    // Create a nexus with a single healthy child.
    let nexus_hdl = &mut test.grpc_handle("ms1").await.unwrap();
    create_nexus(nexus_hdl, vec![children[1].share_uri.clone()]).await;

    let degraded_children = children[2 ..= 4].to_vec();
    // Add children and pause rebuilds.
    for child in &degraded_children {
        add_child(nexus_hdl, &child.share_uri, true).await;
        pause_rebuild(nexus_hdl, &child.share_uri).await;
    }
    assert_eq!(
        get_num_rebuilds(nexus_hdl).await as usize,
        degraded_children.len()
    );

    // Resume rebuilds and wait for completion then remove the children.
    for child in &degraded_children {
        resume_rebuild(nexus_hdl, &child.share_uri)
            .await
            .expect("Failed to resume rebuild");
        assert!(
            wait_for_rebuild_completion(
                nexus_hdl,
                &child.share_uri,
                Duration::from_secs(10),
            )
            .await
        );
        remove_child(nexus_hdl, &child.share_uri).await;
    }
    assert_eq!(get_num_rebuilds(nexus_hdl).await, 0);

    // Add the children back again
    for child in &degraded_children {
        add_child(nexus_hdl, &child.share_uri, true).await;
    }

    // Wait for rebuilds to complete
    for child in &degraded_children {
        assert!(
            wait_for_rebuild_completion(
                nexus_hdl,
                &child.share_uri,
                Duration::from_secs(10),
            )
            .await
        );
    }
}

/// Test rebuild while running front-end I/O.
/// Note: This test can take some time to complete because it is running fio and
/// then comparing the contents of the children to make sure they are in-sync.
#[tokio::test]
async fn rebuild_with_load() {
    init_tracing();
    let test = start_infrastructure("rebuild_with_load").await;
    let nexus_hdl = &mut test.grpc_handle("ms1").await.unwrap();
    let ms2 = &mut test.grpc_handle("ms2").await.unwrap();
    let ms3 = &mut test.grpc_handle("ms3").await.unwrap();

    const CHILD_SIZE_MB: u64 = 100;

    // Create a nexus with 1 child.
    let child1 =
        bdev_create_and_share(ms2, Some(CHILD_SIZE_MB), Some("disk1".into()))
            .await;
    create_nexus(nexus_hdl, vec![child1.clone()]).await;

    // Connect to nexus over NVMf.
    let nexus_uri = publish_nexus(nexus_hdl).await;
    let nexus_tgt = nvmf_connect(nexus_uri.clone());

    // Run fio against nexus.
    let (s, r) = unbounded::<i32>();
    let nvmf_tgt = nexus_tgt.clone();
    std::thread::spawn(move || {
        if let Err(e) = s.send(common::fio_verify_size(&nvmf_tgt, NEXUS_SIZE)) {
            tracing::error!("Failed to send fio complete with error {}", e);
        }
    });

    // Let fio run for a bit.
    std::thread::sleep(Duration::from_secs(2));

    // Add a child and rebuild.
    let child2 =
        bdev_create_and_share(ms3, Some(CHILD_SIZE_MB), Some("disk2".into()))
            .await;
    add_child(nexus_hdl, &child2, true).await;

    // Wait for fio to complete
    let fio_result = r.recv().unwrap();
    assert_eq!(fio_result, 0, "Failed to run fio_verify_size");

    // Wait for rebuild to complete.
    assert!(
        wait_for_rebuild_completion(nexus_hdl, &child2, Duration::from_secs(1))
            .await
    );

    // Disconnect and destroy nexus
    nvmf_disconnect(nexus_uri);
    destroy_nexus(nexus_hdl).await;

    // Check children are in-sync.
    let child1_tgt = nvmf_connect(child1.clone());
    let child2_tgt = nvmf_connect(child2.clone());
    common::compare_devices(&child1_tgt, &child2_tgt, CHILD_SIZE_MB, true);
    nvmf_disconnect(child1);
    nvmf_disconnect(child2);
}

/// Build the infrastructure required to run the tests.
async fn start_infrastructure(test_name: &str) -> ComposeTest {
    Builder::new()
        .name(test_name)
        .network("10.1.0.0/16")
        .add_container("ms1")
        .add_container("ms2")
        .add_container("ms3")
        .with_clean(true)
        .with_prune(true)
        .build()
        .await
        .unwrap()
}

/// Set up the prerequisites for the tests.
/// Create a nexus on ms1 and create NVMf shares from ms2 & ms3.
/// The number of children to be added to the nexus is passed in as a parameter.
async fn setup_test(
    test: &ComposeTest,
    num_nexus_children: usize,
) -> (RpcHandle, RpcHandle, RpcHandle) {
    // Currently only support creating a nexus with up to 2 children.
    assert!(num_nexus_children < 3);

    let mut ms1 = test.grpc_handle("ms1").await.unwrap();
    let mut ms2 = test.grpc_handle("ms2").await.unwrap();
    let mut ms3 = test.grpc_handle("ms3").await.unwrap();

    let mut replicas = vec![];
    replicas.push(bdev_create_and_share(&mut ms2, None, None).await);
    replicas.push(bdev_create_and_share(&mut ms3, None, None).await);
    create_nexus(&mut ms1, replicas[0 .. num_nexus_children].to_vec()).await;
    (ms1, ms2, ms3)
}

/// Publish the nexus and return the share uri.
async fn publish_nexus(hdl: &mut RpcHandle) -> String {
    let reply = hdl
        .mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: NEXUS_UUID.into(),
            key: "".to_string(),
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .unwrap()
        .into_inner();
    reply.device_uri
}

/// Create and share a bdev and return the share uri.
async fn bdev_create_and_share(
    hdl: &mut RpcHandle,
    child_size_mb: Option<u64>,
    disk_name: Option<String>,
) -> String {
    let size_mb = child_size_mb.unwrap_or(100);
    let disk_name = match disk_name {
        Some(n) => n,
        None => "disk0".to_string(),
    };
    bdev_create(hdl, size_mb, disk_name.clone()).await;
    bdev_share(hdl, disk_name).await
}

/// Create a bdev and return the uri.
async fn bdev_create(
    hdl: &mut RpcHandle,
    size_mb: u64,
    disk_name: String,
) -> String {
    let uri = format!("malloc:///{}?size_mb={}", disk_name, size_mb,);
    hdl.bdev
        .create(BdevUri {
            uri: uri.clone(),
        })
        .await
        .unwrap();
    uri
}

/// Destroy a bdev.
async fn bdev_destroy(hdl: &mut RpcHandle, uri: String) {
    hdl.bdev
        .destroy(BdevUri {
            uri,
        })
        .await
        .expect("Failed to destroy bdev");
}

/// Share a bdev and return the share uri.
async fn bdev_share(hdl: &mut RpcHandle, name: String) -> String {
    let result = hdl
        .bdev
        .share(BdevShareRequest {
            name,
            proto: "nvmf".into(),
        })
        .await
        .expect("Failed to share bdev")
        .into_inner();
    result.uri
}

/// Get a bdev share uri.
fn get_share_uri(hdl: &RpcHandle) -> String {
    format!(
        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
        hdl.endpoint.ip()
    )
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

/// Create a nexus.
async fn create_nexus(hdl: &mut RpcHandle, children: Vec<String>) {
    hdl.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: NEXUS_UUID.into(),
            size: NEXUS_SIZE,
            children,
        })
        .await
        .unwrap();
}

/// Delete a nexus.
async fn destroy_nexus(hdl: &mut RpcHandle) {
    hdl.mayastor
        .destroy_nexus(DestroyNexusRequest {
            uuid: NEXUS_UUID.into(),
        })
        .await
        .expect("Failed to destroy nexus");
}

/// Add a child to the nexus.
async fn add_child(hdl: &mut RpcHandle, child: &str, rebuild: bool) {
    hdl.mayastor
        .add_child_nexus(AddChildNexusRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
            norebuild: !rebuild,
        })
        .await
        .unwrap();
}

/// Remove a child from the nexus.
async fn remove_child(hdl: &mut RpcHandle, child: &str) {
    hdl.mayastor
        .remove_child_nexus(RemoveChildNexusRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
        .expect("Failed to remove child");
}

/// Start a rebuild for the given child.
async fn start_rebuild(hdl: &mut RpcHandle, child: &str) -> Result<(), ()> {
    match hdl
        .mayastor
        .start_rebuild(StartRebuildRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}

/// Stop a rebuild for the given child.
async fn stop_rebuild(hdl: &mut RpcHandle, child: &str) {
    hdl.mayastor
        .stop_rebuild(StopRebuildRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
        .expect("Failed to stop rebuild");
}

/// Pause a rebuild for the given child.
async fn pause_rebuild(hdl: &mut RpcHandle, child: &str) {
    hdl.mayastor
        .pause_rebuild(PauseRebuildRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
        .expect("Failed to pause rebuild");
}

/// Resume a rebuild for the given child.
async fn resume_rebuild(hdl: &mut RpcHandle, child: &str) -> Result<(), ()> {
    match hdl
        .mayastor
        .resume_rebuild(ResumeRebuildRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}

/// Get the number of rebuilds.
async fn get_num_rebuilds(hdl: &mut RpcHandle) -> u32 {
    let n = get_nexus(hdl, NEXUS_UUID).await;
    n.rebuilds
}

/// Get the rebuild progress for the given child.
async fn get_rebuild_progress(hdl: &mut RpcHandle, child: &str) -> u32 {
    let reply = hdl
        .mayastor
        .get_rebuild_progress(RebuildProgressRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
        .expect("Failed to get rebuild progress");
    reply.into_inner().progress
}

/// Waits on the given rebuild state or times out.
/// Returns false if a timeout occurs.
async fn wait_for_rebuild_state(
    hdl: &mut RpcHandle,
    child: &str,
    state: &str,
    timeout: Duration,
) -> Option<bool> {
    let time = std::time::Instant::now();
    while time.elapsed().as_millis() < timeout.as_millis() {
        match get_rebuild_state(hdl, child).await {
            Some(rebuild_state) => {
                if rebuild_state == state {
                    return Some(true);
                }
            }
            None => return None,
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Some(false)
}

/// Get the current state of the rebuild for the given child uri.
/// Returns None if the rebuild job isn't found.
async fn get_rebuild_state(hdl: &mut RpcHandle, child: &str) -> Option<String> {
    match hdl
        .mayastor
        .get_rebuild_state(RebuildStateRequest {
            uuid: NEXUS_UUID.into(),
            uri: child.into(),
        })
        .await
    {
        Ok(rebuild_state) => Some(rebuild_state.into_inner().state),
        Err(_) => None,
    }
}

/// Returns true if the rebuild has completed.
/// A rebuild is deemed to be complete if the destination child is online.
async fn wait_for_rebuild_completion(
    hdl: &mut RpcHandle,
    child: &str,
    timeout: Duration,
) -> bool {
    wait_for_child_state(hdl, child, ChildState::ChildOnline, timeout).await
}

/// Wait on the given child state or times out.
/// Returns false if a timeout occurs.
async fn wait_for_child_state(
    hdl: &mut RpcHandle,
    child: &str,
    state: ChildState,
    timeout: Duration,
) -> bool {
    let time = std::time::Instant::now();
    while time.elapsed().as_millis() < timeout.as_millis() {
        let c = get_child(hdl, NEXUS_UUID, child).await;
        if c.state == state as i32 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
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

/// Returns the nexus with the given uuid.
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

/// Returns a child with the given URI.
async fn get_child(
    hdl: &mut RpcHandle,
    nexus_uuid: &str,
    child_uri: &str,
) -> Child {
    let n = get_nexus(hdl, nexus_uuid).await;
    let c = n
        .children
        .iter()
        .filter(|c| c.uri == child_uri)
        .collect::<Vec<_>>();
    assert_eq!(c.len(), 1);
    c[0].clone()
}

/// Connect to NVMf target and return device name.
fn nvmf_connect(uri: String) -> String {
    let target = nvmeadm::NvmeTarget::try_from(uri).unwrap();
    let devices = target.connect().unwrap();
    devices[0].path.to_string()
}

// Disconnect from NVMf target.
fn nvmf_disconnect(uri: String) {
    let target = nvmeadm::NvmeTarget::try_from(uri).unwrap();
    target.disconnect().unwrap();
}

/// Initialise tracing.
fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

/// Checks if the nexus state matches the expected state.
async fn check_nexus_state(nexus_hdl: &mut RpcHandle, state: NexusState) {
    assert_eq!(
        get_nexus_state(nexus_hdl, NEXUS_UUID).await.unwrap(),
        state as i32
    );
}
