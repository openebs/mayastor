//! Nexus IO tests for multipath NVMf, reservation, and write-zeroes
use common::bdev_io;
use io_engine::{
    bdev::nexus::{
        nexus_create,
        nexus_create_v2,
        nexus_lookup,
        nexus_lookup_mut,
        NexusNvmeParams,
        NexusPauseState,
        NvmeAnaState,
    },
    constants::NVME_NQN_PREFIX,
    core::{MayastorCliArgs, Protocol},
    lvs::Lvs,
    pool_backend::PoolArgs,
};

use crossbeam::channel::unbounded;
use once_cell::sync::OnceCell;
use std::process::Command;

pub mod common;

use common::{
    compose::{
        rpc::v0::{
            mayastor::{
                CreateNexusRequest,
                CreateNexusV2Request,
                CreatePoolRequest,
                CreateReplicaRequest,
                DestroyNexusRequest,
                Null,
                PublishNexusRequest,
            },
            GrpcConnect,
        },
        Binary,
        Builder,
        ComposeTest,
    },
    nvme::{
        get_nvme_resv_report,
        list_mayastor_nvme_devices,
        nvme_connect,
        nvme_disconnect_nqn,
    },
    MayastorTest,
};
use io_engine::{
    bdev::nexus::{
        ChildState,
        Error,
        FaultReason,
        NexusNvmePreemption,
        NexusStatus,
        NvmeReservation,
    },
    core::Mthread,
    grpc::v1::nexus::nexus_destroy,
};
use io_engine_tests::{
    compose::rpc::v0::RpcHandle,
    file_io::{test_write_to_file, DataSize},
    nvme::NmveConnectGuard,
    reactor_poll,
};

extern crate libnvme_rs;

static POOL_NAME: &str = "tpool";
static NXNAME: &str = "nexus0";
static NEXUS_UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static REPL_UUID: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";
static REPL2_UUID: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a5";
static HOSTNQN: &str = NVME_NQN_PREFIX;
static HOSTID0: &str = "53b35ce9-8e71-49a9-ab9b-cba7c5670fad";
static HOSTID1: &str = "c1affd2d-ef79-4ba4-b5cf-8eb48f9c07d0";
static HOSTID2: &str = "3f264cc3-1c95-44ca-bc1f-ed7fb68f3894";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";
static BDEVNAME11: &str = "aio:///host/tmp/disk1.img?blk_size=512";
static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///host/tmp/disk2.img?blk_size=512";

static PTPL_HOST_DIR: &str = "/tmp/ptpl";
static PTPL_CONTAINER_DIR: &str = "/host/tmp/ptpl";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

fn get_mayastor_nvme_device() -> String {
    let nvme_ms = list_mayastor_nvme_devices();
    assert_eq!(nvme_ms.len(), 1);
    format!("/dev/{}", nvme_ms[0].device)
}

#[tokio::test]
#[ignore]
/// Create the same nexus on both nodes with a replica on 1 node as their child.
async fn nexus_io_multipath() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_ANA_ENABLE", "1");
    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    // create a new composeTest
    let test = Builder::new()
        .name("nexus_shared_replica_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_dbg("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

    // create a pool on remote node
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    // create nexus on remote node with local replica as child
    hdls[0]
        .mayastor
        .create_nexus(CreateNexusRequest {
            uuid: NEXUS_UUID.to_string(),
            size: 32 * 1024 * 1024,
            children: [format!("loopback:///{REPL_UUID}")].to_vec(),
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let nexus_name = format!("nexus-{NEXUS_UUID}");
    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            // create nexus on local node with remote replica as child
            nexus_create(
                &name,
                32 * 1024 * 1024,
                Some(NEXUS_UUID),
                &[format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")],
            )
            .await
            .unwrap();
            // publish nexus on local node over nvmf
            nexus_lookup_mut(&name)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .unwrap();
        })
        .await;

    // publish nexus on other node
    hdls[0]
        .mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: NEXUS_UUID.to_string(),
            key: "".to_string(),
            share: Protocol::Nvmf as i32,
            ..Default::default()
        })
        .await
        .unwrap();

    let nqn = format!("{HOSTNQN}:nexus-{NEXUS_UUID}");
    nvme_connect("127.0.0.1", &nqn, "tcp", true);

    // The first attempt will fail with "Duplicate cntlid x with y" error from
    // kernel
    for i in 0 .. 2 {
        let status_c0 = nvme_connect(&ip0.to_string(), &nqn, "tcp", false);
        if i == 0 && status_c0.success() {
            break;
        }
        assert!(
            status_c0.success() || i != 1,
            "failed to connect to remote nexus, {}",
            status_c0
        );
    }

    let ns = get_mayastor_nvme_device();

    mayastor
        .spawn(async move {
            // set nexus on local node ANA state to non-optimized
            nexus_lookup_mut(&nexus_name)
                .unwrap()
                .set_ana_state(NvmeAnaState::NonOptimizedState)
                .await
                .unwrap();
        })
        .await;

    //  +- nvme0 tcp traddr=127.0.0.1 trsvcid=8420 live <ana_state>
    let output_subsys = Command::new("nvme")
        .args(["list-subsys"])
        .args([ns])
        .output()
        .unwrap();
    assert!(
        output_subsys.status.success(),
        "failed to list nvme subsystem, {}",
        output_subsys.status
    );
    let subsys = String::from_utf8(output_subsys.stdout).unwrap();
    let nvmec: Vec<&str> = subsys
        .lines()
        .filter(|line| line.contains("traddr=127.0.0.1"))
        .collect();
    assert_eq!(nvmec.len(), 1);
    let nv: Vec<&str> = nvmec[0].split(' ').collect();
    assert_eq!(nv[7], "non-optimized", "incorrect ANA state");

    // NQN:<nqn> disconnected 2 controller(s)
    let output_dis = Command::new("nvme")
        .args(["disconnect"])
        .args(["-n", &nqn])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect from nexuses, {}",
        output_dis.status
    );
    let s = String::from_utf8(output_dis.stdout).unwrap();
    let v: Vec<&str> = s.split(' ').collect();
    tracing::info!("nvme disconnected: {:?}", v);
    assert_eq!(v.len(), 4);
    assert_eq!(v[1], "disconnected");
    assert_eq!(v[0], format!("NQN:{}", &nqn), "mismatched NQN disconnected");
    assert_eq!(v[2], "2", "mismatched number of controllers disconnected");

    // Connect to remote replica to check key registered
    let rep_nqn = format!("{HOSTNQN}:{REPL_UUID}");
    nvme_connect(&ip0.to_string(), &rep_nqn, "tcp", true);

    let rep_dev = get_mayastor_nvme_device();

    let v = get_nvme_resv_report(&rep_dev);
    assert_eq!(v["rtype"], 0, "should have no reservation type");
    assert_eq!(v["regctl"], 1, "should have 1 registered controller");
    assert_eq!(
        v["ptpls"], 0,
        "should have Persist Through Power Loss State as 0"
    );
    assert_eq!(
        v["regctlext"][0]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v["regctlext"][0]["rcsts"], 0,
        "should have reservation status as no reservation"
    );
    assert_eq!(
        v["regctlext"][0]["rkey"], 0x12345678,
        "should have default registered key"
    );

    nvme_disconnect_nqn(&rep_nqn);

    // destroy nexus on remote node
    hdls[0]
        .mayastor
        .destroy_nexus(DestroyNexusRequest {
            uuid: NEXUS_UUID.to_string(),
        })
        .await
        .unwrap();

    // verify that the replica is still shared over nvmf
    assert!(hdls[0]
        .mayastor
        .list_replicas(Null {})
        .await
        .unwrap()
        .into_inner()
        .replicas[0]
        .uri
        .contains("nvmf://"));
}

#[tokio::test]
/// Create a nexus with a remote replica on 1 node as its child.
/// Create another nexus with the same remote replica as its child, verifying
/// that the write exclusive, all registrants reservation has also been
/// registered by the new nexus.
async fn nexus_io_resv_acquire() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    std::env::set_var("MAYASTOR_NVMF_HOSTID", HOSTID0);

    let test = Builder::new()
        .name("nexus_resv_acquire_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms2",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1),
        )
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

    // create a pool on remote node 1
    // grpc handles can be returned in any order, we simply define the first
    // as "node 1"
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let resv_key = 0xabcd_ef00_1234_5678;
    mayastor
        .spawn(async move {
            let mut nvme_params = NexusNvmeParams::default();
            nvme_params.set_resv_key(resv_key);
            // create nexus on local node with remote replica as child
            nexus_create_v2(
                NXNAME,
                32 * 1024 * 1024,
                NEXUS_UUID,
                nvme_params,
                &[format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")],
                None,
            )
            .await
            .unwrap();
            bdev_io::write_some(NXNAME, 0, 2, 0xff).await.unwrap();
            bdev_io::read_some(NXNAME, 0, 2, 0xff).await.unwrap();
        })
        .await;

    // Connect to remote replica to check key registered
    let rep_nqn = format!("{HOSTNQN}:{REPL_UUID}");
    nvme_connect(&ip0.to_string(), &rep_nqn, "tcp", true);

    let rep_dev = get_mayastor_nvme_device();

    let v = get_nvme_resv_report(&rep_dev);
    assert_eq!(
        v["rtype"], 5,
        "should have write exclusive, all registrants reservation"
    );
    assert_eq!(v["regctl"], 1, "should have 1 registered controller");
    assert_eq!(
        v["ptpls"], 0,
        "should have Persist Through Power Loss State as 0"
    );
    assert_eq!(
        v["regctlext"][0]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v["regctlext"][0]["rcsts"], 1,
        "should have reservation status as reserved"
    );
    assert_eq!(
        v["regctlext"][0]["hostid"].as_str().unwrap(),
        HOSTID0.to_string().replace('-', ""),
        "should match host ID of NVMe client"
    );
    assert_eq!(
        v["regctlext"][0]["rkey"], resv_key,
        "should have configured registered key"
    );

    // create nexus on remote node 2 with replica on node 1 as child
    let resv_key2 = 0xfeed_f00d_bead_5678;
    hdls[1]
        .mayastor
        .create_nexus_v2(CreateNexusV2Request {
            name: NXNAME.to_string(),
            uuid: NEXUS_UUID.to_string(),
            size: 32 * 1024 * 1024,
            min_cntl_id: 1,
            max_cntl_id: 0xffef,
            resv_key: resv_key2,
            preempt_key: 0,
            children: [format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")]
                .to_vec(),
            nexus_info_key: "".to_string(),
            resv_type: None,
            preempt_policy: 0,
        })
        .await
        .unwrap();

    // Verify that the second nexus has registered
    let v2 = get_nvme_resv_report(&rep_dev);
    assert_eq!(
        v2["rtype"], 5,
        "should have write exclusive, all registrants reservation"
    );
    assert_eq!(v2["regctl"], 2, "should have 2 registered controllers");
    assert_eq!(
        v2["ptpls"], 0,
        "should have Persist Through Power Loss State as 0"
    );
    assert_eq!(
        v2["regctlext"][1]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v2["regctlext"][1]["rcsts"].as_u64().unwrap() & 0x1,
        0,
        "should have reservation status as not reserved"
    );
    assert_eq!(
        v2["regctlext"][1]["rkey"], resv_key2,
        "should have configured registered key"
    );
    assert_eq!(
        v2["regctlext"][1]["hostid"].as_str().unwrap(),
        HOSTID1.to_string().replace('-', ""),
        "should match host ID of NVMe client"
    );

    mayastor
        .spawn(async move {
            bdev_io::write_some(NXNAME, 0, 2, 0xff)
                .await
                .expect("writes should still succeed");
            bdev_io::read_some(NXNAME, 0, 2, 0xff)
                .await
                .expect("reads should succeed");

            nexus_lookup_mut(NXNAME).unwrap().destroy().await.unwrap();
        })
        .await;

    nvme_disconnect_nqn(&rep_nqn);
}

#[tokio::test]
/// Create a nexus with a remote replica on 1 node as its child.
/// Create another nexus with the same remote replica as its child, verifying
/// that exclusive access prevents the first nexus from accessing the data.
async fn nexus_io_resv_preempt() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    std::env::set_var("MAYASTOR_NVMF_HOSTID", HOSTID0);

    common::delete_file(&[DISKNAME1.into(), PTPL_HOST_DIR.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);

    let ptpl_dir = |ms| format!("{PTPL_CONTAINER_DIR}/{ms}");

    let test = Builder::new()
        .name("nexus_io_resv_preempt_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms2",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1)
                .with_args(vec!["--ptpl-dir", ptpl_dir("ms2").as_str()])
                .with_bind("/tmp", "/host/tmp"),
        )
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID2)
                .with_args(vec!["--ptpl-dir", ptpl_dir("ms1").as_str()])
                .with_bind("/tmp", "/host/tmp"),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

    // create a pool on remote node 1
    // grpc handles can be returned in any order, we simply define the first
    // as "node 1"
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec![BDEVNAME11.into()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let resv_key = 0xabcd_ef00_1234_5678;
    mayastor
        .spawn(async move {
            let mut nvme_params = NexusNvmeParams::default();
            nvme_params.set_resv_key(resv_key);
            nvme_params.set_resv_type(NvmeReservation::ExclusiveAccess);
            nvme_params.set_preempt_policy(NexusNvmePreemption::Holder);
            // create nexus on local node with remote replica as child
            nexus_create_v2(
                NXNAME,
                32 * 1024 * 1024,
                NEXUS_UUID,
                nvme_params,
                &[format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")],
                None,
            )
            .await
            .unwrap();
            bdev_io::write_some(NXNAME, 0, 2, 0xff).await.unwrap();
            bdev_io::read_some(NXNAME, 0, 2, 0xff).await.unwrap();
        })
        .await;

    // Connect to remote replica to check key registered
    let rep_nqn = format!("{HOSTNQN}:{REPL_UUID}");

    nvme_connect(&ip0.to_string(), &rep_nqn, "tcp", true);

    let rep_dev = get_mayastor_nvme_device();

    let v = get_nvme_resv_report(&rep_dev);
    assert_eq!(v["rtype"], 2, "should have exclusive access");
    assert_eq!(v["regctl"], 1, "should have 1 registered controller");
    assert_eq!(
        v["ptpls"], 0,
        "should have Persist Through Power Loss State as 0"
    );
    assert_eq!(
        v["regctlext"][0]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v["regctlext"][0]["rcsts"], 1,
        "should have reservation status as reserved"
    );
    assert_eq!(
        v["regctlext"][0]["hostid"].as_str().unwrap(),
        HOSTID0.to_string().replace('-', ""),
        "should match host ID of NVMe client"
    );
    assert_eq!(
        v["regctlext"][0]["rkey"], resv_key,
        "should have configured registered key"
    );

    // create nexus on remote node 2 with replica on node 1 as child
    let resv_key2 = 0xfeed_f00d_bead_5678;
    hdls[1]
        .mayastor
        .create_nexus_v2(CreateNexusV2Request {
            name: NXNAME.to_string(),
            uuid: NEXUS_UUID.to_string(),
            size: 32 * 1024 * 1024,
            min_cntl_id: 1,
            max_cntl_id: 0xffef,
            resv_key: resv_key2,
            preempt_key: 0,
            children: [format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")]
                .to_vec(),
            nexus_info_key: "".to_string(),
            resv_type: Some(NvmeReservation::ExclusiveAccess as i32),
            preempt_policy: NexusNvmePreemption::Holder as i32,
        })
        .await
        .unwrap();

    // Verify that the second nexus has registered
    let v2 = get_nvme_resv_report(&rep_dev);
    assert_eq!(v["rtype"], 2, "should have exclusive access");
    assert_eq!(v2["regctl"], 1, "should have 1 registered controllers");
    assert_eq!(
        v2["ptpls"], 1,
        "should have Persist Through Power Loss State enabled"
    );
    assert_eq!(
        v2["regctlext"][0]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v2["regctlext"][0]["rcsts"].as_u64().unwrap() & 0x1,
        1,
        "should have reservation status as reserved"
    );
    assert_eq!(
        v2["regctlext"][0]["rkey"], resv_key2,
        "should have configured registered key"
    );
    assert_eq!(
        v2["regctlext"][0]["hostid"].as_str().unwrap(),
        HOSTID2.to_string().replace('-', ""),
        "should match host ID of NVMe client"
    );

    // Initiate I/O to trigger reservation conflict and initiate nexus self
    // shutdown.
    mayastor
        .spawn(async move {
            bdev_io::write_some(NXNAME, 0, 2, 0xff)
                .await
                .expect_err("writes should fail");
            bdev_io::read_some(NXNAME, 0, 2, 0xff)
                .await
                .expect_err("reads should fail");
        })
        .await;

    // Wait a bit to let nexus complete self-shutdown sequence.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    mayastor
        .spawn(async move {
            let nexus = nexus_lookup_mut(NXNAME).unwrap();

            // Make sure nexus is in Shutdown state.
            assert_eq!(
                nexus.status(),
                NexusStatus::Shutdown,
                "Nexus must transition into Shutdown state"
            );

            // Make sure all child devices are in faulted state and don't have any associated
            // devices and I/O handles.
            nexus.children().iter().for_each(|c| {
                assert_eq!(c.state(), ChildState::Faulted(FaultReason::IoError));

                assert!(
                    c.get_device().is_err(),
                    "Child device still has its block device after nexus shutdown"
                );

                assert!(
                    c.get_io_handle().is_err(),
                    "Child device still has I/O handle after nexus shutdown"
                );
            });

            nexus.destroy().await.unwrap();
        })
        .await;

    nvme_disconnect_nqn(&rep_nqn);

    test.restart("ms2").await.unwrap();
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    loop {
        if start.elapsed() > timeout {
            panic!("Timed out waiting for container to restart");
        }
        if hdls[0].bdev.list(Null {}).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec![BDEVNAME11.into()],
        })
        .await
        .unwrap();

    nvme_connect(&ip0.to_string(), &rep_nqn, "tcp", true);
    let rep_dev = get_mayastor_nvme_device();

    // After restart the reservations should still be in place!
    let v2 = get_nvme_resv_report(&rep_dev);
    assert_eq!(v["rtype"], 2, "should have exclusive access");
    assert_eq!(v2["regctl"], 1, "should have 1 registered controllers");
    assert_eq!(
        v2["ptpls"], 1,
        "should have Persist Through Power Loss State enabled"
    );
    assert_eq!(
        v2["regctlext"][0]["cntlid"], 0xffff,
        "should have dynamic controller ID"
    );
    assert_eq!(
        v2["regctlext"][0]["rcsts"].as_u64().unwrap() & 0x1,
        1,
        "should have reservation status as reserved"
    );
    assert_eq!(
        v2["regctlext"][0]["rkey"], resv_key2,
        "should have configured registered key"
    );
    assert_eq!(
        v2["regctlext"][0]["hostid"].as_str().unwrap(),
        HOSTID2.to_string().replace('-', ""),
        "should match host ID of NVMe client"
    );

    nvme_disconnect_nqn(&rep_nqn);
}

#[tokio::test]
/// Create a nexus with a remote replica on 1 node as its child.
/// Create another nexus with the same remote replica as its child, verifying
/// that exclusive access prevents the first nexus from accessing the data.
async fn nexus_io_resv_preempt_tabled() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    std::env::set_var("MAYASTOR_NVMF_HOSTID", HOSTID0);

    let test = Builder::new()
        .name("nexus_io_resv_preempt_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms2",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1),
        )
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID2),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

    // create a pool on remote node 1
    // grpc handles can be returned in any order, we simply define the first
    // as "node 1"
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec!["malloc:///disk0?size_mb=64".into()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    async fn test_fn(
        hdls: &mut [RpcHandle],
        resv: NvmeReservation,
        resv_key: u64,
        local: bool,
    ) {
        let mayastor = get_ms();
        let ip0 = hdls[0].endpoint.ip();
        println!("Using resv {} and key {}", resv as u8, resv_key);
        if local {
            mayastor
                .spawn(async move {
                    let mut nvme_params = NexusNvmeParams::default();
                    nvme_params.set_resv_key(resv_key);
                    nvme_params.set_resv_type(resv);
                    nvme_params.set_preempt_policy(NexusNvmePreemption::Holder);
                    // create nexus on local node with remote replica as child
                    nexus_create_v2(
                        NXNAME,
                        32 * 1024 * 1024,
                        NEXUS_UUID,
                        nvme_params,
                        &[format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}")],
                        None,
                    )
                    .await
                    .unwrap();
                    bdev_io::write_some(NXNAME, 0, 2, 0xff).await.unwrap();
                    bdev_io::read_some(NXNAME, 0, 2, 0xff).await.unwrap();
                })
                .await;
        } else {
            hdls[1]
                .mayastor
                .create_nexus_v2(CreateNexusV2Request {
                    name: NXNAME.to_string(),
                    uuid: NEXUS_UUID.to_string(),
                    size: 32 * 1024 * 1024,
                    min_cntl_id: 1,
                    max_cntl_id: 0xffef,
                    resv_key,
                    preempt_key: 0,
                    children: [format!(
                        "nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}"
                    )]
                    .to_vec(),
                    nexus_info_key: "".to_string(),
                    resv_type: Some(resv as i32),
                    preempt_policy: NexusNvmePreemption::Holder as i32,
                })
                .await
                .unwrap();
        }

        // Connect to remote replica to check key registered
        let rep_nqn = format!("{HOSTNQN}:{REPL_UUID}");

        nvme_connect(&ip0.to_string(), &rep_nqn, "tcp", true);

        let rep_dev = get_mayastor_nvme_device();

        let v = get_nvme_resv_report(&rep_dev);

        assert_eq!(
            v["ptpls"], 0,
            "should have Persist Through Power Loss State as 0"
        );

        let shared = matches!(
            resv,
            NvmeReservation::ExclusiveAccessAllRegs
                | NvmeReservation::WriteExclusiveAllRegs
        );
        if shared {
            // we don't currently distinguish between
            assert!(v["rtype"] == 5 || v["rtype"] == 6);
        } else {
            assert_eq!(v["rtype"], resv as u8);
        }

        let mut reserved = false;
        let registrants = v["regctl"].as_u64().unwrap() as usize;
        for i in 0 .. registrants {
            let entry = &v["regctlext"][i];
            assert_eq!(
                entry["cntlid"], 0xffff,
                "should have dynamic controller ID"
            );
            if entry["rcsts"] == 1 && !shared {
                reserved = true;

                let host = if local { HOSTID0 } else { HOSTID2 };
                assert_eq!(
                    entry["hostid"].as_str().unwrap(),
                    host.to_string().replace('-', ""),
                    "should match host ID of NVMe client"
                );
                assert_eq!(
                    entry["rkey"], resv_key,
                    "should have configured registered key"
                );
            }
        }

        assert!(
            reserved || shared,
            "should have reservation status as reserved"
        );

        nvme_disconnect_nqn(&rep_nqn);

        if local {
            mayastor
                .spawn(async move {
                    nexus_destroy(NEXUS_UUID).await.unwrap();
                })
                .await;
        } else {
            hdls[1]
                .mayastor
                .destroy_nexus(DestroyNexusRequest {
                    uuid: NEXUS_UUID.to_string(),
                })
                .await
                .unwrap();
        }
    }

    let test_matrix = [
        NvmeReservation::WriteExclusiveAllRegs,
        NvmeReservation::ExclusiveAccess,
        NvmeReservation::ExclusiveAccess,
        NvmeReservation::WriteExclusive,
        NvmeReservation::ExclusiveAccess,
        NvmeReservation::WriteExclusiveAllRegs,
        NvmeReservation::WriteExclusiveAllRegs,
        NvmeReservation::ExclusiveAccessAllRegs,
        NvmeReservation::ExclusiveAccess,
        NvmeReservation::ExclusiveAccessAllRegs,
        NvmeReservation::WriteExclusiveRegsOnly,
        NvmeReservation::ExclusiveAccess,
        NvmeReservation::WriteExclusiveRegsOnly,
        NvmeReservation::ExclusiveAccess,
    ];

    let resv_key = 0x1;
    for test_resv in test_matrix {
        test_fn(&mut hdls, test_resv, resv_key, true).await;
    }

    let mut resv_key = 0x1;
    for test_resv in test_matrix {
        test_fn(&mut hdls, test_resv, resv_key, true).await;
        resv_key += 1;
    }

    let resv_key = 0x1;
    for test_resv in test_matrix {
        test_fn(&mut hdls, test_resv, resv_key, (resv_key % 2) == 1).await;
    }

    let mut resv_key = 0x1;
    for test_resv in test_matrix {
        test_fn(&mut hdls, test_resv, resv_key, (resv_key % 2) == 1).await;
        resv_key += 1;
    }
}

#[tokio::test]
/// Create a nexus with a local and a remote replica.
/// Verify that write-zeroes does actually write zeroes.
async fn nexus_io_write_zeroes() {
    common::composer_init();

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let test = Builder::new()
        .name("nexus_io_write_zeroes_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_bind("/tmp", "/host/tmp"),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

    // create a pool on remote node
    hdls[0]
        .mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec![BDEVNAME2.to_string()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdls[0]
        .mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let nexus_name = format!("nexus-{NEXUS_UUID}");
    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            // Create local pool and replica
            Lvs::create_or_import(PoolArgs {
                name: POOL_NAME.to_string(),
                disks: vec![BDEVNAME1.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

            let pool = Lvs::lookup(POOL_NAME).unwrap();
            pool.create_lvol(REPL_UUID, 32 * 1024 * 1024, None, true, None)
                .await
                .unwrap();

            // create nexus on local node with 2 children, local and remote
            nexus_create(
                &name,
                32 * 1024 * 1024,
                Some(NEXUS_UUID),
                &[
                    format!("loopback:///{REPL_UUID}"),
                    format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}"),
                ],
            )
            .await
            .unwrap();

            bdev_io::write_some(&name, 0, 2, 0xff).await.unwrap();
            // Read twice to ensure round-robin read from both replicas
            bdev_io::read_some(&name, 0, 2, 0xff)
                .await
                .expect("read should return block of 0xff");
            bdev_io::read_some(&name, 0, 2, 0xff)
                .await
                .expect("read should return block of 0xff");
            bdev_io::write_zeroes_some(&name, 0, 512).await.unwrap();
            bdev_io::read_some(&name, 0, 2, 0)
                .await
                .expect("read should return block of 0");
            bdev_io::read_some(&name, 0, 2, 0)
                .await
                .expect("read should return block of 0");
        })
        .await;
}

#[tokio::test]
async fn nexus_io_freeze() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_ANA_ENABLE", "1");
    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    // create a new composeTest
    let test = Builder::new()
        .name("nexus_io_freeze")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_bind("/tmp", "/host/tmp"),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();
    create_pool_replicas(&test, 0).await;

    let mayastor = get_ms();
    let ip0 = test.container_ip("ms1");
    let nexus_name = format!("nexus-{NEXUS_UUID}");
    let nexus_children = [
        format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL_UUID}"),
        format!("nvmf://{ip0}:8420/{HOSTNQN}:{REPL2_UUID}"),
    ];

    let name = nexus_name.clone();
    let children = nexus_children.clone();
    mayastor
        .spawn(async move {
            // create nexus on local node with remote replica as child
            nexus_create(&name, 32 * 1024 * 1024, Some(NEXUS_UUID), &children)
                .await
                .unwrap();
            // publish nexus on local node over nvmf
            nexus_lookup_mut(&name)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .unwrap();
            assert_eq!(
                nexus_pause_state(&name),
                Some(NexusPauseState::Unpaused)
            );
        })
        .await;

    // This will lead into a child retire, which means the nexus will be faulted
    // and subsystem frozen!
    test.restart("ms1").await.unwrap();
    wait_nexus_faulted(&nexus_name, std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Frozen));

            nexus_lookup_mut(&name).unwrap().pause().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Frozen));

            nexus_lookup_mut(&name).unwrap().resume().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Frozen));

            nexus_lookup_mut(&name).unwrap().destroy().await.unwrap();
        })
        .await;

    create_pool_replicas(&test, 0).await;

    let name = nexus_name.clone();
    let children = nexus_children.clone();
    mayastor
        .spawn(async move {
            nexus_create(&name, 32 * 1024 * 1024, Some(NEXUS_UUID), &children)
                .await
                .unwrap();
            nexus_lookup_mut(&name)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .unwrap();

            // Pause, so now WE must be the ones which resume to frozen!
            nexus_lookup_mut(&name).unwrap().pause().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));
        })
        .await;

    test.restart("ms1").await.unwrap();
    wait_nexus_faulted(&nexus_name, std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let name = nexus_name.clone();
    let children = nexus_children.clone();
    mayastor
        .spawn(async move {
            nexus_lookup_mut(&name).unwrap().pause().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));

            nexus_lookup_mut(&name).unwrap().resume().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Paused));

            // Final resume, transition to Frozen!
            nexus_lookup_mut(&name).unwrap().resume().await.unwrap();
            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Frozen));

            nexus_lookup_mut(&name)
                .unwrap()
                .unshare_nexus()
                .await
                .unwrap();

            assert_eq!(nexus_pause_state(&name), Some(NexusPauseState::Frozen));

            let status = nexus_lookup_mut(&name)
                .unwrap()
                .add_child("malloc:///disk?size_mb=32", false)
                .await;
            assert!(matches!(status, Err(Error::OperationNotAllowed { .. })));
            let status = nexus_lookup_mut(&name)
                .unwrap()
                .online_child(&children[0])
                .await;
            assert!(matches!(status, Err(Error::OperationNotAllowed { .. })));

            nexus_lookup_mut(&name).unwrap().destroy().await.unwrap();
        })
        .await;

    create_pool_replicas(&test, 0).await;

    let name = nexus_name.clone();
    let children = nexus_children.first().cloned().unwrap();
    let guard = mayastor
        .spawn(async move {
            nexus_create(
                &name,
                32 * 1024 * 1024,
                Some(NEXUS_UUID),
                &[children.to_string()],
            )
            .await
            .unwrap();
            let share = nexus_lookup_mut(&name)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .unwrap();
            tracing::info!("{share}");

            // Connect to remote replica to check key registered
            let nqn = format!("{HOSTNQN}:nexus-{NEXUS_UUID}");

            let (s, r) = unbounded::<NmveConnectGuard>();
            Mthread::spawn_unaffinitized(move || {
                s.send(NmveConnectGuard::connect("127.0.0.1", &nqn))
            });
            let guard: NmveConnectGuard;
            reactor_poll!(r, guard);
            guard
        })
        .await;

    let (s, r) = unbounded::<()>();
    tokio::spawn(async move {
        let device = get_mayastor_nvme_device();
        test_write_to_file(
            device,
            DataSize::default(),
            32,
            DataSize::from_mb(1),
        )
        .await
        .ok();
        s.send(())
    });
    mayastor
        .spawn(async move {
            let _wait: ();
            reactor_poll!(r, _wait);
        })
        .await;
    drop(guard);

    wait_nexus_faulted(&nexus_name, std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            let enospc = nexus_lookup(&name)
                .map(|n| n.children().iter().all(|c| c.state().is_enospc()));
            assert_eq!(enospc, Some(true));
            // We're not Paused, because the nexus is faulted due to ENOSPC!
            assert_eq!(
                nexus_pause_state(&name),
                Some(NexusPauseState::Unpaused)
            );
            nexus_lookup_mut(&name).unwrap().destroy().await.unwrap();
        })
        .await;
}

fn nexus_pause_state(name: &str) -> Option<NexusPauseState> {
    nexus_lookup(name).unwrap().io_subsystem_state()
}

async fn create_pool_replicas(test: &ComposeTest, index: usize) {
    let grpc = GrpcConnect::new(test);
    let mut hdls = grpc.grpc_handles().await.unwrap();
    let hdl = &mut hdls[index];

    // create a pool on remote node
    hdl.mayastor
        .create_pool(CreatePoolRequest {
            name: POOL_NAME.to_string(),
            disks: vec!["malloc:///disk0?size_mb=128".into()],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdl.mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: true,
            share: 1,
            allowed_hosts: vec![],
        })
        .await
        .unwrap();

    // create replica, shared over nvmf
    hdl.mayastor
        .create_replica(CreateReplicaRequest {
            uuid: REPL2_UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 100 * 1024 * 1024,
            thin: false,
            share: 1,
            allowed_hosts: vec![],
        })
        .await
        .unwrap();
}

async fn wait_nexus_faulted(
    name: &str,
    timeout: std::time::Duration,
) -> Result<(), std::time::Duration> {
    let mayastor = get_ms();
    let start = std::time::Instant::now();

    while start.elapsed() <= timeout {
        let name = name.to_string();
        let faulted = mayastor
            .spawn(async move {
                nexus_lookup(&name).unwrap().status() == NexusStatus::Faulted
            })
            .await;
        if faulted {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Err(start.elapsed())
}
