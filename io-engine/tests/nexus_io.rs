//! Nexus IO tests for multipath NVMf, reservation, and write-zeroes
use common::bdev_io;
use io_engine::{
    bdev::nexus::{
        nexus_create,
        nexus_create_v2,
        nexus_lookup_mut,
        NexusNvmeParams,
        NvmeAnaState,
    },
    constants::{NVME_CONTROLLER_MODEL_ID, NVME_NQN_PREFIX},
    core::{MayastorCliArgs, Protocol},
    lvs::Lvs,
    pool::PoolArgs,
};

use once_cell::sync::OnceCell;
use rpc::mayastor::{
    CreateNexusRequest,
    CreateNexusV2Request,
    CreatePoolRequest,
    CreateReplicaRequest,
    DestroyNexusRequest,
    Null,
    PublishNexusRequest,
};
use std::process::{Command, ExitStatus};

pub mod common;
use common::{compose::Builder, MayastorTest};

extern crate libnvme_rs;

static POOL_NAME: &str = "tpool";
static NXNAME: &str = "nexus0";
static UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static HOSTNQN: &str = NVME_NQN_PREFIX;
static HOSTID0: &str = "53b35ce9-8e71-49a9-ab9b-cba7c5670fad";
static HOSTID1: &str = "c1affd2d-ef79-4ba4-b5cf-8eb48f9c07d0";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";
static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///host/tmp/disk2.img?blk_size=512";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn nvme_connect(
    target_addr: &str,
    nqn: &str,
    must_succeed: bool,
) -> ExitStatus {
    let status = Command::new("nvme")
        .args(&["connect"])
        .args(&["-t", "tcp"])
        .args(&["-a", target_addr])
        .args(&["-s", "8420"])
        .args(&["-n", nqn])
        .status()
        .unwrap();

    if !status.success() {
        let msg = format!(
            "failed to connect to {}, nqn {}: {}",
            target_addr, nqn, status,
        );
        if must_succeed {
            panic!("{}", msg);
        } else {
            eprintln!("{}", msg);
        }
    } else {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    status
}

fn get_mayastor_nvme_device() -> String {
    let nvme_devices = libnvme_rs::NvmeTarget::list();
    let nvme_ms: Vec<&String> = nvme_devices
        .iter()
        .filter(|dev| dev.model.contains(NVME_CONTROLLER_MODEL_ID))
        .map(|dev| &dev.device)
        .collect();
    assert_eq!(nvme_ms.len(), 1);
    format!("/dev/{}", nvme_ms[0])
}

fn get_nvme_resv_report(nvme_dev: &str) -> serde_json::Value {
    let output_resv = Command::new("nvme")
        .args(&["resv-report"])
        .args(&[nvme_dev])
        .args(&["-c", "1"])
        .args(&["-o", "json"])
        .output()
        .unwrap();
    assert!(
        output_resv.status.success(),
        "failed to get reservation report from {}: {}",
        nvme_dev,
        output_resv.status
    );
    let resv_rep = String::from_utf8(output_resv.stdout).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&resv_rep).expect("JSON was not well-formatted");
    v
}

fn nvme_disconnect_nqn(nqn: &str) {
    let output_dis = Command::new("nvme")
        .args(&["disconnect"])
        .args(&["-n", nqn])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect from {}: {}",
        nqn,
        output_dis.status
    );
}

#[tokio::test]
#[ignore]
/// Create the same nexus on both nodes with a replica on 1 node as their child.
async fn nexus_io_multipath() {
    std::env::set_var("NEXUS_NVMF_ANA_ENABLE", "1");
    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    // create a new composeTest
    let test = Builder::new()
        .name("nexus_shared_replica_test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

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
            uuid: UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
        })
        .await
        .unwrap();

    // create nexus on remote node with local replica as child
    hdls[0]
        .mayastor
        .create_nexus(CreateNexusRequest {
            uuid: UUID.to_string(),
            size: 32 * 1024 * 1024,
            children: [format!("loopback:///{}", UUID)].to_vec(),
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let nexus_name = format!("nexus-{}", UUID);
    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            // create nexus on local node with remote replica as child
            nexus_create(
                &name,
                32 * 1024 * 1024,
                Some(UUID),
                &[format!("nvmf://{}:8420/{}:{}", ip0, HOSTNQN, UUID)],
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
            uuid: UUID.to_string(),
            key: "".to_string(),
            share: Protocol::Nvmf as i32,
        })
        .await
        .unwrap();

    let nqn = format!("{}:nexus-{}", HOSTNQN, UUID);
    nvme_connect("127.0.0.1", &nqn, true);

    // The first attempt will fail with "Duplicate cntlid x with y" error from
    // kernel
    for i in 0 .. 2 {
        let status_c0 = nvme_connect(&ip0.to_string(), &nqn, false);
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
        .args(&["list-subsys"])
        .args(&[ns])
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
        .args(&["disconnect"])
        .args(&["-n", &nqn])
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
    let rep_nqn = format!("{}:{}", HOSTNQN, UUID);
    nvme_connect(&ip0.to_string(), &rep_nqn, true);

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
            uuid: UUID.to_string(),
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
    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    std::env::set_var("MAYASTOR_NVMF_HOSTID", HOSTID0);
    let test = Builder::new()
        .name("nexus_resv_acquire_test")
        .network("10.1.0.0/16")
        .add_container_bin(
            "ms2",
            composer::Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1),
        )
        .add_container_bin(
            "ms1",
            composer::Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID1),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

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
            uuid: UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
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
                &NXNAME.to_string(),
                32 * 1024 * 1024,
                UUID,
                nvme_params,
                &[format!("nvmf://{}:8420/{}:{}", ip0, HOSTNQN, UUID)],
                None,
            )
            .await
            .unwrap();
            bdev_io::write_some(&NXNAME.to_string(), 0, 0xff)
                .await
                .unwrap();
            bdev_io::read_some(&NXNAME.to_string(), 0, 0xff)
                .await
                .unwrap();
        })
        .await;

    // Connect to remote replica to check key registered
    let rep_nqn = format!("{}:{}", HOSTNQN, UUID);
    nvme_connect(&ip0.to_string(), &rep_nqn, true);

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
        HOSTID0.to_string().replace("-", ""),
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
            uuid: UUID.to_string(),
            size: 32 * 1024 * 1024,
            min_cntl_id: 1,
            max_cntl_id: 0xffef,
            resv_key: resv_key2,
            preempt_key: 0,
            children: [format!("nvmf://{}:8420/{}:{}", ip0, HOSTNQN, UUID)]
                .to_vec(),
            nexus_info_key: "".to_string(),
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
        HOSTID1.to_string().replace("-", ""),
        "should match host ID of NVMe client"
    );

    mayastor
        .spawn(async move {
            bdev_io::write_some(&NXNAME.to_string(), 0, 0xff)
                .await
                .expect("writes should still succeed");
            bdev_io::read_some(&NXNAME.to_string(), 0, 0xff)
                .await
                .expect("reads should succeed");

            nexus_lookup_mut(&NXNAME.to_string())
                .unwrap()
                .destroy()
                .await
                .unwrap();
        })
        .await;

    nvme_disconnect_nqn(&rep_nqn);
}

#[tokio::test]
/// Create a nexus with a local and a remote replica.
/// Verify that write-zeroes does actually write zeroes.
async fn nexus_io_write_zeroes() {
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let test = Builder::new()
        .name("nexus_io_write_zeroes_test")
        .network("10.1.0.0/16")
        .add_container_bin(
            "ms1",
            composer::Binary::from_dbg("io-engine")
                .with_bind("/tmp", "/host/tmp"),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

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
            uuid: UUID.to_string(),
            pool: POOL_NAME.to_string(),
            size: 32 * 1024 * 1024,
            thin: false,
            share: 1,
        })
        .await
        .unwrap();

    let mayastor = get_ms();
    let ip0 = hdls[0].endpoint.ip();
    let nexus_name = format!("nexus-{}", UUID);
    let name = nexus_name.clone();
    mayastor
        .spawn(async move {
            // Create local pool and replica
            Lvs::create_or_import(PoolArgs {
                name: POOL_NAME.to_string(),
                disks: vec![BDEVNAME1.to_string()],
                uuid: None,
            })
            .await
            .unwrap();

            let pool = Lvs::lookup(POOL_NAME).unwrap();
            pool.create_lvol(&UUID.to_string(), 32 * 1024 * 1024, None, true)
                .await
                .unwrap();

            // create nexus on local node with 2 children, local and remote
            nexus_create(
                &name,
                32 * 1024 * 1024,
                Some(UUID),
                &[
                    format!("loopback:///{}", UUID),
                    format!("nvmf://{}:8420/{}:{}", ip0, HOSTNQN, UUID),
                ],
            )
            .await
            .unwrap();

            bdev_io::write_some(&name, 0, 0xff).await.unwrap();
            // Read twice to ensure round-robin read from both replicas
            bdev_io::read_some(&name, 0, 0xff)
                .await
                .expect("read should return block of 0xff");
            bdev_io::read_some(&name, 0, 0xff)
                .await
                .expect("read should return block of 0xff");
            bdev_io::write_zeroes_some(&name, 0, 512).await.unwrap();
            bdev_io::read_some(&name, 0, 0)
                .await
                .expect("read should return block of 0");
            bdev_io::read_some(&name, 0, 0)
                .await
                .expect("read should return block of 0");
        })
        .await;
}
