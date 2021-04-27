//! Multipath NVMf tests
//! Create the same nexus on both nodes with a replica on 1 node as their child.
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::MayastorCliArgs,
};
use rpc::mayastor::{
    CreateNexusRequest,
    CreatePoolRequest,
    CreateReplicaRequest,
    NvmeAnaState,
    PublishNexusRequest,
    ShareProtocolNexus,
};
use std::process::Command;

pub mod common;
use common::{compose::Builder, MayastorTest};

static POOL_NAME: &str = "tpool";
static UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static HOSTNQN: &str = "nqn.2019-05.io.openebs";

fn get_mayastor_nvme_device() -> String {
    let output_list = Command::new("nvme").args(&["list"]).output().unwrap();
    assert!(
        output_list.status.success(),
        "failed to list nvme devices, {}",
        output_list.status
    );
    let sl = String::from_utf8(output_list.stdout).unwrap();
    let nvmems: Vec<&str> = sl
        .lines()
        .filter(|line| line.contains("Mayastor NVMe controller"))
        .collect();
    assert_eq!(nvmems.len(), 1);
    let ns = nvmems[0].split(' ').collect::<Vec<_>>()[0];
    ns.to_string()
}

#[tokio::test]
async fn nexus_multipath() {
    std::env::set_var("NEXUS_NVMF_ANA_ENABLE", "1");
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

    let mayastor = MayastorTest::new(MayastorCliArgs::default());
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
            nexus_lookup(&name)
                .unwrap()
                .share(ShareProtocolNexus::NexusNvmf, None)
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
            share: ShareProtocolNexus::NexusNvmf as i32,
        })
        .await
        .unwrap();

    let nqn = format!("{}:nexus-{}", HOSTNQN, UUID);
    let status = Command::new("nvme")
        .args(&["connect"])
        .args(&["-t", "tcp"])
        .args(&["-a", "127.0.0.1"])
        .args(&["-s", "8420"])
        .args(&["-n", &nqn])
        .status()
        .unwrap();
    assert!(
        status.success(),
        "failed to connect to local nexus, {}",
        status
    );

    // The first attempt will fail with "Duplicate cntlid x with y" error from
    // kernel
    for i in 0 .. 2 {
        let status_c0 = Command::new("nvme")
            .args(&["connect"])
            .args(&["-t", "tcp"])
            .args(&["-a", &ip0.to_string()])
            .args(&["-s", "8420"])
            .args(&["-n", &nqn])
            .status()
            .unwrap();
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
            nexus_lookup(&nexus_name)
                .unwrap()
                .set_ana_state(NvmeAnaState::NvmeAnaNonOptimizedState)
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
    let status = Command::new("nvme")
        .args(&["connect"])
        .args(&["-t", "tcp"])
        .args(&["-a", &ip0.to_string()])
        .args(&["-s", "8420"])
        .args(&["-n", &rep_nqn])
        .status()
        .unwrap();
    assert!(
        status.success(),
        "failed to connect to remote replica, {}",
        status
    );

    let rep_dev = get_mayastor_nvme_device();

    let output_resv = Command::new("nvme")
        .args(&["resv-report"])
        .args(&[rep_dev])
        .args(&["-c", "1"])
        .args(&["-o", "json"])
        .output()
        .unwrap();
    assert!(
        output_resv.status.success(),
        "failed to get reservation report from remote replica, {}",
        output_resv.status
    );
    let resv_rep = String::from_utf8(output_resv.stdout).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&resv_rep).expect("JSON was not well-formatted");
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

    let output_dis2 = Command::new("nvme")
        .args(&["disconnect"])
        .args(&["-n", &rep_nqn])
        .output()
        .unwrap();
    assert!(
        output_dis2.status.success(),
        "failed to disconnect from remote replica, {}",
        output_dis2.status
    );
}
