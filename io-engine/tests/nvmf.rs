use std::time::Duration;
use io_engine::{
    bdev_api::bdev_create,
    constants::NVME_NQN_PREFIX,
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        UntypedBdev,
    },
    subsys::{NvmfSubsystem, SubType},
};

use io_engine_tests::{delete_rdma_rxe_device, setup_rdma_rxe_device};

pub mod common;
use common::compose::{
    rpc::v1::{
        nexus::{
            CreateNexusRequest,
            PublishNexusRequest,
        },
        pool::CreatePoolRequest,
        replica::CreateReplicaRequest,
        GrpcConnect as v1GrpcConnect,
        RpcHandle,
    },
    rpc::v0::{
        mayastor::{BdevShareRequest, BdevUri, CreateReply, ShareProtocolNexus},
        GrpcConnect,
    },
    Binary,
    Builder,
    ComposeTest,
    NetworkMode,
};
use common::nvme::{nvme_connect, nvme_disconnect_nqn};
use regex::Regex;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

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

pub async fn create_nexus(h: &mut RpcHandle, children: Vec<String>) {
    h.nexus
        .create_nexus(CreateNexusRequest {
            name: nexus_name(),
            uuid: nexus_uuid(),
            size: 60 * 1024 * 1024,
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

#[common::spdk_test]
fn nvmf_target() {
    common::mayastor_test_init();
    common::truncate_file(DISKNAME1, 64 * 1024);
    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };
    MayastorEnvironment::new(args)
        .start(|| {
            // test we can create a nvmf subsystem
            Reactor::block_on(async {
                let b = bdev_create(BDEVNAME1).await.unwrap();
                let bdev = UntypedBdev::lookup_by_name(&b).unwrap();

                let ss = NvmfSubsystem::try_from(&bdev).unwrap();
                ss.start(false).await.unwrap();
            });

            // test we can not create the same one again
            Reactor::block_on(async {
                let bdev = UntypedBdev::lookup_by_name(BDEVNAME1).unwrap();

                let should_err = NvmfSubsystem::try_from(&bdev);
                assert!(should_err.is_err());
            });

            // we should have at least 2 subsystems
            Reactor::block_on(async {
                assert_eq!(
                    NvmfSubsystem::first().unwrap().into_iter().count(),
                    2
                );
            });

            // verify the bdev is claimed by our target -- make sure we skip
            // over the discovery controller
            Reactor::block_on(async {
                let bdev = UntypedBdev::bdev_first().unwrap();
                assert!(bdev.is_claimed());
                assert!(bdev.is_claimed_by("NVMe-oF Target"));

                let ss = NvmfSubsystem::first().unwrap();
                for s in ss {
                    if s.subtype() == SubType::Discovery {
                        continue;
                    }
                    s.stop().await.unwrap();
                    let sbdev = s.bdev().unwrap();
                    assert_eq!(sbdev.name(), bdev.name());

                    assert!(bdev.is_claimed());
                    assert!(bdev.is_claimed_by("NVMe-oF Target"));

                    unsafe {
                        s.shutdown_unsafe();
                    }
                    assert!(!bdev.is_claimed());
                }
            });
            // this should clean/up kill the discovery controller
            mayastor_env_stop(0);
        })
        .unwrap();

    common::delete_file(&[DISKNAME1.into()]);
}

#[tokio::test]
async fn nvmf_set_target_interface() {
    async fn start_ms(network: &str, args: Vec<&str>) -> ComposeTest {
        common::composer_init();

        Builder::new()
            .name("cargo-test")
            .network(network)
            .unwrap()
            .add_container_bin(
                "ms1",
                Binary::from_dbg("io-engine").with_args(args),
            )
            .with_clean(true)
            .build()
            .await
            .unwrap()
    }

    async fn test_ok(network: &str, args: Vec<&str>, tgt_ip: Option<&str>) {
        let test = start_ms(network, args).await;
        let grpc = GrpcConnect::new(&test);
        let hdl = &mut grpc.grpc_handle("ms1").await.unwrap();

        let tgt_ip = match tgt_ip {
            Some(s) => s.to_string(),
            None => {
                let cnt = test
                    .list_cluster_containers()
                    .await
                    .unwrap()
                    .pop()
                    .unwrap();
                let networks = cnt.network_settings.unwrap().networks.unwrap();
                let ip_addr = networks
                    .get("cargo-test")
                    .unwrap()
                    .ip_address
                    .clone()
                    .unwrap();
                ip_addr
            }
        };

        hdl.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=64".into(),
            })
            .await
            .unwrap();

        let bdev_uri = hdl
            .bdev
            .share(BdevShareRequest {
                name: "disk0".into(),
                proto: "nvmf".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .uri;

        let re = Regex::new(r"^nvmf(\+rdma\+tcp|\+tcp)://([0-9.]+):[0-9]+/.*$")
            .unwrap();
        let cap = re.captures(&bdev_uri).unwrap();
        let shared_ip = cap.get(2).unwrap().as_str();

        hdl.bdev
            .unshare(CreateReply {
                name: "disk0".into(),
            })
            .await
            .unwrap();

        hdl.bdev
            .destroy(BdevUri {
                uri: "bdev:///disk0".into(),
            })
            .await
            .unwrap();

        assert_eq!(tgt_ip, shared_ip);
    }

    // async fn test_fail(network: &str, args: Vec<&str>) {
    //     let test = start_ms(network, args).await;
    //     assert!(test.grpc_handle("ms1").await.is_err());
    // }

    test_ok("10.15.0.0/16", vec!["-T", "name:lo"], Some("127.0.0.1")).await;
    test_ok("10.15.0.0/16", vec!["-T", "subnet:10.15.0.0/16"], None).await;
    test_ok(
        "192.168.133.0/24",
        vec!["-T", "subnet:192.168.133.0/24"],
        None,
    )
    .await;
    // test_fail("10.15.0.0/16", vec!["-T", "abc"]).await;
    // test_fail("10.15.0.0/16", vec!["-T", "mac:123"]).await;
    // test_fail("10.15.0.0/16", vec!["-T", "ip:hello"]).await;
}

#[tokio::test]
async fn test_rdma_target() {
    common::composer_init();

    let iface = setup_rdma_rxe_device();
    let test = Builder::new()
        .name("cargo-test")
        .network_mode(NetworkMode::Host)
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2", "--enable-rdma", "-T", iface.as_str()]).with_privileged(Some(true)),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = v1GrpcConnect::new(&test);
    let mut hdl = conn.grpc_handle("ms_0").await.unwrap();
    println!("ms_0 grpc endpoint {:?}", hdl.endpoint);

    hdl.pool
        .create_pool(CreatePoolRequest {
            name: pool_name(),
            uuid: Some(pool_uuid()),
            pooltype: 0,
            disks: vec!["malloc:///disk0?size_mb=100".into()],
            cluster_size: None,
            md_args: None,
        })
        .await
        .unwrap();

    hdl.replica
        .create_replica(CreateReplicaRequest {
            name: repl_name(),
            uuid: repl_uuid(),
            pooluuid: pool_uuid(),
            size: 80 * 1024 * 1024,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let child0 = format!("bdev:///{}", repl_name());
    create_nexus(&mut hdl, vec![child0.clone()]).await;
    let device_uri = hdl
        .nexus
        .publish_nexus(PublishNexusRequest {
            uuid: nexus_uuid(),
            key: "".to_string(),
            share: ShareProtocolNexus::NexusNvmf as i32,
            ..Default::default()
        })
        .await
        .expect("Failed to publish nexus")
        .into_inner()
        .nexus
        .unwrap()
        .device_uri;

    let url =  url::Url::parse(device_uri.as_str()).unwrap();
    assert!(url.scheme() == "nvmf+rdma+tcp");

    let host = url.host_str().unwrap();
    let nqn = format!("{NVME_NQN_PREFIX}:{}", nexus_name());
    let conn_status = nvme_connect(host, &nqn, "rdma", true);
    assert!(conn_status.success());

    tokio::time::sleep(Duration::from_secs(2)).await;

    nvme_disconnect_nqn(&nqn);
    // Explicitly destroy this test's containers so that rxe device can be deleted.
    test.down().await;
    delete_rdma_rxe_device();
}

