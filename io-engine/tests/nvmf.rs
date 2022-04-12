use io_engine::{
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        UntypedBdev,
    },
    nexus_uri::bdev_create,
    subsys::{NvmfSubsystem, SubType},
};

pub mod common;
use common::compose::Builder;
use composer::{Binary, ComposeTest};
use regex::Regex;
use rpc::mayastor::{BdevShareRequest, BdevUri, CreateReply};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

#[test]
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
                ss.start().await.unwrap();
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
                assert_eq!(bdev.claimed_by().unwrap(), "NVMe-oF Target");

                let ss = NvmfSubsystem::first().unwrap();
                for s in ss {
                    if s.subtype() == SubType::Discovery {
                        continue;
                    }
                    s.stop().await.unwrap();
                    let sbdev = s.bdev().unwrap();
                    assert_eq!(sbdev.name(), bdev.name());

                    assert!(bdev.is_claimed());
                    assert_eq!(bdev.claimed_by().unwrap(), "NVMe-oF Target");

                    s.destroy();
                    assert!(!bdev.is_claimed());
                    assert_eq!(bdev.claimed_by(), None);
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
        let test = Builder::new()
            .name("cargo-test")
            .network(network)
            .add_container_bin(
                "ms1",
                Binary::from_dbg("io-engine").with_args(args),
            )
            .with_clean(true)
            .build()
            .await
            .unwrap();

        test
    }

    async fn test_ok(network: &str, args: Vec<&str>, tgt_ip: Option<&str>) {
        let test = start_ms(network, args).await;
        let hdl = &mut test.grpc_handle("ms1").await.unwrap();

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
            })
            .await
            .unwrap()
            .into_inner()
            .uri;

        let re = Regex::new(r"^nvmf://([0-9.]+):[0-9]+/.*$").unwrap();
        let cap = re.captures(&bdev_uri).unwrap();
        let shared_ip = cap.get(1).unwrap().as_str();

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
