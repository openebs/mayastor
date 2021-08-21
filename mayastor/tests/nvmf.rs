use std::convert::TryFrom;

use mayastor::{
    core::{
        mayastor_env_stop, Bdev, MayastorCliArgs, MayastorEnvironment, Reactor,
    },
    nexus_uri::bdev_create,
    subsys::{NvmfSubsystem, SubType},
};

pub mod common;

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
                let bdev = Bdev::lookup_by_name(&b).unwrap();

                let ss = NvmfSubsystem::try_from(bdev).unwrap();
                ss.start().await.unwrap();
            });

            // test we can not create the same one again
            Reactor::block_on(async {
                let bdev = Bdev::lookup_by_name(BDEVNAME1).unwrap();

                let should_err = NvmfSubsystem::try_from(bdev);
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
                let bdev = Bdev::bdev_first().unwrap();
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
