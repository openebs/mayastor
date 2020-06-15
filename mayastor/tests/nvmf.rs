use std::convert::TryFrom;

use log::info;

use mayastor::{
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    nexus_uri::bdev_create,
    subsys::NvmfSubsystem,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

#[test]
fn nvmf_target() {
    common::mayastor_test_init();
    common::truncate_file(DISKNAME1, 64 * 1024);
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();
    MayastorEnvironment::new(args)
        .start(|| {
            Reactor::block_on(async {
                let b = bdev_create(BDEVNAME1).await.unwrap();
                let bdev = Bdev::lookup_by_name(&b).unwrap();

                let ss = NvmfSubsystem::try_from(&bdev).unwrap();
                ss.start().await.unwrap();
            });

            Reactor::block_on(async {
                NvmfSubsystem::first()
                    .into_iter()
                    .for_each(|s| info!("{:?}", s));

                let bdev = Bdev::bdev_first().unwrap();
                assert_eq!(bdev.is_claimed(), true);
                assert_eq!(bdev.claimed_by().unwrap(), "NVMe-oF Target");

                let ss = NvmfSubsystem::first().unwrap();

                ss.stop().await.unwrap();
                let sbdev = ss.bdev();

                assert_eq!(sbdev.name(), bdev.name());

                assert_eq!(bdev.is_claimed(), true);
                assert_eq!(bdev.claimed_by().unwrap(), "NVMe-oF Target");

                ss.destroy();
                assert_eq!(bdev.is_claimed(), false);
                assert_eq!(bdev.claimed_by(), None);
            });

            mayastor_env_stop(0);
        })
        .unwrap();
}
