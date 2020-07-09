use mayastor::{
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    nexus_uri::bdev_create,
    target::{iscsi, Side},
};

pub mod common;
static BDEV: &str = "malloc:///malloc0?size_mb=64";

#[test]
fn iscsi_target() {
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();
    MayastorEnvironment::new(args)
        .start(|| {
            // test we can create a nvmf subsystem
            Reactor::block_on(async {
                let b = bdev_create(BDEV).await.unwrap();
                let bdev = Bdev::lookup_by_name(&b).unwrap();
                iscsi::share(&b, &bdev, Side::Nexus).unwrap();
            });

            // test we can not create the same one again
            Reactor::block_on(async {
                let bdev = Bdev::lookup_by_name("malloc0").unwrap();
                let should_err = iscsi::share("malloc0", &bdev, Side::Nexus);
                assert_eq!(should_err.is_err(), true);
            });

            // verify the bdev is claimed by our target
            Reactor::block_on(async {
                let bdev = Bdev::bdev_first().unwrap();
                assert_eq!(bdev.is_claimed(), true);
                assert_eq!(bdev.claimed_by().unwrap(), "iSCSI Target");
            });

            // unshare the iSCSI target
            Reactor::block_on(async {
                let bdev = Bdev::lookup_by_name("malloc0").unwrap();
                let should_err = iscsi::unshare(&bdev.name()).await;
                assert_eq!(should_err.is_err(), false);
            });

            // verify the bdev is not claimed by our target anymore
            Reactor::block_on(async {
                let bdev = Bdev::bdev_first().unwrap();
                assert_eq!(bdev.is_claimed(), false);
            });

            mayastor_env_stop(0);
        })
        .unwrap();
}
