use mayastor::{
    core::{MayastorCliArgs, UntypedBdev},
    nexus_uri::bdev_create,
    target::{iscsi, Side},
};

pub mod common;
static BDEV: &str = "malloc:///malloc0?size_mb=64";

#[tokio::test]
async fn iscsi_target() {
    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };

    let ms = common::MayastorTest::new(args);
    ms.spawn(async {
        // test we can create a nvmf subsystem
        let b = bdev_create(BDEV).await.unwrap();
        let mut bdev = UntypedBdev::lookup_by_name(&b).unwrap();
        iscsi::share(&b, &mut bdev, Side::Nexus).unwrap();

        // test we can not create the same one again
        let mut bdev = UntypedBdev::lookup_by_name("malloc0").unwrap();
        let should_err = iscsi::share("malloc0", &mut bdev, Side::Nexus);
        assert!(should_err.is_err());

        // verify the bdev is claimed by our target
        let bdev = UntypedBdev::bdev_first().unwrap();
        assert!(bdev.is_claimed());
        assert_eq!(bdev.claimed_by().unwrap(), "iSCSI Target");

        // unshare the iSCSI target
        let bdev = UntypedBdev::lookup_by_name("malloc0").unwrap();
        let should_err = iscsi::unshare(bdev.name()).await;
        assert!(!should_err.is_err());

        // verify the bdev is not claimed by our target anymore
        let bdev = UntypedBdev::bdev_first().unwrap();
        assert!(!bdev.is_claimed());
    })
    .await
}
