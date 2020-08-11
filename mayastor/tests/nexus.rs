use std::panic::catch_unwind;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Protocol,
        Reactor,
        Share,
    },
};

pub mod common;

#[test]
fn nexus_test() {
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x3".into();

    let result = catch_unwind(|| {
        MayastorEnvironment::new(args)
            .start(|| {
                // create a nexus and share it via iSCSI
                Reactor::block_on(async {
                    nexus_create(
                        "nexus0",
                        48 * 1024 * 1024,
                        None,
                        &[
                            "malloc:///malloc0?size_mb=64".into(),
                            "malloc:///malloc1?size_mb=64".into(),
                        ],
                    )
                    .await
                    .unwrap();

                    let nexus = nexus_lookup("nexus0").unwrap();

                    // this should be idempotent so validate that sharing the
                    // same thing over the same protocol
                    // works
                    let share = nexus.share_iscsi().await.unwrap();
                    let share2 = nexus.share_iscsi().await.unwrap();
                    assert_eq!(share, share2);
                    assert_eq!(nexus.shared(), Some(Protocol::Iscsi));
                });

                // sharing the nexus over nvmf should fail
                Reactor::block_on(async {
                    let nexus = nexus_lookup("nexus0").unwrap();
                    assert_eq!(nexus.share_nvmf().await.is_err(), true);
                    assert_eq!(nexus.shared(), Some(Protocol::Iscsi));
                });

                // unshare the nexus and then share over nvmf
                Reactor::block_on(async {
                    let nexus = nexus_lookup("nexus0").unwrap();
                    nexus.unshare().await.unwrap();
                    let shared = nexus.shared();
                    assert_eq!(shared, None);

                    let shared = nexus.share_nvmf().await.unwrap();
                    let shared2 = nexus.share_nvmf().await.unwrap();

                    assert_eq!(shared, shared2);
                    assert_eq!(nexus.shared(), Some(Protocol::Nvmf));
                });
                mayastor_env_stop(0);
            })
            .unwrap();
    });
}
