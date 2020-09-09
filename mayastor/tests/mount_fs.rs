use crossbeam::channel::unbounded;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Mthread,
        Reactor,
    },
};
use rpc::mayastor::ShareProtocolNexus;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;

#[test]
fn mount_fs() {
    // test xfs as well as ext4
    async fn mirror_fs_test<'a>(fstype: String) {
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = common::device_path_from_uri(
            nexus
                .share(ShareProtocolNexus::NexusNbd, None)
                .await
                .unwrap(),
        );

        // create an XFS filesystem on the nexus device
        {
            let (s, r) = unbounded();
            let mkfs_dev = device.clone();
            Mthread::spawn_unaffinitized(move || {
                if !common::mkfs(&mkfs_dev, &fstype) {
                    s.send(format!(
                        "Failed to format {} with {}",
                        mkfs_dev, fstype
                    ))
                    .unwrap();
                } else {
                    s.send("".to_string()).unwrap();
                }
            });

            assert_reactor_poll!(r, "");
        }

        // mount the device, create a file and return the md5 of that file
        {
            let (s, r) = unbounded();
            Mthread::spawn_unaffinitized(move || {
                s.send(match common::mount_and_write_file(&device) {
                    Ok(_) => "".to_owned(),
                    Err(err) => err,
                })
            });

            assert_reactor_poll!(r, "");
        }
        // destroy the share and the nexus
        nexus.unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();

        // create a split nexus, i.e two nexus devices which each one leg of the
        // mirror
        create_nexus_splitted().await;

        let left = nexus_lookup("left").unwrap();
        let right = nexus_lookup("right").unwrap();

        // share both nexuses
        // TODO: repeat this test for NVMF and ISCSI, and permutations?
        let left_device = common::device_path_from_uri(
            left.share(ShareProtocolNexus::NexusNbd, None)
                .await
                .unwrap(),
        );

        let right_device = common::device_path_from_uri(
            right
                .share(ShareProtocolNexus::NexusNbd, None)
                .await
                .unwrap(),
        );

        let (s, r) = unbounded();
        let s1 = s.clone();
        Mthread::spawn_unaffinitized(move || {
            s1.send(common::mount_and_get_md5(&left_device))
        });
        let md5_left;
        reactor_poll!(r, md5_left);
        assert!(md5_left.is_ok());

        left.unshare_nexus().await.unwrap();
        left.destroy().await.unwrap();

        let s1 = s.clone();
        // read the md5 of the right side of the mirror
        Mthread::spawn_unaffinitized(move || {
            s1.send(common::mount_and_get_md5(&right_device))
        });

        let md5_right;
        reactor_poll!(r, md5_right);
        assert!(md5_right.is_ok());
        right.unshare_nexus().await.unwrap();
        right.destroy().await.unwrap();
        assert_eq!(md5_left.unwrap(), md5_right.unwrap());
    }

    test_init!();

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    Reactor::block_on(async {
        mirror_fs_test("xfs".into()).await;
        mirror_fs_test("ext4".into()).await;
    });
}

#[test]
fn mount_fs_1() {
    test_init!();
    Reactor::block_on(async {
        let (s, r) = unbounded::<String>();
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = common::device_path_from_uri(
            nexus
                .share(ShareProtocolNexus::NexusNbd, None)
                .await
                .unwrap(),
        );

        Mthread::spawn_unaffinitized(move || {
            for _i in 0 .. 10 {
                if let Err(err) = common::mount_umount(&device) {
                    return s.send(err);
                }
            }
            s.send("".into())
        });

        assert_reactor_poll!(r, "");
        nexus.destroy().await.unwrap();
    });
}

#[test]
fn mount_fs_2() {
    test_init!();
    Reactor::block_on(async {
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = common::device_path_from_uri(
            nexus
                .share(ShareProtocolNexus::NexusNbd, None)
                .await
                .unwrap(),
        );
        let (s, r) = unbounded::<String>();

        Mthread::spawn_unaffinitized(move || {
            s.send(match common::fio_run_verify(&device) {
                Ok(_) => "".to_owned(),
                Err(err) => err,
            })
        });
        assert_reactor_poll!(r, "");
        nexus.destroy().await.unwrap();
    });

    mayastor_env_stop(0);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn create_nexus_splitted() {
    let ch = vec![BDEVNAME1.to_string()];
    nexus_create("left", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();

    let ch = vec![BDEVNAME2.to_string()];
    nexus_create("right", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}
