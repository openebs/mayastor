use tracing::error;

use once_cell::sync::OnceCell;

use common::MayastorTest;
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{Bdev, MayastorCliArgs},
};

pub mod common;

async fn create_nexus(size: u64) -> bool {
    let children = vec![
        String::from("malloc:///m0?size_mb=32"),
        format!("malloc:///m1?size_mb={}", size),
    ];
    if let Err(error) =
        nexus_create("core_nexus", size * 1024 * 1024, None, &children).await
    {
        error!("nexus_create() failed: {}", error);
        return false;
    }
    true
}

static MS: OnceCell<MayastorTest> = OnceCell::new();

fn mayastor() -> &'static MayastorTest<'static> {
    let ms = MS.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()));
    &ms
}

#[tokio::test]
async fn child_size_ok() {
    mayastor()
        .spawn(async {
            assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
            assert!(create_nexus(16).await);

            let bdev = Bdev::lookup_by_name("core_nexus").unwrap();
            assert_eq!(bdev.name(), "core_nexus");

            let bdev =
                Bdev::lookup_by_name("m0").expect("child bdev m0 not found");
            assert_eq!(bdev.name(), "m0");

            let bdev =
                Bdev::lookup_by_name("m1").expect("child bdev m1 not found");
            assert_eq!(bdev.name(), "m1");

            let nexus = nexus_lookup("core_nexus").expect("nexus not found");
            nexus.destroy().await.unwrap();

            assert!(nexus_lookup("core_nexus").is_none());
            assert!(Bdev::lookup_by_name("core_nexus").is_none());
            assert!(Bdev::lookup_by_name("m0").is_none());
            assert!(Bdev::lookup_by_name("m1").is_none());
            assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
        })
        .await;
}

#[tokio::test]
async fn child_too_small() {
    mayastor()
        .spawn(async {
            assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
            assert!(!create_nexus(4).await);

            assert!(nexus_lookup("core_nexus").is_none());
            assert!(Bdev::lookup_by_name("core_nexus").is_none());
            assert!(Bdev::lookup_by_name("m0").is_none());
            assert!(Bdev::lookup_by_name("m1").is_none());
            assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
        })
        .await;
}
