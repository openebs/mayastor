use tracing::error;

use once_cell::sync::OnceCell;

use common::MayastorTest;
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, UntypedBdev},
};

pub mod common;

async fn create_nexus(nexus_size: u64, child_sizes: Vec<u64>) -> bool {
    let children: Vec<String> = (0 .. child_sizes.len())
        .map(|i| format!("malloc:///m{}?size_mb={}", i, child_sizes[i]))
        .collect();

    if let Err(error) =
        nexus_create("core_nexus", nexus_size * 1024 * 1024, None, &children)
            .await
    {
        error!("nexus_create() failed: {}", error);
        return false;
    }
    true
}

static MS: OnceCell<MayastorTest> = OnceCell::new();

fn mayastor() -> &'static MayastorTest<'static> {
    MS.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

#[tokio::test]
async fn child_size_ok() {
    mayastor()
        .spawn(async {
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
            assert!(create_nexus(16, vec![32, 24, 16]).await);

            let bdev = UntypedBdev::lookup_by_name("core_nexus").unwrap();
            assert_eq!(bdev.name(), "core_nexus");

            let bdev = UntypedBdev::lookup_by_name("m0")
                .expect("child bdev m0 not found");
            assert_eq!(bdev.name(), "m0");

            let bdev = UntypedBdev::lookup_by_name("m1")
                .expect("child bdev m1 not found");
            assert_eq!(bdev.name(), "m1");

            let bdev = UntypedBdev::lookup_by_name("m2")
                .expect("child bdev m2 not found");
            assert_eq!(bdev.name(), "m2");

            let nexus =
                nexus_lookup_mut("core_nexus").expect("nexus not found");
            nexus.destroy().await.unwrap();

            assert!(nexus_lookup_mut("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("m0").is_none());
            assert!(UntypedBdev::lookup_by_name("m1").is_none());
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
        })
        .await;
}

#[tokio::test]
async fn child_too_small() {
    mayastor()
        .spawn(async {
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
            assert!(!create_nexus(16, vec![16, 16, 8]).await);

            assert!(nexus_lookup_mut("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("m0").is_none());
            assert!(UntypedBdev::lookup_by_name("m1").is_none());
            assert!(UntypedBdev::lookup_by_name("m2").is_none());
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
        })
        .await;
}

#[tokio::test]
async fn too_small_for_metadata() {
    mayastor()
        .spawn(async {
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
            assert!(!create_nexus(4, vec![16, 8, 4]).await);

            assert!(nexus_lookup_mut("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("core_nexus").is_none());
            assert!(UntypedBdev::lookup_by_name("m0").is_none());
            assert!(UntypedBdev::lookup_by_name("m1").is_none());
            assert!(UntypedBdev::lookup_by_name("m2").is_none());
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
        })
        .await;
}
