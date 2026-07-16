use once_cell::sync::OnceCell;

use common::MayastorTest;
use io_engine::{
    bdev::nexus::{nexus_create_ext, nexus_lookup_mut},
    core::{MayastorCliArgs, UntypedBdev},
    gpt::LabelVersion,
};

pub mod common;

async fn create_nexus(
    size: u64,
    child_size: u64,
    label: LabelVersion,
) -> Result<(), io_engine::bdev::nexus::Error> {
    let children = vec![format!("malloc:///m0?size={}B", child_size)];

    let size_exact = match label {
        LabelVersion::V1 => false,
        LabelVersion::V2 => true,
    };

    nexus_create_ext("nexus", size, size_exact, None, &children, label).await
}

static MS: OnceCell<MayastorTest> = OnceCell::new();

fn mayastor() -> &'static MayastorTest<'static> {
    MS.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn last_usable(size: u64) -> u64 {
    size / 512 - 34
}

#[tokio::test]
async fn nexus_bdev_size() {
    mayastor()
        .spawn(async {
            assert_eq!(UntypedBdev::bdev_first().into_iter().count(), 0);
            let ss = 512;
            let mb = 1024 * 1024;

            let size = 13 * mb;
            let child_size = 13 * mb;
            let label = LabelVersion::V1;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            let bdev_size = last_usable(child_size) - label.data_start_blks(ss) + 1;
            assert_eq!(nexus.as_ref().num_blocks(), bdev_size);
            nexus.destroy().await.unwrap();

            let size = 13 * mb;
            let child_size = 13 * mb;
            let label = LabelVersion::V2;

            create_nexus(size, child_size, label)
                .await
                .expect_err("Can't create nexus with the same size as the child");

            let size = 6 * mb;
            create_nexus(size, child_size, label)
                .await
                .expect_err("Not enough space for the nexus data area in the child");

            let size = 5 * mb;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            assert_eq!(nexus.size_in_bytes(), size);
            assert_eq!(nexus.as_ref().num_blocks(), size / ss);
            nexus.destroy().await.unwrap();

            let size = 7 * mb;
            let child_size = 14 * mb;
            create_nexus(size, child_size, label)
                .await
                .expect_err("Not enough space for the nexus data area in the child");

            let size = 7 * mb;
            let child_size = 15 * mb;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            assert_eq!(nexus.size_in_bytes(), size);
            assert_eq!(nexus.as_ref().num_blocks(), size / ss);
            nexus.destroy().await.unwrap();

            let size = 5 * mb;
            let child_size = 12 * mb;
            create_nexus(size, child_size, label)
                .await
                .expect_err("Not enough space for the nexus data area in the child");

            let size = 8 * mb;
            let child_size = 17 * mb;
            let label = LabelVersion::V2;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            assert_eq!(nexus.size_in_bytes(), size);
            assert_eq!(nexus.as_ref().num_blocks(), size / ss);
            nexus.destroy().await.unwrap();

            let size = 12 * mb;
            let child_size = 20 * mb;
            let label = LabelVersion::V2;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            assert_eq!(nexus.size_in_bytes(), size);
            assert_eq!(nexus.as_ref().num_blocks(), size / ss);
            nexus.destroy().await.unwrap();

            let size = 5 * mb;
            let child_size = 16 * mb;
            let label = LabelVersion::V2;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.num_blocks(), size / ss);
            assert_eq!(nexus.size_in_bytes(), size);
            nexus.destroy().await.unwrap();
        })
        .await;
}
