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

    nexus_create_ext("nexus", size, None, &children, label).await
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
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            let bdev_size = last_usable(child_size) - label.data_start_blks(ss);
            let f4mb_blks = 4 * mb / ss;
            assert_eq!(
                nexus.as_ref().num_blocks(),
                align_down(bdev_size, f4mb_blks)
            );
            nexus.destroy().await.unwrap();

            let size = 17 * mb;
            let child_size = 17 * mb;
            let label = LabelVersion::V2;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            let bdev_size = last_usable(child_size) - label.data_start_blks(ss);
            let f4mb_blks = 4 * mb / ss;
            println!("bdev_size: {}", nexus.as_ref().num_blocks());
            assert_eq!(
                nexus.as_ref().num_blocks(),
                align_down(bdev_size, f4mb_blks)
            );
            nexus.destroy().await.unwrap();

            let size = 12 * mb;
            let child_size = 20 * mb;
            let label = LabelVersion::V2;
            create_nexus(size, child_size, label).await.unwrap();
            let nexus = nexus_lookup_mut("nexus").unwrap();
            assert_eq!(nexus.block_len(), ss);
            assert_eq!(nexus.req_size(), size);
            let bdev_size = last_usable(child_size) - label.data_start_blks(ss);
            let f4mb_blks = 4 * mb / ss;
            println!("bdev_size: {}", nexus.as_ref().num_blocks());
            assert_eq!(
                nexus.as_ref().num_blocks(),
                align_down(bdev_size, f4mb_blks)
            );
            nexus.destroy().await.unwrap();
        })
        .await;
}

fn align_down(value: u64, align: u64) -> u64 {
    value & !(align - 1)
}
