use common::MayastorTest;
use io_engine::{
    core::{MayastorCliArgs, UntypedBdev},
    nexus_uri::{bdev_create, bdev_destroy},
};
use spdk_rs::DmaBuf;
pub mod common;

#[tokio::test]
async fn malloc_bdev() {
    let ms = MayastorTest::new(MayastorCliArgs::default());
    ms.spawn(async {
        bdev_create("malloc:///malloc0?blk_size=512&size_mb=100")
            .await
            .unwrap();

        bdev_create(&format!(
            "malloc:///malloc1?blk_size=512&num_blocks={}",
            (100 << 20) / 512
        ))
        .await
        .unwrap();
    })
    .await;

    ms.spawn(async {
        let m0 = UntypedBdev::open_by_name("malloc0", true).unwrap();
        let m1 = UntypedBdev::open_by_name("malloc1", true).unwrap();

        assert_eq!(
            m0.get_bdev().size_in_bytes(),
            m1.get_bdev().size_in_bytes()
        );
        assert_eq!(m0.get_bdev().num_blocks(), m1.get_bdev().num_blocks());
        assert_ne!(
            m0.get_bdev().uuid_as_string(),
            m1.get_bdev().uuid_as_string()
        );

        let h0 = m0.into_handle().unwrap();
        let h1 = m1.into_handle().unwrap();

        let mut buf = DmaBuf::new(4096, 9).unwrap();
        buf.fill(3);

        h0.write_at(0, &buf).await.unwrap();
        h1.write_at(0, &buf).await.unwrap();

        let mut b0 = h0.dma_malloc(4096).unwrap();
        let mut b1 = h1.dma_malloc(4096).unwrap();

        b0.fill(1);
        b0.fill(2);

        h0.read_at(0, &mut b0).await.unwrap();
        h1.read_at(0, &mut b1).await.unwrap();

        let s0 = b0.as_slice();
        let s1 = b1.as_slice();

        for i in 0 .. s0.len() {
            assert_eq!(s0[i], 3);
            assert_eq!(s0[i], s1[i])
        }
    })
    .await;

    ms.spawn(async {
        bdev_destroy("malloc:///malloc0?blk_size=512&size_mb=100")
            .await
            .unwrap();
        bdev_destroy("malloc:///malloc1?blk_size=512&size_mb=100")
            .await
            .unwrap();
    })
    .await;
}
