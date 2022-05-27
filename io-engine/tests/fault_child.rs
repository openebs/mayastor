use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut, Reason},
    core::MayastorCliArgs,
};

pub mod common;

static NEXUS_NAME: &str = "FaultChildNexus";
static NEXUS_SIZE: u64 = 10 * 1024 * 1024;
static CHILD_1: &str = "malloc:///malloc0?blk_size=512&size_mb=10";
static CHILD_2: &str = "malloc:///malloc1?blk_size=512&size_mb=10";

#[tokio::test]
async fn fault_child() {
    let ms = common::MayastorTest::new(MayastorCliArgs::default());
    ms.spawn(async {
        nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &[CHILD_1.to_string()])
            .await
            .unwrap();
        let mut nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        // child will stay in a degraded state because we are not rebuilding
        nexus.as_mut().add_child(CHILD_2, true).await.unwrap();
        // it should not be possible to fault the only healthy child
        assert!(nexus
            .as_mut()
            .fault_child(CHILD_1, Reason::Unknown)
            .await
            .is_err());
        // it should be possible to fault an unhealthy child
        assert!(nexus
            .as_mut()
            .fault_child(CHILD_2, Reason::Unknown)
            .await
            .is_ok());
    })
    .await;
}
