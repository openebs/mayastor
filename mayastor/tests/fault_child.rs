use mayastor::{
    bdev::{nexus_create, nexus_lookup, Reason},
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
};

pub mod common;

static NEXUS_NAME: &str = "FaultChildNexus";
static NEXUS_SIZE: u64 = 10 * 1024 * 1024;
static CHILD_1: &str = "malloc:///malloc0?blk_size=512&size_mb=10";
static CHILD_2: &str = "malloc:///malloc1?blk_size=512&size_mb=10";

#[test]
fn fault_child() {
    common::mayastor_test_init();
    let ms = MayastorEnvironment::new(MayastorCliArgs::default());
    ms.start(|| {
        Reactor::block_on(async {
            nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &[CHILD_1.to_string()])
                .await
                .unwrap();
            let nexus = nexus_lookup(NEXUS_NAME).unwrap();
            // child will stay in a degraded state because we are not rebuilding
            nexus.add_child(CHILD_2, true).await.unwrap();

            // it should not be possible to fault the only healthy child
            assert!(nexus.fault_child(CHILD_1, Reason::Unknown).await.is_err());
            // it should be possible to fault an unhealthy child
            assert!(nexus.fault_child(CHILD_2, Reason::Unknown).await.is_ok());

            mayastor_env_stop(0);
        });
    })
    .unwrap();
}
