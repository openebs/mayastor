pub mod common;

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

/// These tests check for proper creation of nexus with thin provisioned
/// replicas: it is not okay to mix thin and thick replicas in one nexus.
#[allow(dead_code)]
struct ThinTest {
    test: ComposeTest,
    ms_0: SharedRpcHandle,
    pool_0: PoolBuilder,
    repl_0_thin: ReplicaBuilder,
    repl_1_thin: ReplicaBuilder,
    repl_2_thick: ReplicaBuilder,
    repl_3_bad_size: ReplicaBuilder,
}

impl ThinTest {
    async fn new() -> Self {
        let test = Builder::new()
            .name("cargo-test")
            .network("10.1.0.0/16")
            .unwrap()
            .add_container_bin(
                "ms_0",
                Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
            )
            .with_clean(true)
            .build()
            .await
            .unwrap();

        let conn = GrpcConnect::new(&test);

        let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();

        let mut pool_0 = PoolBuilder::new(ms_0.clone())
            .with_name("pool0")
            .with_new_uuid()
            .with_malloc("mem0", 200);

        let mut repl_0_thin = ReplicaBuilder::new(ms_0.clone())
            .with_pool(&pool_0)
            .with_name("r0_thin")
            .with_new_uuid()
            .with_size_mb(50)
            .with_thin(true);

        let mut repl_1_thin = ReplicaBuilder::new(ms_0.clone())
            .with_pool(&pool_0)
            .with_name("r1_thin")
            .with_new_uuid()
            .with_size_mb(50)
            .with_thin(true);

        let mut repl_2_thick = ReplicaBuilder::new(ms_0.clone())
            .with_pool(&pool_0)
            .with_name("r2_thick")
            .with_new_uuid()
            .with_size_mb(50)
            .with_thin(false);

        let mut repl_3_bad_size = ReplicaBuilder::new(ms_0.clone())
            .with_pool(&pool_0)
            .with_name("r3_bad_size")
            .with_new_uuid()
            .with_size_mb(30)
            .with_thin(false);

        pool_0.create().await.unwrap();
        repl_0_thin.create().await.unwrap();
        repl_1_thin.create().await.unwrap();
        repl_2_thick.create().await.unwrap();
        repl_3_bad_size.create().await.unwrap();

        Self {
            test,
            ms_0,
            pool_0,
            repl_0_thin,
            repl_1_thin,
            repl_2_thick,
            repl_3_bad_size,
        }
    }
}

#[tokio::test]
async fn nexus_thin_create_1() {
    common::composer_init();

    let t = ThinTest::new().await;

    let mut nex_0 = NexusBuilder::new(t.ms_0.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_replica(&t.repl_0_thin)
        .with_replica(&t.repl_3_bad_size);

    let r = nex_0.create().await;

    assert_eq!(r.unwrap_err().code(), tonic::Code::Internal);
}

#[tokio::test]
async fn nexus_thin_create_2() {
    common::composer_init();

    let t = ThinTest::new().await;

    let mut nex_0 = NexusBuilder::new(t.ms_0.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_replica(&t.repl_0_thin)
        .with_replica(&t.repl_1_thin);

    nex_0.create().await.unwrap();
}
