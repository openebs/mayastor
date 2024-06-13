pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason},
            GrpcConnect,
            SharedRpcHandle,
        },
        Binary,
        Builder,
    },
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

const POOL_SIZE: u64 = 200;
const REPL_SIZE: u64 = 50;
const NEXUS_SIZE: u64 = REPL_SIZE;

struct TestNode {
    idx: usize,
    ms: SharedRpcHandle,
    pool: PoolBuilder,
    replicas: Vec<ReplicaBuilder>,
}

impl TestNode {
    async fn next_replica(&mut self) -> ReplicaBuilder {
        let mut repl = ReplicaBuilder::new(self.ms.clone())
            .with_pool(&self.pool)
            .with_name(&format!(
                "repl_{i}_{j}",
                i = self.idx,
                j = self.replicas.len()
            ))
            .with_new_uuid()
            .with_size_mb(REPL_SIZE);

        repl.create().await.unwrap();
        repl.share().await.unwrap();
        self.replicas.push(repl.clone());
        repl
    }

    async fn clear(&mut self) {
        for i in 0 .. self.replicas.len() {
            self.replicas[i].destroy().await.unwrap();
        }
        self.replicas.clear();
    }
}

async fn test_src_selection(
    nodes: &mut Vec<TestNode>,
    nex_node: usize,
    child_cfg: Vec<usize>,
    dst: usize,
    expected_src_idx: usize,
) {
    let to = std::time::Duration::from_secs(1);

    let mut replicas = Vec::new();
    for i in 0 .. child_cfg.len() {
        replicas.push(nodes[child_cfg[i]].next_replica().await);
    }

    let mut nex = NexusBuilder::new(nodes[nex_node].ms.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replicas(&replicas);

    nex.create().await.unwrap();

    println!("---------");
    println!(
        "> {child_cfg:?}: expect to rebuild #{dst} from #{expected_src_idx}"
    );
    let children = nex.get_nexus().await.unwrap().children;

    for (idx, child) in children.iter().enumerate() {
        println!("    [{idx}] {c:?}", c = child.uri);
    }

    let r = &replicas[dst];
    println!("    rebuilding #{dst}: {uri}", uri = nex.replica_uri(r));

    nex.offline_child_replica(r).await.unwrap();
    nex.wait_replica_state(
        r,
        ChildState::Degraded,
        Some(ChildStateReason::ByClient),
        to,
    )
    .await
    .unwrap();
    nex.online_child_replica(r).await.unwrap();
    nex.wait_children_online(to).await.unwrap();

    let rec = nex
        .get_rebuild_history()
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();

    let dst_idx = children
        .iter()
        .position(|c| c.uri == rec.child_uri)
        .unwrap();
    let src_idx = children.iter().position(|c| c.uri == rec.src_uri).unwrap();

    println!(
        "    rebuilt #{dst_idx}: {dst} from #{src_idx}: {src}",
        src = rec.src_uri,
        dst = rec.child_uri
    );

    assert_eq!(
        src_idx, expected_src_idx,
        "Expected child index {expected_src_idx}, got {src_idx}"
    );

    nex.destroy().await.unwrap();
    for node in nodes {
        node.clear().await;
    }
}

/// Should prefer a local replica for rebuild source.
#[tokio::test]
async fn nexus_rebuild_prefer_local_replica() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "3,4",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .add_container_bin(
            "ms_2",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "5,6",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let mut nodes = Vec::new();

    for idx in 0 .. 3 {
        let ms = conn.grpc_handle_shared(&format!("ms_{idx}")).await.unwrap();

        let mut pool = PoolBuilder::new(ms.clone())
            .with_name(&format!("pool_{idx}"))
            .with_new_uuid()
            .with_malloc(&format!("mem_{idx}"), POOL_SIZE);

        pool.create().await.unwrap();

        nodes.push(TestNode {
            idx,
            ms,
            pool,
            replicas: Vec::new(),
        });
    }

    // All local, should select first avail.
    test_src_selection(&mut nodes, 0, vec![0, 0, 0], 0, 1).await;
    test_src_selection(&mut nodes, 0, vec![0, 0, 0], 1, 0).await;
    test_src_selection(&mut nodes, 0, vec![0, 0, 0], 2, 0).await;

    // Local-remote-remote, should prefer the local one (here it is #0).
    test_src_selection(&mut nodes, 0, vec![0, 1, 2], 0, 1).await;
    test_src_selection(&mut nodes, 0, vec![0, 1, 2], 1, 0).await;
    test_src_selection(&mut nodes, 0, vec![0, 1, 2], 2, 0).await;

    // Remote-local-remote, should prefer the local one (here it is #1).
    test_src_selection(&mut nodes, 0, vec![1, 0, 2], 0, 1).await;
    test_src_selection(&mut nodes, 0, vec![1, 0, 2], 1, 0).await;
    test_src_selection(&mut nodes, 0, vec![1, 0, 2], 2, 1).await;

    // Remote-remote-local, should prefer the local one (here it is #2).
    test_src_selection(&mut nodes, 0, vec![1, 2, 0], 0, 2).await;
    test_src_selection(&mut nodes, 0, vec![1, 2, 0], 1, 2).await;
    test_src_selection(&mut nodes, 0, vec![1, 2, 0], 2, 0).await;

    // Remote-local-local, should prefer the first avail local one (#1 or #2).
    test_src_selection(&mut nodes, 0, vec![1, 0, 0], 0, 1).await;
    test_src_selection(&mut nodes, 0, vec![1, 0, 0], 1, 2).await;
    test_src_selection(&mut nodes, 0, vec![1, 0, 0], 2, 1).await;

    // All remote, should prefer the first avail.
    test_src_selection(&mut nodes, 0, vec![1, 1, 1], 0, 1).await;
    test_src_selection(&mut nodes, 0, vec![1, 1, 1], 1, 0).await;
    test_src_selection(&mut nodes, 0, vec![1, 1, 1], 2, 0).await;
}
