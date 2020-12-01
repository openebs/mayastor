mod test;
use mbus_api::{
    v0::{GetNodes, GetPools, NodeState, PoolState},
    Message,
};
use rest_client::{versions::v0::*, ActixRestClient};
use rpc::mayastor::Null;
use test::{Binary, Builder, ComposeTest, ContainerSpec};
use tracing::info;

async fn wait_for_node() -> Result<(), Box<dyn std::error::Error>> {
    let _ = GetNodes {}.request().await?;
    Ok(())
}
async fn wait_for_pool() -> Result<(), Box<dyn std::error::Error>> {
    let _ = GetPools {
        filter: Default::default(),
    }
    .request()
    .await?;
    Ok(())
}

// to avoid waiting for timeouts
async fn orderly_start(
    test: &ComposeTest,
) -> Result<(), Box<dyn std::error::Error>> {
    test.start_containers(vec!["nats", "node", "pool", "rest"])
        .await?;

    test::bus_init("localhost").await?;
    wait_for_node().await?;
    wait_for_pool().await?;

    test.start("mayastor").await?;

    let mut hdl = test.grpc_handle("mayastor").await?;
    hdl.mayastor.list_nexus(Null {}).await?;
    Ok(())
}

#[actix_rt::test]
async fn client() -> Result<(), Box<dyn std::error::Error>> {
    test::init();

    let mayastor = "node-test-name";
    let test = Builder::new()
        .name("rest")
        .add_container_spec(
            ContainerSpec::from_binary(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .with_portmap("4222", "4222"),
        )
        .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
        .add_container_bin("pool", Binary::from_dbg("pool").with_nats("-n"))
        .add_container_spec(
            ContainerSpec::from_binary(
                "rest",
                Binary::from_dbg("rest").with_nats("-n"),
            )
            .with_portmap("8080", "8080"),
        )
        .add_container_bin(
            "mayastor",
            Binary::from_dbg("mayastor")
                .with_nats("-n")
                .with_args(vec!["-N", mayastor])
                .with_args(vec!["-g", "10.1.0.6:10124"]),
        )
        .autorun(false)
        .build()
        .await?;

    let result = client_test(mayastor, &test).await;

    // run with --nocapture to see all the logs
    test.logs_all().await?;

    result?;

    Ok(())
}

async fn client_test(
    mayastor: &str,
    test: &ComposeTest,
) -> Result<(), Box<dyn std::error::Error>> {
    orderly_start(&test).await?;

    let client = ActixRestClient::new("https://localhost:8080")?.v0();
    let nodes = client.get_nodes().await?;
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes.first().unwrap(),
        &Node {
            id: mayastor.to_string(),
            grpc_endpoint: "10.1.0.6:10124".to_string(),
            state: NodeState::Online,
        }
    );
    info!("Nodes: {:#?}", nodes);
    let _ = client.get_pools(Filter::None).await?;
    let pool = client.create_pool(CreatePool {
        node: mayastor.to_string(),
        name: "pooloop".to_string(),
        disks: vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".to_string()]
    }).await?;
    info!("Pools: {:#?}", pool);
    assert_eq!(
        pool,
        Pool {
            node: "node-test-name".to_string(),
            name: "pooloop".to_string(),
            disks: vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".to_string()],
            state: PoolState::Online,
            capacity: 100663296,
            used: 0,
        }
    );
    assert_eq!(Some(&pool), client.get_pools(Filter::None).await?.first());
    let _ = client.get_replicas(Filter::None).await?;
    let replica = client
        .create_replica(CreateReplica {
            node: pool.node.clone(),
            pool: pool.name.clone(),
            uuid: "replica1".to_string(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Nvmf,
        })
        .await?;
    info!("Replica: {:#?}", replica);
    assert_eq!(
        replica,
        Replica {
            node: pool.node.clone(),
            uuid: "replica1".to_string(),
            pool: pool.name.clone(),
            thin: false,
            size: 12582912,
            share: Protocol::Nvmf,
            uri: "nvmf://10.1.0.6:8420/nqn.2019-05.io.openebs:replica1"
                .to_string(),
        }
    );
    assert_eq!(
        Some(&replica),
        client.get_replicas(Filter::None).await?.first()
    );
    client
        .destroy_replica(DestroyReplica {
            node: replica.node.clone(),
            pool: replica.pool.clone(),
            uuid: replica.uuid,
        })
        .await?;
    assert_eq!(client.get_replicas(Filter::None).await?.is_empty(), true);
    client
        .destroy_pool(DestroyPool {
            node: pool.node.clone(),
            name: pool.name,
        })
        .await?;
    assert_eq!(client.get_pools(Filter::None).await?.is_empty(), true);

    test.stop("mayastor").await?;
    tokio::time::delay_for(std::time::Duration::from_millis(250)).await;
    assert!(client.get_nodes().await?.is_empty());
    Ok(())
}
