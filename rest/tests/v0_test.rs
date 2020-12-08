mod test;
use mbus_api::{
    v0::{GetNodes, NodeState},
    Message,
};
use rest_client::{versions::v0::*, ActixRestClient};
use rpc::mayastor::Null;
use test::{Binary, Builder, ComposeTest, ContainerSpec};

async fn wait_for_node() -> Result<(), Box<dyn std::error::Error>> {
    let _ = GetNodes {}.request().await?;
    Ok(())
}

// to avoid waiting for timeouts
async fn orderly_start(
    test: &ComposeTest,
) -> Result<(), Box<dyn std::error::Error>> {
    test.start_containers(vec!["nats", "node", "rest"]).await?;

    test::bus_init("localhost").await?;
    wait_for_node().await?;

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
                .with_args(vec!["-N", mayastor]),
        )
        .with_clean(true)
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
            grpc_endpoint: "0.0.0.0:10124".to_string(),
            state: NodeState::Online,
        }
    );
    test.stop("mayastor").await?;
    tokio::time::delay_for(std::time::Duration::from_millis(250)).await;
    assert!(client.get_nodes().await?.is_empty());
    Ok(())
}
