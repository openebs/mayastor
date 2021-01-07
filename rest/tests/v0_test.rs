use composer::{Binary, Builder, ComposeTest, ContainerSpec};
use mbus_api::{
    v0::{ChannelVs, Liveness, NodeState, PoolState},
    Message,
};
use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use rest_client::{versions::v0::*, ActixRestClient};
use rpc::mayastor::Null;
use tracing::info;

async fn wait_for_services() {
    Liveness {}.request_on(ChannelVs::Node).await.unwrap();
    Liveness {}.request_on(ChannelVs::Pool).await.unwrap();
    Liveness {}.request_on(ChannelVs::Volume).await.unwrap();
}

// to avoid waiting for timeouts
async fn orderly_start(test: &ComposeTest) {
    test.start_containers(vec![
        "nats", "node", "pool", "volume", "rest", "jaeger",
    ])
    .await
    .unwrap();

    test.connect_to_bus("nats").await;
    wait_for_services().await;

    test.start("mayastor").await.unwrap();

    let mut hdl = test.grpc_handle("mayastor").await.unwrap();
    hdl.mayastor.list_nexus(Null {}).await.unwrap();
}

#[actix_rt::test]
async fn client() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let (_tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name("rest-client")
        .install()
        .unwrap();

    let mayastor = "node-test-name";
    let test = Builder::new()
        .name("rest")
        .add_container_spec(ContainerSpec::from_binary(
            "nats",
            Binary::from_nix("nats-server").with_arg("-DV"),
        ))
        .add_container_bin("node", Binary::from_dbg("node").with_nats("-n"))
        .add_container_bin("pool", Binary::from_dbg("pool").with_nats("-n"))
        .add_container_bin("volume", Binary::from_dbg("volume").with_nats("-n"))
        .add_container_spec(
            ContainerSpec::from_binary(
                "rest",
                Binary::from_dbg("rest")
                    .with_nats("-n")
                    .with_args(vec!["-j", "10.1.0.8:6831"]),
            )
            .with_portmap("8080", "8080")
            .with_portmap("8081", "8081"),
        )
        .add_container_bin(
            "mayastor",
            Binary::from_dbg("mayastor")
                .with_nats("-n")
                .with_args(vec!["-N", mayastor])
                .with_args(vec!["-g", "10.1.0.7:10124"]),
        )
        .add_container_spec(
            ContainerSpec::from_image(
                "jaeger",
                "jaegertracing/all-in-one:latest",
            )
            .with_portmap("16686", "16686")
            .with_portmap("6831/udp", "6831/udp")
            .with_portmap("6832/udp", "6832/udp"),
        )
        // uncomment to run alpine commands within the containers
        //.with_base_image("alpine:latest".to_string())
        .with_default_tracing()
        .autorun(false)
        // uncomment to leave containers running allowing us access the jaeger
        // traces at localhost:16686
        //.with_clean(false)
        .build()
        .await
        .unwrap();

    client_test(&mayastor.into(), &test).await;
}

async fn client_test(mayastor: &NodeId, test: &ComposeTest) {
    orderly_start(&test).await;

    let client = ActixRestClient::new("https://localhost:8080", true)
        .unwrap()
        .v0();
    let nodes = client.get_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes.first().unwrap(),
        &Node {
            id: mayastor.clone(),
            grpc_endpoint: "10.1.0.7:10124".to_string(),
            state: NodeState::Online,
        }
    );
    info!("Nodes: {:#?}", nodes);
    let _ = client.get_pools(Filter::None).await.unwrap();
    let pool = client.create_pool(CreatePool {
        node: mayastor.clone(),
        id: "pooloop".into(),
        disks:
    vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".to_string()] }).await.unwrap();
    info!("Pools: {:#?}", pool);
    assert_eq!(
        pool,
        Pool {
            node: "node-test-name".into(),
            id: "pooloop".into(),
            disks: vec!["malloc:///malloc0?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".to_string()],
            state: PoolState::Online,
            capacity: 100663296,
            used: 0,
        }
    );
    assert_eq!(
        Some(&pool),
        client.get_pools(Filter::None).await.unwrap().first()
    );
    let _ = client.get_replicas(Filter::None).await.unwrap();
    let replica = client
        .create_replica(CreateReplica {
            node: pool.node.clone(),
            pool: pool.id.clone(),
            uuid: "replica1".into(),
            size: 12582912, /* actual size will be a multiple of 4MB so just
                             * create it like so */
            thin: true,
            share: Protocol::Nvmf,
        })
        .await
        .unwrap();
    info!("Replica: {:#?}", replica);
    assert_eq!(
        replica,
        Replica {
            node: pool.node.clone(),
            uuid: "replica1".into(),
            pool: pool.id.clone(),
            thin: false,
            size: 12582912,
            share: Protocol::Nvmf,
            uri: "nvmf://10.1.0.7:8420/nqn.2019-05.io.openebs:replica1"
                .to_string(),
        }
    );
    assert_eq!(
        Some(&replica),
        client.get_replicas(Filter::None).await.unwrap().first()
    );
    client
        .destroy_replica(DestroyReplica {
            node: replica.node.clone(),
            pool: replica.pool.clone(),
            uuid: replica.uuid,
        })
        .await
        .unwrap();
    assert!(client.get_replicas(Filter::None).await.unwrap().is_empty());

    let nexuses = client.get_nexuses(Filter::None).await.unwrap();
    assert_eq!(nexuses.len(), 0);
    let nexus = client
        .create_nexus(CreateNexus {
            node: "node-test-name".into(),
            uuid: "058a95e5-cee6-4e81-b682-fe864ca99b9c".into(),
            size: 12582912,
            children: vec!["malloc:///malloc1?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".into()]})
        .await.unwrap();
    info!("Nexus: {:#?}", nexus);

    assert_eq!(
        nexus,
        Nexus {
            node: "node-test-name".into(),
            uuid: "058a95e5-cee6-4e81-b682-fe864ca99b9c".into(),
            size: 12582912,
            state: NexusState::Online,
            children: vec![Child {
                uri: "malloc:///malloc1?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b0".into(),
                state: ChildState::Online,
                rebuild_progress: None
            }],
            device_uri: "".to_string(),
            rebuilds: 0,
        }
    );

    let child = client.add_nexus_child(AddNexusChild {
        node: nexus.node.clone(),
        nexus: nexus.uuid.clone(),
        uri: "malloc:///malloc2?blk_size=512&size_mb=100&uuid=b940f4f2-d45d-4404-8167-3b0366f9e2b1".into(),
        auto_rebuild: true,
    }).await.unwrap();

    assert_eq!(
        Some(&child),
        client
            .get_nexus_children(Filter::Nexus(nexus.uuid.clone()))
            .await
            .unwrap()
            .last()
    );

    client
        .destroy_nexus(DestroyNexus {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
        })
        .await
        .unwrap();
    assert!(client.get_nexuses(Filter::None).await.unwrap().is_empty());

    let volume = client
        .create_volume(CreateVolume {
            uuid: "058a95e5-cee6-4e81-b682-fe864ca99b9c".into(),
            size: 12582912,
            nexuses: 1,
            replicas: 1,
            allowed_nodes: vec![],
            preferred_nodes: vec![],
            preferred_nexus_nodes: vec![],
        })
        .await
        .unwrap();

    tracing::info!("Volume: {:#?}", volume);
    assert_eq!(
        Some(&volume),
        client
            .get_volumes(Filter::Volume(VolumeId::from(
                "058a95e5-cee6-4e81-b682-fe864ca99b9c"
            )))
            .await
            .unwrap()
            .first()
    );

    client
        .destroy_volume(DestroyVolume {
            uuid: "058a95e5-cee6-4e81-b682-fe864ca99b9c".into(),
        })
        .await
        .unwrap();

    assert!(client.get_volumes(Filter::None).await.unwrap().is_empty());

    client
        .destroy_pool(DestroyPool {
            node: pool.node.clone(),
            id: pool.id,
        })
        .await
        .unwrap();
    assert!(client.get_pools(Filter::None).await.unwrap().is_empty());

    test.stop("mayastor").await.unwrap();
    tokio::time::delay_for(std::time::Duration::from_millis(250)).await;
    assert!(client.get_nodes().await.unwrap().is_empty());
}
