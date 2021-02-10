use mbus_api::{v0::*, *};
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    url: String,
}

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

const NODE_NAME: &str = "mayastor-node";
const POOL_NAME: &str = "test-pool";

#[tokio::main]
async fn main() {
    init_tracing();
    client().await;
}

/// Client interactions with the Pool service.
async fn client() {
    let cli_args = CliArgs::from_args();
    mbus_api::message_bus_init(cli_args.url).await;
    create_pool(NODE_NAME, POOL_NAME).await;
    list_pools().await;
    destroy_pool(NODE_NAME, POOL_NAME).await;
    list_pools().await;
}

/// Create a pool on a given storage node with the given name.
async fn create_pool(node: &str, pool: &str) {
    CreatePool {
        node: node.into(),
        id: pool.into(),
        disks: vec!["malloc:///disk0?size_mb=100".into()],
    }
    .request()
    .await
    .unwrap();
}

// Destroy a pool on the given node with the given name.
async fn destroy_pool(node: &str, pool: &str) {
    DestroyPool {
        node: node.into(),
        id: pool.into(),
    }
    .request()
    .await
    .unwrap();
}

/// List all pools.
async fn list_pools() {
    let pools = GetPools::default().request().await.unwrap();
    info!("Received Pools: {:?}", pools);
}
