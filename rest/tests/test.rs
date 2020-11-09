pub use composer::*;
pub use tracing::info;

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

pub fn init() {
    init_tracing();
}

pub async fn bus_init(nats: &str) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        mbus_api::message_bus_init(nats.into()).await
    })
    .await?;
    Ok(())
}
