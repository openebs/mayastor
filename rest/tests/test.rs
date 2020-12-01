pub use composer::*;
use mbus_api::{message_bus_init_options, TimeoutOptions};
use std::time::Duration;
pub use tracing::info;

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("debug,h2=info,bollard=info,hyper=info,trust_dns_resolver=info,rustls=info,tower_buffer=info").init();
    }
}

pub fn init() {
    init_tracing();
}

pub async fn bus_init(nats: &str) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        message_bus_init_options(
            nats.into(),
            TimeoutOptions::new()
                .with_timeout(Duration::from_millis(150))
                .with_max_retries(10)
                .with_timeout_backoff(Duration::from_millis(100)),
        )
        .await
    })
    .await?;
    Ok(())
}
