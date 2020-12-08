use async_trait::async_trait;
use common::*;
use mbus_api::{v0::*, *};
use std::{collections::HashMap, convert::TryInto, marker::PhantomData};
use structopt::StructOpt;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Nats Server URL to connect to
    /// (supports the nats schema)
    /// Default: nats://127.0.0.1:4222
    #[structopt(long, short, default_value = "nats://127.0.0.1:4222")]
    nats: String,
    /// Deadline for the mayastor instance keep alive registration
    /// Default: 20s
    #[structopt(long, short, default_value = "20s")]
    deadline: humantime::Duration,
}

/// Needed so we can implement the ServiceSubscriber trait for
/// the message types external to the crate
#[derive(Clone, Default)]
struct ServiceHandler<T> {
    data: PhantomData<T>,
}

/// Watchdog with which must be pet within the deadline, otherwise
/// it triggers the `on_timeout` future
#[derive(Clone)]
struct Watchdog {
    deadline: std::time::Duration,
    pet_chan: tokio::sync::mpsc::Sender<()>,
}

impl Watchdog {
    /// new empty watchdog with a timeout
    pub fn new(deadline: std::time::Duration) -> Self {
        Self {
            deadline,
            pet_chan: tokio::sync::mpsc::channel(1).0,
        }
    }

    /// arm watchdog with self timeout and execute error callback if
    /// the deadline is not met
    pub fn arm<T>(&mut self, on_timeout: T)
    where
        T: std::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let deadline = self.deadline;
        let (s, mut r) = tokio::sync::mpsc::channel(1);
        self.pet_chan = s;
        tokio::spawn(async move {
            let result = tokio::time::timeout(deadline, r.recv()).await;
            if result.is_err() {
                on_timeout.await;
            }
        });
    }

    /// meet the deadline
    #[allow(dead_code)]
    pub async fn pet(
        &mut self,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        self.pet_chan.send(()).await
    }
}

/// In memory database of all nodes which we know of and their state
#[derive(Default, Clone)]
struct NodeStore {
    inner: std::sync::Arc<NodeStoreInner>,
}
struct NodeStoreInner {
    state: Mutex<HashMap<String, (Node, Watchdog)>>,
    deadline: std::time::Duration,
}
impl Default for NodeStoreInner {
    fn default() -> Self {
        Self {
            deadline: CliArgs::from_args().deadline.into(),
            state: Default::default(),
        }
    }
}

impl NodeStore {
    /// Register a new node through the register information
    async fn register(&self, registration: Register) {
        let mut state = self.inner.state.lock().await;

        let mut watchdog = Watchdog::new(self.inner.deadline);
        let id = registration.id.clone();
        let store = self.clone();
        let deadline = self.inner.deadline;
        watchdog.arm(async move {
            error!(
                "Node id {} missed the registration deadline of {:?}!",
                id, deadline
            );
            store.offline(id).await;
        });

        let id = registration.id.clone();
        let node = Node {
            id: registration.id,
            grpc_endpoint: registration.grpc_endpoint,
            state: NodeState::Online,
        };
        state.insert(id, (node, watchdog));
    }
    /// Deregister a node through the deregister information
    async fn deregister(&self, node: Deregister) {
        let mut state = self.inner.state.lock().await;
        state.remove(&node.id);
    }
    /// Offline node through its id
    async fn offline(&self, id: String) {
        let mut state = self.inner.state.lock().await;
        if let Some(n) = state.get_mut(&id) {
            n.0.state = NodeState::Offline;
        }
    }
    /// Get the list of nodes which we know of
    async fn get_nodes(&self) -> Vec<Node> {
        let nodes = self.inner.state.lock().await;
        nodes
            .values()
            .cloned()
            .collect::<Vec<(Node, Watchdog)>>()
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<Register> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let store: &NodeStore = args.context.get_state();
        store.register(args.request.inner()?).await;
        Ok(())
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![Register::default().id()]
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<Deregister> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let store: &NodeStore = args.context.get_state();
        store.deregister(args.request.inner()?).await;
        Ok(())
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![Deregister::default().id()]
    }
}

#[async_trait]
impl ServiceSubscriber for ServiceHandler<GetNodes> {
    async fn handler(&self, args: Arguments<'_>) -> Result<(), Error> {
        let request: ReceivedMessage<GetNodes, Nodes> =
            args.request.try_into()?;

        let store: &NodeStore = args.context.get_state();
        let nodes = store.get_nodes().await;
        request.reply(Nodes(nodes)).await
    }
    fn filter(&self) -> Vec<MessageId> {
        vec![GetNodes::default().id()]
    }
}

fn init_tracing() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
}

#[tokio::main]
async fn main() {
    init_tracing();

    let cli_args = CliArgs::from_args();
    info!("Using options: {:?}", &cli_args);

    server(cli_args).await;
}

async fn server(cli_args: CliArgs) {
    Service::builder(cli_args.nats, ChannelVs::Registry)
        .with_shared_state(NodeStore::default())
        .with_subscription(ServiceHandler::<Register>::default())
        .with_subscription(ServiceHandler::<Deregister>::default())
        .with_channel(ChannelVs::Node)
        .with_subscription(ServiceHandler::<GetNodes>::default())
        .run()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use composer::*;
    use rpc::mayastor::Null;

    async fn bus_init() -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            mbus_api::message_bus_init("10.1.0.2".into()).await
        })
        .await?;
        Ok(())
    }
    async fn wait_for_node() -> Result<(), Box<dyn std::error::Error>> {
        let _ = GetNodes {}.request().await?;
        Ok(())
    }
    fn init_tracing() {
        if let Ok(filter) =
            tracing_subscriber::EnvFilter::try_from_default_env()
        {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        } else {
            tracing_subscriber::fmt().with_env_filter("info").init();
        }
    }
    // to avoid waiting for timeouts
    async fn orderly_start(
        test: &ComposeTest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        test.start_containers(vec!["nats", "node"]).await?;

        bus_init().await?;
        wait_for_node().await?;

        test.start("mayastor").await?;

        let mut hdl = test.grpc_handle("mayastor").await?;
        hdl.mayastor.list_nexus(Null {}).await?;
        Ok(())
    }

    #[tokio::test]
    async fn node() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();
        let maya_name = "node-test-name";
        let test = Builder::new()
            .name("node")
            .add_container_bin(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .add_container_bin(
                "node",
                Binary::from_dbg("node")
                    .with_nats("-n")
                    .with_args(vec!["-d", "2sec"]),
            )
            .add_container_bin(
                "mayastor",
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", maya_name]),
            )
            .with_clean(true)
            .autorun(false)
            .build()
            .await?;

        orderly_start(&test).await?;

        let nodes = GetNodes {}.request().await?;
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.to_string(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Online,
            }
        );
        tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
        let nodes = GetNodes {}.request().await?;
        tracing::info!("Nodes: {:?}", nodes);
        assert_eq!(nodes.0.len(), 1);
        assert_eq!(
            nodes.0.first().unwrap(),
            &Node {
                id: maya_name.to_string(),
                grpc_endpoint: "0.0.0.0:10124".to_string(),
                state: NodeState::Offline,
            }
        );

        // run with --nocapture to see all the logs
        test.logs_all().await?;
        Ok(())
    }
}
