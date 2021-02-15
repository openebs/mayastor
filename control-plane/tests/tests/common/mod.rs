use composer::*;
use deployer_lib::{
    infra::{Components, Error, Mayastor},
    *,
};
use opentelemetry::{
    global,
    sdk::{propagation::TraceContextPropagator, trace::Tracer},
};

use opentelemetry_jaeger::Uninstall;
pub use rest_client::{
    versions::v0::{self, RestClient},
    ActixRestClient,
};

#[actix_rt::test]
#[ignore]
async fn smoke_test() {
    // make sure the cluster can bootstrap properly
    let _cluster = ClusterBuilder::builder()
        .build()
        .await
        .expect("Should bootstrap the cluster!");
}

/// Default options to create a cluster
pub fn default_options() -> StartOptions {
    StartOptions::default()
        .with_agents(default_agents().split(',').collect())
        .with_jaeger(true)
        .with_mayastors(1)
        .with_show_info(true)
        .with_cluster_name("rest_cluster")
}

/// Cluster with the composer, the rest client and the jaeger pipeline#
#[allow(unused)]
pub struct Cluster {
    composer: ComposeTest,
    rest_client: ActixRestClient,
    jaeger: (Tracer, Uninstall),
    builder: ClusterBuilder,
}

impl Cluster {
    /// node id for `index`
    pub fn node(&self, index: u32) -> v0::NodeId {
        Mayastor::name(index, &self.builder.opts).into()
    }

    /// pool id for `pool` index on `node` index
    pub fn pool(&self, node: u32, pool: u32) -> v0::PoolId {
        format!("{}-pool-{}", self.node(node), pool + 1).into()
    }

    /// replica id with index for `pool` index and `replica` index
    pub fn replica(pool: u32, replica: u32) -> v0::ReplicaId {
        let mut uuid = v0::ReplicaId::default().to_string();
        let _ = uuid.drain(27 .. uuid.len());
        format!("{}{:01x}{:08x}", uuid, pool as u8, replica).into()
    }

    /// rest client v0
    pub fn rest_v0(&self) -> impl RestClient {
        self.rest_client.v0()
    }

    /// New cluster
    async fn new(
        trace_rest: bool,
        components: Components,
        composer: ComposeTest,
        jaeger: (Tracer, Uninstall),
    ) -> Result<Cluster, Error> {
        let rest_client =
            ActixRestClient::new("https://localhost:8080", trace_rest).unwrap();

        components
            .start_wait(&composer, std::time::Duration::from_secs(10))
            .await?;

        let cluster = Cluster {
            composer,
            rest_client,
            jaeger,
            builder: ClusterBuilder::builder(),
        };

        Ok(cluster)
    }
}

fn option_str<F: ToString>(input: Option<F>) -> String {
    match input {
        Some(input) => input.to_string(),
        None => "?".into(),
    }
}

/// Run future and compare result with what's expected
/// Expected result should be in the form Result<TestValue,TestValue>
/// where TestValue is a useful value which will be added to the returned error
/// string Eg, testing the replica share protocol:
/// test_result(Ok(Nvmf), async move { ... })
/// test_result(Err(NBD), async move { ... })
pub async fn test_result<F, O, E, T, R>(
    expected: &Result<O, E>,
    future: F,
) -> Result<(), anyhow::Error>
where
    F: std::future::Future<Output = Result<T, R>>,
    R: std::fmt::Display,
    E: std::fmt::Debug,
    O: std::fmt::Debug,
{
    match future.await {
        Ok(_) if expected.is_ok() => Ok(()),
        Err(_) if expected.is_err() => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "Expected '{:#?}' but failed with '{}'!",
            expected,
            error
        )),
        Ok(_) => {
            Err(anyhow::anyhow!("Expected '{:#?}' but succeeded!", expected))
        }
    }
}

#[macro_export]
macro_rules! result_either {
    ($test:expr) => {
        match $test {
            Ok(v) => v,
            Err(v) => v,
        }
    };
}

/// Builder for the Cluster
pub struct ClusterBuilder {
    opts: StartOptions,
    pools: u32,
    replicas: (u32, u64, v0::Protocol),
    trace: bool,
}

impl ClusterBuilder {
    /// Cluster Builder with default options
    pub fn builder() -> Self {
        ClusterBuilder {
            opts: default_options(),
            pools: 0,
            replicas: (0, 0, v0::Protocol::Off),
            trace: true,
        }
    }
    /// Update the start options
    pub fn with_options<F>(mut self, set: F) -> Self
    where
        F: Fn(StartOptions) -> StartOptions,
    {
        self.opts = set(self.opts);
        self
    }
    /// Enable/Disable jaeger tracing
    pub fn with_tracing(mut self, enabled: bool) -> Self {
        self.trace = enabled;
        self
    }
    /// Add `count` malloc pools (100MiB size) to each node
    pub fn with_pools(mut self, count: u32) -> Self {
        self.pools = count;
        self
    }
    /// Add `count` replicas to each node per pool
    pub fn with_replicas(
        mut self,
        count: u32,
        size: u64,
        share: v0::Protocol,
    ) -> Self {
        self.replicas = (count, size, share);
        self
    }
    /// Build into the resulting Cluster using a composer closure, eg:
    /// .compose_build(|c| c.with_logs(false))
    pub async fn compose_build<F>(self, set: F) -> Result<Cluster, Error>
    where
        F: Fn(Builder) -> Builder,
    {
        let (components, composer) = self.build_prepare()?;
        let composer = set(composer);
        let mut cluster = self.new_cluster(components, composer).await?;
        cluster.builder = self;
        Ok(cluster)
    }
    /// Build into the resulting Cluster
    pub async fn build(self) -> Result<Cluster, Error> {
        let (components, composer) = self.build_prepare()?;
        let mut cluster = self.new_cluster(components, composer).await?;
        cluster.builder = self;
        Ok(cluster)
    }
    fn build_prepare(&self) -> Result<(Components, Builder), Error> {
        let components = Components::new(self.opts.clone());
        let composer = Builder::new()
            .name(&self.opts.cluster_name)
            .configure(components.clone())?
            .with_base_image(self.opts.base_image.clone())
            .autorun(false)
            .with_default_tracing()
            .with_clean(true)
            // test script will clean up containers if ran on CI/CD
            .with_clean_on_panic(false)
            .with_logs(true);
        Ok((components, composer))
    }
    async fn new_cluster(
        &self,
        components: Components,
        compose_builder: Builder,
    ) -> Result<Cluster, Error> {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let jaeger = opentelemetry_jaeger::new_pipeline()
            .with_service_name("tests-client")
            .install()
            .unwrap();

        let composer = compose_builder.build().await?;
        let cluster =
            Cluster::new(self.trace, components, composer, jaeger).await?;

        if self.opts.show_info {
            for container in cluster.composer.list_cluster_containers().await? {
                let networks =
                    container.network_settings.unwrap().networks.unwrap();
                let ip = networks
                    .get(&self.opts.cluster_name)
                    .unwrap()
                    .ip_address
                    .clone();
                tracing::debug!(
                    "{:?} [{}] {}",
                    container.names.clone().unwrap_or_default(),
                    ip.clone().unwrap_or_default(),
                    option_str(container.command.clone())
                );
            }
        }
        for pool in &self.pools() {
            cluster
                .rest_v0()
                .create_pool(v0::CreatePool {
                    node: pool.node.clone().into(),
                    id: pool.id(),
                    disks: vec![pool.disk()],
                })
                .await
                .unwrap();
            for replica in &pool.replicas {
                cluster
                    .rest_v0()
                    .create_replica(replica.clone())
                    .await
                    .unwrap();
            }
        }

        Ok(cluster)
    }
    fn pools(&self) -> Vec<Pool> {
        let mut pools = vec![];
        for pool_index in 0 .. self.pools {
            for node in 0 .. self.opts.mayastors {
                let mut pool = Pool {
                    node: Mayastor::name(node, &self.opts),
                    kind: PoolKind::Malloc,
                    size_mb: 100,
                    index: (pool_index + 1) as u32,
                    replicas: vec![],
                };
                for replica_index in 0 .. self.replicas.0 {
                    pool.replicas.push(v0::CreateReplica {
                        node: pool.node.clone().into(),
                        uuid: Cluster::replica(pool_index, replica_index),
                        pool: pool.id(),
                        size: self.replicas.1,
                        thin: false,
                        share: self.replicas.2.clone(),
                    });
                }
                pools.push(pool);
            }
        }
        pools
    }
}

#[allow(dead_code)]
enum PoolKind {
    Malloc,
    Aio,
    Uring,
    Nvmf,
}

struct Pool {
    node: String,
    kind: PoolKind,
    size_mb: u32,
    index: u32,
    replicas: Vec<v0::CreateReplica>,
}

impl Pool {
    fn id(&self) -> v0::PoolId {
        format!("{}-pool-{}", self.node, self.index).into()
    }
    fn disk(&self) -> String {
        match self.kind {
            PoolKind::Malloc => {
                format!("malloc:///disk{}?size_mb={}", self.index, self.size_mb)
            }
            _ => panic!("kind not supported!"),
        }
    }
}
