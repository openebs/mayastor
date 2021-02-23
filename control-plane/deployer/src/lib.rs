pub mod infra;

use infra::*;

use composer::Builder;
use std::convert::TryInto;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct CliArgs {
    #[structopt(subcommand)]
    action: Action,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Deployment actions")]
pub(crate) enum Action {
    Start(StartOptions),
    Stop(StopOptions),
    List(ListOptions),
}

const DEFAULT_CLUSTER_NAME: &str = "cluster";

#[derive(Debug, StructOpt)]
#[structopt(about = "Stop and delete all components")]
pub struct StopOptions {
    /// Name of the cluster
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    pub cluster_name: String,
}

#[derive(Debug, Default, StructOpt)]
#[structopt(about = "List all running components")]
pub struct ListOptions {
    /// Simple list without using the docker executable
    #[structopt(short, long)]
    pub no_docker: bool,

    /// Format the docker output
    #[structopt(short, long, conflicts_with = "no_docker")]
    pub format: Option<String>,

    /// Name of the cluster
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    pub cluster_name: String,
}

pub fn default_agents() -> &'static str {
    "Node, Pool, Volume"
}

#[derive(Debug, Default, Clone, StructOpt)]
#[structopt(about = "Create and start all components")]
pub struct StartOptions {
    /// Use the following Control Plane Agents
    /// Specify one agent at a time or as a list.
    /// ( "" for no agents )
    /// todo: specify start arguments, eg: Node="-v"
    #[structopt(
        short,
        long,
        default_value = default_agents(),
        value_delimiter = ","
    )]
    pub agents: Vec<ControlPlaneAgent>,

    /// Kubernetes Config file if using operators
    /// [default: "~/.kube/config"]
    #[structopt(short, long)]
    pub kube_config: Option<String>,

    /// Use a base image for the binary components (eg: alpine:latest)
    #[structopt(long)]
    pub base_image: Option<String>,

    /// Use a jaeger tracing service
    #[structopt(short, long)]
    pub jaeger: bool,

    /// Disable the REST Server
    #[structopt(long)]
    pub no_rest: bool,

    /// Use `N` mayastor instances
    #[structopt(short, long, default_value = "1")]
    pub mayastors: u32,

    /// Use this custom image for the jaeger tracing service
    #[structopt(long, requires = "jaeger")]
    pub jaeger_image: Option<String>,

    /// Cargo Build each component before deploying
    #[structopt(short, long)]
    pub build: bool,

    /// Use a dns resolver for the cluster: defreitas/dns-proxy-server
    /// Note this messes with your /etc/resolv.conf so use at your own risk
    #[structopt(short, long)]
    pub dns: bool,

    /// Show information from the cluster after creation
    #[structopt(short, long)]
    pub show_info: bool,

    /// Name of the cluster - currently only one allowed at a time
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    pub cluster_name: String,
}

impl StartOptions {
    pub fn with_agents(mut self, agents: Vec<&str>) -> Self {
        let agents: ControlPlaneAgents = agents.try_into().unwrap();
        self.agents = agents.into_inner();
        self
    }
    pub fn with_jaeger(mut self, jaeger: bool) -> Self {
        self.jaeger = jaeger;
        self
    }
    pub fn with_build(mut self, build: bool) -> Self {
        self.build = build;
        self
    }
    pub fn with_mayastors(mut self, mayastors: u32) -> Self {
        self.mayastors = mayastors;
        self
    }
    pub fn with_show_info(mut self, show_info: bool) -> Self {
        self.show_info = show_info;
        self
    }
    pub fn with_cluster_name(mut self, cluster_name: &str) -> Self {
        self.cluster_name = cluster_name.to_string();
        self
    }
    pub fn with_base_image(
        mut self,
        base_image: impl Into<Option<String>>,
    ) -> Self {
        self.base_image = base_image.into();
        self
    }
}

impl CliArgs {
    /// Act upon the requested action
    pub async fn execute(&self) -> Result<(), Error> {
        self.action.execute().await
    }
}

impl Action {
    async fn execute(&self) -> Result<(), Error> {
        match self {
            Action::Start(options) => options.start(self).await,
            Action::Stop(options) => options.stop(self).await,
            Action::List(options) => options.list(self).await,
        }
    }
}

impl StartOptions {
    async fn start(&self, _action: &Action) -> Result<(), Error> {
        let components = Components::new(self.clone());
        let composer = Builder::new()
            .name(&self.cluster_name)
            .configure(components.clone())?
            .with_clean(false)
            .with_base_image(self.base_image.clone())
            .autorun(false)
            .build()
            .await?;

        components.start(&composer).await?;

        if self.show_info {
            let lister = ListOptions {
                cluster_name: self.cluster_name.clone(),
                ..Default::default()
            };
            lister.list_simple().await?;
        }
        Ok(())
    }
}
impl StopOptions {
    async fn stop(&self, _action: &Action) -> Result<(), Error> {
        let composer = Builder::new()
            .name(&self.cluster_name)
            .with_prune(false)
            .with_clean(true)
            .build()
            .await?;
        let _ = composer.stop_network_containers().await;
        let _ = composer
            .remove_network_containers(&self.cluster_name)
            .await?;
        Ok(())
    }
}
impl ListOptions {
    fn list_docker(&self) -> Result<(), Error> {
        let label_filter =
            format!("label=io.mayastor.test.name={}", self.cluster_name);
        let mut args = vec!["ps", "-a", "--filter", &label_filter];
        if let Some(format) = &self.format {
            args.push("--format");
            args.push(format)
        }
        let status =
            std::process::Command::new("docker").args(args).status()?;
        build_error("docker", status.code())
    }
    /// Simple listing of all started components
    pub async fn list_simple(&self) -> Result<(), Error> {
        let cfg = Builder::new()
            .name(&self.cluster_name)
            .with_reuse(true)
            .with_clean(false)
            .build()
            .await?;

        for component in cfg.list_cluster_containers().await? {
            let ip = match component.network_settings.clone() {
                None => None,
                Some(networks) => match networks.networks {
                    None => None,
                    Some(network) => match network.get(&self.cluster_name) {
                        None => None,
                        Some(endpoint) => endpoint.ip_address.clone(),
                    },
                },
            };
            println!(
                "[{}] [{}] {}",
                component
                    .names
                    .unwrap_or_default()
                    .first()
                    .unwrap_or(&"?".to_string()),
                ip.unwrap_or_default(),
                option_str(component.command),
            );
        }
        Ok(())
    }
    async fn list(&self, _action: &Action) -> Result<(), Error> {
        match self.no_docker {
            true => self.list_simple().await,
            false => self.list_docker(),
        }
    }
}

fn option_str<F: ToString>(input: Option<F>) -> String {
    match input {
        Some(input) => input.to_string(),
        None => "?".into(),
    }
}
