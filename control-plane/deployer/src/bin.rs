pub mod infra;

use composer::Builder;
use infra::*;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct CliArgs {
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
pub(crate) struct StopOptions {
    /// Name of the cluster
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    cluster_name: String,
}

#[derive(Debug, Default, StructOpt)]
#[structopt(about = "List all running components")]
pub(crate) struct ListOptions {
    /// Simple list without using the docker executable
    #[structopt(short, long)]
    no_docker: bool,

    /// Format the docker output
    #[structopt(short, long, conflicts_with = "no_docker")]
    format: Option<String>,

    /// Name of the cluster
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    cluster_name: String,
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(about = "Create and start all components")]
pub(crate) struct StartOptions {
    /// Use the following Control Plane Agents
    /// Specify one agent at a time or as a list.
    /// ( "" for no agents )
    /// todo: specify start arguments, eg: Node="-v"
    #[structopt(
        short,
        long,
        default_value = "Node, Pool, Volume",
        value_delimiter = ","
    )]
    agents: Vec<ControlPlaneAgent>,

    /// Use the following Control Plane Operators
    /// Specify one operator at a time or as a list
    #[structopt(short, long, value_delimiter = ",")]
    operators: Option<Vec<ControlPlaneOperator>>,

    /// Kubernetes Config file if using operators
    /// [default: "~/.kube/config"]
    #[structopt(short, long)]
    kube_config: Option<String>,

    /// Use a base image for the binary components (eg: alpine:latest)
    #[structopt(long)]
    base_image: Option<String>,

    /// Use a jaeger tracing service
    #[structopt(short, long)]
    jaeger: bool,

    /// Disable the REST Server
    #[structopt(long)]
    no_rest: bool,

    /// Use `N` mayastor instances
    #[structopt(short, long, default_value = "1")]
    mayastors: u32,

    /// Use this custom image for the jaeger tracing service
    #[structopt(long, requires = "jaeger")]
    jaeger_image: Option<String>,

    /// Cargo Build each component before deploying
    #[structopt(short, long)]
    build: bool,

    /// Use a dns resolver for the cluster: defreitas/dns-proxy-server
    /// Note this messes with your /etc/resolv.conf so use at your own risk
    #[structopt(short, long)]
    dns: bool,

    /// Show information from the cluster after creation
    #[structopt(short, long)]
    show_info: bool,

    /// Name of the cluster - currently only one allowed at a time
    #[structopt(short, long, default_value = DEFAULT_CLUSTER_NAME)]
    cluster_name: String,
}

impl Action {
    async fn act(&self) -> Result<(), Error> {
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
    async fn list_simple(&self) -> Result<(), Error> {
        let cfg = Builder::new()
            .name(&self.cluster_name)
            .with_prune(false)
            .with_clean(false)
            .build()
            .await?;

        for component in cfg.list_containers().await? {
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli_args = CliArgs::from_args();
    println!("Using options: {:?}", &cli_args);

    cli_args.action.act().await
}
