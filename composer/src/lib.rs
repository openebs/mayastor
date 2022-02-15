use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use bollard::{
    container::{
        Config,
        CreateContainerOptions,
        ListContainersOptions,
        LogsOptions,
        NetworkingConfig,
        RemoveContainerOptions,
        RestartContainerOptions,
        StopContainerOptions,
    },
    errors::Error,
    network::{CreateNetworkOptions, ListNetworksOptions},
    service::{
        ContainerSummaryInner,
        EndpointIpamConfig,
        EndpointSettings,
        HostConfig,
        Ipam,
        Mount,
        MountTypeEnum,
        Network,
        PortMap,
    },
    Docker,
    API_DEFAULT_VERSION,
};
use futures::{StreamExt, TryStreamExt};
use ipnetwork::Ipv4Network;
use tonic::transport::Channel;

use bollard::{
    image::CreateImageOptions,
    models::ContainerInspectResponse,
    network::DisconnectNetworkOptions,
};
use rpc::mayastor::{
    bdev_rpc_client::BdevRpcClient,
    json_rpc_client::JsonRpcClient,
    mayastor_client::MayastorClient,
};

pub const TEST_NET_NAME: &str = "mayastor-testing-network";
pub const TEST_NET_NETWORK: &str = "10.1.0.0/16";
#[derive(Clone)]
pub struct RpcHandle {
    pub name: String,
    pub endpoint: SocketAddr,
    pub mayastor: MayastorClient<Channel>,
    pub bdev: BdevRpcClient<Channel>,
    pub jsonrpc: JsonRpcClient<Channel>,
}

impl RpcHandle {
    /// connect to the containers and construct a handle
    async fn connect(
        name: String,
        endpoint: SocketAddr,
    ) -> Result<Self, String> {
        let mut attempts = 40;
        loop {
            if TcpStream::connect_timeout(&endpoint, Duration::from_millis(100))
                .is_ok()
            {
                break;
            } else {
                thread::sleep(Duration::from_millis(101));
            }
            attempts -= 1;
            if attempts == 0 {
                return Err(format!(
                    "Failed to connect to {}/{}",
                    name, endpoint
                ));
            }
        }

        let mayastor = MayastorClient::connect(format!("http://{}", endpoint))
            .await
            .unwrap();
        let bdev = BdevRpcClient::connect(format!("http://{}", endpoint))
            .await
            .unwrap();
        let jsonrpc = JsonRpcClient::connect(format!("http://{}", endpoint))
            .await
            .unwrap();

        Ok(Self {
            name,
            endpoint,
            mayastor,
            bdev,
            jsonrpc,
        })
    }
}

/// Path to local binary and arguments
#[derive(Default, Clone)]
pub struct Binary {
    path: String,
    arguments: Vec<String>,
    env: HashMap<String, String>,
    binds: HashMap<String, String>,
}

impl Binary {
    /// Setup local binary from target debug and arguments
    pub fn from_dbg(name: &str) -> Self {
        let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
        let srcdir = path.parent().unwrap().to_string_lossy();

        Self::new(&format!("{}/target/debug/{}", srcdir, name), vec![])
    }
    /// Setup nix shell binary from path and arguments
    pub fn from_nix(name: &str) -> Self {
        Self::new(name, vec![])
    }
    /// Add single argument
    /// Only one argument can be passed per use. So instead of:
    ///
    /// # Self::from_dbg("hello")
    /// # ;
    ///
    /// usage would be:
    ///
    /// # Self::from_dbg("hello")
    /// # ;
    pub fn with_arg(mut self, arg: &str) -> Self {
        self.arguments.push(arg.into());
        self
    }
    /// Add multiple arguments via a vector
    pub fn with_args<S: Into<String>>(mut self, mut args: Vec<S>) -> Self {
        self.arguments.extend(args.drain(..).map(|s| s.into()));
        self
    }
    /// Add environment variables for the container
    pub fn with_env(mut self, key: &str, val: &str) -> Self {
        if let Some(old) = self.env.insert(key.into(), val.into()) {
            println!("Replaced key {} val {} with val {}", key, old, val);
        }
        self
    }
    /// Add volume bind between host path and container path
    pub fn with_bind(mut self, host: &str, container: &str) -> Self {
        self.binds.insert(container.to_string(), host.to_string());
        self
    }

    fn commands(&self) -> Vec<String> {
        let mut v = vec![self.path.clone()];
        v.extend(self.arguments.clone());
        v
    }
    fn which(name: &str) -> std::io::Result<String> {
        let output = std::process::Command::new("which").arg(name).output()?;
        if !output.status.success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                name,
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().into())
    }
    fn new(path: &str, args: Vec<String>) -> Self {
        Self {
            path: Self::which(path).expect("Binary path should exist!"),
            arguments: args,
            ..Default::default()
        }
    }
}

const RUST_LOG_DEFAULT: &str =
    "debug,actix_web=debug,actix=debug,h2=info,hyper=info,tower=info,bollard=info,rustls=info";

/// Specs of the allowed containers include only the binary path
/// (relative to src) and the required arguments
#[derive(Default, Clone)]
pub struct ContainerSpec {
    /// Name of the container
    name: ContainerName,
    /// Base image of the container
    image: Option<String>,
    /// Command to run
    command: Option<String>,
    /// command arguments to run
    arguments: Option<Vec<String>>,
    /// local binary
    binary: Option<Binary>,
    /// Port mapping to host ports
    port_map: Option<PortMap>,
    /// Use Init container
    init: Option<bool>,
    /// Key-Map of environment variables
    /// Starts with RUST_LOG=debug,h2=info
    env: HashMap<String, String>,
    /// Volume bind dst/source
    binds: HashMap<String, String>,
}

impl ContainerSpec {
    /// Create new ContainerSpec from name and binary
    pub fn from_binary(name: &str, binary: Binary) -> Self {
        let mut env = binary.env.clone();
        if !env.contains_key("RUST_LOG") {
            env.insert("RUST_LOG".to_string(), RUST_LOG_DEFAULT.to_string());
        }
        Self {
            name: name.into(),
            binary: Some(binary),
            init: Some(true),
            env,
            ..Default::default()
        }
    }
    /// Create new ContainerSpec from name and image
    pub fn from_image(name: &str, image: &str) -> Self {
        let mut env = HashMap::new();
        env.insert("RUST_LOG".to_string(), RUST_LOG_DEFAULT.to_string());
        Self {
            name: name.into(),
            init: Some(true),
            image: Some(image.into()),
            env,
            ..Default::default()
        }
    }
    /// Add port mapping from container to host
    pub fn with_portmap(mut self, from: &str, to: &str) -> Self {
        let from = format!("{}/tcp", from);
        let binding = bollard::service::PortBinding {
            host_ip: None,
            host_port: Some(to.into()),
        };
        if let Some(pm) = &mut self.port_map {
            pm.insert(from, Some(vec![binding]));
        } else {
            let mut port_map = bollard::service::PortMap::new();
            port_map.insert(from, Some(vec![binding]));
            self.port_map = Some(port_map);
        }
        self
    }
    /// Add environment key-val, eg for setting the RUST_LOG
    /// If a key already exists, the value is replaced
    pub fn with_env(mut self, key: &str, val: &str) -> Self {
        if let Some(old) = self.env.insert(key.into(), val.into()) {
            println!("Replaced key {} val {} with val {}", key, old, val);
        }
        self
    }
    /// use a volume binds between host path and container container
    pub fn with_bind(mut self, host: &str, container: &str) -> Self {
        self.binds.insert(container.to_string(), host.to_string());
        self
    }

    /// List of volume binds with each element as host:container
    fn binds(&self) -> Vec<String> {
        let mut vec = vec![];
        self.binds.iter().for_each(|(container, host)| {
            vec.push(format!("{}:{}", host, container));
        });
        if let Some(binary) = &self.binary {
            binary.binds.iter().for_each(|(container, host)| {
                vec.push(format!("{}:{}", host, container));
            });
        }
        vec
    }

    /// Environment variables as a vector with each element as:
    /// "{key}={value}"
    fn environment(&self) -> Vec<String> {
        let mut vec = vec![];
        self.env.iter().for_each(|(k, v)| {
            vec.push(format!("{}={}", k, v));
        });
        vec
    }
    /// Command/entrypoint followed by/and arguments
    fn commands(&self) -> Vec<String> {
        let mut commands = vec![];
        if let Some(binary) = self.binary.clone() {
            commands.extend(binary.commands());
        } else if let Some(command) = self.command.clone() {
            commands.push(command);
        }
        commands.extend(self.arguments.clone().unwrap_or_default());
        commands
    }
}

pub struct Builder {
    /// name of the experiment this name will be used as a network and labels
    /// this way we can "group" all objects within docker to match this test
    /// test. It is highly recommend you use a sane name for this as it will
    /// help you during debugging
    name: String,
    /// containers we want to create, note these are mayastor containers
    /// only
    containers: Vec<ContainerSpec>,
    /// the network for the tests used
    network: String,
    /// reuse existing containers
    reuse: bool,
    /// allow cleaning up on a panic (if clean is true)
    allow_clean_on_panic: bool,
    /// delete the container and network when dropped
    clean: bool,
    /// destroy existing containers if any
    prune: bool,
    /// run all containers on build
    autorun: bool,
    /// base image for image-less containers
    image: Option<String>,
    /// output container logs on panic
    logs_on_panic: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Builder::new()
    }
}

/// trait to allow extensibility using the Builder pattern
pub trait BuilderConfigure {
    fn configure(
        &self,
        cfg: Builder,
    ) -> Result<Builder, Box<dyn std::error::Error>>;
}

impl Builder {
    /// construct a new builder for `[ComposeTest']
    pub fn new() -> Self {
        Self {
            name: TEST_NET_NAME.to_string(),
            containers: Default::default(),
            network: "10.1.0.0/16".to_string(),
            reuse: false,
            allow_clean_on_panic: true,
            clean: true,
            prune: true,
            autorun: true,
            image: None,
            logs_on_panic: true,
        }
    }

    /// get the name of the experiment
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// configure the `Builder` using the `BuilderConfigure` trait
    pub fn configure(
        self,
        cfg: impl BuilderConfigure,
    ) -> Result<Builder, Box<dyn std::error::Error>> {
        cfg.configure(self)
    }

    /// next ordinal container ip
    pub fn next_container_ip(&self) -> Result<String, Error> {
        let net: Ipv4Network = self.network.parse().map_err(|error| {
            bollard::errors::Error::IOError {
                err: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid network format: {}", error),
                ),
            }
        })?;
        let ip = net.nth((self.containers.len() + 2) as u32);
        match ip {
            None => Err(bollard::errors::Error::IOError {
                err: std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "No available ip",
                ),
            }),
            Some(ip) => Ok(ip.to_string()),
        }
    }

    /// run all containers on build
    pub fn autorun(mut self, run: bool) -> Builder {
        self.autorun = run;
        self
    }

    /// set the network for this test
    pub fn network(mut self, network: &str) -> Builder {
        self.network = network.to_owned();
        self
    }

    /// the name to be used as labels and network name
    pub fn name(mut self, name: &str) -> Builder {
        self.name = name.to_owned();
        self
    }

    /// add a mayastor container with a name
    pub fn add_container(mut self, name: &str) -> Builder {
        self.containers.push(ContainerSpec::from_binary(
            name,
            Binary::from_dbg("mayastor"),
        ));
        self
    }

    /// add a generic container which runs a local binary
    pub fn add_container_spec(mut self, spec: ContainerSpec) -> Builder {
        self.containers.push(spec);
        self
    }

    /// add a generic container which runs a local binary
    pub fn add_container_bin(self, name: &str, bin: Binary) -> Builder {
        self.add_container_spec(ContainerSpec::from_binary(name, bin))
    }

    /// add a docker container which will be pulled if not present
    pub fn add_container_image(self, name: &str, image: Binary) -> Builder {
        self.add_container_spec(ContainerSpec::from_binary(name, image))
    }

    /// attempt to reuse and restart containers instead of starting new ones
    pub fn with_reuse(mut self, reuse: bool) -> Builder {
        self.reuse = reuse;
        self.prune = !reuse;
        self
    }

    /// clean on drop?
    pub fn with_clean(mut self, enable: bool) -> Builder {
        self.clean = enable;
        self
    }

    /// allow clean on panic if clean is set
    pub fn with_clean_on_panic(mut self, enable: bool) -> Builder {
        self.allow_clean_on_panic = enable;
        self
    }

    /// prune containers and networks on start
    pub fn with_prune(mut self, enable: bool) -> Builder {
        self.prune = enable;
        self
    }

    /// output logs on panic
    pub fn with_logs(mut self, enable: bool) -> Builder {
        self.logs_on_panic = enable;
        self
    }

    /// use base image for all binary containers
    pub fn with_base_image<S: Into<Option<String>>>(
        mut self,
        image: S,
    ) -> Builder {
        self.image = image.into();
        self
    }

    /// setup tracing for the cargo test code with `RUST_LOG` const
    pub fn with_default_tracing(self) -> Self {
        self.with_tracing(RUST_LOG_DEFAULT)
    }

    /// setup tracing for the cargo test code with `filter`
    /// ignore when called multiple times
    pub fn with_tracing(self, filter: &str) -> Self {
        let builder = if let Ok(filter) =
            tracing_subscriber::EnvFilter::try_from_default_env()
        {
            tracing_subscriber::fmt().with_env_filter(filter)
        } else {
            tracing_subscriber::fmt().with_env_filter(filter)
        };
        builder.try_init().ok();
        self
    }

    /// build the config and start the containers
    pub async fn build(
        self,
    ) -> Result<ComposeTest, Box<dyn std::error::Error>> {
        let autorun = self.autorun;
        let mut compose = self.build_only().await?;
        if autorun {
            compose.start_all().await?;
        }
        Ok(compose)
    }

    fn override_flags(flag: &mut bool, flag_name: &str) {
        let key = format!("COMPOSE_{}", flag_name.to_ascii_uppercase());
        if let Some(val) = std::env::var_os(&key) {
            let clean = match val.to_str().unwrap_or_default() {
                "true" => true,
                "false" => false,
                _ => return,
            };
            if clean != *flag {
                tracing::warn!(
                    "env::{} => Overriding the {} flag to {}",
                    key,
                    flag_name,
                    clean
                );
                *flag = clean;
            }
        }
    }
    /// override clean flags with environment variable
    /// useful for testing without having to change the code
    fn override_clean(&mut self) {
        Self::override_flags(&mut self.clean, "clean");
        Self::override_flags(
            &mut self.allow_clean_on_panic,
            "allow_clean_on_panic",
        );
        Self::override_flags(&mut self.logs_on_panic, "logs_on_panic");
    }

    /// build the config but don't start the containers
    async fn build_only(
        mut self,
    ) -> Result<ComposeTest, Box<dyn std::error::Error>> {
        self.override_clean();
        let net: Ipv4Network = self.network.parse()?;

        let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
        let srcdir = path.parent().unwrap().to_string_lossy().into();
        // FIXME: We can just use `connect_with_unix_defaults` once this is
        // resolved and released:
        //
        // https://github.com/fussybeaver/bollard/issues/166
        let docker = match std::env::var("DOCKER_HOST") {
            Ok(host) => {
                Docker::connect_with_unix(&host, 120, API_DEFAULT_VERSION)
            }
            Err(_) => Docker::connect_with_unix_defaults(),
        }?;

        let mut cfg = HashMap::new();
        cfg.insert(
            "Subnet".to_string(),
            format!("{}/{}", net.network(), net.prefix()),
        );
        cfg.insert("Gateway".into(), net.nth(1).unwrap().to_string());

        let ipam = Ipam {
            driver: Some("default".into()),
            config: Some(vec![cfg]),
            options: None,
        };

        let mut compose = ComposeTest {
            name: self.name.clone(),
            srcdir,
            docker,
            network_id: "".to_string(),
            containers: Default::default(),
            ipam,
            label_prefix: "io.mayastor.test".to_string(),
            reuse: self.reuse,
            allow_clean_on_panic: self.allow_clean_on_panic,
            clean: self.clean,
            prune: self.prune,
            image: self.image,
            logs_on_panic: self.logs_on_panic,
        };

        compose.network_id =
            compose.network_create().await.map_err(|e| e.to_string())?;

        if self.reuse {
            let containers =
                compose.list_network_containers(&self.name).await?;

            for container in containers {
                let networks = container
                    .network_settings
                    .unwrap_or_default()
                    .networks
                    .unwrap_or_default();
                if let Some(n) = container.names.unwrap_or_default().first() {
                    if let Some(endpoint) = networks.get(&self.name) {
                        if let Some(ip) = endpoint.ip_address.clone() {
                            compose.containers.insert(
                                n[1 ..].into(),
                                (container.id.unwrap_or_default(), ip.parse()?),
                            );
                        }
                    }
                }
            }
        } else {
            // containers are created where the IPs are ordinal
            for (i, spec) in self.containers.iter().enumerate() {
                compose
                    .create_container(
                        spec,
                        &net.nth((i + 2) as u32).unwrap().to_string(),
                    )
                    .await?;
            }
        }

        Ok(compose)
    }
}

///
/// Some types to avoid confusion when
///
/// different networks are referred to, internally as networkId in docker
type NetworkId = String;
/// container name
type ContainerName = String;
/// container ID
type ContainerId = String;

#[derive(Clone, Debug)]
pub struct ComposeTest {
    /// used as the network name
    name: String,
    /// the source dir the tests are run in
    srcdir: String,
    /// handle to the docker daemon
    docker: Docker,
    /// the network id is used to attach containers to networks
    network_id: NetworkId,
    /// the name of containers and their (IDs, Ipv4) we have created
    /// perhaps not an ideal data structure, but we can improve it later
    /// if we need to
    containers: HashMap<ContainerName, (ContainerId, Ipv4Addr)>,
    /// the default network configuration we use for our test cases
    ipam: Ipam,
    /// prefix for labels set on containers and networks
    ///   $prefix.name = $name will be created automatically
    label_prefix: String,
    /// reuse existing containers
    reuse: bool,
    /// allow cleaning up on a panic (if clean is set)
    allow_clean_on_panic: bool,
    /// automatically clean up the things we have created for this test
    clean: bool,
    /// remove existing containers upon creation
    prune: bool,
    /// base image for image-less containers
    image: Option<String>,
    /// output container logs on panic
    logs_on_panic: bool,
}

impl Drop for ComposeTest {
    /// destroy the containers and network. Notice that we use sync code here
    fn drop(&mut self) {
        if thread::panicking() && self.logs_on_panic {
            self.containers.keys().for_each(|name| {
                tracing::error!(
                    "========== Logs from container '{}' start:",
                    &name
                );
                let _ = std::process::Command::new("docker")
                    .args(&["logs", name])
                    .status();
                tracing::error!(
                    "========== Logs from container '{} end':",
                    name
                );
            });
        }

        if self.clean && (!thread::panicking() || self.allow_clean_on_panic) {
            self.containers.keys().for_each(|c| {
                std::process::Command::new("docker")
                    .args(&["kill", c])
                    .output()
                    .unwrap();
                std::process::Command::new("docker")
                    .args(&["rm", "-v", c])
                    .output()
                    .unwrap();
            });

            std::process::Command::new("docker")
                .args(&["network", "rm", &self.name])
                .output()
                .unwrap();
        }
    }
}

impl ComposeTest {
    /// Create a new network, with default settings. If a network with the same
    /// name already exists it will be reused. Note that we do not check the
    /// networking IP and/or subnets
    async fn network_create(&mut self) -> Result<NetworkId, Error> {
        let mut net = self.network_list_labeled().await?;
        if !net.is_empty() {
            let first = net.pop().unwrap();
            if Some(self.name.clone()) == first.name {
                // reuse the same network
                self.network_id = first.id.unwrap();
                if self.prune {
                    // but clean up the existing containers
                    self.remove_network_containers(&self.name).await?;
                }
                return Ok(self.network_id.clone());
            } else {
                self.network_remove_labeled().await?;
            }
        }

        let name_label = format!("{}.name", self.label_prefix);
        // we use the same network everywhere
        let create_opts = CreateNetworkOptions {
            name: self.name.as_str(),
            check_duplicate: true,
            driver: "bridge",
            internal: false,
            attachable: true,
            ingress: false,
            ipam: self.ipam.clone(),
            enable_ipv6: false,
            options: vec![("com.docker.network.bridge.name", "mayabridge0")]
                .into_iter()
                .collect(),
            labels: vec![(name_label.as_str(), self.name.as_str())]
                .into_iter()
                .collect(),
        };

        self.docker.create_network(create_opts).await.map(|r| {
            self.network_id = r.id.unwrap();
            self.network_id.clone()
        })
    }

    async fn network_remove_labeled(&self) -> Result<(), Error> {
        let our_networks = self.network_list_labeled().await?;
        for network in our_networks {
            let name = &network.name.unwrap();
            self.remove_network_containers(name).await?;
            self.network_remove(name).await?;
        }
        Ok(())
    }

    /// remove all containers from the network
    pub async fn remove_network_containers(
        &self,
        name: &str,
    ) -> Result<(), Error> {
        let containers = self.list_network_containers(name).await?;
        for k in &containers {
            let name = k.id.clone().unwrap();
            self.remove_container(&name).await?;
            while let Ok(_c) = self.docker.inspect_container(&name, None).await
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    async fn network_remove(&self, name: &str) -> Result<(), Error> {
        // if the network is not found, its not an error, any other error is
        // reported as such. Networks can only be destroyed when all containers
        // attached to it are removed. To get a list of attached
        // containers, use network_list()
        if let Err(e) = self.docker.remove_network(name).await {
            if !matches!(e, Error::DockerResponseNotFoundError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// list all the docker networks with our filter
    pub async fn network_list_labeled(&self) -> Result<Vec<Network>, Error> {
        self.docker
            .list_networks(Some(ListNetworksOptions {
                filters: vec![("label", vec!["io.mayastor.test.name"])]
                    .into_iter()
                    .collect(),
            }))
            .await
    }

    async fn list_network_containers(
        &self,
        name: &str,
    ) -> Result<Vec<ContainerSummaryInner>, Error> {
        self.docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![("network", vec![name])].into_iter().collect(),
                ..Default::default()
            }))
            .await
    }

    /// list containers
    pub async fn list_cluster_containers(
        &self,
    ) -> Result<Vec<ContainerSummaryInner>, Error> {
        self.docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![(
                    "label",
                    vec![format!("{}.name={}", self.label_prefix, self.name)
                        .as_str()],
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            }))
            .await
    }

    /// remove a container from the configuration
    async fn remove_container(&self, name: &str) -> Result<(), Error> {
        self.docker
            .remove_container(
                name,
                Some(RemoveContainerOptions {
                    v: true,
                    force: true,
                    link: false,
                }),
            )
            .await?;

        Ok(())
    }

    /// remove all containers and its network
    async fn remove_all(&self) -> Result<(), Error> {
        for k in &self.containers {
            self.stop(k.0).await?;
            self.remove_container(k.0).await?;
            while let Ok(_c) = self.docker.inspect_container(k.0, None).await {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        self.network_remove(&self.name).await?;
        Ok(())
    }

    /// we need to construct several objects to create a setup that meets our
    /// liking:
    ///
    /// (1) hostconfig: that configures the host side of the container, i.e what
    /// features/settings from the host perspective do we want too setup
    /// for the container. (2) endpoints: this allows us to plugin in the
    /// container into our network configuration (3) config: the actual
    /// config which includes the above objects
    async fn create_container(
        &mut self,
        spec: &ContainerSpec,
        ipv4: &str,
    ) -> Result<(), Error> {
        if self.prune {
            let _ = self
                .docker
                .stop_container(
                    &spec.name,
                    Some(StopContainerOptions {
                        t: 0,
                    }),
                )
                .await;
            let _ = self
                .docker
                .remove_container(
                    &spec.name,
                    Some(RemoveContainerOptions {
                        v: false,
                        force: false,
                        link: false,
                    }),
                )
                .await;
        }
        let mut binds = vec![
            format!("{}:{}", self.srcdir, self.srcdir),
            "/nix:/nix:ro".into(),
            "/dev/hugepages:/dev/hugepages:rw".into(),
        ];
        binds.extend(spec.binds());
        let host_config = HostConfig {
            binds: Some(binds),
            mounts: Some(vec![
                // DPDK needs to have a /tmp
                Mount {
                    target: Some("/tmp".into()),
                    typ: Some(MountTypeEnum::TMPFS),
                    ..Default::default()
                },
                // mayastor needs to have a /var/tmp
                Mount {
                    target: Some("/var/tmp".into()),
                    typ: Some(MountTypeEnum::TMPFS),
                    ..Default::default()
                },
            ]),
            cap_add: Some(vec![
                "SYS_ADMIN".to_string(),
                "IPC_LOCK".into(),
                "SYS_NICE".into(),
            ]),
            security_opt: Some(vec!["seccomp=unconfined".into()]),
            init: spec.init,
            port_bindings: spec.port_map.clone(),
            ..Default::default()
        };

        let mut endpoints_config = HashMap::new();
        endpoints_config.insert(
            self.name.as_str(),
            EndpointSettings {
                network_id: Some(self.network_id.to_string()),
                ipam_config: Some(EndpointIpamConfig {
                    ipv4_address: Some(ipv4.into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );

        let mut env = spec.environment();
        env.push(format!("MY_POD_IP={}", ipv4));

        // figure out which ports to expose based on the port mapping
        let mut exposed_ports = HashMap::new();
        if let Some(map) = spec.port_map.as_ref() {
            map.iter().for_each(|binding| {
                exposed_ports.insert(binding.0.as_str(), HashMap::new());
            })
        }

        let name = spec.name.as_str();
        let cmd = spec.commands();
        let cmd = cmd.iter().map(|s| s.as_str()).collect();
        let image = spec
            .image
            .as_ref()
            .map_or_else(|| self.image.as_deref(), |s| Some(s.as_str()));
        let name_label = format!("{}.name", self.label_prefix);
        let config = Config {
            cmd: Some(cmd),
            env: Some(env.iter().map(|s| s.as_str()).collect()),
            image,
            hostname: Some(name),
            host_config: Some(host_config),
            networking_config: Some(NetworkingConfig {
                endpoints_config,
            }),
            working_dir: Some(self.srcdir.as_str()),
            volumes: Some(
                vec![
                    ("/dev/hugepages", HashMap::new()),
                    ("/nix", HashMap::new()),
                    (self.srcdir.as_str(), HashMap::new()),
                ]
                .into_iter()
                .collect(),
            ),
            labels: Some(
                vec![(name_label.as_str(), self.name.as_str())]
                    .into_iter()
                    .collect(),
            ),
            exposed_ports: Some(exposed_ports),
            ..Default::default()
        };

        self.pull_missing_image(&spec.image).await;

        let container = self
            .docker
            .create_container(
                Some(CreateContainerOptions {
                    name,
                }),
                config,
            )
            .await
            .unwrap();

        self.containers
            .insert(name.to_string(), (container.id, ipv4.parse().unwrap()));

        Ok(())
    }

    /// Pulls the docker image, if one is specified and is not present locally
    async fn pull_missing_image(&self, image: &Option<String>) {
        if let Some(image) = image {
            let image = if !image.contains(':') {
                format!("{}:latest", image)
            } else {
                image.clone()
            };
            if !self.image_exists(&image).await {
                self.pull_image(&image).await;
            }
        }
    }

    /// Check if image exists locally
    async fn image_exists(&self, image: &str) -> bool {
        let images = self.docker.list_images::<String>(None).await.unwrap();
        images
            .iter()
            .any(|i| i.repo_tags.iter().any(|t| t == image))
    }

    /// Pulls the docker image
    async fn pull_image(&self, image: &str) {
        let mut stream = self
            .docker
            .create_image(
                Some(CreateImageOptions {
                    from_image: image,
                    ..Default::default()
                }),
                None,
                None,
            )
            .into_future()
            .await;

        while let Some(result) = stream.0.as_ref() {
            let info = result.as_ref().unwrap();
            tracing::trace!("{:?}", &info);
            stream = stream.1.into_future().await;
        }
    }

    /// start the container
    pub async fn start(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).ok_or(
            bollard::errors::Error::IOError {
                err: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "Can't start container {} as it was not configured",
                        name
                    ),
                ),
            },
        )?;
        if !self.reuse {
            self.docker
                .start_container::<&str>(id.0.as_str(), None)
                .await?;
        }

        Ok(())
    }

    /// stop the container
    pub async fn stop(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.stop_id(id.0.as_str()).await
    }

    /// stop the container by its id
    pub async fn stop_id(&self, id: &str) -> Result<(), Error> {
        if let Err(e) = self
            .docker
            .stop_container(
                id,
                Some(StopContainerOptions {
                    t: 3,
                }),
            )
            .await
        {
            // where already stopped
            if !matches!(e, Error::DockerResponseNotModifiedError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// restart the container
    pub async fn restart(&self, name: &str) -> Result<(), Error> {
        let (id, _) = self.containers.get(name).unwrap();
        self.restart_id(id.as_str()).await
    }

    /// restart the container id
    pub async fn restart_id(&self, id: &str) -> Result<(), Error> {
        if let Err(e) = self
            .docker
            .restart_container(
                id,
                Some(RestartContainerOptions {
                    t: 3,
                }),
            )
            .await
        {
            // where already stopped
            if !matches!(e, Error::DockerResponseNotModifiedError { .. }) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// disconnect container from the network
    pub async fn disconnect(&self, container_name: &str) -> Result<(), Error> {
        let id = self.containers.get(container_name).unwrap();
        self.docker
            .disconnect_network(
                &self.network_id,
                DisconnectNetworkOptions {
                    container: id.0.as_str(),
                    force: false,
                },
            )
            .await
    }

    /// get the logs from the container. It would be nice to make it implicit
    /// that is, when you make a rpc call, whatever logs where created due to
    /// that are returned
    pub async fn logs(&self, name: &str) -> Result<(), Error> {
        let logs = self
            .docker
            .logs(
                name,
                Some(LogsOptions {
                    follow: false,
                    stdout: true,
                    stderr: true,
                    since: 0, // TODO log lines since last call?
                    until: 0,
                    timestamps: false,
                    tail: "all",
                }),
            )
            .try_collect::<Vec<_>>()
            .await?;

        logs.iter().for_each(|l| print!("{}:{}", name, l));
        Ok(())
    }

    /// get the logs from all of the containers. It would be nice to make it
    /// implicit that is, when you make a rpc call, whatever logs where
    /// created due to that are returned
    pub async fn logs_all(&self) -> Result<(), Error> {
        for container in &self.containers {
            let _ = self.logs(container.0).await;
        }
        Ok(())
    }

    /// start all the containers
    async fn start_all(&mut self) -> Result<(), Error> {
        for k in &self.containers {
            self.start(k.0).await?;
        }

        Ok(())
    }

    /// start the containers
    pub async fn start_containers(
        &self,
        containers: Vec<&str>,
    ) -> Result<(), Error> {
        for k in containers {
            self.start(k).await?;
        }
        Ok(())
    }

    /// stop all the containers part of the network
    /// returns the last error, if any or Ok
    pub async fn stop_network_containers(&self) -> Result<(), Error> {
        let mut result = Ok(());
        let containers = self.list_network_containers(&self.name).await?;
        for container in containers {
            if let Some(id) = container.id {
                if let Err(e) = self.stop_id(&id).await {
                    println!("Failed to stop container id {:?}", id);
                    result = Err(e);
                }
            }
        }
        result
    }

    /// restart all the containers part of the network
    /// returns the last error, if any or Ok
    pub async fn restart_network_containers(&self) -> Result<(), Error> {
        let mut result = Ok(());
        let containers = self.list_network_containers(&self.name).await?;
        for container in containers {
            if let Some(id) = container.id {
                if let Err(e) = self.restart_id(&id).await {
                    println!("Failed to restart container id {:?}", id);
                    result = Err(e);
                }
            }
        }
        result
    }

    /// inspect the given container
    pub async fn inspect(
        &self,
        name: &str,
    ) -> Result<ContainerInspectResponse, Error> {
        self.docker.inspect_container(name, None).await
    }

    /// pause the container; unfortunately, when the API returns it does not
    /// mean that the container indeed is frozen completely, in the sense
    /// that it's not to be assumed that right after a call -- the container
    /// stops responding.
    pub async fn pause(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.docker.pause_container(id.0.as_str()).await?;

        Ok(())
    }

    /// un_pause the container
    pub async fn thaw(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.docker.unpause_container(id.0.as_str()).await
    }

    /// return grpc handles to the containers
    pub async fn grpc_handles(&self) -> Result<Vec<RpcHandle>, String> {
        let mut handles = Vec::new();
        for v in &self.containers {
            handles.push(
                RpcHandle::connect(
                    v.0.clone(),
                    format!("{}:10124", v.1 .1).parse::<SocketAddr>().unwrap(),
                )
                .await?,
            );
        }

        Ok(handles)
    }

    /// return grpc handle to the container
    pub async fn grpc_handle(&self, name: &str) -> Result<RpcHandle, String> {
        match self.containers.iter().find(|&c| c.0 == name) {
            Some(container) => Ok(RpcHandle::connect(
                container.0.clone(),
                format!("{}:10124", container.1 .1)
                    .parse::<SocketAddr>()
                    .unwrap(),
            )
            .await?),
            None => Err(format!("Container {} not found!", name)),
        }
    }

    /// explicitly remove all containers
    pub async fn down(&self) {
        self.remove_all().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rpc::mayastor::Null;

    #[tokio::test]
    async fn compose() {
        let test = Builder::new()
            .name("composer")
            .network("10.1.0.0/16")
            .add_container("mayastor")
            .add_container_bin("mayastor2", Binary::from_dbg("mayastor"))
            .with_clean(true)
            .build()
            .await
            .unwrap();

        let mut hdl = test.grpc_handle("mayastor").await.unwrap();
        hdl.mayastor.list_nexus(Null {}).await.expect("list nexus");

        // run with --nocapture to get the logs
        test.logs_all().await.unwrap();
    }
}
