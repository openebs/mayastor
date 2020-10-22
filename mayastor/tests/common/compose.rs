use std::{
    collections::HashMap,
    future::Future,
    net::{Ipv4Addr, SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use crossbeam::crossbeam_channel::bounded;

use bollard::{
    container::{
        Config,
        CreateContainerOptions,
        ListContainersOptions,
        LogsOptions,
        NetworkingConfig,
        RemoveContainerOptions,
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
    },
    Docker,
};
use futures::TryStreamExt;
use ipnetwork::Ipv4Network;
use tokio::sync::oneshot::channel;
use tonic::transport::Channel;

use crate::common::mayastor_test_init;
use ::rpc::mayastor::{
    bdev_rpc_client::BdevRpcClient,
    mayastor_client::MayastorClient,
};
use bollard::models::ContainerInspectResponse;
use mayastor::core::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    Reactor,
    Reactors,
};

#[derive(Clone)]
pub struct RpcHandle {
    pub name: String,
    pub endpoint: SocketAddr,
    pub mayastor: MayastorClient<Channel>,
    pub bdev: BdevRpcClient<Channel>,
}

impl RpcHandle {
    /// connect to the containers and construct a handle
    async fn connect(name: String, endpoint: SocketAddr) -> Self {
        loop {
            if TcpStream::connect_timeout(&endpoint, Duration::from_millis(100))
                .is_ok()
            {
                break;
            } else {
                thread::sleep(Duration::from_millis(101));
            }
        }

        let mayastor =
            MayastorClient::connect(format!("http://{}", endpoint.to_string()))
                .await
                .unwrap();
        let bdev =
            BdevRpcClient::connect(format!("http://{}", endpoint.to_string()))
                .await
                .unwrap();

        Self {
            name,
            mayastor,
            bdev,
            endpoint,
        }
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
    containers: Vec<ContainerName>,
    /// the network for the tests used
    network: String,
    /// delete the container and network when dropped
    clean: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Builder::new()
    }
}

impl Builder {
    /// construct a new builder for `[ComposeTest']
    pub fn new() -> Self {
        Self {
            name: "".to_string(),
            containers: Default::default(),
            network: "10.1.0.0".to_string(),
            clean: false,
        }
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
        self.containers.push(name.to_owned());
        self
    }

    /// clean on drop?
    pub fn with_clean(mut self, enable: bool) -> Builder {
        self.clean = enable;
        self
    }

    /// build the config and start the containers
    pub async fn build(
        self,
    ) -> Result<ComposeTest, Box<dyn std::error::Error>> {
        let net: Ipv4Network = self.network.parse()?;

        let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
        let srcdir = path.parent().unwrap().to_string_lossy().into();
        let binary = format!("{}/target/debug/mayastor", srcdir);

        let docker = Docker::connect_with_unix_defaults()?;

        let mut cfg = HashMap::new();
        cfg.insert(
            "Subnet".to_string(),
            format!("{}/{}", net.network().to_string(), net.prefix()),
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
            binary,
            docker,
            network_id: "".to_string(),
            containers: Default::default(),
            ipam,
            label: format!("io.mayastor.test.{}", self.name),
            clean: self.clean,
        };

        compose.network_id =
            compose.network_create().await.map_err(|e| e.to_string())?;

        // containers are created where the IPs are ordinal
        for (i, name) in self.containers.iter().enumerate() {
            compose
                .create_container(
                    name,
                    &net.nth((i + 2) as u32).unwrap().to_string(),
                )
                .await?;
        }

        compose.start_all().await?;
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

#[derive(Clone)]
pub struct ComposeTest {
    /// used as the network name
    name: String,
    /// the source dir the tests are run in
    srcdir: String,
    /// the binary we are using relative to srcdir
    binary: String,
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
    /// set on containers and networks
    label: String,
    /// automatically clean up the things we have created for this test
    clean: bool,
}

impl Drop for ComposeTest {
    /// destroy the containers and network. Notice that we use sync code here
    fn drop(&mut self) {
        if self.clean {
            self.containers.keys().for_each(|c| {
                std::process::Command::new("docker")
                    .args(&["stop", c])
                    .output()
                    .unwrap();
                std::process::Command::new("docker")
                    .args(&["rm", c])
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
        let mut net = self.network_list().await?;

        if !net.is_empty() {
            let first = net.pop().unwrap();
            self.network_id = first.id.unwrap();
            return Ok(self.network_id.clone());
        }

        let create_opts = CreateNetworkOptions {
            name: self.name.as_str(),
            check_duplicate: true,
            driver: "bridge",
            internal: true,
            attachable: true,
            ingress: false,
            ipam: self.ipam.clone(),
            enable_ipv6: false,
            options: vec![("com.docker.network.bridge.name", "mayabridge0")]
                .into_iter()
                .collect(),
            labels: vec![(self.label.as_str(), "true")].into_iter().collect(),
        };

        self.docker.create_network(create_opts).await.map(|r| {
            self.network_id = r.id.unwrap();
            self.network_id.clone()
        })
    }

    async fn network_remove(&mut self) -> Result<(), Error> {
        // if the network is not found, its not an error, any other error is
        // reported as such. Networks can only be destroyed when all containers
        // attached to it are removed. To get a list of attached
        // containers, use network_list()
        if let Err(e) = self.docker.remove_network(&self.name).await {
            if !matches!(e, Error::DockerResponseNotFoundError{..}) {
                return Err(e);
            }
        }

        Ok(())
    }

    /// list all the docker networks
    pub async fn network_list(&self) -> Result<Vec<Network>, Error> {
        self.docker
            .list_networks(Some(ListNetworksOptions {
                filters: vec![("name", vec![self.name.as_str()])]
                    .into_iter()
                    .collect(),
            }))
            .await
    }

    /// list containers
    pub async fn list_containers(
        &self,
    ) -> Result<Vec<ContainerSummaryInner>, Error> {
        self.docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: vec![(
                    "label",
                    vec![format!("{}=true", self.label).as_str()],
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

    /// remove all containers
    pub async fn remove_all(&mut self) -> Result<(), Error> {
        for k in &self.containers {
            self.stop(&k.0).await?;
            self.remove_container(&k.0).await?;
        }
        self.network_remove().await?;
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
        name: &str,
        ipv4: &str,
    ) -> Result<(), Error> {
        let host_config = HostConfig {
            binds: Some(vec![
                format!("{}:{}", self.srcdir, self.srcdir),
                "/nix:/nix:ro".into(),
                "/dev/hugepages:/dev/hugepages:rw".into(),
            ]),
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
            security_opt: Some(vec!["seccomp:unconfined".into()]),
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

        let env = format!("MY_POD_IP={}", ipv4);

        let config = Config {
            cmd: Some(vec![self.binary.as_str(), "-g", "0.0.0.0"]),
            env: Some(vec![&env]),
            image: None, // notice we do not have a base image here
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
                vec![(self.label.as_str(), "true")].into_iter().collect(),
            ),
            ..Default::default()
        };

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

    /// start the container
    async fn start(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        self.docker
            .start_container::<&str>(id.0.as_str(), None)
            .await?;

        Ok(())
    }

    /// stop the container
    async fn stop(&self, name: &str) -> Result<(), Error> {
        let id = self.containers.get(name).unwrap();
        if let Err(e) = self
            .docker
            .stop_container(
                id.0.as_str(),
                Some(StopContainerOptions {
                    t: 5,
                }),
            )
            .await
        {
            // where already stopped
            if !matches!(e, Error::DockerResponseNotModifiedError{..}) {
                return Err(e);
            }
        }

        Ok(())
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

    /// start all the containers
    async fn start_all(&mut self) -> Result<(), Error> {
        for k in &self.containers {
            self.start(&k.0).await?;
        }

        Ok(())
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
    pub async fn grpc_handles(&self) -> Result<Vec<RpcHandle>, ()> {
        let mut handles = Vec::new();
        for v in &self.containers {
            handles.push(
                RpcHandle::connect(
                    v.0.clone(),
                    format!("{}:10124", v.1.1).parse::<SocketAddr>().unwrap(),
                )
                .await,
            );
        }

        Ok(handles)
    }
}

/// Mayastor test structure that simplifies sending futures. Mayastor has
/// its own reactor, which is not tokio based, so we need to handle properly
pub struct MayastorTest<'a> {
    reactor: &'a Reactor,
    thdl: Option<std::thread::JoinHandle<()>>,
}

impl<'a> MayastorTest<'a> {
    /// spawn a future on this mayastor instance collecting the output
    /// notice that rust will not allow sending Bdevs as they are !Send
    pub async fn spawn<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = channel::<T>();
        self.reactor.send_future(async move {
            let arg = future.await;
            let _ = tx.send(arg);
        });

        rx.await.unwrap()
    }

    pub fn send<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.reactor.send_future(future);
    }

    /// starts mayastor, notice we start mayastor on a thread and return a
    /// handle to the management core. This handle is used to spawn futures,
    /// and the thread handle can be used to synchronize the shutdown
    async fn _new(args: MayastorCliArgs) -> MayastorTest<'static> {
        let (tx, rx) = channel::<&'static Reactor>();

        let thdl = std::thread::spawn(|| {
            MayastorEnvironment::new(args).init();
            tx.send(Reactors::master()).unwrap();
            Reactors::master().running();
            Reactors::master().poll_reactor();
        });

        let reactor = rx.await.unwrap();
        MayastorTest {
            reactor,
            thdl: Some(thdl),
        }
    }

    pub fn new(args: MayastorCliArgs) -> MayastorTest<'static> {
        let (tx, rx) = bounded(1);
        mayastor_test_init();
        let thdl = std::thread::Builder::new()
            .name("mayastor_master".into())
            .spawn(move || {
                MayastorEnvironment::new(args).init();
                tx.send(Reactors::master()).unwrap();
                Reactors::master().running();
                Reactors::master().poll_reactor();
            })
            .unwrap();

        let reactor = rx.recv().unwrap();
        MayastorTest {
            reactor,
            thdl: Some(thdl),
        }
    }

    /// explicitly stop mayastor
    pub async fn stop(mut self) {
        self.spawn(async { mayastor_env_stop(0) }).await;
        let hdl = self.thdl.take().unwrap();
        hdl.join().unwrap()
    }
}

impl<'a> Drop for MayastorTest<'a> {
    fn drop(&mut self) {
        self.reactor.send_future(async { mayastor_env_stop(0) });
        // wait for mayastor to stop
        let hdl = self.thdl.take().unwrap();
        hdl.join().unwrap()
    }
}
