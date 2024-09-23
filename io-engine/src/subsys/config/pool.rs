use futures::channel::oneshot;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, fs, path::Path, sync::Mutex};
use tonic::Status;

use crate::{
    core::{runtime, Cores, Reactor, Share, VerboseError},
    grpc::rpc_submit,
    lvs::{Lvs, LvsBdev, LvsError},
    pool_backend::{PoolArgs, PoolBackend},
};

static CONFIG_FILE: OnceCell<String> = OnceCell::new();

/// Initialise the config file location
fn init_config_file<P>(file: P)
where
    P: AsRef<Path> + Display + ToString,
{
    CONFIG_FILE.get_or_init(|| file.to_string());
}

/// Return the config file location
fn get_config_file() -> Option<&'static String> {
    CONFIG_FILE.get()
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolConfig {
    pools: Option<Vec<Pool>>,
}

impl PoolConfig {
    /// Load pool configuration from a file
    pub fn load<P>(file: P) -> Result<PoolConfig, serde_yaml::Error>
    where
        P: AsRef<Path> + Display + ToString,
    {
        init_config_file(&file);

        let bytes = fs::read(&file).unwrap_or_default();

        if bytes.is_empty() {
            return Ok(PoolConfig::default());
        }

        serde_yaml::from_slice(&bytes)
    }

    /// Write this pool configuration to a file
    fn write<P>(&self, file: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        let config = serde_yaml::to_string(&self).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("serialization error: {error}"),
            )
        })?;

        fs::write(&file, config.as_bytes())
    }

    /// Export current pool configuration
    pub async fn export(self) {
        static MUTEX: Lazy<Mutex<u32>> = Lazy::new(|| Mutex::new(0));

        if let Some(file) = get_config_file() {
            debug!("saving pool configuration");

            let (sender, receiver) = oneshot::channel::<()>();

            runtime::spawn(async move {
                let future = runtime::spawn_blocking(move || {
                    let _guard = MUTEX.lock().unwrap();
                    if let Err(error) = self.write(file) {
                        error!("error saving pool configuration: {}", error);
                    } else {
                        info!("pool configuration saved to {}", file);
                    }
                });

                if let Err(error) = future.await {
                    error!("error joining thread: {}", error);
                }

                let future = Reactor::spawn_at_primary(async move {
                    if sender.send(()).is_err() {
                        error!("error sending completion");
                    }
                });

                if let Err(error) = future.unwrap().await {
                    error!("cancelled completion: {}", error);
                }
            });

            if let Err(error) = receiver.await {
                error!("cancelled completion: {}", error);
            }
        }
    }

    /// Remove named pool from this pool configuration
    pub fn delete(&mut self, name: &str) {
        if let Some(pools) = self.pools.as_mut() {
            pools.retain(|pool| pool.name != name);
        }
    }

    /// Capture current pool configuration
    pub fn capture() -> PoolConfig {
        let pools = LvsBdev::iter().map(Pool::from).collect();
        PoolConfig {
            pools: Some(pools),
        }
    }

    /// Create pools specified in this configuration
    async fn create_pools(&self) -> usize {
        let mut failures = 0;
        if let Some(pools) = self.pools.as_ref() {
            for pool in pools.iter() {
                info!("creating pool {}", pool.name);
                if let Err(error) = create_pool(pool.into()).await {
                    error!(
                        "failed to create pool {}: {}",
                        pool.name,
                        error.verbose()
                    );
                    failures += 1;
                }
            }
        }
        failures
    }

    /// Import pools
    pub fn import_pools(self) {
        assert_eq!(Cores::current(), Cores::first());
        Reactor::block_on(async move {
            let errors = self.create_pools().await;
            if errors != 0 {
                warn!(
                    "Not all pools were imported successfully ({} errors)",
                    errors
                );
            }
        });
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
/// Pools that we create. Future work will include the ability to create RAID0
/// or RAID5.
struct Pool {
    /// name of the pool to be created or imported
    name: String,
    /// bdevs to create outside of the nexus control
    disks: Vec<String>,
    /// list of replicas (not required, informational only)
    #[serde(skip_serializing)]
    replicas: Option<Vec<Replica>>,
    backend: PoolBackend,
}

/// Convert a Pool into a gRPC request payload
impl From<&Pool> for PoolArgs {
    fn from(pool: &Pool) -> Self {
        Self {
            name: pool.name.clone(),
            disks: pool.disks.clone(),
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: pool.backend,
        }
    }
}

/// Convert an LvsBdev into a Pool
impl From<LvsBdev> for Pool {
    fn from(lvs_bdev: LvsBdev) -> Self {
        let base = lvs_bdev.base_bdev();
        Self {
            name: lvs_bdev.name(),
            disks: vec![base
                .bdev_uri_str()
                .unwrap_or_else(|| base.name().to_string())],
            replicas: None,
            backend: PoolBackend::Lvs,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
/// Types of remote access storage protocols and IDs for sharing replicas.
pub enum ShareType {
    Nvmf,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
/// Pool replicas that we share via `ShareType`
struct Replica {
    /// name of the replica
    name: String,
    /// share type if shared
    #[serde(skip_serializing_if = "Option::is_none")]
    share: Option<ShareType>,
}

async fn create_pool(
    args: PoolArgs,
) -> Result<io_engine_api::v0::Pool, Status> {
    if args.disks.is_empty() {
        return Err(Status::invalid_argument("Missing devices"));
    }

    let rx = rpc_submit::<_, _, LvsError>(async move {
        let pool = Lvs::create_or_import(args).await?;
        Ok(pool.into())
    })?;

    rx.await
        .map_err(|_| Status::cancelled("cancelled"))?
        .map_err(Status::from)
}
