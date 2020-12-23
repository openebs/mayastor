//! Different subsystems use a different method to set/get options. So per
//! subsystem, you will see small but subtle differences. The config setting
//! should be applied *before* any call is made to mayastor.
//!
//! Partial config options are supported i.e you do not have to fully
//! spell out the YAML spec for a given sub component. Serde will fill
//! in the default when missing, which are defined within the individual
//! options.
use std::{
    convert::TryFrom,
    fmt::Display,
    fs,
    fs::File,
    io::Write,
    path::Path,
};

use byte_unit::Byte;
use futures::FutureExt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use spdk_sys::{
    spdk_json_write_ctx,
    spdk_json_write_val_raw,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};

use crate::{
    bdev::{
        nexus::{
            instances,
            nexus_child::{ChildState, NexusChild, Reason},
            nexus_child_status_config::ChildStatusConfig,
        },
        nexus_create,
        VerboseError,
    },
    core::{Bdev, Cores, Reactor, Share},
    jsonrpc::{jsonrpc_register, Code, RpcErrorCode},
    lvs::Lvs,
    nexus_uri::bdev_create,
    pool::PoolsIter,
    replica::{ReplicaIter, ShareType},
    subsys::{
        config::opts::{
            BdevOpts,
            ErrStoreOpts,
            GetOpts,
            IscsiTgtOpts,
            NexusOpts,
            NvmeBdevOpts,
            NvmfTgtConfig,
            PosixSocketOpts,
        },
        NvmfSubsystem,
    },
};

#[derive(Debug, Clone, Snafu)]
pub enum Error {}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        Code::InternalError
    }
}
pub(crate) mod opts;

pub static CONFIG: OnceCell<Config> = OnceCell::new();

pub struct ConfigSubsystem(pub *mut spdk_subsystem);

impl Default for ConfigSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigSubsystem {
    extern "C" fn init() {
        debug!("mayastor subsystem init");

        // write the config out to disk where the target is the same as source
        // if no config file is given, simply return Ok().
        jsonrpc_register::<(), _, _, Error>("mayastor_config_export", |_| {
            let f = async move {
                let cfg = Config::get().refresh();
                if let Some(target) = cfg.source.as_ref() {
                    if let Err(e) = cfg.write(&target) {
                        error!("error writing config file {} {}", target, e);
                    }
                } else {
                    warn!("request to save config file but no source file was given, guess \
                    you have to scribble it down yourself {}", '\u{1f609}');
                }
                Ok(())
            };

            f.boxed_local()
        });

        unsafe { spdk_subsystem_init_next(0) };
    }

    extern "C" fn fini() {
        debug!("mayastor subsystem fini");
        unsafe { spdk_subsystem_fini_next() };
    }

    extern "C" fn config(w: *mut spdk_json_write_ctx) {
        let data = match serde_json::to_string(Config::get()) {
            Ok(it) => it,
            _ => return,
        };

        unsafe {
            spdk_json_write_val_raw(
                w,
                data.as_ptr() as *const _,
                data.as_bytes().len() as u64,
            );
        }
    }

    pub fn new() -> Self {
        static MAYASTOR_SUBSYS: &str = "MayastorConfig";
        debug!("creating Mayastor subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = std::ffi::CString::new(MAYASTOR_SUBSYS).unwrap().into_raw();
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = Some(Self::config);

        Self(Box::into_raw(ss))
    }
}

/// Main config structure of Mayastor. This structure can be persisted to disk.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// location of the config file that we loaded
    pub source: Option<String>,
    /// these options are not set/copied but are applied
    /// on target creation.
    pub nvmf_tcp_tgt_conf: NvmfTgtConfig,
    /// generic iSCSI options
    pub iscsi_tgt_conf: IscsiTgtOpts,
    /// options specific to NVMe bdev types
    pub nvme_bdev_opts: NvmeBdevOpts,
    /// generic bdev options
    pub bdev_opts: BdevOpts,
    /// nexus specific options
    pub nexus_opts: NexusOpts,
    /// error store opts
    pub err_store_opts: ErrStoreOpts,
    /// list of pools to create on load
    pub pools: Option<Vec<Pool>>,
    ///
    /// The next options are intended for usage during testing
    ///
    /// list of bdevs to be created on load
    pub base_bdevs: Option<Vec<BaseBdev>>,
    /// list of nexus bdevs that will create the base bdevs implicitly
    pub nexus_bdevs: Option<Vec<NexusBdev>>,
    /// any base bdevs created implicitly are shared over nvmf
    pub implicit_share_base: bool,
    /// flag to enable or disable config sync
    pub sync_disable: bool,
    pub socket_opts: PosixSocketOpts,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            source: None,
            nvmf_tcp_tgt_conf: Default::default(),
            iscsi_tgt_conf: Default::default(),
            nvme_bdev_opts: Default::default(),
            bdev_opts: Default::default(),
            nexus_opts: Default::default(),
            err_store_opts: Default::default(),
            base_bdevs: None,
            nexus_bdevs: None,
            pools: None,
            implicit_share_base: false,
            sync_disable: false,
            socket_opts: Default::default(),
        }
    }
}

impl Config {
    /// initialize the config by executing F and return a reference to the
    /// initialized data
    pub fn get_or_init<F>(f: F) -> &'static Self
    where
        F: FnOnce() -> Config,
    {
        CONFIG.get_or_init(f)
    }

    /// mostly similar as above, but we do not need to pass a closure
    pub fn get() -> &'static Self {
        CONFIG.get().unwrap()
    }

    /// read the config file from disk. If the config file is empty, return the
    /// default config, but store the empty config file with in the struct to be
    /// used during saving to disk.
    pub fn read<P>(file: P) -> Result<Config, serde_yaml::Error>
    where
        P: AsRef<Path> + Display + ToString,
    {
        debug!("loading configuration file from {}", file);
        let cfg = fs::read(&file).unwrap_or_default();
        let mut config;
        // only parse the file when its not empty, otherwise
        // just store the filepath to write it out later
        if !cfg.is_empty() {
            match serde_yaml::from_slice(&cfg) {
                Ok(v) => config = v,
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            };
        } else {
            info!("Config file {} is empty, reverting to default config", file);
            // the file is empty
            config = Config::default();
        }

        if !config.sync_disable {
            // use the source luke!
            config.source = Some(file.to_string());
        }
        Ok(config)
    }

    /// collect current configuration snapshot into a new Config object that can
    /// be exported to a file (YAML or JSON)
    pub fn refresh(&self) -> Self {
        // the config is immutable, so we construct a new one which is mutable
        // such that we can scribble in the current bdevs. The config
        // gets loaded with the current settings, as we know that these
        // are immutable, we can copy them with any locks held
        let mut current = Config {
            source: self.source.clone(),
            nvmf_tcp_tgt_conf: self.nvmf_tcp_tgt_conf.get(),
            iscsi_tgt_conf: self.iscsi_tgt_conf.get(),
            nvme_bdev_opts: self.nvme_bdev_opts.get(),
            bdev_opts: self.bdev_opts.get(),
            nexus_opts: self.nexus_opts.get(),
            base_bdevs: None,
            nexus_bdevs: None,
            pools: None,
            implicit_share_base: self.implicit_share_base,
            err_store_opts: self.err_store_opts.get(),
            sync_disable: self.sync_disable,
            socket_opts: self.socket_opts.get(),
        };

        // collect nexus bdevs and insert them into the config
        let nexus_bdevs = instances()
            .iter()
            .map(|nexus| NexusBdev {
                name: nexus.name.clone(),
                uuid: nexus.bdev.uuid_as_string(),
                size: nexus.bdev.size_in_bytes().to_string(),
                children: nexus
                    .children
                    .iter()
                    .map(|child| child.name.clone())
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();

        // collect base bdevs and insert them into the config
        if let Some(bdevs) = Bdev::bdev_first() {
            let result = bdevs
                .into_iter()
                .filter(|b| url::Url::try_from(b.clone()).is_ok())
                .map(|b| BaseBdev {
                    uri: url::Url::try_from(b.clone())
                        .map_or(b.name(), |u| u.to_string()),
                })
                .collect::<Vec<_>>();

            current.base_bdevs = Some(result);
        }

        current.nexus_bdevs = Some(nexus_bdevs);

        // collect any pools that are on the system, and insert them
        let pools = PoolsIter::new()
            .map(|p| {
                let base = p.get_base_bdev();
                Pool {
                    name: p.get_name().into(),
                    disks: vec![base.bdev_uri().unwrap_or_else(|| base.name())],
                    replicas: ReplicaIter::new()
                        .map(|p| Replica {
                            name: p.get_uuid().to_string(),
                            share: p.get_share_type(),
                        })
                        .collect::<Vec<_>>(),
                }
            })
            .collect::<Vec<_>>();

        current.pools = Some(pools);

        current
    }

    /// write the current pool configuration to disk
    pub fn write_pools<P>(&self, file: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        let pools = serde_json::json!({
            "pools": self.pools.clone()
        });

        if let Ok(s) = serde_yaml::to_string(&pools) {
            let mut file = File::create(file)?;
            return file.write_all(s.as_bytes());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to serialize the pool config",
        ))
    }

    /// write the current configuration to disk
    pub fn write<P>(&self, file: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        if let Ok(s) = serde_yaml::to_string(&self) {
            let mut file = File::create(file)?;
            return file.write_all(s.as_bytes());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to serialize config",
        ))
    }

    /// apply the hybrid configuration that is loaded from YAML. Hybrid in the
    /// sense that options not defined, will default to values defined by the
    /// default trait for that structure.
    pub fn apply(&self) {
        info!("Applying Mayastor configuration settings");
        assert_eq!(self.socket_opts.set(), true);
        // note: nvmf target does not have a set method
        assert_eq!(self.nvme_bdev_opts.set(), true);
        self.bdev_opts.set();
        self.iscsi_tgt_conf.set();
    }

    /// create any nexus bdevs any failure will be logged, but we will silently
    /// continue and try to create as many as possible. Returns the number of
    /// bdevs for which the creation failed.
    async fn create_nexus_bdevs(&self) -> usize {
        let mut failures = 0;
        info!("creating nexus devices");

        // we can't use iterators here as they are not async
        if let Some(bdevs) = self.nexus_bdevs.as_ref() {
            for nexus in bdevs {
                info!("creating nexus {}", nexus.name);
                match Byte::from_str(&nexus.size) {
                    Ok(val) => {
                        if let Err(e) = nexus_create(
                            &nexus.name,
                            val.get_bytes() as u64,
                            Some(&nexus.uuid),
                            &nexus.children,
                        )
                        .await
                        {
                            error!(
                                "Failed to create nexus {}, error={}",
                                nexus.name,
                                e.verbose()
                            );
                            failures += 1;
                        }
                    }
                    Err(_e) => {
                        failures += 1;
                        error!(
                            "Invalid size {} for {}",
                            &nexus.size, nexus.name
                        );
                    }
                }
            }
        }

        // Apply the saved child status and start rebuilding any degraded
        // children
        ChildStatusConfig::apply().await;
        self.start_rebuilds().await;

        failures
    }

    /// start rebuilding any child that is in the degraded state
    async fn start_rebuilds(&self) {
        if let Some(nexuses) = self.nexus_bdevs.as_ref() {
            for nexus in nexuses {
                if let Some(nexus_instance) =
                    instances().iter().find(|n| n.name == nexus.name)
                {
                    let degraded_children: Vec<&NexusChild> = nexus_instance
                        .children
                        .iter()
                        .filter(|child| {
                            child.state()
                                == ChildState::Faulted(Reason::OutOfSync)
                        })
                        .collect::<Vec<_>>();

                    // Get a mutable reference to the nexus instance. We can't
                    // do this when we first get the nexus instance (above)
                    // because it would cause multiple mutable borrows.
                    // We use "expect" here because we have already checked that
                    // the nexus exists above so we don't expect this to fail.
                    let nexus_instance = instances()
                        .iter_mut()
                        .find(|n| n.name == nexus.name)
                        .expect("Failed to find nexus");

                    for child in degraded_children {
                        dbg!("Start rebuilding child {}", &child.name);
                        if nexus_instance
                            .start_rebuild(&child.name)
                            .await
                            .is_err()
                        {
                            error!("Failed to start rebuild for {}", child);
                        }
                    }
                }
            }
        }
    }

    /// create base bdevs and export these over nvmf if configured
    async fn create_base_bdevs(&self) -> usize {
        let mut failures: usize = 0;
        if let Some(bdevs) = self.base_bdevs.as_ref() {
            for bdev in bdevs {
                info!("creating bdev {}", bdev.uri);
                if let Err(e) = bdev_create(&bdev.uri).await {
                    warn!(
                        "failed to create bdev {} during config load, error={}",
                        bdev.uri,
                        e.verbose(),
                    );
                    failures += 1;
                    continue;
                }

                let my_bdev = Bdev::lookup_by_name(&bdev.uri).unwrap();
                let uuid = my_bdev.uuid_as_string();

                if !self.implicit_share_base {
                    continue;
                }

                if let Ok(ss) = NvmfSubsystem::new_with_uuid(&uuid, &my_bdev) {
                    ss.start()
                        .await
                        .map_err(|_| {
                            warn!("failed to share {}", my_bdev);
                        })
                        .unwrap();
                }
            }
        }
        failures
    }

    /// Create any pools defined in the config file.
    async fn create_pools(&self) -> usize {
        let mut failures = 0;
        if let Some(pools) = self.pools.as_ref() {
            for pool in pools {
                info!("creating pool {}", pool.name);
                if let Err(e) = Lvs::create_or_import(pool.into()).await {
                    error!(
                        "Failed to create pool {}. {}",
                        pool.name,
                        e.verbose()
                    );
                    failures += 1;
                }
            }
        }
        failures
    }

    pub fn import_nexuses() -> bool {
        match std::env::var_os("IMPORT_NEXUSES") {
            Some(val) => val.into_string().unwrap().parse::<bool>().unwrap(),
            None => true,
        }
    }

    /// Import bdevs with a specific order
    pub fn import_bdevs(&'static self) {
        assert_eq!(Cores::current(), Cores::first());
        Reactor::block_on(async move {
            // There should not be any duplicate bdevs in the config
            // file. We count any creation failures, but we do not retry.

            // The nexus should be created after the pools as it may be using
            // the pool's lvol
            // The base bdevs need to be created after the nexus as otherwise
            // the nexus create will fail

            let mut errors = self.create_pools().await;

            if Self::import_nexuses() {
                errors += self.create_nexus_bdevs().await;
            }

            errors += self.create_base_bdevs().await;

            if errors != 0 {
                warn!("Not all bdevs({}) were imported successfully", errors);
            }
        });
    }

    /// exports the current configuration to the mayastor config file
    pub(crate) fn export_config() -> Result<(), std::io::Error> {
        let cfg = Config::get().refresh();
        match cfg.source.as_ref() {
            Some(target) => cfg.write_pools(&target),
            // no config file to export to
            None => Ok(()),
        }
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
/// Nexus bdevs to be created. The children are created implicitly this means
/// that if a base_bdev, and a nexus child refer to the same resources, the
/// creation of the nexus will fail.
pub struct NexusBdev {
    /// name of the nexus to be created
    pub name: String,
    /// UUID the nexus should take. Note that we do not check currently if the
    /// GPT labels have the same UUID
    pub uuid: String,
    /// the size of the nexus -- will be removed soon we hope
    pub size: String,
    /// the children the nexus should be created on
    pub children: Vec<String>,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
/// Base bdevs are bdevs that do not belong to a pool. This is used
/// mostly for testing. Typically, our replicas are always served from
/// pools.
pub struct BaseBdev {
    /// bdevs to create outside of the nexus control
    pub uri: String,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
/// Pools that we create. Future work will include the ability to create RAID0
/// or RAID5.
pub struct Pool {
    /// name of the pool to be created or imported
    pub name: String,
    /// bdevs to create outside of the nexus control
    pub disks: Vec<String>,
    /// list of replicas (not required, informational only)
    pub replicas: Vec<Replica>,
}

/// Convert Pool into a gRPC request payload
impl From<&Pool> for rpc::mayastor::CreatePoolRequest {
    fn from(o: &Pool) -> Self {
        Self {
            name: o.name.clone(),
            disks: o.disks.clone(),
        }
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize, Clone)]
/// Pool replicas that we share via `ShareType`
pub struct Replica {
    /// name of the replica
    pub name: String,
    /// share type if shared
    pub share: Option<ShareType>,
}
