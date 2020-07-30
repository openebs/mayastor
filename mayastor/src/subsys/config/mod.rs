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
    bdev::{nexus::instances, nexus_create, VerboseError},
    core::{Bdev, Cores, Reactor},
    jsonrpc::{jsonrpc_register, Code, RpcErrorCode},
    nexus_uri::bdev_create,
    pool::{create_pool, PoolsIter},
    subsys::{
        config::opts::{
            BdevOpts,
            ErrStoreOpts,
            GetOpts,
            IscsiTgtOpts,
            NexusOpts,
            NvmeBdevOpts,
            NvmfTgtConfig,
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
                let cfg = Config::get().refresh().unwrap();
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
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
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
    ///
    /// The next options are intended for usage during testing
    ///
    /// list of bdevs to be created on load
    pub base_bdevs: Option<Vec<BaseBdev>>,
    /// list of nexus bdevs that will create the base bdevs implicitly
    pub nexus_bdevs: Option<Vec<NexusBdev>>,
    /// list of pools to create on load, the base_bdevs should be created first
    pub pools: Option<Vec<Pool>>,
    /// any  base bdevs created implicitly share them over nvmf
    pub implicit_share_base: bool,
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
    pub fn read<P>(file: P) -> Result<Config, ()>
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
                    return Err(());
                }
            };
        } else {
            info!("Config file {} is empty, reverting to default config", file);
            // the file is empty
            config = Config::default();
        }
        // use the source luke!
        config.source = Some(file.to_string());
        Ok(config)
    }

    /// collect current configuration snapshot into a new Config object that can
    /// be exported to a file (YAML or JSON)
    pub fn refresh(&self) -> Result<Self, ()> {
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
            implicit_share_base: true,
            err_store_opts: self.err_store_opts.get(),
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
            .map(|p| Pool {
                name: p.get_name().into(),
                disks: vec![p.get_base_bdev().name()],
                blk_size: p.get_base_bdev().block_len(),
                io_if: 0, // AIO
            })
            .collect::<Vec<_>>();

        current.pools = Some(pools);

        Ok(current)
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
        // note: nvmf target does not have a set method
        self.nvme_bdev_opts.set();
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
        failures
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
                if let Err(e) = create_pool(pool.into()).await {
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
    /// Import bdevs with a specific order
    pub fn import_bdevs(&'static self) {
        assert_eq!(Cores::current(), Cores::first());
        Reactor::block_on(async move {
            // the order is pretty arbitrary the only key thing here is that
            // there should not be any duplicate bdevs in the config
            // file. We count any creation failures, but we do not retry.

            let mut errors = self.create_nexus_bdevs().await;
            errors += self.create_base_bdevs().await;
            errors += self.create_pools().await;

            if errors != 0 {
                warn!("Not all bdevs({}) where imported successfully", errors);
            }
        });
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
/// Nexus bdevs to be created. The children are created implicitly this means
/// that if a base_bdev, and a nexus child refer to the same resources, the
/// creation of the nexus will fail.
pub struct NexusBdev {
    /// name of the nexus to be crated
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

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
/// Pools that we create. Future work will include the ability to create RAID0
/// or RAID5.
pub struct Pool {
    /// name of the pool to be created or imported
    pub name: String,
    /// bdevs to create outside of the nexus control
    pub disks: Vec<String>,
    /// the block_size the pool should use
    pub blk_size: u32,
    /// use AIO, uring or auto detect
    pub io_if: i32,
}

/// Convert Pool into a gRPC request payload
impl From<&Pool> for rpc::mayastor::CreatePoolRequest {
    fn from(o: &Pool) -> Self {
        Self {
            name: o.name.clone(),
            disks: o.disks.clone(),
            block_size: o.blk_size,
            io_if: o.io_if,
        }
    }
}
