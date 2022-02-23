//! Different subsystems use a different method to set/get options. So per
//! subsystem, you will see small but subtle differences. The config setting
//! should be applied *before* any call is made to mayastor.
//!
//! Partial config options are supported i.e you do not have to fully
//! spell out the YAML spec for a given sub component. Serde will fill
//! in the default when missing, which are defined within the individual
//! options.
use std::{fmt::Display, fs, io::Write, path::Path};

use futures::FutureExt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use spdk_rs::libspdk::{
    spdk_json_write_ctx,
    spdk_json_write_val_raw,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};

use crate::{
    jsonrpc::{jsonrpc_register, Code, RpcErrorCode},
    subsys::config::opts::{
        BdevOpts,
        GetOpts,
        NexusOpts,
        NvmeBdevOpts,
        NvmfTgtConfig,
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
pub(crate) mod pool;

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
#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// location of the config file that we loaded
    pub source: Option<String>,
    /// these options are not set/copied but are applied
    /// on target creation.
    pub nvmf_tcp_tgt_conf: NvmfTgtConfig,
    /// options specific to NVMe bdev types
    pub nvme_bdev_opts: NvmeBdevOpts,
    /// generic bdev options
    pub bdev_opts: BdevOpts,
    /// nexus specific options
    pub nexus_opts: NexusOpts,
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

        config.source = Some(file.to_string());

        Ok(config)
    }

    /// collect current configuration snapshot into a new Config object that can
    /// be exported to a file (YAML or JSON)
    pub fn refresh(&self) -> Self {
        // the config is immutable, so we construct a new one which is mutable
        // such that we can scribble in the current bdevs. The config
        // gets loaded with the current settings, as we know that these
        // are immutable, we can copy them with any locks held
        Config {
            source: self.source.clone(),
            nvmf_tcp_tgt_conf: self.nvmf_tcp_tgt_conf.get(),
            nvme_bdev_opts: self.nvme_bdev_opts.get(),
            bdev_opts: self.bdev_opts.get(),
            nexus_opts: self.nexus_opts.get(),
        }
    }

    /// write the current configuration to disk
    pub fn write<P>(&self, file: P) -> Result<(), std::io::Error>
    where
        P: AsRef<Path>,
    {
        if let Ok(s) = serde_yaml::to_string(&self) {
            let mut file = fs::File::create(file)?;
            return file.write_all(s.as_bytes());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to serialize config",
        ))
    }

    /// apply the hybrid configuration that is loaded from YAML. Hybrid in the
    /// sense that options not defined, will default to the impl of Default.
    ///
    /// Note; for nvmf there is no set/get option. This is because the way
    /// transports are constructed. The target accepts an opt parameter, thus
    /// it does not consult a global (mutable) data structure
    pub fn apply(&self) {
        info!("Applying Mayastor configuration settings");
        assert!(self.nvme_bdev_opts.set());
        assert!(self.bdev_opts.set());

        debug!("{:#?}", self);
    }
}
