//! The purpose of this module is to persist child status information across
//! Mayastor restarts.
//!
//! The load function should only be called when Mayastor is initialising. This
//! will read in the child status information from the config file and use it to
//! initialise the in-memory ChildStatusConfig structure. The apply function can
//! then be called which will set the status of all children to match the
//! configuration.
//!
//! The save function should be called whenever a child's status is updated.
//! This will update the configuration file but WILL NOT update the in-memory
//! ChildStatusConfig structure as this is only required on startup and not
//! during runtime.

use crate::bdev::nexus::{
    instances,
    nexus_channel::DrEvent,
    nexus_child::{ChildState, NexusChild},
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, fs::File, io::Write, sync::Once};

type ChildName = String;
static mut CONFIG_FILE: Option<String> = None;
static INIT: Once = Once::new();
pub static STATUS_CONFIG: OnceCell<ChildStatusConfig> = OnceCell::new();

#[derive(Serialize, Deserialize, Debug)]
pub struct ChildStatusConfig {
    status: HashMap<ChildName, ChildState>,
}

impl Default for ChildStatusConfig {
    fn default() -> Self {
        Self {
            status: Default::default(),
        }
    }
}

impl ChildStatusConfig {
    /// Initialise the ChildStatusConfig structure by executing F and returning
    /// a reference to the initialized data.
    pub(crate) fn get_or_init<F>(f: F) -> &'static Self
    where
        F: FnOnce() -> ChildStatusConfig,
    {
        STATUS_CONFIG.get_or_init(f)
    }

    /// Similar to get_or_init above, but we do not need to pass a closure.
    pub(crate) fn get() -> &'static Self {
        STATUS_CONFIG.get().unwrap()
    }

    /// Load the configuration file if it exists otherwise use default values.
    pub(crate) fn load(
        cfg_file_path: &Option<String>,
    ) -> Result<ChildStatusConfig, ()> {
        if let Some(cfg_location) = cfg_file_path {
            ChildStatusConfig::init_config_location(cfg_location);

            debug!("Loading configuration file from {}", cfg_location);
            let cfg = fs::read(cfg_location).unwrap_or_default();
            if cfg.is_empty() {
                Ok(ChildStatusConfig::default())
            } else {
                match serde_yaml::from_slice(&cfg) {
                    Ok(config) => Ok(config),
                    Err(e) => {
                        error!("{}", e);
                        Err(())
                    }
                }
            }
        } else {
            Ok(ChildStatusConfig::default())
        }
    }

    /// Apply the status in the configuration to each child.
    pub(crate) async fn apply() {
        debug!("Applying child status");
        let store = &ChildStatusConfig::get().status;
        for nexus in instances() {
            nexus.children.iter_mut().for_each(|child| {
                if let Some(status) = store.get(child.get_name()) {
                    info!(
                        "Apply state to child {}, reasons {:?}",
                        child.get_name(),
                        status
                    );
                    child.set_state(*status);
                }
            });
            nexus.reconfigure(DrEvent::ChildStatusSync).await;
        }
    }

    /// A public wrapper around the actual save function.
    pub(crate) fn save() -> Result<(), std::io::Error> {
        ChildStatusConfig::do_save(None)
    }

    /// Save the status of all children to the configuration file.
    fn do_save(cfg: Option<ChildStatusConfig>) -> Result<(), std::io::Error> {
        let cfg_file;
        unsafe {
            match CONFIG_FILE.clone() {
                Some(cfg) => cfg_file = cfg,
                None => {
                    // If a configuration file wasn't specified, nothing has to
                    // be done.
                    return Ok(());
                }
            }
        }

        debug!("Saving child status");
        let mut status_cfg = match cfg {
            Some(cfg) => cfg,
            None => ChildStatusConfig {
                status: HashMap::new(),
            },
        };

        instances().iter().for_each(|nexus| {
            nexus.children.iter().for_each(|child| {
                status_cfg
                    .status
                    .insert(child.get_name().to_string(), child.state());
            });
        });

        match serde_yaml::to_string(&status_cfg) {
            Ok(s) => {
                let mut cfg_file = File::create(cfg_file)?;
                cfg_file.write_all(s.as_bytes())
            }
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "failed to serialize status config",
            )),
        }
    }

    /// Add the child to the configuration and then save it.
    /// The configuration is updated on a status change and expects the child to
    /// already be listed as a nexus child. However, when a child is added,
    /// the status is changed before it is added to the nexus children list.
    /// Therefore, we have to explicitly add the child to the configuration
    /// here.
    pub(crate) fn add(child: &NexusChild) -> Result<(), std::io::Error> {
        let mut cfg = ChildStatusConfig {
            status: HashMap::new(),
        };
        cfg.status
            .insert(child.get_name().to_string(), child.state());
        ChildStatusConfig::do_save(Some(cfg))
    }

    /// Initialise the config file location
    fn init_config_location(path: &str) {
        INIT.call_once(|| unsafe {
            CONFIG_FILE = Some(path.to_string());
        });
    }
}
