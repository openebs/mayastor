//! The purpose of this module is to persist child status information across
//! Mayastor restarts.
//!
//! The load function should only be called when Mayastor is initialising. This
//! will read in the child status information from the config file and use it to
//! initialise the in-memory ChildStatusConfig structure. The apply function can
//! then be called which will set the status of all children to match the
//! configuration.
//!
//! The update function should be called whenever a child's status is changed.
//! This will update the configuration file but WILL NOT update the in-memory
//! ChildStatusConfig structure as this is only required on startup and not
//! during runtime.

use crate::bdev::nexus::{
    instances,
    nexus_channel::DREvent,
    nexus_child::{NexusChild, StatusReasons},
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
    status: HashMap<ChildName, StatusReasons>,
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
                if let Some(status) = store.get(&child.name) {
                    info!(
                        "Apply state to child {}, reasons {:?}",
                        child.name, status
                    );
                    child.status_reasons = *status;
                }
            });
            nexus.reconfigure(DREvent::ChildStatusSync).await;
        }
    }

    /// Add the child status to the configuration.
    /// This function is called before the child is added to the list of nexus
    /// children, therefore the child has to be explicitly added to the
    /// configuration here as it won't be part of the running configuration yet.
    pub(crate) fn add(
        child_name: &str,
        status: &StatusReasons,
    ) -> Result<(), std::io::Error> {
        let mut cfg = ChildStatusConfig::get_running_cfg();
        if cfg.status.contains_key(child_name) {
            // We shouldn't ever get here because you can't add a child if it
            // is already part of the nexus. However, just in case, log a
            // message and don't update the child status.
            error!(
                "The child {} is already added as a nexus child",
                child_name
            );
            return Ok(());
        }
        cfg.status.insert(child_name.to_string(), *status);
        ChildStatusConfig::do_save(&cfg)
    }

    /// Update the configuration to remove the child.
    /// This function is called before the child is removed from the list of
    /// nexus children, therefore the child has to be explicitly removed from
    /// the running configuration here.
    pub(crate) fn remove(child_name: &str) -> Result<(), std::io::Error> {
        let mut cfg = ChildStatusConfig::get_running_cfg();
        if cfg.status.contains_key(child_name) {
            debug!("Removing child {} from configuration", child_name);
            cfg.status.remove(child_name);
        }
        ChildStatusConfig::do_save(&cfg)
    }

    // Update the status of the child in the configuration.
    pub(crate) fn update(child: &NexusChild) -> Result<(), std::io::Error> {
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

        let mut status_cfg = ChildStatusConfig {
            status: HashMap::new(),
        };

        instances().iter().for_each(|nexus| {
            nexus.children.iter().for_each(|child| {
                status_cfg
                    .status
                    .insert(child.name.clone(), child.status_reasons);
            });
        });

        if status_cfg
            .status
            .insert(child.name.clone(), child.status_reasons)
            .is_none()
        {
            debug!(
                "Added child {} with status {:?} to configuration",
                child.name, child.status_reasons
            );
        }

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

    /// Save the state of the running configuration.
    pub(crate) fn save() -> Result<(), std::io::Error> {
        let cfg = ChildStatusConfig::get_running_cfg();
        ChildStatusConfig::do_save(&cfg)
    }

    /// Initialise the config file location
    fn init_config_location(path: &str) {
        INIT.call_once(|| unsafe {
            CONFIG_FILE = Some(path.to_string());
        });
    }

    /// Generates a configuration with the current state of the running system.
    fn get_running_cfg() -> ChildStatusConfig {
        let mut cfg = ChildStatusConfig {
            status: HashMap::new(),
        };
        instances().iter().for_each(|nexus| {
            nexus.children.iter().for_each(|child| {
                cfg.status.insert(child.name.clone(), child.status_reasons);
            });
        });
        cfg
    }

    /// Save the passed in configuration to a file.
    fn do_save(cfg: &ChildStatusConfig) -> Result<(), std::io::Error> {
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

        debug!("Saving child status configuration to {}", cfg_file);

        match serde_yaml::to_string(cfg) {
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
}
