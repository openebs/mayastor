use crate::{parse_value, NvmeError};
use failure::Error;
use glob::glob;
use std::{fs::OpenOptions, io::Write, path::Path};

use std::str::FromStr;
/// Subsystem struct shows us all the connect fabrics. This does not include
/// NVMe devices that are connected by trtype=PCIe
#[derive(Default, Clone, Debug)]
pub struct Subsystem {
    /// name of the subsystem
    pub name: String,
    /// instance number of the subsystem (controller)
    pub instance: u32,
    /// NVme Qualified Name (NQN)
    pub nqn: String,
    /// state of the connection, will contain live if online
    pub state: String,
    /// the transport type being used (tcp or RDMA)
    pub transport: String,
    /// address contains traddr=X,trsvcid=Y
    pub address: String,
    /// serial number
    pub serial: String,
    /// model number
    pub model: String,
}

impl Subsystem {
    /// scans the sysfs directory for attached subsystems skips any transport
    /// that does not contain a value that is being read in the implementation
    pub fn new(source: &Path) -> Result<Self, Error> {
        let name = source
            .strip_prefix("/sys/devices/virtual/nvme-fabrics/ctl")?
            .display()
            .to_string();
        let instance = u32::from_str(name.trim_start_matches("nvme")).unwrap();
        let nqn = parse_value::<String>(&source, "subsysnqn")?;
        let state = parse_value::<String>(&source, "state")?;
        let transport = parse_value::<String>(&source, "transport")?;
        let address = parse_value::<String>(&source, "address")?;
        let serial = parse_value::<String>(&source, "serial")?;
        let model = parse_value::<String>(&source, "model")?;

        if serial == "" || model == "" {
            return Err(
                NvmeError::CtlNotFound("discovery controller".into()).into()
            );
        }

        // if it does not have a serial and or model -- its a discovery
        // controller so we skip it

        let model = parse_value::<String>(&source, "model")?;
        Ok(Subsystem {
            name,
            instance,
            nqn,
            state,
            transport,
            address,
            serial,
            model,
        })
    }
    /// issue a rescan to the controller to find new namespaces
    pub fn rescan(&self) -> Result<(), Error> {
        let target = format!("/sys/class/nvme/{}/rescan_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.write_all(b"1")?;
        Ok(())
    }
    /// disconnects the transport dropping all namespaces
    pub fn disconnect(&self) -> Result<(), Error> {
        let target = format!("/sys/class/nvme/{}/delete_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.write_all(b"1")?;
        Ok(())
    }
    /// resets the nvme controller
    pub fn reset(&self) -> Result<(), Error> {
        let target = format!("/sys/class/nvme/{}/reset_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.write_all(b"1")?;
        Ok(())
    }
}

/// list of subsystems found on the system
#[derive(Default, Debug)]
pub struct NvmeSubsystems {
    entries: Vec<String>,
}

impl Iterator for NvmeSubsystems {
    type Item = Result<Subsystem, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.entries.pop() {
            return Some(Subsystem::new(Path::new(&e)));
        }
        None
    }
}

impl NvmeSubsystems {
    /// Construct a new list of subsystems
    pub fn new() -> Result<Self, Error> {
        let path_entries = glob("/sys/devices/virtual/nvme-fabrics/ctl/nvme*")?;
        let mut entries = Vec::new();
        for entry in path_entries {
            if let Ok(path) = entry {
                entries.push(path.display().to_string())
            }
        }
        Ok(NvmeSubsystems {
            entries,
        })
    }
}
