use crate::{parse_value, NvmeError};
use glob::glob;
use snafu::{ResultExt, Snafu};
use std::{fs::OpenOptions, io::Write, path::Path, str::FromStr};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
#[snafu(visibility = "pub(crate)")]
pub enum NvmfError {
    #[snafu(display("IO error:"))]
    IoError { source: std::io::Error },
    #[snafu(display("File IO error:{}, {}", filename, source))]
    FileIoError {
        filename: String,
        source: std::io::Error,
    },
    #[snafu(display("controller with nqn: {} not found", text))]
    CtlNotFound { text: String },
    #[snafu(display("Invalid path {}: {}", path, source))]
    InvalidPath {
        source: std::path::StripPrefixError,
        path: String,
    },
    #[snafu(display("Failed to parse {} : {}", path, contents))]
    ParseError { path: String, contents: String },
    #[snafu(display("error during Nvme discovery"))]
    PathPatternParseError { source: glob::PatternError },
    #[snafu(display("{}", source))]
    ValueError { source: NvmeError },
}

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
    pub fn new(source: &Path) -> Result<Self, NvmfError> {
        let name = source
            .strip_prefix("/sys/devices/virtual/nvme-fabrics/ctl")
            .context(InvalidPath {
                path: format!("{:?}", source),
            })?
            .display()
            .to_string();
        let instance = u32::from_str(name.trim_start_matches("nvme")).unwrap();
        let nqn = parse_value::<String>(&source, "subsysnqn")
            .context(ValueError {})?;
        let state =
            parse_value::<String>(&source, "state").context(ValueError {})?;
        let transport = parse_value::<String>(&source, "transport")
            .context(ValueError {})?;
        let address =
            parse_value::<String>(&source, "address").context(ValueError {})?;
        let serial =
            parse_value::<String>(&source, "serial").context(ValueError {})?;
        let model =
            parse_value::<String>(&source, "model").context(ValueError {})?;

        if serial == "" || model == "" {
            return Err(NvmfError::CtlNotFound {
                text: "discovery controller".into(),
            });
        }

        // if it does not have a serial and or model -- its a discovery
        // controller so we skip it

        let model =
            parse_value::<String>(&source, "model").context(ValueError)?;
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
    pub fn rescan(&self) -> Result<(), NvmfError> {
        let target = format!("/sys/class/nvme/{}/rescan_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path).context(
            FileIoError {
                filename: &target,
            },
        )?;
        file.write_all(b"1").context(FileIoError {
            filename: &target,
        })?;
        Ok(())
    }
    /// disconnects the transport dropping all namespaces
    pub fn disconnect(&self) -> Result<(), NvmfError> {
        let target = format!("/sys/class/nvme/{}/delete_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path).context(
            FileIoError {
                filename: &target,
            },
        )?;
        file.write_all(b"1").context(FileIoError {
            filename: &target,
        })?;
        Ok(())
    }
    /// resets the nvme controller
    pub fn reset(&self) -> Result<(), NvmfError> {
        let target = format!("/sys/class/nvme/{}/reset_controller", self.name);
        let path = Path::new(&target);

        let mut file = OpenOptions::new().write(true).open(&path).context(
            FileIoError {
                filename: &target,
            },
        )?;
        file.write_all(b"1").context(FileIoError {
            filename: &target,
        })?;
        Ok(())
    }
}

/// list of subsystems found on the system
#[derive(Default, Debug)]
pub struct NvmeSubsystems {
    entries: Vec<String>,
}

impl Iterator for NvmeSubsystems {
    type Item = Result<Subsystem, NvmfError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.entries.pop() {
            return Some(Subsystem::new(Path::new(&e)));
        }
        None
    }
}

impl NvmeSubsystems {
    /// Construct a new list of subsystems
    pub fn new() -> Result<Self, NvmfError> {
        //FIXME the ?
        let path_entries = glob("/sys/devices/virtual/nvme-fabrics/ctl/nvme*")
            .context(PathPatternParseError)?;
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
