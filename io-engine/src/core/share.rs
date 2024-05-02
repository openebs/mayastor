use async_trait::async_trait;
use pin_utils::core_reexport::fmt::Formatter;
use std::{convert::TryFrom, fmt::Display, pin::Pin};

use crate::lvs::LvsError;

/// Indicates what protocol the bdev is shared as.
#[derive(Debug, Default, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub enum Protocol {
    /// not shared by any of the variants
    #[default]
    Off,
    /// shared as NVMe-oF TCP
    Nvmf,
}

impl TryFrom<i32> for Protocol {
    type Error = LvsError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Off),
            1 => Ok(Self::Nvmf),
            // 2 was for iSCSI
            // the gRPC code does not validate enums so we have
            // to do it here
            _ => Err(LvsError::ReplicaShareProtocol {
                value,
            }),
        }
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let p = match self {
            Self::Off => "Not shared",
            Self::Nvmf => "NVMe-oF TCP",
        };
        write!(f, "{p}")
    }
}

/// Share properties when sharing a device.
#[derive(Debug)]
pub enum ShareProps {
    Nvmf(NvmfShareProps),
}
impl ShareProps {
    /// Get the share protocol.
    pub fn protocol(&self) -> Protocol {
        match self {
            ShareProps::Nvmf(_) => Protocol::Nvmf,
        }
    }
    /// Get the allowed hosts list.
    pub fn allowed_hosts(self) -> Vec<String> {
        match self {
            Self::Nvmf(props) => props.allowed_hosts,
        }
    }
}

/// Persist Through Power Loss properties
#[derive(Debug)]
pub struct PtplProps {
    /// The path to the json file where the reservations will be stored.
    file: std::path::PathBuf,
}
impl PtplProps {
    /// Create a new `Self` with the given json file path.
    pub fn new(file: std::path::PathBuf) -> Self {
        Self {
            file,
        }
    }
    /// Get the json file path.
    pub fn path(&self) -> &std::path::PathBuf {
        &self.file
    }
}

/// Share properties when sharing a device.
#[derive(Default, Debug)]
pub struct NvmfShareProps {
    /// Controller Id range.
    cntlid_range: Option<(u16, u16)>,
    /// Enable ANA reporting.
    ana: bool,
    /// Hosts allowed to connect.
    allowed_hosts: Vec<String>,
    /// Persistent-Power-Loss settings.
    ptpl: Option<PtplProps>,
}
impl NvmfShareProps {
    /// Returns a new `Self`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Modify the controller id range.
    #[must_use]
    pub fn with_range(mut self, cntlid_range: Option<(u16, u16)>) -> Self {
        self.cntlid_range = cntlid_range;
        self
    }
    /// Modify the ana reporting.
    #[must_use]
    pub fn with_ana(mut self, ana: bool) -> Self {
        self.ana = ana;
        self
    }
    /// Modify the ptpl properties.
    #[must_use]
    pub fn with_ptpl<P: Into<Option<PtplProps>>>(mut self, ptpl: P) -> Self {
        self.ptpl = ptpl.into();
        self
    }
    /// Get the controller id range.
    pub fn cntlid_range(&self) -> Option<(u16, u16)> {
        self.cntlid_range
    }
    /// Get the ana reporting.
    pub fn ana(&self) -> bool {
        self.ana
    }
    /// Any host is allowed to connect.
    pub fn host_any(&self) -> bool {
        self.allowed_hosts.is_empty()
    }
    /// Modify the allowed hosts which can connect to the subsystem.
    #[must_use]
    pub fn with_allowed_hosts(mut self, hosts: Vec<String>) -> Self {
        self.allowed_hosts = hosts;
        self
    }
    /// Get the allowed hosts which can connect to the subsystem.
    pub fn allowed_hosts(&self) -> &Vec<String> {
        &self.allowed_hosts
    }
    /// Get the persistence through power loss properties.
    pub fn ptpl(&self) -> &Option<PtplProps> {
        &self.ptpl
    }
}
impl From<Option<NvmfShareProps>> for NvmfShareProps {
    fn from(opts: Option<NvmfShareProps>) -> Self {
        match opts {
            None => Self::new(),
            Some(props) => props,
        }
    }
}
impl From<NvmfShareProps> for UpdateProps {
    fn from(value: NvmfShareProps) -> Self {
        UpdateProps::new().with_allowed_hosts(value.allowed_hosts)
    }
}
impl From<ShareProps> for NvmfShareProps {
    fn from(value: ShareProps) -> Self {
        match value {
            ShareProps::Nvmf(props) => props,
        }
    }
}

/// Update properties of a shared device.
#[derive(Default)]
pub struct UpdateProps {
    /// Hosts allowed to connect.
    allowed_hosts: Vec<String>,
}
impl UpdateProps {
    /// Returns a new `Self`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Any host is allowed to connect.
    pub fn host_any(&self) -> bool {
        self.allowed_hosts.is_empty()
    }
    /// Add allowed hosts (nqn's) which can connect to the subsystem.
    #[must_use]
    pub fn with_allowed_hosts(mut self, hosts: Vec<String>) -> Self {
        self.allowed_hosts = hosts;
        self
    }
    /// Allowed hosts which can connect to the subsystem.
    pub fn allowed_hosts(&self) -> &Vec<String> {
        &self.allowed_hosts
    }
}
impl From<Option<UpdateProps>> for UpdateProps {
    fn from(opts: Option<UpdateProps>) -> Self {
        match opts {
            None => Self::new(),
            Some(props) => props,
        }
    }
}
impl From<ShareProps> for UpdateProps {
    fn from(opts: ShareProps) -> Self {
        Self::new().with_allowed_hosts(opts.allowed_hosts())
    }
}

#[async_trait(?Send)]
pub trait Share: std::fmt::Debug {
    type Error;
    type Output: std::fmt::Display + std::fmt::Debug;

    async fn share_nvmf(
        self: Pin<&mut Self>,
        props: Option<NvmfShareProps>,
    ) -> Result<Self::Output, Self::Error>;
    fn create_ptpl(&self) -> Result<Option<PtplProps>, Self::Error>;

    async fn update_properties<P: Into<Option<UpdateProps>>>(
        self: Pin<&mut Self>,
        props: P,
    ) -> Result<(), Self::Error>;

    /// TODO
    async fn unshare(self: Pin<&mut Self>) -> Result<(), Self::Error>;

    /// TODO
    fn shared(&self) -> Option<Protocol>;

    /// TODO
    fn share_uri(&self) -> Option<String>;

    /// Get the currently allowed host nqn's.
    fn allowed_hosts(&self) -> Vec<String>;

    /// TODO
    fn bdev_uri(&self) -> Option<url::Url>;
    fn bdev_uri_str(&self) -> Option<String> {
        self.bdev_uri().map(|uri| uri.to_string())
    }

    /// TODO
    fn bdev_uri_original(&self) -> Option<url::Url>;
    fn bdev_uri_original_str(&self) -> Option<String> {
        self.bdev_uri_original().map(|uri| uri.to_string())
    }
}
