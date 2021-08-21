use std::{
    convert::TryFrom,
    fmt,
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    net::IpAddr,
    os::unix::io::AsRawFd,
    path::Path,
    str::FromStr,
};

use error::{ConnectError, DiscoveryError, FileIoError, NvmeError};
use nix::libc::ioctl as nix_ioctl;
use num_traits::FromPrimitive;
use snafu::ResultExt;

use crate::{
    error,
    nvme_page::{NvmeAdminCmd, NvmfDiscRspPageEntry, NvmfDiscRspPageHdr},
    nvmf_subsystem::{NvmeSubsystems, Subsystem},
    NVME_ADMIN_CMD_IOCTL, NVME_FABRICS_PATH,
};

/// when connecting to a NVMF target, we MAY send a NQN that we want to be
/// referred as.
const MACHINE_UUID_PATH: &str = "/sys/class/dmi/id/product_uuid";

static HOST_ID: once_cell::sync::Lazy<String> =
    once_cell::sync::Lazy::new(|| {
        let mut host_id = uuid::Uuid::new_v4().to_string();
        if let Ok(mut fd) = std::fs::File::open(MACHINE_UUID_PATH) {
            let mut content = String::new();
            if fd.read_to_string(&mut content).is_ok() {
                content = content.trim().to_string();
                if !content.is_empty() {
                    host_id = content;
                }
            }
        }
        host_id
    });

/// The TrType struct for all known transports types note: we are missing loop
#[derive(Clone, Debug, Primitive)]
#[allow(non_camel_case_types)]
pub enum TrType {
    rdma = 1,
    fc = 2,
    tcp = 3,
}

impl Default for TrType {
    fn default() -> Self {
        Self::tcp
    }
}

impl fmt::Display for TrType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// AddressFamily, in case of TCP and RDMA we use IPv6 or IPc4 only
#[derive(Clone, Debug, Primitive)]
pub enum AddressFamily {
    Pci = 0,
    Ipv4 = 1,
    Ipv6 = 2,
    Ib = 3,
    Fc = 4,
}

impl fmt::Display for AddressFamily {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// There are two built in subsystems available normal, i.e an NVMe device
/// or a discovery controller. We are always exporting a discovery controller
/// even when we are not actively serving out any devices
#[derive(Clone, Debug, Primitive)]
pub enum SubType {
    Discovery = 1,
    Nvme = 2,
}

/// Check if the string contains a valid tcp port number.
fn is_valid_port(value: &str) -> Result<(), String> {
    match value.parse::<u16>() {
        Err(_) => Err(format!("port should be a number: {}", value)),
        Ok(_) => Ok(()),
    }
}

/// Check if the string contains a valid IP address.
fn is_valid_ip(value: &str) -> Result<(), String> {
    match IpAddr::from_str(value) {
        Err(_) => Err(format!("invalid IP address: {}", value)),
        Ok(_) => Ok(()),
    }
}

#[derive(Clone, Debug)]
pub struct DiscoveryLogEntry {
    pub tr_type: TrType,
    pub adr_fam: AddressFamily,
    pub subtype: SubType,
    pub port_id: u32,
    pub trsvcid: String,
    pub traddr: String,
    pub subnqn: String,
}

/// Creates a new Disovery struct to find new NVMe devices
///
///
/// # Example
/// ```
/// use nvmeadm::nvmf_discovery::DiscoveryBuilder;
///
/// let mut disc = DiscoveryBuilder::default()
///     .transport("tcp".to_string())
///     .traddr("127.0.0.1".to_string())
///     .trsvcid(4420)
///     .build()
///     .unwrap();
///
/// let pages = disc.discover();
/// pages.iter().map(|e| println!("{:#?}", e)).for_each(drop);
/// ```
#[derive(Default, Debug, Builder)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct Discovery {
    transport: String,
    traddr: String,
    trsvcid: u32,
    #[builder(setter(skip))]
    ctl_id: u32,
    #[builder(setter(skip))]
    arg_string: String,
    #[builder(setter(skip))]
    entries: Vec<DiscoveryLogEntry>,
}

impl fmt::Display for Discovery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TryFrom<DiscoveryLogEntry> for ConnectArgs {
    type Error = ConnectArgsBuilderError;

    fn try_from(ent: DiscoveryLogEntry) -> Result<Self, Self::Error> {
        ConnectArgsBuilder::default()
            .transport(ent.tr_type)
            .trsvcid(ent.trsvcid)
            .traddr(ent.traddr)
            .nqn(ent.subnqn)
            .build()
    }
}

impl Discovery {
    ///
    /// Runs the actual discovery process and returns a vector of found
    /// [struct.DiscoveryLogEntry]. Through the pages we can connect to the
    /// targets.
    ///
    /// The pages are iteratable so you can filter exactly what you are looing
    /// for
    pub fn discover(&mut self) -> Result<&Vec<DiscoveryLogEntry>, NvmeError> {
        self.arg_string = format!(
            "nqn=nqn.2014-08.org.nvmexpress.discovery,transport={},traddr={},trsvcid={}",
            self.transport, self.traddr, self.trsvcid
        );
        let p = Path::new(NVME_FABRICS_PATH);

        let mut file =
            OpenOptions::new().write(true).read(true).open(&p).context(
                FileIoError {
                    filename: NVME_FABRICS_PATH,
                },
            )?;

        file.write_all(self.arg_string.as_bytes())
            .context(FileIoError {
                filename: NVME_FABRICS_PATH,
            })?;
        let mut buf = String::new();
        file.read_to_string(&mut buf).context(FileIoError {
            filename: NVME_FABRICS_PATH,
        })?;
        // get the ctl=value from the controller
        let v = buf.split(',').collect::<Vec<_>>()[0]
            .split('=')
            .collect::<Vec<_>>()[1];

        let ctl_id = u32::from_str(v).unwrap();
        self.ctl_id = ctl_id;
        self.get_discovery_response_pages()?;
        Ok(&self.entries)
    }

    // private function that retrieves number of records
    fn get_discovery_response_page_entries(&self) -> Result<u64, NvmeError> {
        let target = format!("/dev/nvme{}", self.ctl_id);
        let f = OpenOptions::new()
            .read(true)
            .open(Path::new(&target))
            .context(FileIoError { filename: target })?;

        // See NVM-Express1_3d 5.14
        let hdr_len = std::mem::size_of::<NvmfDiscRspPageHdr>() as u32;
        let h = NvmfDiscRspPageHdr::default();
        let mut cmd = NvmeAdminCmd {
            opcode: 0x02,
            nsid: 0,
            dptr: &h as *const _ as u64,
            dptr_len: hdr_len,
            ..Default::default()
        };

        // bytes to dwords, divide by 4. Spec says 0's value

        let dword_count = (hdr_len >> 2) - 1;
        let numdl = dword_count & 0xFFFF;

        // 0x70 is the discovery OP code see
        // NVMe_over_Fabrics_1_0_Gold_20160605-1.pdf  5.3
        cmd.cdw10 = 0x70 | numdl << 16;

        let _ret = unsafe {
            convert_ioctl_res!(nix_ioctl(
                f.as_raw_fd(),
                u64::from(NVME_ADMIN_CMD_IOCTL),
                &cmd
            ))
            .context(DiscoveryError)?;
        };

        Ok(h.numrec)
    }

    // note we can only transfer max_io size. This means that if the number of
    // controllers is larger we have to do {} while() we control the size for
    // our controllers not others! This means in the future we will have to come
    // back to this
    //
    // What we really want is a stream of pages where we can filter process them
    // one by one.

    fn get_discovery_response_pages(&mut self) -> Result<usize, NvmeError> {
        let target = format!("/dev/nvme{}", self.ctl_id);
        let f = OpenOptions::new()
            .read(true)
            .open(Path::new(&target))
            .context(FileIoError { filename: target })?;

        let count = self.get_discovery_response_page_entries()?;

        let hdr_len = std::mem::size_of::<NvmfDiscRspPageHdr>();
        let response_len = std::mem::size_of::<NvmfDiscRspPageEntry>();
        let total_length = hdr_len + (response_len * count as usize);
        // TODO: fixme
        let buffer = unsafe { libc::calloc(1, total_length) };

        let mut cmd = NvmeAdminCmd {
            opcode: 0x02,
            nsid: 0,
            dptr: buffer as _,
            dptr_len: total_length as _,
            ..Default::default()
        };

        let dword_count = ((total_length >> 2) - 1) as u32;
        let numdl: u16 = (dword_count & 0xFFFF) as u16;
        let numdu: u16 = (dword_count >> 16) as u16;

        cmd.cdw10 = 0x70 | u32::from(numdl) << 16_u32;
        cmd.cdw11 = u32::from(numdu);

        let ret = unsafe {
            convert_ioctl_res!(nix_ioctl(
                f.as_raw_fd(),
                u64::from(NVME_ADMIN_CMD_IOCTL),
                &cmd
            ))
        };

        if let Err(e) = ret {
            unsafe { libc::free(buffer) };
            return Err(NvmeError::DiscoveryError { source: e });
        }

        let hdr = unsafe { &mut *(buffer as *mut NvmfDiscRspPageHdr) };
        let entries = unsafe { hdr.entries.as_slice(hdr.numrec as usize) };

        for e in entries {
            let tr_type = TrType::from_u8(e.trtype);
            let adr_fam = AddressFamily::from_u8(e.adrfam);
            let subtype = SubType::from_u8(e.subtype);

            if tr_type.is_none() || adr_fam.is_none() || subtype.is_none() {
                eprintln!("Invalid discovery record, skipping");
                continue;
            }

            let record = DiscoveryLogEntry {
                tr_type: tr_type.unwrap(),
                adr_fam: adr_fam.unwrap(),
                port_id: u32::from(e.portid),
                subtype: subtype.unwrap(),

                trsvcid: unsafe {
                    std::ffi::CStr::from_ptr(&e.trsvcid[0])
                        .to_string_lossy()
                        .into_owned()
                        .trim()
                        .into()
                },
                traddr: unsafe {
                    std::ffi::CStr::from_ptr(&e.traddr[0])
                        .to_string_lossy()
                        .into_owned()
                        .trim()
                        .into()
                },
                subnqn: unsafe {
                    std::ffi::CStr::from_ptr(&e.subnqn[0])
                        .to_string_lossy()
                        .into_owned()
                        .trim()
                        .into()
                },
            };
            self.entries.push(record);
        }

        self.remove_controller()?;
        unsafe { libc::free(buffer) };
        Ok(self.entries.len())
    }

    // we need to close the discovery controller when we are done and before we
    // connect
    fn remove_controller(&self) -> Result<(), NvmeError> {
        let target =
            format!("/sys/class/nvme/nvme{}/delete_controller", self.ctl_id);
        let path = Path::new(&target);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .context(FileIoError { filename: &target })?;
        file.write_all(b"1")
            .context(FileIoError { filename: target })?;
        Ok(())
    }

    /// Connect to all discovery log page entries found during the discovery
    /// phase
    pub fn connect_all(&mut self) -> Result<(), NvmeError> {
        if self.entries.is_empty() {
            return Err(NvmeError::NoSubsystems {});
        }
        // we are ignoring errors here, and connect to all possible devices
        if let Err(connections) = self
            .entries
            .iter_mut()
            .map(|e| match ConnectArgs::try_from(e.clone()) {
                Ok(c) => c.connect(),
                Err(err) => Err(NvmeError::InvalidParam {
                    text: err.to_string(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()
        {
            return Err(connections);
        }
        Ok(())
    }

    /// This method the actual thing we care about. We want to connect to
    /// something and all we have is the name of the node we are supposed to
    /// connect to a port this port in our case may vary depending on which
    /// side of the fabric we are but in general, this will be either 4420
    /// or 4421.
    ///
    /// When we are already connected to the same host we will get back
    /// connection in progress. These errors are propagated back to us as
    /// we use the ? marker
    ///
    ///  # Example
    ///  ```rust
    ///  use nvmeadm::nvmf_discovery::DiscoveryBuilder;
    ///
    ///  let mut discovered_targets = DiscoveryBuilder::default()
    ///          .transport("tcp".to_string())
    ///          .traddr("127.0.0.1".to_string())
    ///          .trsvcid(4420)
    ///          .build()
    ///          .unwrap();
    ///
    ///  let result = discovered_targets.connect("mynqn");
    /// ```
    ///
    pub fn connect(&mut self, nqn: &str) -> Result<String, NvmeError> {
        if let Some(ss) = self.entries.iter_mut().find(|p| p.subnqn == nqn) {
            match ConnectArgs::try_from(ss.clone()) {
                Ok(c) => c.connect(),
                Err(err) => Err(NvmeError::InvalidParam {
                    text: err.to_string(),
                }),
            }
        } else {
            Err(NvmeError::NqnNotFound { text: nqn.into() })
        }
    }
}

impl DiscoveryBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(transport) = &self.transport {
            if transport != "tcp" && transport != "rdma" {
                return Err("invalid transport specified".into());
            }
        }
        if let Some(traddr) = &self.traddr {
            is_valid_ip(traddr)?;
        }

        Ok(())
    }
}

#[derive(Default, Debug, Builder)]
#[builder(setter(into))]
#[builder(build_fn(validate = "Self::validate"))]
pub struct ConnectArgs {
    /// IP address of the target
    traddr: String,
    /// Port number of the target
    trsvcid: String,
    /// NQN of the target
    nqn: String,
    /// Transport type
    #[builder(default = "TrType::tcp")]
    transport: TrType,
    /// controller loss timeout period in seconds
    #[builder(default = "None")]
    ctrl_loss_tmo: Option<u32>,
    /// reconnect timeout period in seconds
    #[builder(default = "None")]
    reconnect_delay: Option<u32>,
    /// keep alive timeout period in seconds
    #[builder(default = "None")]
    keep_alive_tmo: Option<u32>,
}

impl ConnectArgsBuilder {
    fn validate(&self) -> Result<(), String> {
        match &self.transport {
            Some(TrType::tcp) => {
                match &self.trsvcid {
                    Some(trsvcid) => is_valid_port(trsvcid),
                    None => Err("missing svcid".into()),
                }?;
                match &self.traddr {
                    Some(traddr) => is_valid_ip(traddr),
                    None => Err("missing traddr".into()),
                }
            }
            Some(TrType::rdma) => Ok(()),
            Some(val) => Err(format!("invalid transport type: {}", val)),
            None => Ok(()), // using the default transport type
        }
    }
}

impl fmt::Display for ConnectArgs {
    /// The output is used for writing to nvme-fabrics file so be careful
    /// when making changes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host_id = HOST_ID.as_str();
        write!(f, "nqn={},", self.nqn)?;
        write!(f, "hostnqn=nqn.2019-05.io.openebs.mayastor:{},", host_id)?;
        write!(f, "hostid={},", host_id)?;
        write!(f, "transport={},", self.transport)?;
        write!(f, "traddr={},", self.traddr)?;
        write!(f, "trsvcid={}", self.trsvcid)?;
        if let Some(val) = self.keep_alive_tmo {
            write!(f, ",keep_alive_tmo={}", val)?;
        }
        if let Some(val) = self.reconnect_delay {
            write!(f, ",reconnect_delay={}", val)?;
        }
        if let Some(val) = self.ctrl_loss_tmo {
            write!(f, ",ctrl_loss_tmo={}", val)?;
        }
        Ok(())
    }
}

impl ConnectArgs {
    /// This method connects to a specific NVMf device available over tcp,
    /// identified by its ip address, port and nqn.
    ///
    /// When we are already connected to the same host we will get back
    /// connection in progress. These errors are propagated back to us as
    /// we use the ? marker
    ///
    ///  # Example
    ///  ```rust
    ///  use nvmeadm::nvmf_discovery::ConnectArgsBuilder;
    ///
    ///  let result = ConnectArgsBuilder::default()
    ///      .traddr("192.168.122.99")
    ///      .trsvcid("8420")
    ///      .nqn("mynqn")
    ///      .ctrl_loss_tmo(60)
    ///      .reconnect_delay(10)
    ///      .keep_alive_tmo(5)
    ///      .build()
    ///      .unwrap()
    ///      .connect();
    /// ```
    ///
    pub fn connect(&self) -> Result<String, NvmeError> {
        let p = Path::new(NVME_FABRICS_PATH);

        let mut file =
            OpenOptions::new().write(true).read(true).open(&p).context(
                ConnectError {
                    filename: NVME_FABRICS_PATH,
                },
            )?;
        if let Err(e) = file.write_all(format!("{}", self).as_bytes()) {
            return match e.kind() {
                ErrorKind::AlreadyExists => Err(NvmeError::ConnectInProgress),
                _ => Err(NvmeError::IoError { source: e }),
            };
        }
        let mut buf = String::new();
        file.read_to_string(&mut buf).context(ConnectError {
            filename: NVME_FABRICS_PATH,
        })?;
        Ok(buf)
    }
}

/// This method disconnects a specific NVMf device, identified by its nqn.
///
///  # Example
///  ```rust
///  let num_disconnects = nvmeadm::nvmf_discovery::disconnect("mynqn");
///  ```

pub fn disconnect(nqn: &str) -> Result<usize, NvmeError> {
    let subsys: Result<Vec<Subsystem>, NvmeError> = NvmeSubsystems::new()?
        .filter_map(Result::ok)
        .filter(|e| e.nqn == nqn)
        .map(|e| {
            e.disconnect()?;
            Ok(e)
        })
        .collect();
    Ok(subsys?.len())
}
