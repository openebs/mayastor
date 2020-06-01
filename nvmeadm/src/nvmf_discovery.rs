use crate::nvme_page::{
    NvmeAdminCmd,
    NvmfDiscRspPageEntry,
    NvmfDiscRspPageHdr,
};
use std::fmt;

use nix::libc::ioctl as nix_ioctl;

use crate::nvmf_subsystem::{NvmeSubsystems, Subsystem};

/// when connecting to a NVMF target, we MAY send a NQN that we want to be
/// referred as.
const MACHINE_UUID_PATH: &str = "/sys/class/dmi/id/product_uuid";

use crate::{NvmeError, NVME_ADMIN_CMD_IOCTL, NVME_FABRICS_PATH};
use failure::Error;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
    str::FromStr,
};

use num_traits::FromPrimitive;
use std::{net::IpAddr, os::unix::io::AsRawFd};

/// The TrType struct for all known transports types note: we are missing loop
#[derive(Debug, Primitive)]
#[allow(non_camel_case_types)]
pub enum TrType {
    rdma = 1,
    fc = 2,
    tcp = 3,
}

impl fmt::Display for TrType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// AddressFamily, in case of TCP and RDMA we use IPv6 or IPc4 only
#[derive(Debug, Primitive)]
pub enum AddressFamily {
    PCI = 0,
    IPv4 = 1,
    IPv6 = 2,
    IB = 3,
    FC = 4,
}

impl fmt::Display for AddressFamily {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// There are two built in subsystems available normal, i.e an NVMe device
/// or a discovery controller. We are always exporting a discovery controller
/// even when we are not actively serving out any devices
#[derive(Debug, Primitive)]
pub enum SubType {
    DISCOVERY = 1,
    NVME = 2,
}

#[derive(Debug)]
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

impl Discovery {
    ///
    /// Runs the actual discovery process and returns a vector of found
    /// [struct.DiscoveryLogEntry]. Through the pages we can connect to the
    /// targets.
    ///
    /// The pages are iterable, so you can filter exactly what you are looking
    /// for
    pub fn discover(&mut self) -> Result<&Vec<DiscoveryLogEntry>, Error> {
        self.arg_string = format!(
            "nqn=nqn.2014-08.org.nvmexpress.discovery,transport={},traddr={},trsvcid={}",
            self.transport, self.traddr, self.trsvcid
        );
        let p = Path::new(NVME_FABRICS_PATH);

        let mut file = OpenOptions::new().write(true).read(true).open(&p)?;

        file.write_all(self.arg_string.as_bytes())?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
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
    fn get_discovery_response_page_entries(&self) -> Result<u64, Error> {
        let f = OpenOptions::new()
            .read(true)
            .open(Path::new(&format!("/dev/nvme{}", self.ctl_id)))?;

        // See NVM-Express1_3d 5.14
        let hdr_len = std::mem::size_of::<NvmfDiscRspPageHdr>() as u32;
        let h = NvmfDiscRspPageHdr::default();
        let mut cmd = NvmeAdminCmd::default();
        cmd.opcode = 0x02;
        cmd.nsid = 0;
        cmd.dptr_len = hdr_len;
        cmd.dptr = &h as *const _ as u64;

        // bytes to dwords, devide by 4. Spec says 0's value

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
            ))?
        };

        Ok(h.numrec)
    }

    // note we can only transfer max_io size. This means that if the number of
    // controllers
    // is larger we have to do {] while() we control the size for our
    // controllers not others! This means in the future we will have to come
    // back to this
    //
    // What really want is a stream of pages where we can filter process them
    // one by one.

    fn get_discovery_response_pages(&mut self) -> Result<usize, Error> {
        let f = OpenOptions::new()
            .read(true)
            .open(Path::new(&format!("/dev/nvme{}", self.ctl_id)))?;

        let count = self.get_discovery_response_page_entries()?;

        let hdr_len = std::mem::size_of::<NvmfDiscRspPageHdr>();
        let response_len = std::mem::size_of::<NvmfDiscRspPageEntry>();
        let total_length = hdr_len + (response_len * count as usize);
        let buffer = unsafe { libc::calloc(1, total_length) };

        let mut cmd = NvmeAdminCmd::default();

        cmd.opcode = 0x02;
        cmd.nsid = 0;
        cmd.dptr_len = total_length as u32;
        cmd.dptr = buffer as *const _ as u64;

        let dword_count = ((total_length >> 2) - 1) as u32;
        let numdl: u16 = (dword_count & 0xFFFF) as u16;
        let numdu: u16 = (dword_count >> 16) as u16;

        cmd.cdw10 = 0x70 | u32::from(numdl) << 16 as u32;
        cmd.cdw11 = u32::from(numdu);

        let _ret = unsafe {
            convert_ioctl_res!(nix_ioctl(
                f.as_raw_fd(),
                u64::from(NVME_ADMIN_CMD_IOCTL),
                &cmd
            ))?
        };

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
    fn remove_controller(&self) -> Result<(), Error> {
        let target =
            format!("/sys/class/nvme/nvme{}/delete_controller", self.ctl_id);
        let path = Path::new(&target);
        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.write_all(b"1")?;
        Ok(())
    }

    /// Connect to all discovery log page entries found during the discovery
    /// phase
    pub fn connect_all(&mut self) -> Result<(), Error> {
        if self.entries.is_empty() {
            return Err(Error::from(NvmeError::NoSubsystems));
        }
        let p = Path::new(NVME_FABRICS_PATH);

        let mut file = OpenOptions::new().write(true).read(true).open(&p)?;
        // we are ignoring errors here, and connect to all possible devices
        if let Err(connections) = self
            .entries
            .iter_mut()
            .map(|e| {
                file.write_all(e.build_connect_args().unwrap().as_bytes())
                    .and_then(|_e| {
                        let mut buf = String::new();
                        file.read_to_string(&mut buf)?;
                        Ok(buf)
                    })
            })
            .collect::<Result<Vec<_>, _>>()
        {
            return Err(connections.into());
        }

        Ok(())
    }

    ///
    /// This method the actual thing we care about. We want to connect to
    /// something and all we have is the name of the node we are supposed to
    /// connect to a port this port in our case may vary depending on which
    /// side of the fabric we are but in general, this will be either 4420
    /// or 4421.
    ///
    /// When we are already connected to the same host we will get back
    /// connection in progress. These errors are propagated back to use as
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

    pub fn connect(&mut self, nqn: &str) -> Result<String, Error> {
        let p = Path::new(NVME_FABRICS_PATH);

        if let Some(ss) = self.entries.iter_mut().find(|p| p.subnqn == nqn) {
            let mut file =
                OpenOptions::new().write(true).read(true).open(&p)?;
            file.write_all(ss.build_connect_args().unwrap().as_bytes())?;
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok(buf)
        } else {
            Err(NvmeError::NqnNotFound(nqn.into()).into())
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
            if let Err(ip) = IpAddr::from_str(&traddr) {
                return Err(format!("Invalid IP address: {}", ip.to_string()));
            }
        }

        Ok(())
    }
}

impl DiscoveryLogEntry {
    fn read_file(&self, file_path: &str) -> Result<String, Error> {
        let mut fd = std::fs::File::open(file_path)?;
        let mut content = String::new();
        fd.read_to_string(&mut content)?;
        Ok(content.trim().to_string())
    }

    fn get_host_id(&self) -> Result<String, Error> {
        let id = self.read_file(MACHINE_UUID_PATH)?;
        Ok(id)
    }

    //TODO: we need to pass in the HOSTNAME suggestion is to use env::

    fn my_nqn_name(&self) -> Result<String, Error> {
        let hostid = self.get_host_id()?;
        Ok(format!("nqn.2019-05.io.openebs.mayastor:{}", hostid))
    }

    pub fn build_connect_args(&mut self) -> Result<String, Error> {
        let mut connect_args = String::new();

        connect_args.push_str(&format!("nqn={},", self.subnqn));
        connect_args.push_str(&format!("hostnqn={},", self.my_nqn_name()?));
        connect_args.push_str(&format!("hostid={},", self.get_host_id()?));

        connect_args.push_str(&format!("transport={},", self.tr_type));
        connect_args.push_str(&format!("traddr={},", self.traddr));
        connect_args.push_str(&format!("trsvcid={}", self.trsvcid));

        Ok(connect_args)
    }
}
/// This method disconnects a specific NVMf device, identified by its nqn.
///
///  # Example
///  ```rust
///  let num_disconnects = nvmeadm::nvmf_discovery::disconnect("mynqn");
///  ```

pub fn disconnect(nqn: &str) -> Result<usize, Error> {
    let subsys: Result<Vec<Subsystem>, Error> = NvmeSubsystems::new()?
        .filter_map(Result::ok)
        .filter(|e| e.nqn == nqn)
        .map(|e| {
            e.disconnect()?;
            Ok(e)
        })
        .collect();
    Ok(subsys.unwrap().len())
}
