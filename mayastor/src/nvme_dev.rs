// see https://github.com/rust-lang/rust-clippy/issues/3988
#![allow(clippy::needless_lifetimes)]

use crate::{bdev::nexus, executor::cb_arg, nexus_uri::UriError};
use futures::channel::oneshot;
use spdk_sys::{
    spdk_bdev_nvme_create,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
};
use std::{convert::TryFrom, ffi::CString, fmt, os::raw::c_void};
use url::Url;

/// NVMe error is purposely kept simple (just an enum) as we deal with lots of
/// libc errors coming back from SPDK. In the future we can make it more of an
/// object and create proper from/to implementations.
#[derive(Debug)]
pub enum Code {
    /// construction arguments are invalid
    InvalidArgs,
    /// The URI we are connecting
    Local,
    /// Failed to create the bdev
    Creation,
    /// nvme controller exists
    Exists,
    /// nvme controller does not exists
    NotFound,
    /// not enough free memory to construct request
    NoMemory,
}

impl From<std::ffi::NulError> for Code {
    fn from(_: std::ffi::NulError) -> Self {
        Code::NoMemory
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match *self {
            Code::InvalidArgs => "Invalid arguments",
            Code::Local => "Uri points to a device that is local to this node",
            Code::Creation => "Internal error during creation of target",
            Code::Exists => "Target already exists",
            Code::NotFound => "Target not found",
            Code::NoMemory => "Not enough memory available to honour request",
        };

        write!(f, "{}", p)
    }
}

/// nvme_bdev create arguments, ideally you should not use this directly but use
/// a NvmfUri struct. This structure is processed by [NvmeCreateCtx]
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct NvmfBdev {
    /// name of the bdev that should be created
    pub name: String,
    /// transport type (only TCP for now)
    pub trtype: String,
    /// the addres family either ipv4 or ipv6
    pub adrfam: String,
    /// the remote target address
    pub traddr: String,
    /// the service id (port)
    pub trsvcid: String,
    /// the nqn of the subsystem we want to connect to
    pub subnqn: String,
    /// advertise our own nqn as hostnqn
    pub hostnqn: String,
    /// our connection address
    pub hostaddr: String,
    /// our svcid
    pub hostsvcid: String,
    /// Enable protection information checking of the Logical Block Reference
    /// Tag field
    pub prchk_reftag: bool,
    /// Enable protection information checking of the Application Tag    field
    pub prchk_guard: bool,
}

impl NvmfBdev {
    unsafe extern "C" fn nvme_done(
        ctx: *mut c_void,
        _bdev_count: usize,
        rc: i32,
    ) {
        let sender = Box::from_raw(ctx as *mut oneshot::Sender<i32>);

        sender.send(rc).expect("NVMe creation cb receiver is gone");
    }

    /// async function to construct a bdev given a NvmfUri
    pub async fn create(self) -> Result<String, nexus::Error> {
        let mut ctx = NvmeCreateCtx::new(&self);
        let (sender, receiver) = oneshot::channel::<u32>();

        if crate::bdev::bdev_lookup_by_name(&self.name).is_some() {
            return Err(nexus::Error::ChildExists);
        }

        let str;
        // TODO add this to ctx
        let hostnqn = if self.hostnqn.is_empty() {
            std::ptr::null_mut()
        } else {
            str = CString::new(self.hostnqn.clone()).unwrap();
            str.as_ptr()
        };

        let mut flags: u32 = 0;

        if self.prchk_reftag {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
        }

        if self.prchk_guard {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
        }

        let ret = unsafe {
            spdk_bdev_nvme_create(
                &mut ctx.transport_id,
                &mut ctx.host_id,
                ctx.name,
                &mut ctx.names[0],
                ctx.count,
                hostnqn,
                flags,
                Some(NvmfBdev::nvme_done),
                cb_arg(sender),
            )
        };

        if ret != 0 {
            return Err(nexus::Error::Internal(
                "Failed to create nvme bdev".to_owned(),
            ));
        }

        let result = receiver
            .await
            .expect("internal error in nvme bdev creation");

        if result == 0 {
            Ok(unsafe {
                std::ffi::CStr::from_ptr(ctx.names[0])
                    .to_str()
                    .unwrap()
                    .to_string()
            })
        } else {
            Err(nexus::Error::CreateFailed)
        }
    }
    /// destroy an nvme controller and its namespaces, it is not possible to
    /// destroy a nvme_bdev directly
    pub fn destroy(self) -> Result<(), nexus::Error> {
        let mut name = self.name;
        let name = name.split_off(name.len() - 2);
        let cname = CString::new(name).unwrap();
        let res = unsafe { spdk_sys::spdk_bdev_nvme_delete(cname.as_ptr()) };

        match res {
            libc::ENODEV => Err(nexus::Error::NotFound),
            libc::ENOMEM => Err(nexus::Error::OutOfMemory),
            0 => Ok(()),
            _ => Err(nexus::Error::Internal(
                "Failed to delete nvme device".into(),
            )),
        }
    }
}

/// converts a nvmf URL to NVMF args
impl TryFrom<&Url> for NvmfBdev {
    type Error = UriError;

    fn try_from(u: &Url) -> Result<Self, Self::Error> {
        let mut n = NvmfBdev::default();

        // defaults we currently only support
        n.trtype = "TCP".into();
        n.adrfam = "IPv4".into();
        n.subnqn = match u
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            None => return Err(UriError::InvalidPathSegment),
            // TODO validate that the nqn is a valid v4 UUID
            Some(s) => s[0].to_string(),
        };

        // if no port number is explicitly provided within the URL we can use
        // the scheme to determine if the URL should use the nexus fabric (n) or
        // the storage service fabric (s) if that too fails, we error
        // out.

        if let Some(port) = u.port() {
            n.trsvcid = port.to_string();
        } else {
            n.trsvcid = match u.scheme() {
                "nvmf" => "4420".into(),
                "nvmfn" => "4420".into(),
                "nvmfs" => "4421".into(),
                _ => return Err(UriError::InvalidScheme),
            }
        }

        n.traddr = u.host_str().unwrap().to_string();
        n.name = u.to_string();
        let qp = u.query_pairs();

        for i in qp {
            match i.0.as_ref() {
                // the host nqn we connect with
                "hostnqn" => n.hostnqn = i.1.to_string(),
                // enable Protection Information (PI)tag IO
                "reftag" => n.prchk_reftag = true,
                // PI guard for IO -- 512 + 8
                // see nvme spec 1.3+ sec 8.3
                "guard" => n.prchk_guard = true,
                _ => warn!("query parameter {} ignored", i.0),
            }
        }
        Ok(n)
    }
}

/// The Maximum number of namespaces that a single bdev will connect to
pub const MAX_NAMESPACES: usize = 1;

// closures are not allowed to take themselves as arguments so we do not store
// the closure here

/// This C structure is passed as an argument to the callback of
/// nvme_create_bdev() function its contents is defined by the C side of things.
/// In the future we would like to have some methods perhaps around these fields
/// such you dont have to deal with raw pointers directly or as nvmf tcp becomes
/// more stable write our own implementation of bdev_create()
#[repr(C)]
pub struct NvmeCreateCtx {
    // the name is used internally to construct bdev names this seems rather
    // odd as the
    /// name of the to be created bdev
    pub name: *const libc::c_char,
    /// array of bdev names per namespace for example, this will create
    /// my_name{n}{i}
    pub names: [*const libc::c_char; MAX_NAMESPACES],
    /// the amount of actual bdevs that are created
    pub count: u32,
    /// nvme transport id contains the information needed to connect to a
    /// remote target
    pub transport_id: spdk_sys::spdk_nvme_transport_id,
    /// nvme hostid contains the information that describes the client this
    /// field is optional when not supplied, the nvme stack internally
    /// creates a random NQNs.
    pub host_id: spdk_sys::spdk_nvme_host_id,
}

impl Drop for NvmeCreateCtx {
    fn drop(&mut self) {
        let _ = unsafe { CString::from_raw(self.name as *mut i8) };
    }
}

impl From<NvmfBdev> for NvmeCreateCtx {
    fn from(a: NvmfBdev) -> Self {
        NvmeCreateCtx::new(&a)
    }
}

impl NvmeCreateCtx {
    pub fn new(args: &NvmfBdev) -> Self {
        let mut transport = spdk_sys::spdk_nvme_transport_id::default();
        let mut hostid = spdk_sys::spdk_nvme_host_id::default();

        unsafe {
            std::ptr::copy_nonoverlapping(
                args.traddr.as_ptr() as *const _ as *mut libc::c_void,
                &mut transport.traddr[0] as *const _ as *mut libc::c_void,
                args.traddr.len(),
            );
            std::ptr::copy_nonoverlapping(
                args.trsvcid.as_ptr() as *const _ as *mut libc::c_void,
                &mut transport.trsvcid[0] as *const _ as *mut libc::c_void,
                args.trsvcid.len(),
            );
            std::ptr::copy_nonoverlapping(
                args.subnqn.as_ptr() as *const _ as *mut libc::c_void,
                &mut transport.subnqn[0] as *const _ as *mut libc::c_void,
                args.subnqn.len(),
            );
        }

        // we can not test RDMA nor IPv6 at the moment
        transport.trtype = SPDK_NVME_TRANSPORT_TCP;
        transport.adrfam = SPDK_NVMF_ADRFAM_IPV4;

        // the following parameters are optional, but we should fill them in to
        // get a proper topo mapping of the whole thing as soon as we
        // get it to work to begin with.
        if !args.hostsvcid.is_empty() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    args.hostsvcid.as_ptr() as *const _ as *mut libc::c_void,
                    &mut hostid.hostaddr[0] as *const _ as *mut libc::c_void,
                    args.hostsvcid.len(),
                );
            }
        }

        if !args.hostaddr.is_empty() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    args.hostaddr.as_ptr() as *const _ as *mut libc::c_void,
                    &mut hostid.hostaddr[0] as *const _ as *mut libc::c_void,
                    args.hostaddr.len(),
                );
            }
        }

        NvmeCreateCtx {
            host_id: hostid,
            transport_id: transport,
            count: MAX_NAMESPACES as u32,
            name: CString::new(args.name.clone()).unwrap().into_raw(), /* drop this */
            names: [std::ptr::null_mut() as *mut libc::c_char; MAX_NAMESPACES],
        }
    }
}
