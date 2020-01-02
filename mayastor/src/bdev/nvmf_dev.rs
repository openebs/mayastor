use crate::{
    core::Bdev,
    executor::{cb_arg, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, BdevCreateDestroy},
};
use futures::channel::oneshot;
use snafu::{ResultExt, Snafu};
use spdk_sys::{
    spdk_bdev_nvme_create,
    spdk_bdev_nvme_delete,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
};
use std::{convert::TryFrom, ffi::CString, os::raw::c_void};
use url::Url;

#[derive(Debug, Snafu)]
pub enum NvmfParseError {
    #[snafu(display("Missing path component"))]
    PathMissing {},
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
    /// Enable protection information checking of the Application Tag field
    pub prchk_guard: bool,
}

impl NvmfBdev {
    unsafe extern "C" fn nvme_done(
        ctx: *mut c_void,
        _bdev_count: usize,
        rc: i32,
    ) {
        let sender =
            Box::from_raw(ctx as *mut oneshot::Sender<ErrnoResult<()>>);

        sender
            .send(errno_result_from_i32((), rc))
            .expect("NVMe creation cb receiver is gone");
    }

    /// async function to construct a bdev given a NvmfUri
    pub async fn create(self) -> Result<String, BdevCreateDestroy> {
        let mut ctx = NvmeCreateCtx::new(&self);
        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();

        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevCreateDestroy::BdevExists {
                name: self.name.clone(),
            });
        }

        let c_hostnqn;
        // TODO add this to ctx
        let hostnqn = if self.hostnqn.is_empty() {
            std::ptr::null_mut()
        } else {
            c_hostnqn = CString::new(self.hostnqn.clone()).unwrap();
            c_hostnqn.as_ptr()
        };

        let mut flags: u32 = 0;

        if self.prchk_reftag {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
        }
        if self.prchk_guard {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
        }

        let errno = unsafe {
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
        errno_result_from_i32((), errno).context(nexus_uri::InvalidParams {
            name: self.name.clone(),
        })?;

        receiver
            .await
            .expect("Cancellation is not supported")
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        Ok(unsafe {
            std::ffi::CStr::from_ptr(ctx.names[0])
                .to_str()
                .unwrap()
                .to_string()
        })
    }

    /// destroy nvme bdev
    pub fn destroy(self) -> Result<(), BdevCreateDestroy> {
        // the namespace instance is appended to the nvme bdev, we currently
        // only support one namespace per bdev.

        if Bdev::lookup_by_name(&format!("{}{}", &self.name, "n1")).is_none() {
            return Err(BdevCreateDestroy::BdevNotFound {
                name: self.name,
            });
        }
        let cname = CString::new(self.name.clone()).unwrap();
        let errno = unsafe { spdk_bdev_nvme_delete(cname.as_ptr()) };

        errno_result_from_i32((), errno).context(nexus_uri::DestroyBdev {
            name: self.name,
        })
    }
}

/// converts a nvmf URL to NVMF args
impl TryFrom<&Url> for NvmfBdev {
    type Error = NvmfParseError;

    fn try_from(u: &Url) -> std::result::Result<Self, Self::Error> {
        let mut n = NvmfBdev::default();

        // defaults we currently only support
        n.trtype = "TCP".into();
        n.adrfam = "IPv4".into();
        n.subnqn = match u
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            None => return Err(NvmfParseError::PathMissing {}),
            // TODO validate that the nqn is a valid v4 UUID
            Some(s) => s[0].to_string(),
        };

        n.trsvcid = match u.port() {
            Some(port) => port.to_string(),
            None => "4420".to_owned(),
        };

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

/// This C structure is passed as an argument that internally is passed again,
/// to a callback. This means, that for now, we accept certain design choices
/// being made for now. One of which is the fact that creating a NVMe
/// controller, will result in bdevs showing up. This implies that the URI
/// refers to the controller rather than the bdev names directly. Thus, we need
/// to be careful when we try to destroy a NVMe bdev (you can't) you can only
/// destroy the controller.
#[repr(C)]
pub struct NvmeCreateCtx {
    /// name of the NVMe controller we create. For each name space we will get
    /// a name + n + I where I is an ordinal number starting from 1. We
    /// allow only for one namespace to be created per controller
    /// which is defined by [MAX_NAMESPACES]
    pub name: *const libc::c_char,
    /// the array holding names of the created bdevs
    pub names: [*const libc::c_char; MAX_NAMESPACES],
    /// the amount of actual bdevs that are created
    pub count: u32,
    /// NVMe transport id contains the information needed to connect to a
    /// remote target
    pub transport_id: spdk_sys::spdk_nvme_transport_id,
    /// The NVMe host id contains the information that describes the client
    /// this field is optional when not supplied, the NVMe stack internally
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

        // we cannot test RDMA nor IPv6 at the moment
        transport.trtype = SPDK_NVME_TRANSPORT_TCP;
        transport.adrfam = SPDK_NVMF_ADRFAM_IPV4;

        // the following parameters are optional, but we should fill them in to
        // get a proper topology mapping of the whole thing as soon as we
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
