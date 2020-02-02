use std::{convert::TryFrom, ffi::CString, os::raw::c_void};

use futures::channel::oneshot;
use snafu::{ResultExt, Snafu};
use url::Url;

use spdk_sys::{
    spdk_bdev_nvme_create,
    spdk_bdev_nvme_delete,
    spdk_nvme_host_id,
    spdk_nvme_transport_id,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, BdevCreateDestroy},
};

#[derive(Debug, Snafu)]
pub enum NvmfParseError {
    #[snafu(display("Missing path component"))]
    PathMissing {},
}

#[derive(Debug, Default)]
pub struct NvmeCtlAttachReq {
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

impl NvmeCtlAttachReq {
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
        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();

        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevCreateDestroy::BdevExists {
                name: self.name.clone(),
            });
        }

        let mut trid = spdk_nvme_transport_id::default();
        let mut hostid = spdk_nvme_host_id::default();
        let tridstring = "TCP";

        unsafe {
            std::ptr::copy_nonoverlapping(
                tridstring.as_ptr() as *const _ as *mut libc::c_void,
                &mut trid.trstring[0] as *const _ as *mut libc::c_void,
                tridstring.len(),
            );
            std::ptr::copy_nonoverlapping(
                self.traddr.as_ptr() as *const _ as *mut libc::c_void,
                &mut trid.traddr[0] as *const _ as *mut libc::c_void,
                self.traddr.len(),
            );
            std::ptr::copy_nonoverlapping(
                self.trsvcid.as_ptr() as *const _ as *mut libc::c_void,
                &mut trid.trsvcid[0] as *const _ as *mut libc::c_void,
                self.trsvcid.len(),
            );
            std::ptr::copy_nonoverlapping(
                self.subnqn.as_ptr() as *const _ as *mut libc::c_void,
                &mut trid.subnqn[0] as *const _ as *mut libc::c_void,
                self.subnqn.len(),
            );
        }

        trid.trtype = SPDK_NVME_TRANSPORT_TCP;
        trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;

        if !self.hostsvcid.is_empty() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.hostsvcid.as_ptr() as *const _ as *mut libc::c_void,
                    &mut hostid.hostaddr[0] as *const _ as *mut libc::c_void,
                    self.hostsvcid.len(),
                );
            }
        }

        if !self.hostaddr.is_empty() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.hostaddr.as_ptr() as *const _ as *mut libc::c_void,
                    &mut hostid.hostaddr[0] as *const _ as *mut libc::c_void,
                    self.hostaddr.len(),
                );
            }
        }

        let mut flags: u32 = 0;

        if self.prchk_reftag {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
        }
        if self.prchk_guard {
            flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
        }

        let ctl_name = CString::new(self.name.clone()).unwrap();
        let mut ctx = NvmeCreateCtx::new(self);

        let errno = unsafe {
            spdk_bdev_nvme_create(
                &mut trid,
                &mut hostid,
                ctl_name.as_ptr(),
                &mut ctx.names[0],
                ctx.count,
                std::ptr::null_mut(),
                flags,
                Some(NvmeCtlAttachReq::nvme_done),
                cb_arg(sender),
            )
        };
        errno_result_from_i32((), errno).context(nexus_uri::InvalidParams {
            name: ctx.req.name.clone(),
        })?;

        receiver
            .await
            .expect("Cancellation is not supported")
            .context(nexus_uri::CreateBdev {
                name: ctx.req.name,
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
impl TryFrom<&Url> for NvmeCtlAttachReq {
    type Error = NvmfParseError;

    fn try_from(u: &Url) -> std::result::Result<Self, Self::Error> {
        let mut n = NvmeCtlAttachReq::default();

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

#[repr(C)]
pub struct NvmeCreateCtx {
    req: NvmeCtlAttachReq,
    names: [*const libc::c_char; MAX_NAMESPACES],
    count: u32,
}

impl NvmeCreateCtx {
    pub fn new(args: NvmeCtlAttachReq) -> Self {
        NvmeCreateCtx {
            req: args,
            count: MAX_NAMESPACES as u32,
            names: [std::ptr::null_mut() as *mut libc::c_char; MAX_NAMESPACES],
        }
    }
}
