use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::{CStr, CString},
    os::raw::{c_char, c_int, c_ulong, c_void},
    ptr::copy_nonoverlapping,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;

use spdk_sys::{
    self,
    spdk_bdev_nvme_create,
    spdk_bdev_nvme_delete,
    spdk_nvme_host_id,
    spdk_nvme_transport_id,
};

use crate::{
    bdev::{util::uri, CreateDestroy, GetName},
    core::Bdev,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, NexusBdevError},
};

const DEFAULT_NVMF_PORT: u16 = 4420;

#[derive(Debug)]
pub(super) struct Nvmf {
    /// name of the bdev that should be created
    name: String,
    /// the remote target host (address)
    host: String,
    /// the transport service id (ie. port)
    port: u16,
    /// the nqn of the subsystem we want to connect to
    subnqn: String,
    /// Enable protection information checking (reftag, guard)
    prchk_flags: u32,
}

/// Convert a URI to an Nvmf "object"
impl TryFrom<&Url> for Nvmf {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let host =
            url.host_str().ok_or_else(|| NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("missing host"),
            })?;

        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segment"),
            });
        }

        if segments.len() > 1 {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("too many path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let mut prchk_flags: u32 = 0;

        if let Some(value) = parameters.remove("reftag") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("reftag"),
                },
            )? {
                prchk_flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
            }
        }

        if let Some(value) = parameters.remove("guard") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("guard"),
                },
            )? {
                prchk_flags |= spdk_sys::SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
            }
        }

        if let Some(keys) = uri::keys(parameters) {
            warn!("ignored parameters: {}", keys);
        }

        Ok(Nvmf {
            name: url.to_string(),
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NVMF_PORT),
            subnqn: segments[0].to_string(),
            prchk_flags,
        })
    }
}

impl GetName for Nvmf {
    fn get_name(&self) -> String {
        // The namespace instance is appended to the nvme bdev.
        // We currently only support one namespace per bdev.
        format!("{}n1", self.name)
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Nvmf {
    type Error = NexusBdevError;

    /// Create an NVMF bdev
    async fn create(&self) -> Result<String, Self::Error> {
        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        extern "C" fn done_nvme_create_cb(
            arg: *mut c_void,
            _bdev_count: c_ulong,
            errno: c_int,
        ) {
            let sender = unsafe {
                Box::from_raw(arg as *mut oneshot::Sender<ErrnoResult<()>>)
            };

            sender
                .send(errno_result_from_i32((), errno))
                .expect("done callback receiver side disappeared");
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let mut context = NvmeCreateContext::new(self);

        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();

        let errno = unsafe {
            spdk_bdev_nvme_create(
                &mut context.trid,
                &mut context.hostid,
                cname.as_ptr(),
                &mut context.names[0],
                context.count,
                std::ptr::null_mut(),
                context.prchk_flags,
                Some(done_nvme_create_cb),
                cb_arg(sender),
            )
        };

        errno_result_from_i32((), errno).context(nexus_uri::InvalidParams {
            name: self.name.clone(),
        })?;

        receiver
            .await
            .context(nexus_uri::CancelBdev {
                name: self.name.clone(),
            })?
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        Ok(unsafe { CStr::from_ptr(context.names[0]) }
            .to_str()
            .unwrap()
            .to_string())
    }

    /// Destroy the given NVMF bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        match Bdev::lookup_by_name(&self.get_name()) {
            Some(_) => {
                let cname = CString::new(self.name.clone()).unwrap();

                let errno = unsafe { spdk_bdev_nvme_delete(cname.as_ptr()) };

                async {
                    errno_result_from_i32((), errno).context(
                        nexus_uri::DestroyBdev {
                            name: self.name.clone(),
                        },
                    )
                }
                .await
            }
            None => Err(NexusBdevError::BdevNotFound {
                name: self.name.clone(),
            }),
        }
    }
}

/// The Maximum number of namespaces that a single bdev will connect to
const MAX_NAMESPACES: usize = 1;

struct NvmeCreateContext {
    trid: spdk_nvme_transport_id,
    hostid: spdk_nvme_host_id,
    names: [*const c_char; MAX_NAMESPACES],
    prchk_flags: u32,
    count: u32,
}

unsafe impl Send for NvmeCreateContext {}

impl NvmeCreateContext {
    pub fn new(nvmf: &Nvmf) -> NvmeCreateContext {
        let port = format!("{}", nvmf.port);
        let protocol = "TCP";

        let mut trid = spdk_nvme_transport_id::default();

        unsafe {
            copy_nonoverlapping(
                protocol.as_ptr() as *const c_void,
                &mut trid.trstring[0] as *const _ as *mut c_void,
                protocol.len(),
            );
            copy_nonoverlapping(
                nvmf.host.as_ptr() as *const c_void,
                &mut trid.traddr[0] as *const _ as *mut c_void,
                nvmf.host.len(),
            );
            copy_nonoverlapping(
                port.as_ptr() as *const c_void,
                &mut trid.trsvcid[0] as *const _ as *mut c_void,
                port.len(),
            );
            copy_nonoverlapping(
                nvmf.subnqn.as_ptr() as *const c_void,
                &mut trid.subnqn[0] as *const _ as *mut c_void,
                nvmf.subnqn.len(),
            );
        }

        trid.trtype = spdk_sys::SPDK_NVME_TRANSPORT_TCP;
        trid.adrfam = spdk_sys::SPDK_NVMF_ADRFAM_IPV4;

        let hostid = spdk_nvme_host_id::default();

        NvmeCreateContext {
            trid,
            hostid,
            names: [std::ptr::null_mut() as *mut c_char; MAX_NAMESPACES],
            prchk_flags: nvmf.prchk_flags,
            count: MAX_NAMESPACES as u32,
        }
    }
}
