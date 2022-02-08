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

use spdk_rs::libspdk::{
    bdev_nvme_create,
    bdev_nvme_delete,
    spdk_nvme_transport_id,
    SPDK_NVME_IO_FLAGS_PRCHK_GUARD,
    SPDK_NVME_IO_FLAGS_PRCHK_REFTAG,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
};

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    core::Bdev,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
    nexus_uri::{self, NexusBdevError},
};

const DEFAULT_NVMF_PORT: u16 = 4420;

#[derive(Debug)]
pub(super) struct Nvmf {
    /// name of the nvme controller and base name of the bdev
    /// that should be created for each namespace found
    name: String,
    /// alias which can be used to open the bdev
    alias: String,
    /// the remote target host (address)
    host: String,
    /// the transport service id (ie. port)
    port: u16,
    /// the nqn of the subsystem we want to connect to
    subnqn: String,
    /// Enable protection information checking (reftag, guard)
    prchk_flags: u32,
    /// uuid of the spdk bdev
    uuid: Option<uuid::Uuid>,
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
                    value: value.to_string(),
                },
            )? {
                prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
            }
        }

        if let Some(value) = parameters.remove("guard") {
            if uri::boolean(&value, true).context(
                nexus_uri::BoolParamParseError {
                    uri: url.to_string(),
                    parameter: String::from("guard"),
                    value: value.to_string(),
                },
            )? {
                prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
            }
        }

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: url.to_string(),
            },
        )?;

        reject_unknown_parameters(url, parameters)?;

        Ok(Nvmf {
            name: url[url::Position::BeforeHost .. url::Position::AfterPath]
                .into(),
            alias: url.to_string(),
            host: host.to_string(),
            port: url.port().unwrap_or(DEFAULT_NVMF_PORT),
            subnqn: segments[0].to_string(),
            prchk_flags,
            uuid,
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
        if Bdev::lookup_by_name(&self.get_name()).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.get_name(),
            });
        }

        extern "C" fn done_nvme_create_cb(
            arg: *mut c_void,
            bdev_count: c_ulong,
            errno: c_int,
        ) {
            let sender = unsafe {
                Box::from_raw(arg as *mut oneshot::Sender<ErrnoResult<usize>>)
            };

            sender
                .send(errno_result_from_i32(bdev_count as usize, errno))
                .expect("done callback receiver side disappeared");
        }

        let cname = CString::new(self.name.clone()).unwrap();
        let mut context = NvmeCreateContext::new(self);

        let (sender, receiver) = oneshot::channel::<ErrnoResult<usize>>();

        let errno = unsafe {
            bdev_nvme_create(
                &mut context.trid,
                cname.as_ptr(),
                &mut context.names[0],
                context.count,
                context.prchk_flags,
                Some(done_nvme_create_cb),
                cb_arg(sender),
                std::ptr::null_mut(),
                false,
                0,
                0,
                0,
            )
        };

        errno_result_from_i32((), errno).context(
            nexus_uri::CreateBdevInvalidParams {
                name: self.name.clone(),
            },
        )?;

        let bdev_count = receiver
            .await
            .context(nexus_uri::CancelBdev {
                name: self.name.clone(),
            })?
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        if bdev_count == 0 {
            error!("No nvme bdev created, no namespaces?");
            // Remove partially created nvme bdev which doesn't show up in
            // the list of bdevs
            let errno =
                unsafe { bdev_nvme_delete(cname.as_ptr(), std::ptr::null()) };
            info!(
                "removed partially created bdev {}, returned {}",
                self.name, errno
            );
            return Err(NexusBdevError::BdevNotFound {
                name: self.name.clone(),
            });
        }
        if let Some(mut bdev) = Bdev::lookup_by_name(&self.get_name()) {
            if let Some(u) = self.uuid {
                if bdev.uuid_as_string() != u.to_hyphenated().to_string() {
                    error!("Connected to device {} but expect to connect to {} instead", bdev.uuid_as_string(), u.to_hyphenated().to_string());
                }
            };
            if !bdev.as_mut().add_alias(&self.alias) {
                error!(
                    "Failed to add alias {} to device {}",
                    self.alias,
                    self.get_name()
                );
            }
        };

        Ok(unsafe { CStr::from_ptr(context.names[0]) }
            .to_str()
            .unwrap()
            .to_string())
    }

    /// Destroy the given NVMF bdev
    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        match Bdev::lookup_by_name(&self.get_name()) {
            Some(mut bdev) => {
                bdev.as_mut().remove_alias(&self.alias);
                let cname = CString::new(self.name.clone()).unwrap();

                let errno = unsafe {
                    bdev_nvme_delete(cname.as_ptr(), std::ptr::null())
                };

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
                name: self.get_name(),
            }),
        }
    }
}

/// The Maximum number of namespaces that a single bdev will connect to
const MAX_NAMESPACES: usize = 1;

struct NvmeCreateContext {
    trid: spdk_nvme_transport_id,
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

        trid.trtype = SPDK_NVME_TRANSPORT_TCP;
        trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;

        NvmeCreateContext {
            trid,
            names: [std::ptr::null_mut() as *mut c_char; MAX_NAMESPACES],
            prchk_flags: nvmf.prchk_flags,
            count: MAX_NAMESPACES as u32,
        }
    }
}
