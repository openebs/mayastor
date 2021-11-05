use std::{convert::TryFrom, time::Duration};

use url::{ParseError, Url};

use crate::{
    error::NvmeError,
    nvme_namespaces::{NvmeDevice, NvmeDeviceList},
    nvme_tree::{
        NvmeCtrlrIterator,
        NvmeHostIterator,
        NvmeRoot,
        NvmeSubsystemIterator,
    },
};

/// Wrapper for caller-owned C-strings from libnvme
pub struct NvmeStringWrapper {
    s: *mut i8,
}

impl NvmeStringWrapper {
    pub fn new(s: *mut i8) -> Self {
        NvmeStringWrapper {
            s,
        }
    }
    pub fn as_ptr(&self) -> *const i8 {
        self.s
    }
}

impl Drop for NvmeStringWrapper {
    fn drop(&mut self) {
        unsafe { libc::free(self.s as *mut _) }
    }
}

/// Structure representing an NVMe-oF target
pub struct NvmeTarget {
    host: String,
    port: u16,
    subsysnqn: String,
    trtype: String,
}

impl TryFrom<String> for NvmeTarget {
    type Error = NvmeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        NvmeTarget::try_from(value.as_str())
    }
}

impl TryFrom<&str> for NvmeTarget {
    type Error = NvmeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(value).map_err(|source| NvmeError::UrlError {
            source,
        })?;

        let trtype = match url.scheme() {
            "nvmf" | "nvmf+tcp" => Ok("tcp"),
            _ => Err(NvmeError::UrlError {
                source: ParseError::IdnaError,
            }),
        }?
        .into();

        let host = url
            .host_str()
            .ok_or(NvmeError::UrlError {
                source: ParseError::EmptyHost,
            })?
            .into();

        let subnqn = match url.path_segments() {
            None => Err(NvmeError::UrlError {
                source: ParseError::RelativeUrlWithCannotBeABaseBase,
            }),
            Some(s) => {
                let segments = s.collect::<Vec<&str>>();
                if segments[0].is_empty() {
                    Err(NvmeError::UrlError {
                        source: ParseError::RelativeUrlWithCannotBeABaseBase,
                    })
                } else {
                    Ok(segments[0].to_string())
                }
            }
        }?;

        Ok(Self {
            trtype,
            host,
            port: url.port().unwrap_or(4420),
            subsysnqn: subnqn,
        })
    }
}

impl NvmeTarget {
    /// Connect to NVMe target
    /// Returns vector of NVMe devices that were connected
    pub fn connect(&self) -> Result<Vec<NvmeDevice>, NvmeError> {
        if self.trtype != "tcp" {
            return Err(NvmeError::TransportError {
                trtype: self.trtype.clone(),
            });
        }

        let r = NvmeRoot::new(unsafe { crate::nvme_scan(std::ptr::null()) });
        let hostnqn =
            NvmeStringWrapper::new(unsafe { crate::nvmf_hostnqn_from_file() });
        let hostid =
            NvmeStringWrapper::new(unsafe { crate::nvmf_hostid_from_file() });

        let h = unsafe {
            crate::nvme_lookup_host(
                r.as_mut_ptr(),
                hostnqn.as_ptr(),
                hostid.as_ptr(),
            )
        };
        if h.is_null() {
            return Err(NvmeError::LookupHostError {
                rc: -libc::ENOMEM,
            });
        }

        let subsysnqn = std::ffi::CString::new(self.subsysnqn.clone()).unwrap();
        let transport = std::ffi::CString::new("tcp").unwrap();
        let traddr = std::ffi::CString::new(self.host.clone()).unwrap();
        let host_traddr = std::ptr::null();
        let host_iface = std::ptr::null();
        let trsvcid = std::ffi::CString::new(self.port.to_string()).unwrap();
        let nvme_ctrl = unsafe {
            crate::nvme_create_ctrl(
                subsysnqn.as_ptr(),
                transport.as_ptr(),
                traddr.as_ptr(),
                host_traddr,
                host_iface,
                trsvcid.as_ptr(),
            )
        };
        let cfg = crate::nvme_fabrics_config {
            queue_size: 0,
            nr_io_queues: 0,
            reconnect_delay: 0,
            ctrl_loss_tmo: crate::NVMF_DEF_CTRL_LOSS_TMO as i32,
            fast_io_fail_tmo: 0,
            keep_alive_tmo: 0,
            nr_write_queues: 0,
            nr_poll_queues: 0,
            tos: -1,
            duplicate_connect: false,
            disable_sqflow: false,
            hdr_digest: false,
            data_digest: false,
        };
        let ret = unsafe {
            crate::nvmf_add_ctrl(h, nvme_ctrl, &cfg, cfg.disable_sqflow)
        };
        if ret != 0 {
            return Err(NvmeError::AddCtrlrError {
                rc: ret,
            });
        }

        let mut retries = 10;
        let mut all_nvme_devices;
        loop {
            std::thread::sleep(Duration::from_millis(1000));

            all_nvme_devices = NvmeDeviceList::new()
                .filter_map(Result::ok)
                .filter(|b| b.subsysnqn == self.subsysnqn)
                .collect::<Vec<_>>();

            retries -= 1;
            if retries == 0 || !all_nvme_devices.is_empty() {
                break;
            }
        }

        Ok(all_nvme_devices)
    }

    /// Disconnect from NVMe target
    /// Returns number of controllers disconnected
    pub fn disconnect(&self) -> Result<usize, NvmeError> {
        let r = NvmeRoot::new(unsafe { crate::nvme_scan(std::ptr::null()) });
        let mut i = 0;
        let hostiter = NvmeHostIterator::new(&r);
        for host in hostiter {
            let subsysiter = NvmeSubsystemIterator::new(host);
            for subsys in subsysiter {
                let cstr = unsafe {
                    std::ffi::CStr::from_ptr(crate::nvme_subsystem_get_nqn(
                        subsys,
                    ))
                };
                if cstr.to_str().unwrap() != self.subsysnqn {
                    continue;
                }
                let ctrlriter = NvmeCtrlrIterator::new(subsys);
                for c in ctrlriter {
                    if unsafe { crate::nvme_disconnect_ctrl(c) == 0 } {
                        i += 1;
                    }
                }
            }
        }
        Ok(i)
    }
}

#[test]
fn nvme_parse_uri() {
    let target =
        NvmeTarget::try_from("nvmf://1.2.3.4:1234/testnqn.what-ever.foo")
            .unwrap();

    assert_eq!(target.port, 1234);
    assert_eq!(target.host, "1.2.3.4");
    assert_eq!(target.trtype, "tcp");
    assert_eq!(target.subsysnqn, "testnqn.what-ever.foo");

    let target =
        NvmeTarget::try_from("nvmf+tcp://1.2.3.4:1234/testnqn.what-ever.foo")
            .unwrap();

    assert_eq!(target.port, 1234);
    assert_eq!(target.host, "1.2.3.4");
    assert_eq!(target.trtype, "tcp");
    assert_eq!(target.subsysnqn, "testnqn.what-ever.foo");
}
