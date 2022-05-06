//! Handlers for custom NVMe Admin commands

use std::{
    convert::TryFrom,
    ffi::c_void,
    ptr::NonNull,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    bdev::nexus,
    core::{Bdev, Reactors},
    lvs::Lvol,
};

use spdk_rs::{
    libspdk::{
        nvme_cmd_cdw10_get,
        nvme_cmd_cdw10_get_val,
        nvme_cmd_cdw11_get,
        nvme_cmd_cdw11_get_val,
        nvme_status_get,
        spdk_bdev,
        spdk_bdev_desc,
        spdk_bdev_io,
        spdk_io_channel,
        spdk_nvme_cmd,
        spdk_nvme_cpl,
        spdk_nvme_status,
        spdk_nvmf_bdev_ctrlr_nvme_passthru_admin,
        spdk_nvmf_request,
        spdk_nvmf_request_get_bdev,
        spdk_nvmf_request_get_cmd,
        spdk_nvmf_request_get_response,
        spdk_nvmf_request_get_subsystem,
        spdk_nvmf_set_custom_admin_cmd_hdlr,
        spdk_nvmf_subsystem_get_max_nsid,
    },
    nvme_admin_opc,
};

#[derive(Clone)]
pub struct NvmeCpl(pub(crate) NonNull<spdk_nvme_cpl>);

impl NvmeCpl {
    /// Returns the NVMe status
    pub(crate) fn status(&mut self) -> &mut spdk_nvme_status {
        unsafe { &mut *nvme_status_get(self.0.as_mut()) }
    }
}

#[derive(Clone)]
pub struct NvmfReq(pub(crate) NonNull<spdk_nvmf_request>);

impl NvmfReq {
    /// Returns the NVMe completion
    pub(crate) fn response(&self) -> NvmeCpl {
        NvmeCpl(
            NonNull::new(unsafe {
                &mut *spdk_nvmf_request_get_response(self.0.as_ptr())
            })
            .unwrap(),
        )
    }
}

impl From<*mut c_void> for NvmfReq {
    fn from(ptr: *mut c_void) -> Self {
        NvmfReq(NonNull::new(ptr as *mut spdk_nvmf_request).unwrap())
    }
}

/// Set the snapshot time in an spdk_nvme_cmd struct to the current time
/// Returns seconds since Unix epoch
pub fn set_snapshot_time(cmd: &mut spdk_nvme_cmd) -> u64 {
    // encode snapshot time in cdw10/11
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    unsafe {
        *nvme_cmd_cdw10_get(&mut *cmd) = now as u32;
        *nvme_cmd_cdw11_get(&mut *cmd) = (now >> 32) as u32;
    }
    now as u64
}

/// NVMf custom command handler for opcode c0h
/// Called from nvmf_ctrlr_process_admin_cmd
/// Return: <0 for any error, caller handles it as unsupported opcode
extern "C" fn nvmf_create_snapshot_hdlr(req: *mut spdk_nvmf_request) -> i32 {
    debug!("nvmf_create_snapshot_hdlr {:?}", req);

    let subsys = unsafe { spdk_nvmf_request_get_subsystem(req) };
    if subsys.is_null() {
        debug!("subsystem is null");
        return -1;
    }

    /* Only process this request if it has exactly one namespace */
    if unsafe { spdk_nvmf_subsystem_get_max_nsid(subsys) } != 1 {
        debug!("multiple namespaces");
        return -1;
    }

    /* Forward to first namespace if it supports NVME admin commands */
    let mut bdev: *mut spdk_bdev = std::ptr::null_mut();
    let mut desc: *mut spdk_bdev_desc = std::ptr::null_mut();
    let mut ch: *mut spdk_io_channel = std::ptr::null_mut();
    let rc = unsafe {
        spdk_nvmf_request_get_bdev(1, req, &mut bdev, &mut desc, &mut ch)
    };
    if rc != 0 {
        /* No bdev found for this namespace. Continue. */
        debug!("no bdev found");
        return -1;
    }

    let bd = unsafe { Bdev::checked_from_ptr(bdev).unwrap() };
    if bd.driver() == nexus::NEXUS_MODULE_NAME {
        // Received command on a published Nexus
        set_snapshot_time(unsafe { &mut *spdk_nvmf_request_get_cmd(req) });
        unsafe {
            spdk_nvmf_bdev_ctrlr_nvme_passthru_admin(bdev, desc, ch, req, None)
        }
    } else if let Ok(lvol) = Lvol::try_from(bd) {
        // Received command on a shared replica (lvol)
        let cmd = unsafe { spdk_nvmf_request_get_cmd(req) };
        let snapshot_time = unsafe {
            nvme_cmd_cdw10_get_val(cmd) as u64
                | (nvme_cmd_cdw11_get_val(cmd) as u64) << 32
        };
        let snapshot_name =
            Lvol::format_snapshot_name(&lvol.name(), snapshot_time);
        let nvmf_req = NvmfReq(NonNull::new(req).unwrap());
        // Blobfs operations must be on md_thread
        Reactors::master().send_future(async move {
            lvol.create_snapshot(&nvmf_req, &snapshot_name).await;
        });
        1 // SPDK_NVMF_REQUEST_EXEC_STATUS_ASYNCHRONOUS
    } else {
        debug!("unsupported bdev driver");
        -1
    }
}

pub fn create_snapshot(lvol: Lvol, cmd: &spdk_nvme_cmd, io: *mut spdk_bdev_io) {
    let snapshot_time = unsafe {
        nvme_cmd_cdw10_get_val(&*cmd) as u64
            | (nvme_cmd_cdw11_get_val(&*cmd) as u64) << 32
    };
    let snapshot_name = Lvol::format_snapshot_name(&lvol.name(), snapshot_time);
    // Blobfs operations must be on md_thread
    Reactors::master().send_future(async move {
        lvol.create_snapshot_local(io, &snapshot_name).await;
    });
}

/// Register custom NVMe admin command handler
pub fn setup_create_snapshot_hdlr() {
    unsafe {
        spdk_nvmf_set_custom_admin_cmd_hdlr(
            nvme_admin_opc::CREATE_SNAPSHOT,
            Some(nvmf_create_snapshot_hdlr),
        );
    }
}
