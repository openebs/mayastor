//! Handlers for custom NVMe Admin commands

use spdk_sys::{spdk_bdev, spdk_bdev_desc, spdk_io_channel, spdk_nvmf_request};

use crate::{
    bdev::nexus::nexus_io::nvme_admin_opc,
    core::Bdev,
    replica::Replica,
};

/// NVMf custom command handler for opcode c0h
/// Called from nvmf_ctrlr_process_admin_cmd
/// Return: <0 for any error, caller handles it as unsupported opcode
extern "C" fn nvmf_create_snapshot_hdlr(req: *mut spdk_nvmf_request) -> i32 {
    debug!("nvmf_create_snapshot_hdlr {:?}", req);

    let subsys = unsafe { spdk_sys::spdk_nvmf_request_get_subsystem(req) };
    if subsys.is_null() {
        debug!("subsystem is null");
        return -1;
    }

    /* Only process this request if it has exactly one namespace */
    if unsafe { spdk_sys::spdk_nvmf_subsystem_get_max_nsid(subsys) } != 1 {
        debug!("multiple namespaces");
        return -1;
    }

    /* Forward to first namespace if it supports NVME admin commands */
    let mut bdev: *mut spdk_bdev = std::ptr::null_mut();
    let mut desc: *mut spdk_bdev_desc = std::ptr::null_mut();
    let mut ch: *mut spdk_io_channel = std::ptr::null_mut();
    let rc = unsafe {
        spdk_sys::spdk_nvmf_request_get_bdev(
            1, req, &mut bdev, &mut desc, &mut ch,
        )
    };
    if rc != 0 {
        /* No bdev found for this namespace. Continue. */
        debug!("no bdev found");
        return -1;
    }

    let bd = Bdev::from(bdev);
    if let Some(replica) = Replica::from_bdev(&bd) {
        let cmd = unsafe { &*spdk_sys::spdk_nvmf_request_get_cmd(req) };
        let snapshot_time = unsafe {
            cmd.__bindgen_anon_1.cdw10 as u64
                | (cmd.__bindgen_anon_2.cdw11 as u64) << 32
        };
        let snapshot_name = format!("{}-snap-{}", bd.name(), snapshot_time);
        replica.create_snapshot(req, &snapshot_name);
        1 // SPDK_NVMF_REQUEST_EXEC_STATUS_ASYNCHRONOUS
    } else {
        -1
    }
}

/// Register custom NVMe admin command handler
pub fn setup_create_snapshot_hdlr() {
    unsafe {
        spdk_sys::spdk_nvmf_set_custom_admin_cmd_hdlr(
            nvme_admin_opc::CREATE_SNAPSHOT,
            Some(nvmf_create_snapshot_hdlr),
        );
    }
}
