//! Handlers for custom NVMe Admin commands

use std::{
    ffi::c_void,
    ptr::NonNull,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    bdev::{nexus, nvmx::NvmeSnapshotMessage},
    core::{Bdev, Reactors, SnapshotParams},
};

use crate::{
    core::{ToErrno, UntypedBdev},
    replica_backend::ReplicaFactory,
};
use spdk_rs::{
    libspdk::{
        nvme_cmd_cdw10_get,
        nvme_cmd_cdw11_get,
        nvme_status_get,
        spdk_bdev,
        spdk_bdev_desc,
        spdk_io_channel,
        spdk_nvme_cmd,
        spdk_nvme_cpl,
        spdk_nvme_status,
        spdk_nvmf_bdev_ctrlr_nvme_passthru_admin,
        spdk_nvmf_request,
        spdk_nvmf_request_complete,
        spdk_nvmf_request_copy_to_buf,
        spdk_nvmf_request_get_bdev,
        spdk_nvmf_request_get_cmd,
        spdk_nvmf_request_get_response,
        spdk_nvmf_request_get_subsystem,
        spdk_nvmf_set_custom_admin_cmd_hdlr,
        spdk_nvmf_subsystem_get_max_nsid,
    },
    nvme_admin_opc,
};

#[warn(unused_variables)]
#[derive(Clone)]
pub struct NvmeCpl(pub(crate) NonNull<spdk_nvme_cpl>);
impl NvmeCpl {
    /// Returns the NVMe status
    pub(crate) fn status(&mut self) -> &mut spdk_nvme_status {
        unsafe { &mut *nvme_status_get(self.0.as_mut()) }
    }

    pub(crate) fn set_cdw0(&mut self, cdw0: u32) {
        unsafe {
            self.0.as_mut().cdw0 = cdw0;
        }
    }
}

#[derive(Clone)]
pub struct NvmfReq(pub(crate) NonNull<spdk_nvmf_request>);
impl NvmfReq {
    /// Returns the NVMe completion
    pub fn response(&self) -> NvmeCpl {
        NvmeCpl(
            NonNull::new(unsafe {
                &mut *spdk_nvmf_request_get_response(self.0.as_ptr())
            })
            .unwrap(),
        )
    }

    /// Complete NVMf request without error.
    pub fn complete(&self) {
        let mut rsp = self.response();
        let nvme_status = rsp.status();

        nvme_status.set_sct(0); // SPDK_NVME_SCT_GENERIC
        nvme_status.set_sc(0); // SPDK_NVME_SC_SUCCESS

        unsafe {
            spdk_nvmf_request_complete(self.0.as_ptr());
        }
    }

    /// Complete NVMf request with error.
    pub fn complete_error(&self, errno: i32) {
        let mut rsp = self.response();
        let nvme_status = rsp.status();

        nvme_status.set_sct(0); // SPDK_NVME_SCT_GENERIC
        nvme_status.set_sc(0x06); // SPDK_NVME_SC_INTERNAL_DEVICE_ERROR

        rsp.set_cdw0(errno.unsigned_abs());

        unsafe {
            spdk_nvmf_request_complete(self.0.as_ptr());
        }
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
    now
}

/// Decode snapshot information from incoming NVMe admin command data.
fn decode_snapshot_params(
    req: *mut spdk_nvmf_request,
) -> Option<SnapshotParams> {
    const ITEM_SZ: usize = std::mem::size_of::<NvmeSnapshotMessage>();

    let mut val: Vec<u8> = Vec::with_capacity(ITEM_SZ * 2);

    let encoded_msg = unsafe {
        let bytes_copied = spdk_nvmf_request_copy_to_buf(
            req,
            val.as_mut_ptr() as _,
            val.capacity() as u64,
        ) as usize;

        info!(
            "## length = {}, iov_cnt = {}, size = {}",
            (*req).length,
            (*req).iovcnt,
            bytes_copied,
        );

        std::slice::from_raw_parts(val.as_ptr(), bytes_copied)
    };

    let decoded_msg = bincode::deserialize::<NvmeSnapshotMessage>(encoded_msg);

    // Decode versioned snapshot creation request.
    let decoded_msg = match decoded_msg {
        Err(e) => {
            error!("Failed to deserialize snapshot creation message: {:?}", e);
            return None;
        }
        Ok(msg) => msg,
    };

    let snapshot_params = match decoded_msg {
        NvmeSnapshotMessage::V1(v1) => v1.params().clone(),
    };

    Some(snapshot_params)
}

/// NVMf custom command handler for opcode c1h
/// Called from nvmf_ctrlr_process_admin_cmd
/// Return: <0 for any error, caller handles it as unsupported opcode
extern "C" fn nvmf_create_snapshot_hdlr(req: *mut spdk_nvmf_request) -> i32 {
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

    /* Get snapshot parameters from NVMe request */
    let snapshot_params = match decode_snapshot_params(req) {
        None => return -1,
        Some(v) => v,
    };

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

    let bd = Bdev::checked_from_ptr(bdev).unwrap();
    if bd.driver() == nexus::NEXUS_MODULE_NAME {
        // Received command on a published Nexus
        set_snapshot_time(unsafe { &mut *spdk_nvmf_request_get_cmd(req) });
        unsafe {
            spdk_nvmf_bdev_ctrlr_nvme_passthru_admin(bdev, desc, ch, req, None)
        }
    } else {
        // Received command on a shared replica (lvol)
        let nvmf_req = NvmfReq(NonNull::new(req).unwrap());
        // Blobfs operations must be on md_thread
        Reactors::master().send_future(async move {
            create_remote_snapshot(bd, snapshot_params, nvmf_req).await;
        });
        1 // SPDK_NVMF_REQUEST_EXEC_STATUS_ASYNCHRONOUS
    }
}

async fn create_remote_snapshot(
    bdev: UntypedBdev,
    params: SnapshotParams,
    nvmf_req: NvmfReq,
) {
    let Some(mut replica_ops) = ReplicaFactory::bdev_as_replica(bdev) else {
        debug!("unsupported bdev driver");
        nvmf_req.complete_error(nix::errno::Errno::ENOTSUP as i32);
        return;
    };
    let owner = replica_ops.entity_id().unwrap_or("unknown".to_string());
    let replica = replica_ops.uuid();
    info!(owner, replica, ?params, "Creating a remote snapshot");
    match replica_ops.create_snapshot(params).await {
        Ok(_) => {
            info!(
                owner,
                replica, "Successfully created remote-requested snapshot"
            );
            nvmf_req.complete()
        }
        Err(error) => {
            error!(
                ?error,
                owner, replica, "Error creating remote-requested snapshot"
            );
            nvmf_req.complete_error(error.to_errno() as i32)
        }
    }
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
