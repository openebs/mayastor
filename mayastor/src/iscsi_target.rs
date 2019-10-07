//! Methods for creating iscsi targets.
//!
//! We create a wildcard portal and initiator groups when mayastor starts up.
//! These groups allow unauthenticated access for any initiator. Then when
//! exporting a replica we use these default groups and create one target per
//! replica with one lun - LUN0.

use crate::{
    bdev::Bdev,
    executor::{cb_arg, complete_callback_1},
};
use futures::channel::oneshot;
use spdk_sys::{
    spdk_bdev_get_name,
    spdk_iscsi_find_tgt_node,
    spdk_iscsi_init_grp_create_from_initiator_list,
    spdk_iscsi_init_grp_destroy,
    spdk_iscsi_init_grp_unregister,
    spdk_iscsi_portal_create,
    spdk_iscsi_portal_grp_add_portal,
    spdk_iscsi_portal_grp_create,
    spdk_iscsi_portal_grp_open,
    spdk_iscsi_portal_grp_register,
    spdk_iscsi_portal_grp_release,
    spdk_iscsi_portal_grp_unregister,
    spdk_iscsi_shutdown_tgt_node_by_name,
    spdk_iscsi_tgt_node_construct,
};
use std::{
    cell::RefCell,
    ffi::CString,
    os::raw::{c_char, c_int},
    ptr,
};

thread_local! {
    /// iscsi global state. Currently just a counter used for assigning idx
    /// to newly created iscsi targets.
    ///
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    static ISCSI_IDX: RefCell<i32> = RefCell::new(0);
}

/// Generate iqn based on provided uuid
fn target_name(uuid: &str) -> String {
    format!("iqn.2019-09.org.openebs.mayastor:{}", uuid)
}

/// Create iscsi portal and initiator group which will be used later when
/// creating iscsi targets.
pub fn init_iscsi() -> Result<(), String> {
    let portal_host = CString::new("0.0.0.0").unwrap();
    let portal_port = CString::new("3260").unwrap();
    let initiator_host = CString::new("ANY").unwrap();
    let initiator_netmask = CString::new("ANY").unwrap();

    let pg = unsafe { spdk_iscsi_portal_grp_create(0) };
    if pg.is_null() {
        return Err("Failed to create default portal group".to_owned());
    }
    unsafe {
        let p = spdk_iscsi_portal_create(
            portal_host.as_ptr(),
            portal_port.as_ptr(),
            ptr::null_mut(),
        );
        if p.is_null() {
            spdk_iscsi_portal_grp_release(pg);
            return Err("Failed to create default iscsi portal".to_owned());
        }
        spdk_iscsi_portal_grp_add_portal(pg, p);
        if spdk_iscsi_portal_grp_open(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err(
                "Failed to add default portal to portal group".to_owned()
            );
        }
        if spdk_iscsi_portal_grp_register(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err("Failed to register default portal group".to_owned());
        }
    }
    debug!("Created default iscsi portal group");

    unsafe {
        if spdk_iscsi_init_grp_create_from_initiator_list(
            0,
            1,
            &mut (initiator_host.as_ptr() as *mut c_char) as *mut _,
            1,
            &mut (initiator_netmask.as_ptr() as *mut c_char) as *mut _,
        ) != 0
        {
            spdk_iscsi_portal_grp_release(pg);
            return Err("Failed to create default initiator group".to_owned());
        }
    }
    debug!("Created default iscsi initiator group");

    Ok(())
}

/// Destroy iscsi default portal and initiator group.
pub fn fini_iscsi() -> Result<(), String> {
    unsafe {
        let ig = spdk_iscsi_init_grp_unregister(0);
        if !ig.is_null() {
            spdk_iscsi_init_grp_destroy(ig);
        }
        let pg = spdk_iscsi_portal_grp_unregister(0);
        if !pg.is_null() {
            spdk_iscsi_portal_grp_release(pg);
        }
    }
    Ok(())
}

/// Export given bdev over iscsi. That involves creating iscsi target and
/// adding the bdev as LUN to it.
pub fn share(uuid: &str, bdev: &Bdev) -> Result<(), String> {
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let mut group_idx: c_int = 0;
    let mut lun_id: c_int = 0;
    let idx = ISCSI_IDX.with(move |iscsi_idx| {
        let idx = *iscsi_idx.borrow();
        *iscsi_idx.borrow_mut() = idx + 1;
        idx
    });
    let tgt = unsafe {
        spdk_iscsi_tgt_node_construct(
            idx,
            c_iqn.as_ptr(),
            ptr::null(),
            &mut group_idx as *mut _,
            &mut group_idx as *mut _,
            1, // portal and initiator group list length
            &mut spdk_bdev_get_name(bdev.as_ptr()),
            &mut lun_id as *mut _,
            1,     // length of lun id list
            128,   // max queue depth
            false, // disable chap
            false, // require chap
            false, // mutual chap
            0,     // chap group
            false, // header digest
            false, // data digest
        )
    };
    if tgt.is_null() {
        Err(format!("Failed to create iscsi target {}", uuid))
    } else {
        info!("Created iscsi target {}", iqn);
        Ok(())
    }
}

/// Undo export of a bdev over iscsi done above.
pub async fn unshare(uuid: &str) -> Result<(), String> {
    let (sender, receiver) = oneshot::channel::<i32>();
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();

    debug!("Destroying iscsi target {}", iqn);

    unsafe {
        spdk_iscsi_shutdown_tgt_node_by_name(
            c_iqn.as_ptr(),
            Some(complete_callback_1),
            cb_arg(sender),
        );
    }
    let errno = receiver.await.expect("Cancellation is not supported");
    if errno != 0 {
        Err(format!(
            "Failed to destroy iscsi target {} (errno {})",
            uuid, errno
        ))
    } else {
        info!("Destroyed iscsi target {}", uuid);
        Ok(())
    }
}

/// Return target iqn for a replica with uuid.
pub fn get_iqn(uuid: &str) -> Option<String> {
    let iqn = target_name(uuid);
    let c_iqn = CString::new(target_name(uuid)).unwrap();
    let tgt = unsafe { spdk_iscsi_find_tgt_node(c_iqn.as_ptr()) };

    if tgt.is_null() {
        None
    } else {
        Some(iqn)
    }
}
