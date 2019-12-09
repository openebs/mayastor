//! Methods for creating iscsi targets.
//!
//! We create a wildcard portal and initiator groups when mayastor starts up.
//! These groups allow unauthenticated access for any initiator. Then when
//! exporting a replica we use these default groups and create one target per
//! replica with one lun - LUN0.

use crate::{
    bdev::Bdev,
    executor::{cb_arg, done_errno_cb, ErrnoResult},
    jsonrpc::{Code, RpcErrorCode},
};
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};
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

/// iSCSI target related errors
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create default portal group"))]
    CreatePortalGroup {},
    #[snafu(display("Failed to create default iscsi portal"))]
    CreatePortal {},
    #[snafu(display("Failed to add default portal to portal group"))]
    AddPortal {},
    #[snafu(display("Failed to register default portal group"))]
    RegisterPortalGroup {},
    #[snafu(display("Failed to create default initiator group"))]
    CreateInitiatorGroup {},
    #[snafu(display("Failed to create iscsi target"))]
    CreateTarget {},
    #[snafu(display("Failed to destroy iscsi target"))]
    DestroyTarget { source: Errno },
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        Code::InternalError
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// iscsi target port number
const ISCSI_PORT: u16 = 3260;

thread_local! {
    /// iscsi global state.
    ///
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    ///
    /// A counter used for assigning idx to newly created iscsi targets.
    static ISCSI_IDX: RefCell<i32> = RefCell::new(0);
    /// IP address of iscsi portal used for all created iscsi targets.
    static ADDRESS: RefCell<Option<String>> = RefCell::new(None);
}

/// Generate iqn based on provided uuid
fn target_name(uuid: &str) -> String {
    format!("iqn.2019-05.io.openebs:{}", uuid)
}

/// Create iscsi portal and initiator group which will be used later when
/// creating iscsi targets.
pub fn init_iscsi(address: &str) -> Result<()> {
    let portal_host = CString::new(address.to_owned()).unwrap();
    let portal_port = CString::new(ISCSI_PORT.to_string()).unwrap();
    let initiator_host = CString::new("ANY").unwrap();
    let initiator_netmask = CString::new("ANY").unwrap();

    let pg = unsafe { spdk_iscsi_portal_grp_create(0) };
    if pg.is_null() {
        return Err(Error::CreatePortalGroup {});
    }
    unsafe {
        let p = spdk_iscsi_portal_create(
            portal_host.as_ptr(),
            portal_port.as_ptr(),
        );
        if p.is_null() {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::CreatePortal {});
        }
        spdk_iscsi_portal_grp_add_portal(pg, p);
        if spdk_iscsi_portal_grp_open(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::AddPortal {});
        }
        if spdk_iscsi_portal_grp_register(pg) != 0 {
            spdk_iscsi_portal_grp_release(pg);
            return Err(Error::RegisterPortalGroup {});
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
            return Err(Error::CreateInitiatorGroup {});
        }
    }
    ADDRESS.with(move |addr| {
        *addr.borrow_mut() = Some(address.to_owned());
    });
    debug!("Created default iscsi initiator group");

    Ok(())
}

/// Destroy iscsi default portal and initiator group.
pub fn fini_iscsi() {
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
}

/// Export given bdev over iscsi. That involves creating iscsi target and
/// adding the bdev as LUN to it.
pub fn share(uuid: &str, bdev: &Bdev) -> Result<()> {
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
        Err(Error::CreateTarget {})
    } else {
        info!("Created iscsi target {}", iqn);
        Ok(())
    }
}

/// Undo export of a bdev over iscsi done above.
pub async fn unshare(uuid: &str) -> Result<()> {
    let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();

    debug!("Destroying iscsi target {}", iqn);

    unsafe {
        spdk_iscsi_shutdown_tgt_node_by_name(
            c_iqn.as_ptr(),
            Some(done_errno_cb),
            cb_arg(sender),
        );
    }
    receiver
        .await
        .expect("Cancellation is not supported")
        .context(DestroyTarget {})?;
    info!("Destroyed iscsi target {}", uuid);
    Ok(())
}

/// Return iscsi target URI understood by nexus
pub fn get_uri(uuid: &str) -> Option<String> {
    let iqn = target_name(uuid);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let tgt = unsafe { spdk_iscsi_find_tgt_node(c_iqn.as_ptr()) };

    if tgt.is_null() {
        return None;
    }

    ADDRESS.with(move |a| {
        let a_borrow = a.borrow();
        let address = a_borrow.as_ref().unwrap();
        Some(format!("iscsi://{}:{}/{}", address, ISCSI_PORT, iqn))
    })
}
