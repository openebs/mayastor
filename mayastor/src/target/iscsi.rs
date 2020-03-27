//! Methods for creating iscsi targets.
//!
//! We create a wildcard portal and initiator groups when mayastor starts up.
//! These groups allow unauthenticated access for any initiator. Then when
//! exporting a replica we use these default groups and create one target per
//! replica with one lun - LUN0.

use std::{
    cell::RefCell,
    ffi::CString,
    os::raw::{c_char, c_int},
    ptr,
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

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
    jsonrpc::{Code, RpcErrorCode},
    target::Side,
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
const ISCSI_PORT_NEXUS: u16 = 3260;
const ISCSI_PORT_REPLICA: u16 = 3262;

const ISCSI_PORTAL_GROUP_NEXUS: c_int = 0;
const ISCSI_PORTAL_GROUP_REPLICA: c_int = 2;

const ISCSI_INITIATOR_GROUP: c_int = 0; //only 1 for now

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

/// Generate iqn based on provided bdev_name
pub fn target_name(bdev_name: &str) -> String {
    format!("iqn.2019-05.io.openebs:{}", bdev_name)
}

/// Create iscsi portal and initiator group which will be used later when
/// creating iscsi targets.
pub fn init(address: &str) -> Result<()> {
    create_portal_group(
        address,
        ISCSI_PORT_REPLICA,
        ISCSI_PORTAL_GROUP_REPLICA,
    )?;

    if let Err(e) =
        create_portal_group(address, ISCSI_PORT_NEXUS, ISCSI_PORTAL_GROUP_NEXUS)
    {
        destroy_portal_group(ISCSI_PORTAL_GROUP_REPLICA);
        return Err(e);
    }
    if let Err(e) = create_initiator_group(ISCSI_INITIATOR_GROUP) {
        destroy_portal_group(ISCSI_PORTAL_GROUP_REPLICA);
        destroy_portal_group(ISCSI_PORTAL_GROUP_NEXUS);
        return Err(e);
    }
    ADDRESS.with(move |addr| {
        *addr.borrow_mut() = Some(address.to_owned());
    });
    debug!("Created default iscsi initiator group and portal groups for address {}", address);

    Ok(())
}

/// Destroy iscsi portal and initiator groups.
fn destroy_iscsi_groups() {
    destroy_initiator_group(ISCSI_INITIATOR_GROUP);
    destroy_portal_group(ISCSI_PORTAL_GROUP_NEXUS);
    destroy_portal_group(ISCSI_PORTAL_GROUP_REPLICA);
}

pub fn fini() {
    destroy_iscsi_groups();
}

fn share_as_iscsi_target(
    bdev_name: &str,
    bdev: &Bdev,
    mut pg_idx: c_int,
    mut ig_idx: c_int,
) -> Result<String, Error> {
    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();

    let mut lun_id: c_int = 0;
    let idx = ISCSI_IDX.with(move |iscsi_idx| {
        let idx = *iscsi_idx.borrow();
        *iscsi_idx.borrow_mut() = idx + 1;
        idx
    });

    let tgt = unsafe {
        spdk_iscsi_tgt_node_construct(
            idx,                   // target_index
            c_iqn.as_ptr(),        // name
            ptr::null(),           // alias
            &mut pg_idx as *mut _, // pg_tag_list
            &mut ig_idx as *mut _, // ig_tag_list
            1,                     /* portal and initiator
                                    * group list length */
            &mut spdk_bdev_get_name(bdev.as_ptr()), /* bdev name, how iscsi
                                                     * target gets
                                                     * associated with a
                                                     * bdev */
            &mut lun_id as *mut _, // lun id
            1,                     // length of lun id list
            128,                   // max queue depth
            false,                 // disable chap
            false,                 // require chap
            false,                 // mutual chap
            0,                     // chap group
            false,                 // header digest
            false,                 // data digest
        )
    };
    if tgt.is_null() {
        error!("Failed to create iscsi target {}", iqn);
        Err(Error::CreateTarget {})
    } else {
        Ok(iqn)
    }
}

/// Export given bdev over iscsi. That involves creating iscsi target and
/// adding the bdev as LUN to it.
pub fn share(bdev_name: &str, bdev: &Bdev, side: Side) -> Result<()> {
    let iqn = match side {
        Side::Nexus => share_as_iscsi_target(
            bdev_name,
            bdev,
            ISCSI_PORTAL_GROUP_NEXUS,
            ISCSI_INITIATOR_GROUP,
        )?,
        Side::Replica => share_as_iscsi_target(
            bdev_name,
            bdev,
            ISCSI_PORTAL_GROUP_REPLICA,
            ISCSI_INITIATOR_GROUP,
        )?,
    };
    info!("Created iscsi target {} for {}", iqn, bdev_name);
    Ok(())
}

/// Undo export of a bdev over iscsi done above.
pub async fn unshare(bdev_name: &str) -> Result<()> {
    let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();
    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();

    info!("Destroying iscsi target {}", iqn);

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
    info!("Destroyed iscsi target {}", bdev_name);
    Ok(())
}

fn create_initiator_group(ig_idx: c_int) -> Result<()> {
    let initiator_host = CString::new("ANY").unwrap();
    let initiator_netmask = CString::new("ANY").unwrap();

    unsafe {
        if spdk_iscsi_init_grp_create_from_initiator_list(
            ig_idx,
            1,
            &mut (initiator_host.as_ptr() as *mut c_char) as *mut _,
            1,
            &mut (initiator_netmask.as_ptr() as *mut c_char) as *mut _,
        ) != 0
        {
            destroy_iscsi_groups();
            return Err(Error::CreateInitiatorGroup {});
        }
    }
    Ok(())
}

fn destroy_initiator_group(ig_idx: c_int) {
    unsafe {
        let ig = spdk_iscsi_init_grp_unregister(ig_idx);
        if !ig.is_null() {
            spdk_iscsi_init_grp_destroy(ig);
        }
    }
}

fn create_portal_group(
    address: &str,
    port_no: u16,
    pg_no: c_int,
) -> Result<()> {
    let portal_port = CString::new(port_no.to_string()).unwrap();
    let portal_host = CString::new(address.to_owned()).unwrap();
    let pg = unsafe { spdk_iscsi_portal_grp_create(pg_no) };
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
    info!(
        "Created iscsi portal group no {}, address {}, port {}",
        pg_no, address, port_no
    );
    Ok(())
}

fn destroy_portal_group(pg_idx: c_int) {
    unsafe {
        let pg = spdk_iscsi_portal_grp_unregister(pg_idx);
        if !pg.is_null() {
            spdk_iscsi_portal_grp_release(pg);
        }
    }
}

/// Return iscsi target URI understood by nexus
pub fn get_uri(side: Side, bdev_name: &str) -> Option<String> {
    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let tgt = unsafe { spdk_iscsi_find_tgt_node(c_iqn.as_ptr()) };

    if tgt.is_null() {
        return None;
    }
    Some(create_uri(side, &iqn))
}

pub fn create_uri(side: Side, iqn: &str) -> String {
    let port = match side {
        Side::Nexus => ISCSI_PORT_NEXUS,
        Side::Replica => ISCSI_PORT_REPLICA,
    };
    ADDRESS.with(move |a| {
        let a_borrow = a.borrow();
        let address = a_borrow.as_ref().unwrap();
        format!("iscsi://{}:{}/{}", address, port, iqn)
    })
}
