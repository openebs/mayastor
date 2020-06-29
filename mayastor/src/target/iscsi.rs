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
    iscsi_find_tgt_node,
    iscsi_init_grp_create_from_initiator_list,
    iscsi_init_grp_destroy,
    iscsi_init_grp_find_by_tag,
    iscsi_init_grp_unregister,
    iscsi_portal_create,
    iscsi_portal_grp_add_portal,
    iscsi_portal_grp_create,
    iscsi_portal_grp_find_by_tag,
    iscsi_portal_grp_open,
    iscsi_portal_grp_register,
    iscsi_portal_grp_release,
    iscsi_portal_grp_unregister,
    iscsi_shutdown_tgt_node_by_name,
    iscsi_tgt_node_construct,
    spdk_bdev_get_name,
};

use crate::{
    core::Bdev,
    ffihelper::{cb_arg, done_errno_cb, ErrnoResult},
    jsonrpc::{Code, RpcErrorCode},
    subsys::Config,
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

/// Portal Group Tags
const ISCSI_PORTAL_GROUP_NEXUS: c_int = 0;
const ISCSI_PORTAL_GROUP_REPLICA: c_int = 2;

const ISCSI_INITIATOR_GROUP: c_int = 0; //only 1 for now
/// Only one LUN is presented, and this is the LUN value.
const LUN: c_int = 0; //only 1 for now

/// Parameters used for creating iSCSI nexus and replica target portals
struct TargetPortalData {
    /// IP address
    address: String,
    /// port for nexus portal
    nexus_port: u16,
    /// port for replica portal
    replica_port: u16,
}

thread_local! {
    /// iscsi global state.
    ///
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    ///
    /// A counter used for assigning idx to newly created iscsi targets.
    static ISCSI_IDX: RefCell<i32> = RefCell::new(0);
    /// IP address and ports for iSCSI nexus and replica target portals
    static TARGET_PORTAL_DATA: RefCell<Option<TargetPortalData>> = RefCell::new(None);
}

/// Generate iqn based on provided bdev_name
pub fn target_name(bdev_name: &str) -> String {
    format!("iqn.2019-05.io.openebs:{}", bdev_name)
}

/// Create iscsi portal and initiator group which will be used later when
/// creating iscsi targets.
pub fn init(address: &str) -> Result<()> {
    let config = Config::get();
    let nexus_port = config.nexus_opts.iscsi_nexus_port;
    let replica_port = config.nexus_opts.iscsi_replica_port;

    create_portal_group(address, replica_port, ISCSI_PORTAL_GROUP_REPLICA)?;

    if let Err(e) =
        create_portal_group(address, nexus_port, ISCSI_PORTAL_GROUP_NEXUS)
    {
        destroy_portal_group(ISCSI_PORTAL_GROUP_REPLICA);
        return Err(e);
    }

    if let Err(e) = create_initiator_group(ISCSI_INITIATOR_GROUP) {
        destroy_portal_group(ISCSI_PORTAL_GROUP_REPLICA);
        destroy_portal_group(ISCSI_PORTAL_GROUP_NEXUS);
        return Err(e);
    }

    TARGET_PORTAL_DATA.with(move |data| {
        *data.borrow_mut() = Some(TargetPortalData {
            address: address.to_owned(),
            nexus_port,
            replica_port,
        });
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

    let mut lun_id: c_int = LUN;
    let idx = ISCSI_IDX.with(move |iscsi_idx| {
        let idx = *iscsi_idx.borrow();
        *iscsi_idx.borrow_mut() = idx + 1;
        idx
    });

    let tgt = unsafe {
        iscsi_tgt_node_construct(
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
        iscsi_shutdown_tgt_node_by_name(
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

fn initiator_group_exists(tag: i32) -> bool {
    if unsafe { iscsi_init_grp_find_by_tag(tag).is_null() } {
        return false;
    }

    debug!("initiator group {} already exists", tag);
    true
}

fn create_initiator_group(ig_idx: c_int) -> Result<()> {
    if initiator_group_exists(ig_idx) {
        // when we are here we know the IG does not exists however,
        // we do not know for sure if the masks as the same.
        // as the config files are either provided by the control
        // plane or during sets, we assume a difference if any, is
        // intended and we do not verify this.

        return Ok(());
    }

    let initiator_host = CString::new("ANY").unwrap();
    let initiator_netmask = CString::new("ANY").unwrap();

    unsafe {
        if iscsi_init_grp_create_from_initiator_list(
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
        let ig = iscsi_init_grp_unregister(ig_idx);
        if !ig.is_null() {
            iscsi_init_grp_destroy(ig);
        }
    }
}

/// determine if a portal group exists by trying to find it by its tag
fn portal_exists(tag: i32) -> bool {
    if unsafe { iscsi_portal_grp_find_by_tag(tag).is_null() } {
        return false;
    }

    debug!("portal group {} already exists", tag);
    true
}

fn create_portal_group(
    address: &str,
    port_no: u16,
    pg_no: c_int,
) -> Result<()> {
    if portal_exists(pg_no) {
        return Ok(());
    }

    let portal_port = CString::new(port_no.to_string()).unwrap();
    let portal_host = CString::new(address.to_owned()).unwrap();
    let pg = unsafe { iscsi_portal_grp_create(pg_no) };
    if pg.is_null() {
        return Err(Error::CreatePortalGroup {});
    }
    unsafe {
        let p = iscsi_portal_create(portal_host.as_ptr(), portal_port.as_ptr());
        if p.is_null() {
            iscsi_portal_grp_release(pg);
            return Err(Error::CreatePortal {});
        }
        iscsi_portal_grp_add_portal(pg, p);
        if iscsi_portal_grp_open(pg) != 0 {
            iscsi_portal_grp_release(pg);
            return Err(Error::AddPortal {});
        }
        if iscsi_portal_grp_register(pg) != 0 {
            iscsi_portal_grp_release(pg);
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
        let pg = iscsi_portal_grp_unregister(pg_idx);
        if !pg.is_null() {
            iscsi_portal_grp_release(pg);
        }
    }
}

/// Return iscsi target URI understood by nexus
pub fn get_uri(side: Side, bdev_name: &str) -> Option<String> {
    let iqn = target_name(bdev_name);
    let c_iqn = CString::new(iqn.clone()).unwrap();
    let tgt = unsafe { iscsi_find_tgt_node(c_iqn.as_ptr()) };

    if tgt.is_null() {
        return None;
    }
    Some(create_uri(side, &iqn))
}

pub fn create_uri(side: Side, iqn: &str) -> String {
    TARGET_PORTAL_DATA.with(move |data| {
        let borrowed = data.borrow();
        let data = borrowed.as_ref().unwrap();
        let port = match side {
            Side::Nexus => data.nexus_port,
            Side::Replica => data.replica_port,
        };
        format!("iscsi://{}:{}/{}/{}", data.address, port, iqn, LUN)
    })
}
