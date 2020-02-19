//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    fmt,
    fmt::{Display, Formatter},
    os::raw::c_void,
};

use futures::channel::oneshot;
use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_desc,
    spdk_bdev_io,
    spdk_bdev_io_get_buf,
    spdk_bdev_readv_blocks,
    spdk_bdev_register,
    spdk_bdev_unmap_blocks,
    spdk_bdev_unregister,
    spdk_bdev_writev_blocks,
    spdk_io_channel,
    spdk_io_device_register,
    spdk_io_device_unregister,
};

use rpc::mayastor::{RebuildProgressReply, RebuildStateReply};

use crate::{
    bdev::{
        nexus,
        nexus::{
            instances,
            nexus_channel::{DREvent, NexusChannel, NexusChannelInner},
            nexus_child::{ChildError, ChildState, NexusChild},
            nexus_io::{io_status, Bio},
            nexus_iscsi::{NexusIscsiError, NexusIscsiTarget},
            nexus_label::LabelError,
            nexus_nbd::{NbdDisk, NbdError},
        },
    },
    core::{Bdev, DmaBuf, DmaError},
    ffihelper::errno_result_from_i32,
    jsonrpc::{Code, RpcErrorCode},
    nexus_uri::BdevCreateDestroy,
    rebuild::{RebuildError, RebuildTask},
};

/// Common errors for nexus basic operations and child operations
/// which are part of nexus object.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Nexus {} does not exist", name))]
    NexusNotFound { name: String },
    #[snafu(display("Invalid nexus uuid \"{}\"", uuid))]
    InvalidUuid { uuid: String },
    #[snafu(display("Invalid encryption key"))]
    InvalidKey {},
    #[snafu(display("Failed to create crypto bdev for nexus {}", name))]
    CreateCryptoBdev { source: Errno, name: String },
    #[snafu(display("Failed to destroy crypto bdev for nexus {}", name))]
    DestroyCryptoBdev { source: Errno, name: String },
    #[snafu(display(
        "The nexus {} has been already shared with a different protocol",
        name
    ))]
    AlreadyShared { name: String },
    #[snafu(display("The nexus {} has not been shared", name))]
    NotShared { name: String },
    #[snafu(display("Failed to share nexus over NBD {}", name))]
    ShareNbdNexus { source: NbdError, name: String },
    #[snafu(display("Failed to share iscsi nexus {}", name))]
    ShareIscsiNexus {
        source: NexusIscsiError,
        name: String,
    },
    #[snafu(display("Failed to allocate label of nexus {}", name))]
    AllocLabel { source: DmaError, name: String },
    #[snafu(display("Failed to write label of nexus {}", name))]
    WriteLabel { source: LabelError, name: String },
    #[snafu(display("Failed to read label from a child of nexus {}", name))]
    ReadLabel { source: ChildError, name: String },
    #[snafu(display("Labels of the nexus {} are not the same", name))]
    CheckLabels { name: String },
    #[snafu(display("Failed to write protective MBR of nexus {}", name))]
    WritePmbr { source: LabelError, name: String },
    #[snafu(display("Failed to register IO device nexus {}", name))]
    RegisterNexus { source: Errno, name: String },
    #[snafu(display("Failed to create child of nexus {}", name))]
    CreateChild {
        source: BdevCreateDestroy,
        name: String,
    },
    #[snafu(display("Deferring open because nexus {} is incomplete", name))]
    NexusIncomplete { name: String },
    #[snafu(display("Children of nexus {} have mixed block sizes", name))]
    MixedBlockSizes { name: String },
    #[snafu(display(
        "Child {} of nexus {} has incompatible size or block size",
        child,
        name
    ))]
    ChildGeometry { child: String, name: String },
    #[snafu(display("Child {} of nexus {} cannot be found", child, name))]
    ChildMissing { child: String, name: String },
    #[snafu(display("Failed to open child {} of nexus {}", child, name))]
    OpenChild {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Cannot delete the last child {} of nexus {}",
        child,
        name
    ))]
    DestroyLastChild { child: String, name: String },
    #[snafu(display("Failed to destroy child {} of nexus {}", child, name))]
    DestroyChild {
        source: BdevCreateDestroy,
        child: String,
        name: String,
    },
    #[snafu(display("Child {} of nexus {} not found", child, name))]
    ChildNotFound { child: String, name: String },
    #[snafu(display("Child {} of nexus {} is not closed", child, name))]
    ChildNotClosed { child: String, name: String },
    #[snafu(display("Open Child of nexus {} not found", name))]
    OpenChildNotFound { name: String },
    #[snafu(display(
        "Failed to start rebuilding child {} of nexus {}",
        child,
        name
    ))]
    StartRebuild {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Failed to complete rebuild of child {} of nexus {}, reason: {}",
        child,
        name,
        reason,
    ))]
    CompleteRebuild {
        child: String,
        name: String,
        reason: String,
    },
    #[snafu(display("Invalid ShareProtocol value {}", sp_value))]
    InvalidShareProtocol { sp_value: i32 },
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        match self {
            Error::NexusNotFound {
                ..
            } => Code::NotFound,
            Error::InvalidUuid {
                ..
            } => Code::InvalidParams,
            Error::InvalidKey {
                ..
            } => Code::InvalidParams,
            Error::AlreadyShared {
                ..
            } => Code::InvalidParams,
            Error::NotShared {
                ..
            } => Code::InvalidParams,
            Error::CreateChild {
                ..
            } => Code::InvalidParams,
            Error::MixedBlockSizes {
                ..
            } => Code::InvalidParams,
            Error::ChildGeometry {
                ..
            } => Code::InvalidParams,
            Error::OpenChild {
                ..
            } => Code::InvalidParams,
            Error::DestroyLastChild {
                ..
            } => Code::InvalidParams,
            Error::ChildNotFound {
                ..
            } => Code::NotFound,
            Error::InvalidShareProtocol {
                ..
            } => Code::InvalidParams,
            _ => Code::InternalError,
        }
    }
}

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

pub enum NexusTarget {
    NbdDisk(NbdDisk),
    NexusIscsiTarget(NexusIscsiTarget),
}

impl fmt::Debug for NexusTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NexusTarget::NbdDisk(disk) => fmt::Debug::fmt(&disk, f),
            NexusTarget::NexusIscsiTarget(tgt) => fmt::Debug::fmt(&tgt, f),
        }
    }
}

/// The main nexus structure
#[derive(Debug)]
pub struct Nexus {
    /// Name of the Nexus instance
    pub(crate) name: String,
    /// the requested size of the nexus, children are allowed to be larger
    pub(crate) size: u64,
    /// number of children part of this nexus
    pub(crate) child_count: u32,
    /// vector of children
    pub children: Vec<NexusChild>,
    /// inner bdev
    pub(crate) bdev: Bdev,
    /// raw pointer to bdev (to destruct it later using Box::from_raw())
    bdev_raw: *mut spdk_bdev,
    /// represents the current state of the Nexus
    pub(crate) state: NexusState,
    /// Dynamic Reconfigure event
    pub dr_complete_notify: Option<oneshot::Sender<i32>>,
    /// the offset in num blocks where the data partition starts
    pub data_ent_offset: u64,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
    /// vector of rebuild tasks
    pub rebuilds: Vec<RebuildTask>,
    /// enum containing the protocol-specific target used to publish the nexus
    pub nexus_target: Option<NexusTarget>,
}

unsafe impl core::marker::Sync for Nexus {}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// closed
    Closed,
    /// Online
    Online,
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
    /// mule is moving blocks from A to B which is typical for an animal like
    /// this
    Remuling,
}

impl ToString for NexusState {
    fn to_string(&self) -> String {
        match *self {
            NexusState::Init => "init",
            NexusState::Online => "online",
            NexusState::Faulted => "faulted",
            NexusState::Degraded => "degraded",
            NexusState::Closed => "closed",
            NexusState::Remuling => "remuling",
        }
        .parse()
        .unwrap()
    }
}

impl Drop for Nexus {
    fn drop(&mut self) {
        unsafe {
            let b: Box<spdk_bdev> = Box::from_raw(self.bdev_raw);
            let _ = std::ffi::CString::from_raw(b.name);
            let _ = std::ffi::CString::from_raw(b.product_name);
        }
    }
}

impl Nexus {
    /// create a new nexus instance with optionally directly attaching
    /// children to it.
    pub fn new(
        name: &str,
        size: u64,
        uuid: Option<&str>,
        child_bdevs: Option<&[String]>,
    ) -> Box<Self> {
        let mut b = Box::new(spdk_bdev::default());
        b.name = c_str!(name);
        b.product_name = c_str!(NEXUS_PRODUCT_ID);
        b.fn_table = nexus::fn_table().unwrap();
        b.module = nexus::module().unwrap();
        b.blocklen = 0;
        b.blockcnt = 0;
        b.required_alignment = 9;

        let mut n = Box::new(Nexus {
            name: name.to_string(),
            child_count: 0,
            children: Vec::new(),
            bdev: Bdev::from(&*b as *const _ as *mut spdk_bdev),
            state: NexusState::Init,
            bdev_raw: Box::into_raw(b),
            dr_complete_notify: None,
            data_ent_offset: 0,
            share_handle: None,
            size,
            rebuilds: Vec::new(),
            nexus_target: None,
        });

        n.bdev.set_uuid(match uuid {
            Some(uuid) => Some(uuid.to_string()),
            None => None,
        });

        if let Some(child_bdevs) = child_bdevs {
            n.register_children(child_bdevs);
        }

        // store a reference to the Self in the bdev structure.
        unsafe {
            (*n.bdev.as_ptr()).ctxt = n.as_ref() as *const _ as *mut c_void;
        }
        n
    }

    /// set the state of the nexus
    pub(crate) fn set_state(&mut self, state: NexusState) -> NexusState {
        debug!(
            "{} Transitioned state from {:?} to {:?}",
            self.name, self.state, state
        );
        self.state = state;
        state
    }
    /// returns the size in bytes of the nexus instance
    pub fn size(&self) -> u64 {
        u64::from(self.bdev.block_len()) * self.bdev.num_blocks()
    }

    /// reconfigure the child event handler
    pub(crate) async fn reconfigure(&mut self, event: DREvent) {
        let (s, r) = oneshot::channel::<i32>();
        assert!(self.dr_complete_notify.is_none());
        self.dr_complete_notify = Some(s);

        info!(
            "{}: Dynamic reconfiguration event: {:?} started",
            self.name, event
        );

        NexusChannel::reconfigure(self.as_ptr(), &event);

        let result = r.await.expect("Reconfigure notify failure");

        info!(
            "{}: Dynamic reconfiguration event: {:?} completed {}",
            self.name, event, result
        );
    }

    /// Opens the Nexus instance for IO
    pub async fn open(&mut self) -> Result<(), Error> {
        debug!("Opening nexus {}", self.name);

        self.try_open_children()?;
        self.sync_labels().await?;
        self.register()
    }

    pub async fn sync_labels(&mut self) -> Result<(), Error> {
        if let Ok(label) = self.update_child_labels().await {
            // now register the bdev but update its size first to
            // ensure we adhere to the partitions

            // When the GUID does not match the given UUID it means
            // that the PVC has been recreated, in such a
            // case we should consider updating the labels

            info!("{}: {} ", self.name, label);
            self.data_ent_offset = label.offset();
            self.bdev.set_block_count(label.get_block_count());
        } else {
            // one or more children do not have, or have an invalid gpt label.
            // Recalculate what the header should have been and
            // write them out

            info!(
                "{}: Child label(s) mismatch or absent, applying new label(s)",
                self.name
            );

            let mut label = self.generate_label();
            self.data_ent_offset = label.offset();
            self.bdev.set_block_count(label.get_block_count());

            let blk_size = self.bdev.block_len();
            let mut buf = DmaBuf::new(
                (blk_size * (((1 << 14) / blk_size) + 1)) as usize,
                self.bdev.alignment(),
            )
            .context(AllocLabel {
                name: self.name.clone(),
            })?;

            self.write_label(&mut buf, &mut label, true).await.context(
                WriteLabel {
                    name: self.name.clone(),
                },
            )?;
            self.write_label(&mut buf, &mut label, false)
                .await
                .context(WriteLabel {
                    name: self.name.clone(),
                })?;
            info!("{}: {} ", self.name, label);

            self.write_pmbr().await.context(WritePmbr {
                name: self.name.clone(),
            })?;
        }

        Ok(())
    }

    /// close the nexus and any children that are open
    pub(crate) fn destruct(&mut self) -> NexusState {
        // a closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if self.state == NexusState::Closed {
            trace!("{}: already closed", self.name);
            return self.state;
        }

        trace!("{}: closing, from state: {:?} ", self.name, self.state);
        self.children
            .iter_mut()
            .map(|c| {
                if c.state == ChildState::Open || c.state == ChildState::Faulted
                {
                    c.close();
                }
            })
            .for_each(drop);

        unsafe {
            spdk_io_device_unregister(self.as_ptr(), None);
        }

        trace!("{}: closed", self.name);
        self.set_state(NexusState::Closed)
    }

    /// Destroy the nexus
    pub async fn destroy(&mut self) {
        // used to synchronize the destroy call
        extern "C" fn nexus_destroy_cb(arg: *mut c_void, rc: i32) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<bool>) };

            if rc == 0 {
                let _ = s.send(true);
            } else {
                error!("failed to destroy nexus {}", rc);
                let _ = s.send(false);
            }
        }

        let _ = self.unshare().await;
        assert_eq!(self.share_handle, None);

        for child in self.children.iter_mut() {
            let _ = child.close();
            info!("Destroying child bdev {}", child.name);

            let r = child.destroy().await;
            if r.is_err() {
                error!("Failed to destroy child {}", child.name);
            }
        }

        info!("Destroying nexus {}", self.name);

        let (s, r) = oneshot::channel::<bool>();

        unsafe {
            // This will trigger a callback to destruct() in the fn_table.
            spdk_bdev_unregister(
                self.bdev.as_ptr(),
                Some(nexus_destroy_cb),
                Box::into_raw(Box::new(s)) as *mut _,
            );
        }

        let _ = r.await;
    }

    /// register the bdev with SPDK and set the callbacks for io channel
    /// creation. Once this function is called, the device is visible and can
    /// be used for IO.
    pub(crate) fn register(&mut self) -> Result<(), Error> {
        assert_eq!(self.state, NexusState::Init);

        unsafe {
            spdk_io_device_register(
                self.as_ptr(),
                Some(NexusChannel::create),
                Some(NexusChannel::destroy),
                std::mem::size_of::<NexusChannel>() as u32,
                (*self.bdev.as_ptr()).name,
            );
        }

        debug!("{}: IO device registered at {:p}", self.name, self.as_ptr());

        let errno = unsafe { spdk_bdev_register(self.bdev.as_ptr()) };

        match errno_result_from_i32((), errno) {
            Ok(_) => {
                self.set_state(NexusState::Online);
                Ok(())
            }
            Err(err) => {
                unsafe {
                    spdk_io_device_unregister(self.as_ptr(), None);
                }
                self.children.iter_mut().map(|c| c.close()).for_each(drop);
                self.set_state(NexusState::Faulted);
                Err(err).context(RegisterNexus {
                    name: self.name.clone(),
                })
            }
        }
    }

    /// takes self and converts into a raw pointer
    pub(crate) fn as_ptr(&self) -> *mut c_void {
        self as *const _ as *mut _
    }

    /// takes a raw pointer and casts it to Self
    pub(crate) unsafe fn from_raw<'a>(n: *mut c_void) -> &'a mut Self {
        &mut *(n as *mut Nexus)
    }

    /// determine if any of the children do not support the requested
    /// io type. Brake the loop on first occurrence.
    /// TODO: optionally add this check during nexus creation
    pub fn io_is_supported(&self, io_type: u32) -> bool {
        self.children
            .iter()
            .filter_map(|e| e.bdev.as_ref())
            .any(|b| b.io_type_supported(io_type))
    }

    /// main IO completion routine
    unsafe extern "C" fn io_completion(
        child_io: *mut spdk_bdev_io,
        success: bool,
        parent_io: *mut c_void,
    ) {
        let mut pio = Bio(parent_io as *mut _);

        // if any child IO has failed record this within the io context
        if !success {
            trace!(
                "child IO {:?} ({}) of parent {:?} failed",
                Bio(child_io),
                (*child_io).type_,
                pio
            );

            pio.ctx_as_mut_ref().status = io_status::FAILED;
        }
        pio.assess();
        // always free the child IO
        Bio::io_free(child_io);
    }

    /// callback when the IO has buffer associated with itself
    extern "C" fn nexus_get_buf_cb(
        ch: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
        success: bool,
    ) {
        if !success {
            let bio = Bio(io);
            let nexus = bio.nexus_as_ref();
            warn!("{}: Failed to get io buffer for io {:?}", nexus.name, bio);
        }

        let ch = NexusChannel::inner_from_channel(ch);
        let (desc, ch) = ch.ch[ch.previous].io_tuple();
        let ret = Self::readv_impl(io, desc, ch);
        if ret != 0 {
            let bio = Bio(io);
            let nexus = bio.nexus_as_ref();
            error!("{}: Failed to submit IO {:?}", nexus.name, bio);
        }
    }

    /// read vectored io from the underlying children.
    pub(crate) fn readv(
        &self,
        pio: *mut spdk_bdev_io,
        channels: &mut NexusChannelInner,
    ) {
        let mut io = Bio(pio);

        // we use RR to read from the children also, set that we only need
        // to read from one child before we complete the IO to the callee.
        io.ctx_as_mut_ref().in_flight = 1;

        let child = channels.child_select();

        // if there is no buffer space for us allocated within the request
        // allocate it now, taking care of proper alignment
        if io.need_buf() {
            unsafe {
                spdk_bdev_io_get_buf(
                    pio,
                    Some(Self::nexus_get_buf_cb),
                    io.num_blocks() * io.block_len(),
                )
            }
            return;
        }

        let (desc, ch) = channels.ch[child].io_tuple();

        let ret = Self::readv_impl(pio, desc, ch);

        if ret != 0 {
            error!(
                "{}: Failed to submit dispatched IO {:p}",
                io.nexus_as_ref().name,
                pio
            );

            io.fail();
        }
    }

    /// do the actual read
    fn readv_impl(
        pio: *mut spdk_bdev_io,
        desc: *mut spdk_bdev_desc,
        ch: *mut spdk_io_channel,
    ) -> i32 {
        let io = Bio(pio);
        let nexus = io.nexus_as_ref();
        unsafe {
            spdk_bdev_readv_blocks(
                desc,
                ch,
                io.iovs(),
                io.iov_count(),
                io.offset() + nexus.data_ent_offset,
                io.num_blocks(),
                Some(Self::io_completion),
                pio as *mut _,
            )
        }
    }

    /// write vectored IO to the underlying children.
    pub(crate) fn writev(
        &self,
        pio: *mut spdk_bdev_io,
        channels: &NexusChannelInner,
    ) {
        let mut io = Bio(pio);
        // in case of writes, we want to write to all underlying children
        io.ctx_as_mut_ref().in_flight = channels.ch.len() as i8;
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                let (b, c) = c.io_tuple();
                spdk_bdev_writev_blocks(
                    b,
                    c,
                    io.iovs(),
                    io.iov_count(),
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    pio as *mut _,
                )
            })
            .collect::<Vec<_>>();

        // if any of the children failed to dispatch
        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                pio
            );
        }
    }

    pub(crate) fn unmap(
        &self,
        pio: *mut spdk_bdev_io,
        channels: &NexusChannelInner,
    ) {
        let mut io = Bio(pio);
        io.ctx_as_mut_ref().in_flight = channels.ch.len() as i8;
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                let (b, c) = c.io_tuple();
                spdk_bdev_unmap_blocks(
                    b,
                    c,
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    pio as *mut _,
                )
            })
            .collect::<Vec<_>>();

        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                pio
            );
        }
    }

    /// returns the current status of the nexus
    pub fn status(&self) -> NexusState {
        self.state
    }

    pub async fn get_rebuild_state(&self) -> Result<RebuildStateReply, Error> {
        // TODO: add real implementation
        Ok(RebuildStateReply {
            state: "Not implemented".to_string(),
        })
    }

    pub async fn get_rebuild_progress(
        &self,
    ) -> Result<RebuildProgressReply, Error> {
        // TODO: add real implementation
        Ok(RebuildProgressReply {
            progress: "Not implemented".to_string(),
        })
    }
}

/// If we fail to create one of the children we will fail the whole operation
/// destroy any created children and return the error. Once created, and we
/// bring the nexus online, there still might be a configuration mismatch that
/// would prevent the nexus to come online. We can only determine this
/// (currently) when online, so we check the errors twice for now.
pub async fn nexus_create(
    name: &str,
    size: u64,
    uuid: Option<&str>,
    children: &[String],
) -> Result<(), Error> {
    // global variable defined in the nexus module
    let nexus_list = instances();
    if nexus_list.iter().any(|n| n.name == name) {
        // instead of error we return Ok without making sure that also the
        // children are the same, which seems wrong
        return Ok(());
    }

    let mut ni = Nexus::new(name, size, uuid, None);

    for child in children {
        if let Err(err) = ni.register_child(child).await {
            ni.destroy_children().await;
            return Err(err).context(CreateChild {
                name: ni.name.clone(),
            });
        }
    }

    ni.open().await?;
    nexus_list.push(ni);
    Ok(())
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    if let Some(nexus) = instances().iter_mut().find(|n| n.name == name) {
        Some(nexus)
    } else {
        None
    }
}

impl Display for Nexus {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        let _ = writeln!(
            f,
            "{}: state: {:?} blk_cnt: {}, blk_size: {}",
            self.name,
            self.state,
            self.bdev.num_blocks(),
            self.bdev.block_len(),
        );

        self.children
            .iter()
            .map(|c| write!(f, "\t{}", c))
            .for_each(drop);
        Ok(())
    }
}
