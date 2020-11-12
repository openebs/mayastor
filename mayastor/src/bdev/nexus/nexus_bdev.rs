//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    convert::TryFrom,
    fmt::{Display, Formatter},
    os::raw::c_void,
};

use async_mutex::Mutex;
use futures::channel::oneshot;
use nix::errno::Errno;
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use tonic::{Code, Status};

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_desc,
    spdk_bdev_io,
    spdk_bdev_io_get_buf,
    spdk_bdev_nvme_admin_passthru,
    spdk_bdev_readv_blocks,
    spdk_bdev_register,
    spdk_bdev_reset,
    spdk_bdev_unmap_blocks,
    spdk_bdev_unregister,
    spdk_bdev_write_zeroes_blocks,
    spdk_bdev_writev_blocks,
    spdk_io_channel,
    spdk_io_device_register,
    spdk_io_device_unregister,
};

use crate::{
    bdev::{
        nexus,
        nexus::{
            instances,
            nexus_channel::{DREvent, NexusChannel, NexusChannelInner},
            nexus_child::{ChildError, ChildState, NexusChild},
            nexus_io::{nvme_admin_opc, Bio, IoStatus, IoType},
            nexus_label::LabelError,
            nexus_nbd::{NbdDisk, NbdError},
        },
    },
    core::{Bdev, CoreError, DmaError, Protocol, Reactor, Share},
    ffihelper::errno_result_from_i32,
    lvs::Lvol,
    nexus_uri::{bdev_destroy, NexusBdevError},
    rebuild::RebuildError,
    subsys,
    subsys::{Config, NvmfSubsystem},
};

/// Obtain the full error chain
pub trait VerboseError {
    fn verbose(&self) -> String;
}

impl<T> VerboseError for T
where
    T: std::error::Error,
{
    /// loops through the error chain and formats into a single string
    /// containing all the lower level errors
    fn verbose(&self) -> String {
        let mut msg = format!("{}", self);
        let mut opt_source = self.source();
        while let Some(source) = opt_source {
            msg = format!("{}: {}", msg, source);
            opt_source = source.source();
        }
        msg
    }
}

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
    ShareIscsiNexus { source: CoreError, name: String },
    #[snafu(display("Failed to share nvmf nexus {}", name))]
    ShareNvmfNexus { source: CoreError, name: String },
    #[snafu(display("Failed to unshare nexus {}", name))]
    UnshareNexus { source: CoreError, name: String },
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
        source: NexusBdevError,
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
    #[snafu(display("Child {} of nexus {} has no error store", child, name))]
    ChildMissingErrStore { child: String, name: String },
    #[snafu(display("Failed to open child {} of nexus {}", child, name))]
    OpenChild {
        source: ChildError,
        child: String,
        name: String,
    },
    #[snafu(display("Failed to close child {} of nexus {}", child, name))]
    CloseChild {
        source: NexusBdevError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Cannot delete the last child {} of nexus {}",
        child,
        name
    ))]
    DestroyLastChild { child: String, name: String },
    #[snafu(display(
        "Cannot remove the last child {} of nexus {} from the IO path",
        child,
        name
    ))]
    RemoveLastChild { child: String, name: String },
    #[snafu(display(
        "Cannot fault the last healthy child {} of nexus {}",
        child,
        name
    ))]
    FaultingLastHealthyChild { child: String, name: String },
    #[snafu(display("Failed to destroy child {} of nexus {}", child, name))]
    DestroyChild {
        source: NexusBdevError,
        child: String,
        name: String,
    },
    #[snafu(display("Child {} of nexus {} not found", child, name))]
    ChildNotFound { child: String, name: String },
    #[snafu(display("Suitable rebuild source for nexus {} not found", name))]
    NoRebuildSource { name: String },
    #[snafu(display(
        "Failed to create rebuild job for child {} of nexus {}",
        child,
        name,
    ))]
    CreateRebuildError {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Rebuild job not found for child {} of nexus {}",
        child,
        name,
    ))]
    RebuildJobNotFound {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Failed to remove rebuild job {} of nexus {}",
        child,
        name,
    ))]
    RemoveRebuildJob {
        source: RebuildError,
        child: String,
        name: String,
    },
    #[snafu(display(
        "Failed to execute rebuild operation on job {} of nexus {}",
        job,
        name,
    ))]
    RebuildOperationError {
        job: String,
        name: String,
        source: RebuildError,
    },
    #[snafu(display("Invalid ShareProtocol value {}", sp_value))]
    InvalidShareProtocol { sp_value: i32 },
    #[snafu(display("Failed to create nexus {}", name))]
    NexusCreate { name: String },
    #[snafu(display("Failed to destroy nexus {}", name))]
    NexusDestroy { name: String },
    #[snafu(display(
        "Child {} of nexus {} is not degraded but {}",
        child,
        name,
        state
    ))]
    ChildNotDegraded {
        child: String,
        name: String,
        state: String,
    },
    #[snafu(display("Failed to get BdevHandle for snapshot operation"))]
    FailedGetHandle,
    #[snafu(display("Failed to create snapshot on nexus {}", name))]
    FailedCreateSnapshot { name: String, source: CoreError },
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::NexusNotFound {
                ..
            } => Status::not_found(e.to_string()),
            Error::InvalidUuid {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::InvalidKey {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::AlreadyShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::NotShared {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::CreateChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::MixedBlockSizes {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildGeometry {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::OpenChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::DestroyLastChild {
                ..
            } => Status::invalid_argument(e.to_string()),
            Error::ChildNotFound {
                ..
            } => Status::not_found(e.to_string()),
            e => Status::new(Code::Internal, e.to_string()),
        }
    }
}

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

#[derive(Debug)]
pub enum NexusTarget {
    NbdDisk(NbdDisk),
    NexusIscsiTarget,
    NexusNvmfTarget,
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
    pub(super) state: std::sync::Mutex<NexusState>,
    /// Dynamic Reconfigure event
    pub dr_complete_notify: Option<oneshot::Sender<i32>>,
    /// the offset in num blocks where the data partition starts
    pub data_ent_offset: u64,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
    /// enum containing the protocol-specific target used to publish the nexus
    pub nexus_target: Option<NexusTarget>,
    /// the maximum number of times to attempt to send an IO
    pub(crate) max_io_attempts: i32,
    /// mutex to serialise reconfigure
    reconfigure_mutex: Mutex<()>,
}

unsafe impl core::marker::Sync for Nexus {}
unsafe impl core::marker::Send for Nexus {}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusStatus {
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
    /// Online
    Online,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// closed
    Closed,
    /// open
    Open,
}

impl ToString for NexusState {
    fn to_string(&self) -> String {
        match *self {
            NexusState::Init => "init",
            NexusState::Closed => "closed",
            NexusState::Open => "open",
        }
        .parse()
        .unwrap()
    }
}

impl ToString for NexusStatus {
    fn to_string(&self) -> String {
        match *self {
            NexusStatus::Degraded => "degraded",
            NexusStatus::Online => "online",
            NexusStatus::Faulted => "faulted",
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

        let cfg = Config::get();

        let mut n = Box::new(Nexus {
            name: name.to_string(),
            child_count: 0,
            children: Vec::new(),
            bdev: Bdev::from(&*b as *const _ as *mut spdk_bdev),
            state: std::sync::Mutex::new(NexusState::Init),
            bdev_raw: Box::into_raw(b),
            dr_complete_notify: None,
            data_ent_offset: 0,
            share_handle: None,
            size,
            nexus_target: None,
            max_io_attempts: cfg.err_store_opts.max_io_attempts,
            reconfigure_mutex: Mutex::new(()),
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
        *self.state.lock().unwrap() = state;
        state
    }
    /// returns the size in bytes of the nexus instance
    pub fn size(&self) -> u64 {
        u64::from(self.bdev.block_len()) * self.bdev.num_blocks()
    }

    /// reconfigure the child event handler
    pub(crate) async fn reconfigure(&mut self, event: DREvent) {
        let _var = self.reconfigure_mutex.lock().await;
        let (s, r) = oneshot::channel::<i32>();
        assert!(self.dr_complete_notify.is_none());
        self.dr_complete_notify = Some(s);

        info!(
            "{}: Dynamic reconfiguration event: {:?} started",
            self.name, event
        );

        NexusChannel::reconfigure(self.as_ptr(), &event);

        let result = r.await;

        info!(
            "{}: Dynamic reconfiguration event: {:?} completed {:?}",
            self.name, event, result
        );
    }

    /// Opens the Nexus instance for IO
    pub async fn open(&mut self) -> Result<(), Error> {
        debug!("Opening nexus {}", self.name);

        self.try_open_children().await?;
        self.sync_labels().await?;
        self.register().await
    }

    pub async fn sync_labels(&mut self) -> Result<(), Error> {
        let label = self.update_child_labels().await.context(WriteLabel {
            name: self.name.clone(),
        })?;

        // Now register the bdev but update its size first
        // to ensure we adhere to the partitions.
        self.data_ent_offset = label.offset();
        let size_blocks = self.size / self.bdev.block_len() as u64;

        self.bdev.set_block_count(std::cmp::min(
            // nexus is allowed to be smaller than the children
            size_blocks,
            // label might be smaller than expected due to the on disk metadata
            label.get_block_count(),
        ));

        Ok(())
    }

    /// close the nexus and any children that are open
    pub(crate) fn destruct(&mut self) -> NexusState {
        // a closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if *self.state.lock().unwrap() == NexusState::Closed {
            trace!("{}: already closed", self.name);
            return NexusState::Closed;
        }

        trace!("{}: closing, from state: {:?} ", self.name, self.state);

        let nexus_name = self.name.clone();
        Reactor::block_on(async move {
            let nexus = nexus_lookup(&nexus_name).expect("Nexus not found");
            for child in &nexus.children {
                if child.state() == ChildState::Open {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            nexus.name,
                            child.name,
                            e.verbose()
                        );
                    }
                }
            }
        });

        unsafe {
            spdk_io_device_unregister(self.as_ptr(), None);
        }

        trace!("{}: closed", self.name);
        self.set_state(NexusState::Closed)
    }

    /// Destroy the nexus
    pub async fn destroy(&mut self) -> Result<(), Error> {
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

        let _ = self.unshare_nexus().await;
        assert_eq!(self.share_handle, None);

        // no-op when not shared and will be removed once the old share bits are
        // gone
        self.bdev.unshare().await.unwrap();

        // wait for all rebuild jobs to be cancelled before proceeding with the
        // destruction of the nexus
        for child in self.children.iter() {
            self.cancel_child_rebuild_jobs(&child.name).await;
        }

        for child in self.children.iter_mut() {
            info!("Destroying child bdev {}", child.name);
            if let Err(e) = child.close().await {
                // TODO: should an error be returned here?
                error!(
                    "Failed to close child {} with error {}",
                    child.name,
                    e.verbose()
                );
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

        if r.await.unwrap() {
            // Update the child states to remove them from the config file.
            NexusChild::save_state_change();
            Ok(())
        } else {
            Err(Error::NexusDestroy {
                name: self.name.clone(),
            })
        }
    }

    /// resume IO to the bdev
    pub(crate) async fn resume(&self) -> Result<(), Error> {
        match self.shared() {
            Some(Protocol::Nvmf) => {
                if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                    subsystem.resume().await.unwrap();
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// suspend any incoming IO to the bdev pausing the controller allows us to
    /// handle internal events and which is a protocol feature.
    pub(crate) async fn pause(&self) -> Result<(), Error> {
        match self.shared() {
            Some(Protocol::Nvmf) => {
                if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                    subsystem.pause().await.unwrap();
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// register the bdev with SPDK and set the callbacks for io channel
    /// creation. Once this function is called, the device is visible and can
    /// be used for IO.
    pub(crate) async fn register(&mut self) -> Result<(), Error> {
        assert_eq!(*self.state.lock().unwrap(), NexusState::Init);

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
                self.set_state(NexusState::Open);
                Ok(())
            }
            Err(err) => {
                unsafe {
                    spdk_io_device_unregister(self.as_ptr(), None);
                }
                for child in &self.children {
                    if let Err(e) = child.close().await {
                        error!(
                            "{}: child {} failed to close with error {}",
                            self.name,
                            child.name,
                            e.verbose()
                        );
                    }
                }
                self.set_state(NexusState::Closed);
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
    /// io type. Break the loop on first occurrence.
    /// TODO: optionally add this check during nexus creation
    pub fn io_is_supported(&self, io_type: IoType) -> bool {
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
        let mut pio = Bio::from(parent_io);
        let mut chio = Bio::from(child_io);

        // if any child IO has failed record this within the io context
        if !success {
            trace!(
                "child IO {:?} ({:#?}) of parent {:?} failed",
                chio,
                chio.io_type(),
                pio
            );

            pio.ctx_as_mut_ref().status = IoStatus::Failed.into();
        }
        pio.assess(&mut chio, success);
        // always free the child IO
        chio.free();
    }

    /// IO completion for local replica
    pub fn io_completion_local(success: bool, parent_io: *mut c_void) {
        let mut pio = Bio::from(parent_io);
        let pio_ctx = pio.ctx_as_mut_ref();

        if !success {
            pio_ctx.status = IoStatus::Failed.into();
        }

        // As there is no child IO, perform the IO accounting that Bio::assess
        // does here, without error recording or retries.
        pio_ctx.in_flight -= 1;
        debug_assert!(pio_ctx.in_flight >= 0);

        if pio_ctx.in_flight == 0 {
            if IoStatus::from(pio_ctx.status) == IoStatus::Failed {
                pio_ctx.io_attempts -= 1;
                if pio_ctx.io_attempts == 0 {
                    pio.fail();
                }
            } else {
                pio.ok();
            }
        }
    }

    /// callback when the IO has buffer associated with itself
    extern "C" fn nexus_get_buf_cb(
        ch: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
        success: bool,
    ) {
        if !success {
            let bio = Bio::from(io);
            let nexus = bio.nexus_as_ref();
            warn!("{}: Failed to get io buffer for io {:?}", nexus.name, bio);
        }

        let ch = NexusChannel::inner_from_channel(ch);
        let (desc, ch) = ch.readers[ch.previous].io_tuple();
        let ret = Self::readv_impl(io, desc, ch);
        if ret != 0 {
            let bio = Bio::from(io);
            let nexus = bio.nexus_as_ref();
            error!("{}: Failed to submit IO {:?}", nexus.name, bio);
        }
    }

    /// read vectored io from the underlying children.
    pub(crate) fn readv(&self, io: &Bio, channels: &mut NexusChannelInner) {
        // we use RR to read from the children.
        let child = channels.child_select();

        // if there is no buffer space for us allocated within the request
        // allocate it now, taking care of proper alignment
        if io.need_buf() {
            unsafe {
                spdk_bdev_io_get_buf(
                    io.as_ptr(),
                    Some(Self::nexus_get_buf_cb),
                    io.num_blocks() * io.block_len(),
                )
            }
            return;
        }

        let (desc, ch) = channels.readers[child].io_tuple();

        let ret = Self::readv_impl(io.as_ptr(), desc, ch);

        if ret != 0 {
            error!(
                "{}: Failed to submit dispatched IO {:p}",
                io.nexus_as_ref().name,
                io.as_ptr()
            );

            io.fail();
        }
    }

    /// do the actual read
    #[inline]
    pub(crate) fn readv_impl(
        pio: *mut spdk_bdev_io,
        desc: *mut spdk_bdev_desc,
        ch: *mut spdk_io_channel,
    ) -> i32 {
        let io = Bio::from(pio);
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
                io.as_ptr() as *mut _,
            )
        }
    }

    /// send reset IO to the underlying children.
    pub(crate) fn reset(&self, io: &Bio, channels: &NexusChannelInner) {
        // in case of resets, we want to reset all underlying children
        let results = channels
            .writers
            .iter()
            .map(|c| unsafe {
                let (bdev, chan) = c.io_tuple();
                trace!("Dispatched RESET");
                spdk_bdev_reset(
                    bdev,
                    chan,
                    Some(Self::io_completion),
                    io.as_ptr() as *mut _,
                )
            })
            .collect::<Vec<_>>();

        // if any of the children failed to dispatch
        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                io.as_ptr(),
            );
        }
    }

    /// write vectored IO to the underlying children.
    pub(crate) fn writev(&self, io: &Bio, channels: &NexusChannelInner) {
        // in case of writes, we want to write to all underlying children
        let results = channels
            .writers
            .iter()
            .map(|c| unsafe {
                let (desc, chan) = c.io_tuple();
                spdk_bdev_writev_blocks(
                    desc,
                    chan,
                    io.iovs(),
                    io.iov_count(),
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    io.as_ptr() as *mut _,
                )
            })
            .collect::<Vec<_>>();

        // if any of the children failed to dispatch
        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                io.as_ptr()
            );
        }
    }

    pub(crate) fn unmap(&self, io: &Bio, channels: &NexusChannelInner) {
        let results = channels
            .writers
            .iter()
            .map(|c| unsafe {
                let (desc, chan) = c.io_tuple();
                spdk_bdev_unmap_blocks(
                    desc,
                    chan,
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    io.as_ptr() as *mut _,
                )
            })
            .collect::<Vec<_>>();

        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                io.as_ptr()
            );
        }
    }

    pub(crate) fn write_zeroes(&self, io: &Bio, channels: &NexusChannelInner) {
        let results = channels
            .writers
            .iter()
            .map(|c| unsafe {
                let (b, c) = c.io_tuple();
                spdk_bdev_write_zeroes_blocks(
                    b,
                    c,
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    io.as_ptr() as *mut _,
                )
            })
            .collect::<Vec<_>>();

        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                io.as_ptr()
            );
        }
    }

    pub(crate) fn nvme_admin(&self, io: &Bio, channels: &NexusChannelInner) {
        if io.nvme_cmd().opc() == nvme_admin_opc::CREATE_SNAPSHOT as u16 {
            // FIXME: pause IO before dispatching
            debug!("Passing thru create snapshot as NVMe Admin command");
        }
        // for replicas, passthru only works with our vendor commands as the
        // underlying bdev is not nvmf
        let results = channels
            .writers
            .iter()
            .map(|c| unsafe {
                debug!("nvme_admin on {}", c.get_bdev().driver());
                if c.get_bdev().driver() == "lvol" {
                    // Local replica, vbdev_lvol does not support NVMe Admin
                    // so call function directly
                    let lvol = Lvol::try_from(c.get_bdev()).unwrap();
                    match io.nvme_cmd().opc() as u8 {
                        nvme_admin_opc::CREATE_SNAPSHOT => {
                            subsys::create_snapshot(
                                lvol,
                                &io.nvme_cmd(),
                                io.as_ptr(),
                            );
                        }
                        _ => {
                            error!(
                                "{}: Unsupported NVMe Admin command {:x}h from IO {:?}",
                                io.nexus_as_ref().name,
                                io.nvme_cmd().opc(),
                                io.as_ptr()
                            );
                            Self::io_completion_local(
                                false,
                                io.as_ptr().cast(),
                            );
                        }
                    }
                    return 0;
                }
                let (desc, chan) = c.io_tuple();
                spdk_bdev_nvme_admin_passthru(
                    desc,
                    chan,
                    &io.nvme_cmd(),
                    io.nvme_buf(),
                    io.nvme_nbytes(),
                    Some(Self::io_completion),
                    io.as_ptr() as *mut _,
                )
            })
            .collect::<Vec<_>>();

        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:?}",
                io.nexus_as_ref().name,
                io.as_ptr()
            );
        }
    }

    /// Status of the nexus
    /// Online
    /// All children must also be online
    ///
    /// Degraded
    /// At least one child must be online
    ///
    /// Faulted
    /// No child is online so the nexus is faulted
    /// This may be made more configurable in the future
    pub fn status(&self) -> NexusStatus {
        match *self.state.lock().unwrap() {
            NexusState::Init => NexusStatus::Degraded,
            NexusState::Closed => NexusStatus::Faulted,
            NexusState::Open => {
                if self
                    .children
                    .iter()
                    // All children are online, so the Nexus is also online
                    .all(|c| c.state() == ChildState::Open)
                {
                    NexusStatus::Online
                } else if self
                    .children
                    .iter()
                    // at least one child online, so the Nexus is also online
                    .any(|c| c.state() == ChildState::Open)
                {
                    NexusStatus::Degraded
                } else {
                    // nexus has no children or at least no child is online
                    NexusStatus::Faulted
                }
            }
        }
    }
}

/// If we fail to create one of the children we will fail the whole operation
/// destroy any created children and return the error. Once created, and we
/// bring the nexus online, there still might be a configuration mismatch that
/// would prevent the nexus to come online. We can only determine this
/// (currently) when online, so we check the errors twice for now.
#[tracing::instrument(level = "debug")]
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
        if let Err(err) = ni.create_and_register(child).await {
            ni.destroy_children().await;
            return Err(err).context(CreateChild {
                name: ni.name.clone(),
            });
        }
    }

    match ni.open().await {
        // we still have code that waits for children to come online
        // this however only works for config files so we need to clean up
        // if we get the below error
        Err(Error::NexusIncomplete {
            ..
        }) => {
            info!("deleting nexus due to missing children");
            for child in children {
                if let Err(e) = bdev_destroy(child).await {
                    error!("failed to destroy child during cleanup {}", e);
                }
            }

            return Err(Error::NexusCreate {
                name: String::from(name),
            });
        }

        Err(e) => {
            ni.destroy_children().await;
            return Err(e);
        }

        Ok(_) => nexus_list.push(ni),
    }
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
