//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.
//!
//! In order to create a nexus, it requires storage target URI's.
//!
//! Creating a 3 way replica nexus example:
//!
//! # example
//! ```ignore
//! use mayastor::descriptor::{Descriptor, DmaBuf};
//! use mayastor::bdev::nexus::nexus_bdev::nexus_create;
//! let children = vec![
//!        "aio:////disk1.img?blk_size=512".to_string(),
//!        "iscsi://foobar/iqn.2019-05.io.openebs:disk0".into(),
//!        "nvmf://fooo/nqn.2019-05.io-openebs:disk0".into(),
//!    ];
//!
//! // create the nexus using the vector of child devices
//! let nexus = nexus_create(
//!     "nexus-b6565df-af19-4645-9f98-e6a8b8c13b58",
//!     4096,
//!     131_027,
//!     Some("b6565df-af19-4645-9f98-e6a8b8c13b58"),
//!     &children,
//! ).await.unwrap();
//!
//! // open a block descriptor
//! let bd = Descriptor::open(&nexus, true).unwrap();
//!
//! // only use DMA buffers to issue IO, as its a member of the opened device
//! // alignment is handled implicitly
//! let mut buf = bd.dma_zmalloc(4096).unwrap();
//!
//! // fill the buffer with a know value
//! buf.fill(0xff);
//!
//! // write out the buffer to the nexus, all child devices will receive the
//! // same IO. Put differently. A single IO becomes three IOs
//! bd.write_at(0, &mut buf).await.unwrap();
//!
//! // fill the buffer with zeroes and read back the data
//! buf.fill(0x00);
//! bd.read_at(0, &mut buf).await.unwrap();
//!
//! // verify that the buffer is filled with wrote previously
//! buf.as_slice().into_iter().map(|b| assert_eq!(b, 0xff)).for_each(drop);
//! ```
//!
//! The nexus itself can be exported over the network as well
//!
//! # share
//! ```ignore
//! // make the nexus available as a block device to the rest of the system
//! let _device_path = nexus.share().unwrap();
//! ```

use std::{
    fmt::{Display, Formatter},
    ops::Neg,
    os::raw::c_void,
};

use futures::channel::oneshot;
use serde::Serialize;

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

use crate::{
    bdev::{
        nexus::{
            self,
            instances,
            nexus_channel::{DREvent, NexusChannel, NexusChannelInner},
            nexus_child::{ChildState, NexusChild},
            nexus_io::{Bio, IoStatus},
            nexus_nbd as nbd,
            Error,
        },
        Bdev,
    },
    descriptor::DmaBuf,
};

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

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
    pub(crate) children: Vec<NexusChild>,
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
    /// nbd device which the nexus is exposed through
    pub(crate) nbd_disk: Option<nbd::Disk>,
    /// the handle to be used when sharing the nexus, this allows for the bdev
    /// to be shared with vbdevs on top
    pub(crate) share_handle: Option<String>,
}

unsafe impl core::marker::Sync for Nexus {}

#[derive(Debug, Serialize, PartialEq)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// Online
    Online,
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
}

impl ToString for NexusState {
    fn to_string(&self) -> String {
        match *self {
            NexusState::Init => "init",
            NexusState::Online => "online",
            NexusState::Faulted => "faulted",
            NexusState::Degraded => "degraded",
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
    ) -> Result<Box<Self>, nexus::Error> {
        let mut b = Box::new(spdk_bdev::default());
        b.name = c_str!(name);
        b.product_name = c_str!(NEXUS_PRODUCT_ID);
        b.fn_table = nexus::fn_table().unwrap();
        b.module = nexus::module().unwrap().as_ptr();
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
            nbd_disk: None,
            share_handle: None,
            size,
        });

        n.bdev.set_uuid(match uuid {
            Some(uuid) => Some(uuid.to_string()),
            None => None,
        });

        if let Some(child_bdevs) = child_bdevs {
            n.add_children(child_bdevs);
        }

        // store a reference to the Self in the bdev structure.
        unsafe {
            (*n.bdev.inner).ctxt = n.as_ref() as *const _ as *mut c_void;
        }
        Ok(n)
    }

    /// get a mutable reference to a child at index
    pub fn get_child_as_mut_ref(
        &mut self,
        index: usize,
    ) -> Option<&mut NexusChild> {
        Some(&mut self.children[index])
    }

    /// set the state of the nexus
    pub(crate) fn set_state(&mut self, state: NexusState) {
        debug!(
            "{} Transitioned state from {:?} to {:?}",
            self.name, self.state, state
        );
        self.state = state;
    }

    pub(crate) fn is_healthy(&self) -> bool {
        !self.children.iter().any(|c| c.state != ChildState::Open)
    }

    /// returns the name of the nexus instance
    pub fn name(&self) -> &str {
        &self.name
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
            self.name(),
            event
        );

        NexusChannel::reconfigure(self.as_ptr(), &event);

        let result = r.await.expect("Reconfigure notify failure");

        info!(
            "{}: Dynamic reconfiguration event: {:?} completed {}",
            self.name(),
            event,
            result
        );
    }

    /// Opens the Nexus instance for IO
    pub async fn open(&mut self) -> Result<(), nexus::Error> {
        debug!("Opening nexus {}", self.name);

        self.try_open_children()?;
        self.sync_labels().await?;
        self.register()
    }

    pub async fn sync_labels(&mut self) -> Result<(), Error> {
        if let Ok(label) = self.update_child_labels().await {
            // now register the bdev but update its size first to
            // ensure we  adhere to the partitions

            // When the GUID does not match the given UUID it means
            // that the PVC has been recreated, is such as
            // case we should consider updating the labels

            info!("{}: {} ", self.name, label);
            self.data_ent_offset = label.offset();
            self.bdev.set_block_count(label.get_block_count());
        } else {
            // one or more children do not have, or have an invalid gpt label.
            // Recalculate that the header should have been and
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
            )?;

            self.write_label(&mut buf, &mut label, true).await?;
            self.write_label(&mut buf, &mut label, false).await?;
            info!("{}: {} ", self.name, label);

            self.write_pmbr().await?;
        }

        Ok(())
    }

    /// close the nexus and any children that are open
    pub fn close(&mut self) -> Result<(), ()> {
        info!("{}: Closing", self.name);
        self.children
            .iter_mut()
            .map(|c| {
                if c.state == ChildState::Open {
                    let _ = c.close();
                }
            })
            .for_each(drop);

        unsafe {
            spdk_io_device_unregister(self.as_ptr(), None);
        }

        Ok(())
    }

    /// Destroy the nexus.
    ///
    /// NOTE: The nexus may still live after returning from this method
    /// the close method is called from SPDK close callback any time after
    /// the bdev unregister is called so keep this call at the end of this
    /// method!
    pub async fn destroy(&mut self) {
        let _ = self.unshare().await;

        assert_eq!(self.share_handle, None);

        // doing this in the context of nexus_close() would be better
        // however, we cannot change the function in async there, so we
        // do it here.
        for child in self.children.iter_mut() {
            if child.state == ChildState::Open {
                let _ = child.close();
            }
            info!("Destroying child bdev {}", child.name);

            let r = child.destroy().await;
            if r.is_err() {
                warn!("Failed to destroy child {}", child.name);
            }
        }

        info!("Destroying nexus {}", self.name);

        unsafe {
            // This will trigger spdk callback to close() which removes
            // the device from global list of nexus's
            spdk_bdev_unregister(self.bdev_raw, None, std::ptr::null_mut());
        }
    }

    /// register the bdev with SPDK and set the callbacks for io channel
    /// creation. Once this function is called, the device is visible and can
    /// be used for IO.
    ///
    /// The registering is implemented such that any core can call
    /// get_io_channel from the function table. The io_channels, are
    /// constructed on demand and that's basically what this function does.
    ///
    /// Each io device is registered using a io_device as a key, and/or name. In
    /// our case, we dont actually create a channel ourselves, but we reference
    /// channels of the underlying bdevs.

    pub fn register(&mut self) -> Result<(), nexus::Error> {
        if self.state != NexusState::Init {
            error!("{}: Can only call register once", self.name);
            return Err(Error::AlreadyClaimed);
        }

        unsafe {
            spdk_io_device_register(
                self.as_ptr(),
                Some(NexusChannel::create),
                Some(NexusChannel::destroy),
                std::mem::size_of::<NexusChannel>() as u32,
                (*self.bdev.inner).name,
            );
        }

        debug!("{}: IO device registered at {:p}", self.name, self.as_ptr());

        let rc = unsafe { spdk_bdev_register(self.bdev.inner) };

        if rc != 0 {
            error!("{}: Failed to register", self.bdev.name());

            unsafe { spdk_io_device_unregister(self.as_ptr(), None) }
            self.children.iter_mut().map(|c| c.close()).for_each(drop);
            self.set_state(NexusState::Faulted);
            return Err(match rc.neg() {
                libc::EINVAL => Error::Invalid,
                libc::EEXIST => Error::Exists,
                libc::ENOMEM => Error::OutOfMemory,
                _ => Error::Internal("Failed to register bdev".to_owned()),
            });
        }

        self.set_state(NexusState::Online);
        info!("{}", self);
        Ok(())
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
        let mut pio = Bio::from(parent_io);

        // if any child IO has failed record this within the io context
        if !success {
            pio.get_ctx().status = IoStatus::Failed as i32;
        }

        pio.asses();
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
            let nexus = Bio::from(io);
            let nexus = nexus.nexus_as_ref();
            warn!("{}: Failed to get io buffer for io {:p}", nexus.name(), io);
            Bio::from(io).fail();
        }

        let ch = NexusChannel::inner_from_channel(ch);
        let (desc, ch) = ch.ch[ch.previous];
        let ret = Self::readv_impl(io, desc, ch);
        if ret != 0 {
            let nexus = Bio::from(io);
            let nexus = nexus.nexus_as_ref();
            error!("{}: Failed to submit IO {:p}", nexus.name(), io);
        }
    }

    /// read vectored io from the underlying children.
    pub(crate) fn readv(
        &self,
        pio: *mut spdk_bdev_io,
        channels: &mut NexusChannelInner,
    ) {
        let mut io = Bio::from(pio);

        // we use RR to read from the children also, set that we only need
        // to read from one child before we complete the IO to the callee.
        io.get_ctx().pending = 1;

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

        let (desc, ch) = channels.ch[child];

        let ret = Self::readv_impl(pio, desc, ch);

        if ret != 0 {
            error!(
                "{}: Failed to submit dispatched IO {:p}",
                io.nexus_as_ref().name(),
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
        let mut io = Bio::from(pio);
        // in case of writes, we want to write to all underlying children
        io.get_ctx().pending = channels.ch.len() as i8;
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                spdk_bdev_writev_blocks(
                    c.0,
                    c.1,
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
                "{}: Failed to submit dispatched IO {:p}",
                io.nexus_as_ref().name(),
                pio
            );
        }
    }

    pub(crate) fn unmap(
        &self,
        pio: *mut spdk_bdev_io,
        channels: &NexusChannelInner,
    ) {
        let mut io = Bio::from(pio);
        io.get_ctx().pending = channels.ch.len() as i8;
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                spdk_bdev_unmap_blocks(
                    c.0,
                    c.1,
                    io.offset() + io.nexus_as_ref().data_ent_offset,
                    io.num_blocks(),
                    Some(Self::io_completion),
                    pio as *mut _,
                )
            })
            .collect::<Vec<_>>();

        if results.iter().any(|r| *r != 0) {
            error!(
                "{}: Failed to submit dispatched IO {:p}",
                io.nexus_as_ref().name(),
                pio
            );
        }
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
        info!("Nexus with name {} already exists", name);
        return Err(Error::Exists);
    }

    let mut ni = Nexus::new(name, size, uuid, None)
        .expect("Failed to allocate Nexus instance");

    for child in children {
        if let Err(result) = ni.create_and_add_child(child).await {
            error!("{}: Failed to create child bdev {}", ni.name, child);
            ni.destroy_children().await;
            return Err(result);
        }
    }

    let opened = ni.open().await;

    if opened.is_ok() {
        nexus_list.push(ni);
        Ok(())
    } else {
        ni.destroy_children().await;
        Err(Error::Internal("Failed to open the nexus".to_owned()))
    }
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    if let Some(nexus) = instances().iter_mut().find(|n| n.name() == name) {
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
