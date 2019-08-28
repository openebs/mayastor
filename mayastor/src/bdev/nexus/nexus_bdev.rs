///
/// The nexus is one of core components, next to the target services. With
/// the nexus a developer is able to construct a per application volume
/// optimized for the perceived intent. For example, depending on
/// application needs synchronous mirroring may be required.
///
/// In order to create a nexus, it requires storage target URI's.
///
/// Creating a 3 way replica nexus example:
///
/// # example
/// ```ignore
/// use mayastor::descriptor::{Descriptor, DmaBuf};
/// use mayastor::bdev::nexus::nexus_bdev::nexus_create;
/// let children = vec![
///        "aio:////disk1.img?blk_size=512".to_string(),
///        "iscsi://foobar/iqn.2019-05.io.openebs:disk0".into(),
///        "nvmf://fooo/nqn.2019-05.io-openebs:disk0".into(),
///    ];
///
/// // if no UUID given, one will be generated for you
/// let uuid = "b6565df-af19-4645-9f98-e6a8b8c13b58".to_string();
///
/// // create the nexus using the vector of child devices
/// let nexus = nexus_create("mynexus", 4096, 131_027, Some(uuid),  &children).await.unwrap();
///
/// // open a block descriptor
/// let bd = Descriptor::open(&nexus, true).unwrap();
///
/// // only use DMA buffers to issue IO, as its a member of the opened device
/// // alignment is handled implicitly
/// let mut buf = bd.dma_zmalloc(4096).unwrap();
///
/// // fill the buffer with a know value
/// buf.fill(0xff);
///
/// // write out the buffer to the nexus, all child devices will receive the
/// // same IO. Put differently. A single IO becomes three IOs
/// bd.write_at(0, &mut buf).await.unwrap();
///
/// // fill the buffer with zeroes and read back the data
/// buf.fill(0x00);
/// bd.read_at(0, &mut buf).await.unwrap();
///
/// // verify that the buffer is filled with wrote previously
/// buf.as_slice().into_iter().map(|b| assert_eq!(b, 0xff)).for_each(drop);
/// ```
///
/// The nexus itself can be exported over the network as well
///
/// # share
/// ```ignore
/// // make the nexus available over the network, the network settings depend
/// // on other configuration factors
/// nexus.share().unwrap();
/// ```
use crate::bdev::{
    bdev_lookup_by_name,
    nexus::{
        self,
        nexus_channel::NexusChannel,
        nexus_child::{ChildState, NexusChild},
        nexus_io::Nio,
        Error,
    },
    Bdev,
};

use crate::{
    bdev::nexus::{nexus_channel::NexusChannelInner, nexus_io::IoStatus},
    nexus_uri::BdevType,
};

use crate::bdev::nexus::instances;
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
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_io_device_register,
    spdk_io_device_unregister,
    spdk_put_io_channel,
};

use futures::future::join_all;
use std::{
    fmt::{Display, Formatter},
    ops::Neg,
    os::raw::c_void,
};

use crate::{
    bdev::nexus::{nexus_channel::DREvent, Error::Internal},
    nexus_uri::nexus_parse_uri,
};
use futures::channel::oneshot;

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

/// The main nexus structure
#[derive(Debug)]
pub struct Nexus {
    /// Name of the Nexus instance
    name: String,
    /// number of children part of this nexus
    child_count: u32,
    /// vector of children
    pub(crate) children: Vec<NexusChild>,
    /// inner bdev
    pub bdev: Bdev,
    /// raw pointer to bdev (to destruct it later using Box::from_raw())
    bdev_raw: *mut spdk_bdev,
    /// represents the current state of the Nexus
    pub(crate) state: NexusState,
    /// Dynamic Reconfigure event
    pub dr_complete_notify: Option<oneshot::Sender<i32>>,
}

unsafe impl core::marker::Sync for Nexus {}

#[derive(Debug, Serialize, PartialEq)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// Online
    Online,
    /// The nexus can not do any IO
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
        block_len: u32,
        block_cnt: u64,
        child_bdevs: Option<&[String]>,
        uuid: Option<String>,
    ) -> Result<Box<Self>, nexus::Error> {
        let mut b = Box::new(spdk_bdev::default());
        b.name = c_str!(name);
        b.product_name = c_str!(NEXUS_PRODUCT_ID);
        b.fn_table = nexus::fn_table().unwrap();
        b.module = nexus::module().unwrap().as_ptr();
        b.blocklen = block_len;
        b.blockcnt = block_cnt;
        b.required_alignment = 9;

        let mut n = Box::new(Nexus {
            name: name.to_string(),
            child_count: 0,
            children: Vec::new(),
            bdev: Bdev::from(&*b as *const _ as *mut spdk_bdev),
            state: NexusState::Init,
            bdev_raw: Box::into_raw(b),
            dr_complete_notify: None,
        });

        n.bdev.set_uuid(uuid);

        if let Some(child_bdevs) = child_bdevs {
            n.add_children(child_bdevs);
        }

        // store a reference to the Self in the bdev structure.
        unsafe {
            (*n.bdev.inner).ctxt = n.as_ref() as *const _ as *mut c_void;
        }
        Ok(n)
    }

    /// set the state of the nexus
    fn set_state(&mut self, state: NexusState) {
        debug!(
            "{} Transitioned state from {:?} to {:?}",
            self.name, self.state, state
        );
        self.state = state;
    }

    fn is_healty(&self) -> bool {
        !self.children.iter().any(|c| c.state != ChildState::Open)
    }

    /// returns the name of the nexus instance
    pub fn name(&self) -> &str {
        &self.name
    }

    /// add the child bdevs to the nexus instance in the "init state"
    /// this function should be used when bdevs are added asynchronously
    /// like for example, when parsing the init file. The examine callback
    /// will iterate through the list and invoke nexus::online once completed
    pub fn add_children(&mut self, dev_name: &[String]) {
        self.child_count = dev_name.len() as u32;
        dev_name
            .iter()
            .map(|c| {
                debug!("{}: Adding child {}", self.name(), c);
                self.children.push(NexusChild::new(
                    c.clone(),
                    self.name.clone(),
                    bdev_lookup_by_name(c),
                ))
            })
            .for_each(drop);
    }

    /// create a bdev based on its URL and add it to the nexus
    pub async fn create_and_add_child(
        &mut self,
        uri: &str,
    ) -> Result<String, Error> {
        let bdev_type = nexus_parse_uri(uri)?;

        // workaround until we can get async fn trait
        let name = match bdev_type {
            BdevType::Aio(args) => args.create().await?,
            BdevType::Iscsi(args) => args.create().await?,
            BdevType::Nvmf(args) => args.create().await?,
        };

        self.children.push(NexusChild::new(
            name.clone(),
            self.name.clone(),
            bdev_lookup_by_name(&name),
        ));

        self.child_count += 1;

        Ok(name)
    }

    /// offline a child device and reconfigure the IO channels
    pub async fn offline_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{}: Offline child request for {}", self.name(), name);

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.close()?;
            let ch = unsafe { spdk_get_io_channel(self.as_ptr()) };
            self.reconfigure(DREvent::ChildOffline).await;
            unsafe { spdk_put_io_channel(ch) }
            self.set_state(NexusState::Degraded);
            Ok(NexusState::Degraded)
        } else {
            Err(Error::NotFound)
        }
    }

    /// online a chilld and reconfigure the IO channels
    pub async fn online_child(
        &mut self,
        name: &str,
    ) -> Result<NexusState, nexus::Error> {
        trace!("{} Online child request", self.name());

        if let Some(child) = self.children.iter_mut().find(|c| c.name == name) {
            child.open(self.bdev.num_blocks(), self.bdev.block_size())?;
            let ch = unsafe { spdk_get_io_channel(self.as_ptr()) };
            self.reconfigure(DREvent::ChildOnline).await;
            unsafe { spdk_put_io_channel(ch) };
            if self.is_healty() {
                self.set_state(NexusState::Online);
                Ok(NexusState::Online)
            } else {
                Ok(NexusState::Degraded)
            }
        } else {
            Err(Error::NotFound)
        }
    }

    /// reconfigure the child event handler
    async fn reconfigure(&mut self, event: DREvent) {
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

    /// destroy all children that are part of this nexus
    async fn destroy_children(&mut self) {
        let futures = self.children.iter().map(|c| c.destroy());
        let results = join_all(futures).await;
        if results.iter().any(|c| c.is_err()) {
            error!("{}: Failed to destroy child", self.name);
        }
    }

    /// Add a child to the configuration when when an example callback is run.
    /// The nexus is not opened implicitly, call .open() for this manually.
    pub fn examine_child(&mut self, name: &str) -> bool {
        for mut c in &mut self.children {
            if c.name == name && c.state == ChildState::Init {
                if let Some(bdev) = bdev_lookup_by_name(name) {
                    debug!("{}: Adding child {}", self.name, name);
                    c.bdev = Some(bdev);
                    return true;
                }
            }
        }
        false
    }

    /// Opens the Nexus instance for IO
    pub fn open(&mut self) -> Result<(), nexus::Error> {
        debug!("Opening nexus {}", self.name);

        // if the child list is empty -- we can't go online

        if self.children.is_empty() {
            return Err(Error::NexusIncomplete);
        }

        // if one of the children does not have bdev struct yet we are
        // considered to be incomplete.

        if !self.children.iter().any(|c| c.bdev.is_none()) {
            if let Err(register) = self.register() {
                error!("{}: Failed to register nexus", self.name());
                Err(register)
            } else {
                Ok(())
            }
        } else {
            debug!("{}: config incomplete deferring open", self.name);
            Err(Error::NexusIncomplete)
        }
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

    /// register the bdev with SPDK and set the callbacks for io channel
    /// creation. Once this function is called, the device is visible and can
    /// be used for IO.
    ///
    /// The registering is implement such that any core can call get_io_channel
    /// from the function table. The io_channels, are constructed on demand and
    /// that's basically what this function does.
    ///
    /// Each io device is registered using a io_device as a key, and/or name. In
    /// our case, we dont actually create a channel ourselves but we reference
    /// channels of the underlying bdevs.
    pub fn register(&mut self) -> Result<(), nexus::Error> {
        if self.state != NexusState::Init {
            error!("{}: Can only call register once", self.name);
            return Err(Error::AlreadyClaimed);
        }

        let num_blocks = self.bdev.num_blocks();
        let block_size = self.bdev.block_size();

        let (open, error): (Vec<_>, Vec<_>) = self
            .children
            .iter_mut()
            .map(|c| c.open(num_blocks, block_size))
            .partition(Result::is_ok);

        // depending on IO consistency policies, we might be able to go online
        // even if one of the children failed to open. This is work is not
        // completed yet so we fail the registration all together for now.

        if !error.is_empty() {
            open.into_iter()
                .map(Result::unwrap)
                .map(|name| {
                    if let Some(child) =
                        self.children.iter_mut().find(|c| c.name == name)
                    {
                        let _ = child.close();
                    } else {
                        error!("{}: child opened but found!", self.name());
                    }
                })
                .for_each(drop);

            return Err(Error::NexusIncomplete);
        }

        // all children opened successfully, register the nexus, make sure we
        // have proper alignment for all the children by simply setting the
        // alignment to the highest value found on children.

        self.children
            .iter()
            .map(|c| c.bdev.as_ref().unwrap().alignment())
            .collect::<Vec<_>>()
            .iter()
            .map(|s| {
                if self.bdev.alignment() < *s {
                    unsafe {
                        (*self.bdev.inner).required_alignment = *s;
                    }
                }
            })
            .for_each(drop);

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
                _ => Error::Internal,
            });
        }

        self.set_state(NexusState::Online);
        info!("{}", self);
        Ok(())
    }

    /// takes self and converts into a raw pointer
    fn as_ptr(&self) -> *mut c_void {
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
        let mut pio = Nio::from(parent_io);

        // determining if an IO failed or succeeded is handled internally within
        // this functions. it is very rudimentary now, and simply
        // ensures all states are successful. In the longer run, the
        // determination of success of failure should be policy driven.

        if success {
            pio.io_complete(IoStatus::Success);
        } else {
            let nexus = pio.nexus_as_ref();
            trace!(
                "{}: IO failed: parent_io {:p} child_io {:p}",
                nexus.name(),
                parent_io,
                child_io,
            );
            pio.io_complete(IoStatus::Failed);
        }

        Nio::io_free(child_io);
    }

    /// callback when the IO has buffer associated with itself
    extern "C" fn nexus_get_buf_cb(
        ch: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
        success: bool,
    ) {
        if !success {
            let nexus = Nio::from(io);
            let nexus = nexus.nexus_as_ref();
            warn!("{}: Failed to get io buffer for io {:p}", nexus.name(), io);
            let mut pio = Nio::from(io);
            pio.io_complete(IoStatus::Failed);
        }

        let ch = NexusChannel::inner_from_channel(ch);
        let (desc, ch) = ch.ch[ch.previous];
        let ret = Self::readv_impl(io, desc, ch);
        if ret != 0 {
            let nexus = Nio::from(io);
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
        let mut io = Nio::from(pio);

        // we use RR to read from the children and also, set that we only need
        // to read from one child before we complete the IO to the callee.
        io.set_outstanding(1);
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
            io.set_outstanding(0);
            io.nio_set_status(IoStatus::Failed);
        }
    }

    /// do the actual read
    fn readv_impl(
        pio: *mut spdk_bdev_io,
        desc: *mut spdk_bdev_desc,
        ch: *mut spdk_io_channel,
    ) -> i32 {
        let io = Nio::from(pio);

        unsafe {
            spdk_bdev_readv_blocks(
                desc,
                ch,
                io.iovs(),
                io.iov_count(),
                io.offset(),
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
        let mut io = Nio::from(pio);

        // in case of writes, we want to write to all underlying children
        io.set_outstanding(channels.ch.len());
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                spdk_bdev_writev_blocks(
                    c.0,
                    c.1,
                    io.iovs(),
                    io.iov_count(),
                    io.offset(),
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
        let mut io = Nio::from(pio);
        io.set_outstanding(channels.ch.len());
        let results = channels
            .ch
            .iter()
            .map(|c| unsafe {
                spdk_bdev_unmap_blocks(
                    c.0,
                    c.1,
                    io.offset(),
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
/// bring the nexus online, there still might be a configuration mismatch which
/// would prevent the nexus to come online. We can only determine this
/// (currently) when online so we check the errors twice for now.
pub async fn nexus_create(
    name: &str,
    block_len: u32,
    block_cnt: u64,
    uuid: Option<String>,
    children: &[String],
) -> Result<String, Error> {
    // global variable defined in the nexus module
    let name = name.to_string();
    let nexus_list = instances();
    if nexus_list.iter().any(|n| n.name == name) {
        info!("Nexus with name {} already exists", name);
        return Err(Error::Exists);
    }

    let mut ni = Nexus::new(&name, block_len, block_cnt, None, uuid)
        .expect("Failed to allocate Nexus instance");

    for child in children {
        if let Err(result) = ni.create_and_add_child(child).await {
            error!("{}: Failed to create child bdev {}", ni.name, child);
            ni.destroy_children().await;
            return Err(result);
        }
    }

    if ni.open().is_ok() {
        nexus_list.push(ni);
        Ok(name)
    } else {
        ni.destroy_children().await;
        Err(Internal)
    }
}

/// lookup a nexus by its name
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    if let Some(nexus) = instances().iter_mut().find(|n| n.name() == name) {
        Some(nexus)
    } else {
        None
    }
}

/// destroy the a nexus by name
pub async fn nexus_destroy(name: &str) -> Result<String, Error> {
    let nexus_list = instances();
    let name = name.to_string();
    if !nexus_list.iter().any(|n| n.name == name) {
        info!("Nexus with name {} does not exist", name);
        return Err(Error::NotFound);
    }

    let mut removed = nexus_list
        .drain_filter(|n| name == n.name)
        .collect::<Vec<_>>();

    if let Some(mut nexus) = removed.pop() {
        unsafe {
            spdk_bdev_unregister(nexus.bdev_raw, None, std::ptr::null_mut())
        }

        // doing this in the context of nexus_close() would be better
        // however we can not change the the function in async there so we
        // do it here.

        for child in nexus.children.iter_mut() {
            if child.state == ChildState::Open {
                let _ = child.close();
            }
            info!("Destroying child bdev {}", child.name);

            let r = child.destroy().await;
            if r.is_err() {
                warn!("Failed to destroy child {}", child.name);
            }
        }

        info!("Nexus {} destroyed", name);
        Ok(name)
    } else {
        error!("{} Disappeared while trying to delete it", name);
        Err(Error::Internal)
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
            self.bdev.block_size(),
        );

        self.children
            .iter()
            .map(|c| write!(f, "\t{}", c))
            .for_each(drop);
        Ok(())
    }
}
