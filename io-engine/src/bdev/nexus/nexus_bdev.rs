//!
//! The nexus is one of core components, next to the target services. With
//! the nexus a developer is able to construct a per application volume
//! optimized for the perceived intent. For example, depending on
//! application needs synchronous mirroring may be required.

use std::{
    cmp::min,
    collections::HashSet,
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
    marker::PhantomPinned,
    os::raw::c_void,
    pin::Pin,
};

use crossbeam::atomic::AtomicCell;
use futures::channel::oneshot;
use serde::Serialize;
use snafu::ResultExt;
use uuid::Uuid;

use super::{
    nexus_err,
    nexus_injection::Injections,
    nexus_lookup_name_uuid,
    DrEvent,
    Error,
    NbdDisk,
    NexusBio,
    NexusChannel,
    NexusChild,
    NexusModule,
    PersistOp,
};

use crate::{
    bdev::{
        device_destroy,
        device_lookup,
        device_create,
        nexus::{nexus_persistence::PersistentNexusInfo, NexusIoSubsystem},
    },
    core::{
        partition,
        Bdev,
        DeviceEventSink,
        IoType,
        Protocol,
        Reactor,
        Share,
        VerboseError,
    },
    rebuild::HistoryRecord,
    subsys::NvmfSubsystem,
};

use crate::bdev::PtplFileOps;
use spdk_rs::{
    BdevIo,
    BdevOps,
    ChannelTraverseStatus,
    IoChannel,
    IoDevice,
    IoDeviceChannelTraverse,
    JsonWriteContext,
    libspdk::spdk_bdev_is_zoned,
};

pub static NVME_MIN_CNTLID: u16 = 1;
pub static NVME_MAX_CNTLID: u16 = 0xffef;

pub(crate) static NEXUS_PRODUCT_ID: &str = "Nexus CAS Driver v0.0.1";

/// TODO
#[derive(Debug)]
pub enum NexusTarget {
    NbdDisk(NbdDisk),
    NexusNvmfTarget,
}

/// Sensitive nexus operations that might require extra checks against
/// current nexus state in order to be performed.
#[derive(Debug)]
pub enum NexusOperation {
    ReplicaAdd,
    ReplicaRemove,
    ReplicaOnline,
    ReplicaFault,
    NexusSnapshot,
}

/// TODO
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NvmeAnaState {
    InvalidState, // invalid, do not use
    OptimizedState,
    NonOptimizedState,
    InaccessibleState,
    PersistentLossState, // not yet supported
    ChangeState,         // not yet supported
}

impl NvmeAnaState {
    pub fn from_i32(value: i32) -> Result<NvmeAnaState, Error> {
        match value {
            0 => Ok(NvmeAnaState::InvalidState),
            1 => Ok(NvmeAnaState::OptimizedState),
            2 => Ok(NvmeAnaState::NonOptimizedState),
            3 => Ok(NvmeAnaState::InaccessibleState),
            4 => Ok(NvmeAnaState::PersistentLossState),
            15 => Ok(NvmeAnaState::ChangeState),
            _ => Err(Error::InvalidNvmeAnaState {
                ana_value: value,
            }),
        }
    }
}

/// NVMe reservation types.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NvmeReservation {
    Reserved = 0,
    WriteExclusive = 1,
    ExclusiveAccess = 2,
    WriteExclusiveRegsOnly = 3,
    ExclusiveAccessRegsOnly = 4,
    WriteExclusiveAllRegs = 5,
    ExclusiveAccessAllRegs = 6,
}
impl TryFrom<u8> for NvmeReservation {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Reserved,
            1 => Self::WriteExclusive,
            2 => Self::ExclusiveAccess,
            3 => Self::WriteExclusiveRegsOnly,
            4 => Self::ExclusiveAccessRegsOnly,
            5 => Self::WriteExclusiveAllRegs,
            6 => Self::ExclusiveAccessAllRegs,
            reservation => {
                return Err(Error::InvalidReservation {
                    reservation,
                })
            }
        })
    }
}

/// Nexus NVMe preemption policy.
#[derive(Debug, Copy, Clone)]
pub enum NexusNvmePreemption {
    /// A "manual" preemption where we explicitly specify the reservation key,
    /// type and preempt key.
    ArgKey,
    /// An "automatic" preemption where we can preempt whatever is current
    /// holder. Useful when we just want to boot the existing holder out.
    Holder,
}

/// NVMe-specific parameters for the Nexus.
#[derive(Debug)]
pub struct NexusNvmeParams {
    /// The minimum NVMe controller ID for sharing over NVMf.
    pub(crate) min_cntlid: u16,
    /// The maximum NVMe controller ID.
    pub(crate) max_cntlid: u16,
    /// NVMe reservation key for children.
    pub(crate) resv_key: u64,
    /// NVMe preempt key for children, None to not preempt.
    pub(crate) preempt_key: Option<std::num::NonZeroU64>,
    /// NVMe reservation type.
    pub(crate) resv_type: NvmeReservation,
    /// NVMe Preempting policy.
    pub(crate) preempt_policy: NexusNvmePreemption,
}

impl Default for NexusNvmeParams {
    fn default() -> Self {
        NexusNvmeParams {
            min_cntlid: NVME_MIN_CNTLID,
            max_cntlid: NVME_MAX_CNTLID,
            resv_key: 0x1234_5678,
            preempt_key: None,
            resv_type: NvmeReservation::WriteExclusiveAllRegs,
            preempt_policy: NexusNvmePreemption::ArgKey,
        }
    }
}

impl NexusNvmeParams {
    /// Set the minimum controller id.
    pub fn set_min_cntlid(&mut self, min_cntlid: u16) {
        self.min_cntlid = min_cntlid;
    }
    /// Set the maximum controller id.
    pub fn set_max_cntlid(&mut self, max_cntlid: u16) {
        self.max_cntlid = max_cntlid;
    }
    /// Set the reservation key.
    pub fn set_resv_key(&mut self, resv_key: u64) {
        self.resv_key = resv_key;
    }
    /// Set the preemption key.
    pub fn set_preempt_key(
        &mut self,
        preempt_key: Option<std::num::NonZeroU64>,
    ) {
        self.preempt_key = preempt_key;
    }
    /// Set the reservation type.
    pub fn set_resv_type(&mut self, resv_type: NvmeReservation) {
        self.resv_type = resv_type;
    }
    /// Set the preemption policy.
    pub fn set_preempt_policy(&mut self, preempt_policy: NexusNvmePreemption) {
        self.preempt_policy = preempt_policy;
    }
    /// Check if reservations are enabled.
    pub fn reservations_enabled(&self) -> bool {
        self.resv_key != 0
            || self.preempt_key.is_some()
            || !matches!(self.preempt_policy, NexusNvmePreemption::ArgKey)
    }
    /// Check if reservations are valid.
    pub fn reservations_valid(&self) -> bool {
        !(self.resv_key == 0
            || (matches!(self.preempt_policy, NexusNvmePreemption::Holder)
                && self.preempt_key.is_some()))
    }
}

/// The main nexus structure
pub struct Nexus<'n> {
    /// Name of the Nexus instance
    pub(crate) name: String,
    /// The requested size of the Nexus in bytes. Children are allowed to
    /// be larger. The actual Nexus size will be calculated based on the
    /// capabilities of the underlying child devices.
    req_size: u64,
    /// Vector of nexus children.
    pub(super) children: Vec<NexusChild<'n>>,
    /// NVMe parameters
    pub(crate) nvme_params: NexusNvmeParams,
    /// uuid of the nexus (might not be the same as the nexus bdev!)
    nexus_uuid: Uuid,
    /// Bdev wrapper instance.
    bdev: Option<Bdev<Nexus<'n>>>,
    /// represents the current state of the Nexus
    pub(crate) state: parking_lot::Mutex<NexusState>,
    /// The offset in blocks where the data partition starts.
    pub(crate) data_ent_offset: u64,
    /// enum containing the protocol-specific target used to publish the nexus
    pub(super) nexus_target: Option<NexusTarget>,
    /// Indicates if the Nexus has an I/O device.
    pub(super) has_io_device: bool,
    pub(super) is_zoned: bool,
    /// Information associated with the persisted NexusInfo structure.
    pub(super) nexus_info: futures::lock::Mutex<PersistentNexusInfo>,
    /// Nexus I/O subsystem.
    io_subsystem: Option<NexusIoSubsystem<'n>>,
    /// TODO
    event_sink: Option<DeviceEventSink>,
    /// Rebuild history of all children of this nexus instance.
    pub(super) rebuild_history: parking_lot::Mutex<Vec<HistoryRecord>>,
    /// TODO
    #[allow(dead_code)]
    pub(super) injections: Injections,
    /// Flag to control shutdown from I/O path.
    pub(crate) shutdown_requested: AtomicCell<bool>,
    /// Prevent auto-Unpin.
    _pin: PhantomPinned,
    /// Initiators.
    initiators: parking_lot::Mutex<HashSet<String>>,
}

impl<'n> Debug for Nexus<'n> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        write!(f, "Nexus '{}' [{}]", self.name, s)
    }
}

/// Nexus status enumeration.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusStatus {
    /// The nexus cannot perform any IO operation
    Faulted,
    /// Degraded, one or more child is missing but IO can still flow
    Degraded,
    /// Online
    Online,
    /// Shutdown in progress
    ShuttingDown,
    /// Shutdown
    Shutdown,
}

impl Display for NexusStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NexusStatus::Degraded => "degraded",
                NexusStatus::Online => "online",
                NexusStatus::Faulted => "faulted",
                NexusStatus::ShuttingDown => "shutting_down",
                NexusStatus::Shutdown => "shutdown",
            }
        )
    }
}

/// Nexus state enumeration.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd)]
pub enum NexusState {
    /// nexus created but no children attached
    Init,
    /// closed
    Closed,
    /// open
    Open,
    /// reconfiguring internal IO channels
    Reconfiguring,
    /// Shutdown in progress
    ShuttingDown,
    /// nexus has been shutdown
    Shutdown,
}

impl Display for NexusState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NexusState::Init => "init",
                NexusState::Closed => "closed",
                NexusState::Open => "open",
                NexusState::Reconfiguring => "reconfiguring",
                NexusState::ShuttingDown => "shutting_down",
                NexusState::Shutdown => "shutdown",
            }
        )
    }
}

#[derive(Debug)]
pub struct ZoneInfo {
    zoned: bool,
    num_zones: u64,
    zone_size: u64,
    max_zone_append_size: u32,
    max_open_zones: u32,
    max_active_zones: u32,
    optimal_open_zones: u32,
}

impl Default for ZoneInfo {
    fn default() -> ZoneInfo{
        ZoneInfo{
            zoned: false,
            num_zones: Default::default(),
            zone_size: Default::default(),
            max_zone_append_size: Default::default(),
            max_open_zones: Default::default(),
            max_active_zones: Default::default(),
            optimal_open_zones: Default::default(),
        }
    }
}

impl<'n> Nexus<'n> {
    /// create a new nexus instance with optionally directly attaching
    /// children to it.
    fn new(
        name: &str,
        size: u64,
        bdev_uuid: Option<&str>,
        nexus_uuid: Option<uuid::Uuid>,
        nvme_params: NexusNvmeParams,
        nexus_info_key: Option<String>,
        zone_info: ZoneInfo,
    ) -> spdk_rs::Bdev<Nexus<'n>> {
        let n = Nexus {
            name: name.to_string(),
            children: Vec::new(),
            state: parking_lot::Mutex::new(NexusState::Init),
            bdev: None,
            data_ent_offset: 0,
            req_size: size,
            nexus_target: None,
            nvme_params,
            has_io_device: false,
            initiators: parking_lot::Mutex::new(HashSet::new()),
            nexus_info: futures::lock::Mutex::new(PersistentNexusInfo::new(
                nexus_info_key,
            )),
            io_subsystem: None,
            is_zoned: false,
            nexus_uuid: Default::default(),
            event_sink: None,
            rebuild_history: parking_lot::Mutex::new(Vec::new()),
            injections: Injections::new(),
            shutdown_requested: AtomicCell::new(false),
            _pin: Default::default(),
        };

        let mut bdev = NexusModule::current()
            .bdev_builder()
            .with_name(name)
            .with_product_name(NEXUS_PRODUCT_ID)
            .with_uuid(Self::make_uuid(name, bdev_uuid))
            .with_block_length(0)
            .with_block_count(0)
            .with_required_alignment(9)
            .with_data(n)
            .with_zoned(zone_info.zoned)
            .with_num_zones(zone_info.num_zones)
            .with_zone_size(zone_info.zone_size)
            .with_max_zone_append_size(zone_info.max_zone_append_size)
            .with_max_open_zones(zone_info.max_open_zones)
            .with_max_active_zones(zone_info.max_active_zones)
            .with_optimal_open_zones(zone_info.optimal_open_zones)
            .build();

        unsafe {
            let n = bdev.data_mut().get_unchecked_mut();
            n.bdev = Some(Bdev::new(bdev.clone()));

            n.event_sink = Some(DeviceEventSink::new(bdev.data()));

            // Set the nexus UUID to be the specified nexus UUID, otherwise
            // inherit the bdev UUID.
            n.nexus_uuid = nexus_uuid.unwrap_or_else(|| n.bdev().uuid());

            // Set I/O subsystem.
            n.io_subsystem = Some(NexusIoSubsystem::new(
                name.to_string(),
                n.bdev.as_mut().unwrap(),
            ));
            n.is_zoned = zone_info.zoned;
        }

        info!(
            "{:?}: creating new nexus bdev with UUID '{}'",
            bdev.data(),
            bdev.data().uuid()
        );

        bdev
    }

    /// TODO
    pub(crate) fn get_event_sink(&self) -> DeviceEventSink {
        self.event_sink
            .clone()
            .expect("Nexus device event sink not ready")
    }

    /// Makes the UUID of the underlying Bdev of this nexus.
    /// Generates a new UUID if specified uuid is None (or invalid).
    fn make_uuid(name: &str, uuid: Option<&str>) -> spdk_rs::Uuid {
        match uuid {
            Some(s) => match uuid::Uuid::parse_str(s) {
                Ok(u) => {
                    debug!("Nexus '{}': UUID set to '{}'", name, u);
                    return u.into();
                }
                Err(error) => {
                    warn!("Nexus '{}': invalid UUID '{}': {}", name, s, error);
                }
            },
            None => {
                warn!("Nexus '{}': no UUID specified", name);
            }
        }

        let u = spdk_rs::Uuid::generate();
        debug!("Nexus '{}': using generated UUID '{}'", name, u);
        u
    }

    /// Returns nexus name.
    pub(crate) fn nexus_name(&self) -> &str {
        &self.name
    }

    /// Returns the Nexus uuid.
    pub(crate) fn uuid(&self) -> Uuid {
        self.nexus_uuid
    }

    /// Add new initiator to the Nexus
    #[allow(dead_code)]
    pub(crate) fn add_initiator(&self, initiator: &str) {
        debug!("{self:?}: adding initiator '{initiator}'");
        self.initiators.lock().insert(initiator.to_string());
    }

    /// Remove initiator from the Nexus
    #[allow(dead_code)]
    pub(crate) fn rm_initiator(&self, initiator: &str) {
        debug!("{self:?}: removing initiator '{initiator}'");
        self.initiators.lock().remove(initiator);
    }

    /// initiator count from the Nexus
    #[allow(dead_code)]
    pub(crate) fn initiator_cnt(&self) -> usize {
        self.initiators.lock().len()
    }

    /// Sets the state of the Nexus.
    fn set_state(self: Pin<&mut Self>, state: NexusState) -> NexusState {
        debug!("{:?}: changing state to '{}'", self, state);
        *self.state.lock() = state;
        state
    }

    /// Returns name of the underlying Bdev.
    pub(crate) fn bdev_name(&self) -> String {
        unsafe { self.bdev().name().to_string() }
    }

    /// TODO
    pub fn req_size(&self) -> u64 {
        self.req_size
    }

    /// Returns the actual size of the Nexus instance, in bytes.
    pub fn size_in_bytes(&self) -> u64 {
        unsafe { self.bdev().size_in_bytes() }
    }

    /// Returns Nexus's block size in bytes.
    pub fn block_len(&self) -> u64 {
        unsafe { self.bdev().block_len() as u64 }
    }

    /// Returns the actual size of the Nexus instance, in blocks.
    pub fn num_blocks(&self) -> u64 {
        unsafe { self.bdev().num_blocks() }
    }

    /// Returns the required alignment of the Nexus.
    pub fn alignment(&self) -> u64 {
        unsafe { self.bdev().alignment() }
    }

    /// Returns the required alignment of the Nexus.
    pub fn required_alignment(&self) -> u8 {
        unsafe { self.bdev().required_alignment() }
    }

    pub fn is_zoned(&self) -> bool {
        unsafe { spdk_bdev_is_zoned(self.bdev().unsafe_inner_ptr()) }
    }

    /// TODO
    pub fn children(&self) -> &Vec<NexusChild<'n>> {
        &self.children
    }

    /// TODO
    pub(super) unsafe fn child_add_unsafe(
        self: Pin<&mut Self>,
        child: NexusChild<'n>,
    ) {
        self.unpin_mut().children.push(child)
    }

    /// TODO
    pub fn child_at(&self, idx: usize) -> &NexusChild<'n> {
        self.children.get(idx).expect("Bad child index")
    }

    /// TODO
    #[allow(dead_code)]
    pub(super) unsafe fn child_at_mut(
        self: Pin<&mut Self>,
        idx: usize,
    ) -> &mut NexusChild<'n> {
        self.unpin_mut()
            .children
            .get_mut(idx)
            .expect("Bad child index")
    }

    /// TODO
    pub fn children_iter(&self) -> std::slice::Iter<NexusChild<'n>> {
        self.children.iter()
    }

    /// TODO
    pub(super) unsafe fn children_iter_mut(
        self: Pin<&mut Self>,
    ) -> std::slice::IterMut<NexusChild<'n>> {
        self.unpin_mut().children.iter_mut()
    }

    /// TODO
    pub fn child_count(&self) -> usize {
        self.children.len()
    }

    /// Check whether nexus can perform target operation.
    pub(crate) fn check_nexus_operation(
        &self,
        _op: NexusOperation,
    ) -> Result<(), Error> {
        match *self.state.lock() {
            // When nexus under shutdown or is shutdown, no further nexus
            // operations allowed.
            NexusState::ShuttingDown | NexusState::Shutdown => {
                Err(Error::OperationNotAllowed {
                    reason: "Nexus is shutdown".to_string(),
                })
            }
            _ => Ok(()),
        }
    }

    /// Reconfigures the child event handler.
    pub(crate) async fn reconfigure(&self, event: DrEvent) {
        info!(
            "{self:?}: dynamic reconfiguration event: {event}, \
            reconfiguring I/O channels...",
        );

        let (sender, recv) = oneshot::channel::<ChannelTraverseStatus>();

        self.traverse_io_channels(
            sender,
            |chan, _sender| -> ChannelTraverseStatus {
                chan.reconnect_all();
                ChannelTraverseStatus::Ok
            },
            |status, sender| {
                debug!("{self:?}: all I/O channels reconfigured");
                sender.send(status).expect("reconfigure channel gone");
            },
        );

        let result = recv.await.expect("reconfigure sender already dropped");

        info!(
            "{self:?}: dynamic reconfiguration event: {event}, \
            reconfiguring I/O channels completed with result: {result:?}",
        );
    }

    /// Configure nexus's block device to match parameters of the child devices.
    async fn setup_nexus_bdev(mut self: Pin<&mut Self>) -> Result<(), Error> {
        let name = self.name.clone();

        if self.children().is_empty() {
            return Err(Error::NexusIncomplete {
                name,
                reason: "No child devices".to_string(),
            });
        }

        // Determine Nexus block size and data start and end offsets.
        let mut start_blk = 0;
        let mut end_blk = 0;
        let mut blk_size = 0;
        let mut min_dev_size = u64::MAX;

        for child in self.children_iter() {
            let dev = match child.get_device() {
                Ok(dev) => dev,
                Err(_) => {
                    return Err(Error::NexusIncomplete {
                        name,
                        reason: format!(
                            "No block device available for child {}",
                            child.uri(),
                        ),
                    })
                }
            };

            let nb = dev.num_blocks();
            let bs = dev.block_len();

            min_dev_size = min(nb, min_dev_size);

            if blk_size == 0 {
                blk_size = bs;
            } else if bs != blk_size {
                return Err(Error::MixedBlockSizes {
                    name: self.name.clone(),
                });
            }

            match partition::calc_data_partition(self.req_size(), nb, bs) {
                Some((start, end)) => {
                    if start_blk == 0 {
                        start_blk = start;
                        end_blk = end;
                    } else {
                        end_blk = min(end_blk, end);

                        if start_blk != start {
                            return Err(Error::ChildGeometry {
                                child: child.uri().to_owned(),
                                name,
                            });
                        }
                    }
                }
                None => {
                    return Err(Error::ChildTooSmall {
                        child: child.uri().to_owned(),
                        name,
                        num_blocks: nb,
                        block_size: bs,
                    })
                }
            }

            if dev.is_zoned() {
                //TODO: Implement partitioning zoned block devices. This requires handling drive resources like max active/open zones.
                warn!("The device '{}' is zoned. Partitioning zoned block devices into smaller devices is not implemented. Using the whole device.", dev.device_name());
                start_blk = 0;
                end_blk = nb;
            }
        }

        unsafe {
            self.as_mut().set_data_ent_offset(start_blk);
            self.as_mut().set_block_len(blk_size as u32);
            self.as_mut().set_num_blocks(end_blk - start_blk);
        }

        info!(
            "{self:?}: nexus device initialized: \
            requested={req_blk} blocks ({req} bytes) \
            start block={start_blk}, end block={end_blk}, \
            block size={blk_size}, \
            smallest devices size={min_dev_size} blocks",
            req_blk = self.req_size() / blk_size,
            req = self.req_size(),
        );

        Ok(())
    }

    /// Opens the Nexus instance for IO.
    /// Once this function is called, the device is visible and can
    /// be used for IO.
    async fn register_instance(
        bdev: &mut spdk_rs::Bdev<Nexus<'_>>,
    ) -> Result<(), Error> {
        let mut nex = bdev.data_mut();
        assert_eq!(*nex.state.lock(), NexusState::Init);

        info!("{:?}: registering nexus bdev...", nex);

        nex.as_mut().setup_nexus_bdev().await?;

        // Register the bdev with SPDK and set the callbacks for io channel
        // creation.
        nex.register_io_device(Some(&nex.name));

        info!("{:?}: IO device registered", nex);

        if let Err(err) = bdev.register_bdev() {
            error!(
                "{:?}: nexus bdev registration failed: {}",
                nex,
                err.verbose()
            );
            return Err(err).context(nexus_err::RegisterNexus {
                name: nex.name.clone(),
            });
        }

        unsafe { nex.as_mut().unpin_mut().has_io_device = true };

        match nex.as_mut().try_open_children().await {
            Ok(_) => {
                info!("{:?}: children opened successfully", nex);
            }
            Err(err) => {
                error!("{:?} failed to open children: {}", nex, err.verbose());
                bdev.unregister_bdev();
                return Err(err);
            }
        };

        // Persist the fact that the nexus is now successfully open.
        // We have to do this before setting the nexus to open so that
        // nexus list does not return this nexus until it is persisted.
        nex.persist(PersistOp::Create).await;
        nex.as_mut().set_state(NexusState::Open);
        info!("{:?}: nexus bdev registered successfully", nex);

        Ok(())
    }

    /// Destroy the Nexus.
    pub async fn destroy(self: Pin<&mut Self>) -> Result<(), Error> {
        self.destroy_ext(false).await
    }

    /// Destroy the Nexus.
    /// # Arguments
    /// * `sigterm`: Indicates whether this is as a result of process
    ///   termination.
    pub async fn destroy_ext(
        mut self: Pin<&mut Self>,
        sigterm: bool,
    ) -> Result<(), Error> {
        info!("{:?}: destroying nexus...", self);

        self.as_mut().unshare_nexus().await?;

        // wait for all rebuild jobs to be cancelled before proceeding with the
        // destruction of the nexus
        let child_uris = self.child_uris();
        for child in child_uris {
            self.as_mut().cancel_rebuild_jobs(&child).await;
        }

        info!("{:?}: closing {} children...", self, self.children.len());
        for child in self.children_iter() {
            if let Err(e) = child.close().await {
                // TODO: should an error be returned here?
                error!(
                    "{child:?}: child failed to close: {e}",
                    e = e.verbose()
                );
            }
        }
        info!("{:?}: children closed", self);

        // Persist the fact that the nexus destruction has completed.
        self.persist(PersistOp::Shutdown).await;
        if !sigterm {
            if let Err(error) = self.ptpl().destroy() {
                error!(
                    "{self:?}: Failed to clean up persistence through \
                    power loss for nexus: {error}",
                );
            }
        }

        unsafe {
            let name = self.name.clone();

            // After calling unregister_bdev_async(), Nexus is gone.
            match self.as_mut().bdev_mut().unregister_bdev_async().await {
                Ok(_) => {
                    info!("Nexus '{name}': nexus destroyed ok");
                    Ok(())
                }
                Err(err) => {
                    error!(
                        "Nexus '{name}': failed to destroy: {e}",
                        e = err.verbose()
                    );
                    Err(Error::NexusDestroy {
                        name,
                    })
                }
            }
        }
    }

    /// Returns a mutable reference to Nexus I/O.
    fn io_subsystem_mut(self: Pin<&mut Self>) -> &mut NexusIoSubsystem<'n> {
        unsafe { self.get_unchecked_mut().io_subsystem.as_mut().unwrap() }
    }

    /// Resumes I/O to the Bdev.
    /// Note: in order to handle concurrent resumes properly, this function must
    /// be called only from the master core.
    pub async fn resume(self: Pin<&mut Self>) -> Result<(), Error> {
        self.io_subsystem_mut().resume().await
    }

    /// Set the Nexus state to 'reset'
    pub fn set_reset_state(&self) -> bool {
        let mut state = self.state.lock();
        match *state {
            // Reset operation is allowed only when the Nexus is Open state
            NexusState::Open => {
                *state = NexusState::Reconfiguring;
                true
            }
            _ => false,
        }
    }

    /// Set the Nexus state to 'open'
    pub fn set_open_state(&self) -> bool {
        let mut state = self.state.lock();
        match *state {
            // Open operation is allowed only when the Nexus is
            // Init/Reconfiguring state
            NexusState::Reconfiguring | NexusState::Init => {
                *state = NexusState::Open;
                true
            }
            _ => false,
        }
    }

    pub async fn shutdown(mut self: Pin<&mut Self>) -> Result<(), Error> {
        let prev_state = {
            let mut s = self.state.lock();

            match *s {
                // If nexus is already shutdown, operation is idempotent.
                NexusState::Shutdown => {
                    info!(
                        nexus=%self.name,
                        "Nexus is already shutdown, skipping shutdown operation"
                    );
                    return Ok(());
                }
                // In case of active shutdown operation, bail out.
                NexusState::ShuttingDown => {
                    return Err(Error::OperationNotAllowed {
                        reason: "Shutdown operation is already in progress"
                            .to_string(),
                    });
                }
                // Save current state and mark nexus as being under shutdown.
                t => {
                    *s = NexusState::ShuttingDown;
                    t
                }
            }
        };

        // Step 1: pause subsystem.
        // In case of error, restore previous nexus state.
        info!(
            nexus=%self.name,
            "Shutting down nexus"
        );
        self.as_mut().pause().await.map_err(|error| {
            error!(
                %error,
                nexus=%self.name,
                "Failed to pause I/O subsystem, shutdown failed"
            );

            // Restore previous nexus state.
            *self.state.lock() = prev_state;
            error
        })?;

        info!(
            nexus=%self.name,
            "I/O subsystem paused"
        );

        // Step 2: cancel all active rebuild jobs.
        let child_uris = self.child_uris();
        for child in child_uris {
            self.as_mut().cancel_rebuild_jobs(&child).await;
        }

        // Step 3: Close all nexus children.
        self.close_children().await;

        // Step 4: Mark nexus as being properly shutdown in ETCd.
        self.persist(PersistOp::Shutdown).await;

        // Finally, mark nexus as being fully shutdown.
        *self.state.lock() = NexusState::Shutdown;

        info!(
            nexus=%self.name,
            "Nexus successfully shut down"
        );
        Ok(())
    }

    /// Suspend any incoming IO to the bdev pausing the controller allows us to
    /// handle internal events and which is a protocol feature.
    /// In case concurrent pause requests take place, the other callers
    /// will wait till the nexus is resumed and will continue execution
    /// with the nexus paused once they are awakened via resume().
    /// Note: in order to handle concurrent pauses properly, this function must
    /// be called only from the master core.
    pub async fn pause(self: Pin<&mut Self>) -> Result<(), Error> {
        self.io_subsystem_mut().suspend().await
    }

    /// get ANA state of the NVMe subsystem
    pub async fn get_ana_state(&self) -> Result<NvmeAnaState, Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                let ana_state = subsystem.get_ana_state().await? as i32;
                return NvmeAnaState::from_i32(ana_state);
            }
        }

        Err(Error::NotSharedNvmf {
            name: self.name.clone(),
        })
    }

    /// set ANA state of the NVMe subsystem
    pub async fn set_ana_state(
        &self,
        ana_state: NvmeAnaState,
    ) -> Result<(), Error> {
        if let Some(Protocol::Nvmf) = self.shared() {
            if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                subsystem.pause().await?;
                let res = subsystem.set_ana_state(ana_state as u32).await;
                subsystem.resume().await?;
                return Ok(res?);
            }
        }

        Err(Error::NotSharedNvmf {
            name: self.name.clone(),
        })
    }

    /// determine if any of the children do not support the requested
    /// io type. Break the loop on first occurrence.
    /// TODO: optionally add this check during nexus creation
    pub fn io_is_supported(&self, io_type: IoType) -> bool {
        !self
            .children
            .iter()
            .filter_map(|e| e.get_device().ok())
            .any(|b| !b.io_type_supported(io_type))
    }

    /// IO completion for local replica
    pub fn io_completion_local(_success: bool, _parent_io: *mut c_void) {
        unimplemented!();
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
        match *self.state.lock() {
            NexusState::Init => NexusStatus::Degraded,
            NexusState::Closed => NexusStatus::Faulted,
            NexusState::ShuttingDown => NexusStatus::ShuttingDown,
            NexusState::Shutdown => NexusStatus::Shutdown,
            NexusState::Open | NexusState::Reconfiguring => {
                if self
                    .children
                    .iter()
                    // All children are online, so the Nexus is also online
                    .all(|c| c.is_healthy())
                {
                    NexusStatus::Online
                } else if self
                    .children
                    .iter()
                    // at least one child online, so the Nexus is also online
                    .any(|c| c.is_healthy())
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

// Unsafe part of Nexus.
impl<'n> Nexus<'n> {
    /// Returns a mutable reference to the Nexus with the lifetime as the Nexus
    /// itself.
    #[inline(always)]
    pub(super) unsafe fn unpin_mut(self: Pin<&mut Self>) -> &'n mut Nexus<'n> {
        &mut *(self.get_unchecked_mut() as *mut _)
    }

    /// Returns a pinned mutable reference of the same lifetime as the Nexus
    /// itself.
    #[inline(always)]
    pub(super) unsafe fn pinned_mut(
        self: Pin<&mut Self>,
    ) -> Pin<&'n mut Nexus<'n>> {
        Pin::new_unchecked(self.unpin_mut())
    }

    /// Returns a reference to Nexus's Bdev.
    #[inline(always)]
    pub(super) unsafe fn bdev(&self) -> &Bdev<Nexus<'n>> {
        self.bdev
            .as_ref()
            .expect("Nexus Bdev object is not initialized")
    }

    /// Returns a mutable reference to Nexus's Bdev.
    #[inline(always)]
    pub(super) unsafe fn bdev_mut(
        self: Pin<&mut Self>,
    ) -> &mut Bdev<Nexus<'n>> {
        self.get_unchecked_mut().bdev.as_mut().unwrap()
    }

    /// Returns a pinned Bdev reference to allow calling methods that require a
    /// Pin<&mut>, e.g. methods of Share trait.
    #[inline(always)]
    pub(super) fn pin_bdev_mut(self: Pin<&mut Self>) -> Pin<&mut Bdev<Self>> {
        unsafe { Pin::new_unchecked(self.bdev_mut()) }
    }

    /// Sets the required alignment of the Nexus.
    pub(crate) unsafe fn set_required_alignment(
        self: Pin<&mut Self>,
        new_val: u8,
    ) {
        (*self.bdev_mut().unsafe_inner_mut_ptr()).required_alignment = new_val;
    }

    /// Sets the block size of the underlying device.
    pub(crate) unsafe fn set_block_len(self: Pin<&mut Self>, blk_size: u32) {
        self.bdev_mut().set_block_len(blk_size)
    }

    /// Sets number of blocks for this device.
    pub(crate) unsafe fn set_num_blocks(self: Pin<&mut Self>, count: u64) {
        self.bdev_mut().set_num_blocks(count)
    }

    /// TODO
    pub(crate) unsafe fn set_data_ent_offset(self: Pin<&mut Self>, val: u64) {
        self.get_unchecked_mut().data_ent_offset = val;
    }
}

impl Drop for Nexus<'_> {
    fn drop(&mut self) {
        info!("{:?}: dropping nexus bdev", self);
    }
}

impl<'n> IoDevice for Nexus<'n> {
    type ChannelData = NexusChannel<'n>;

    fn io_channel_create(self: Pin<&mut Self>) -> NexusChannel<'n> {
        NexusChannel::new(self)
    }

    fn io_channel_destroy(self: Pin<&mut Self>, chan: NexusChannel<'n>) {
        chan.destroy();
    }
}

impl IoDeviceChannelTraverse for Nexus<'_> {}

unsafe fn unsafe_static_ptr(nexus: &Nexus) -> *mut Nexus<'static> {
    let r = ::std::mem::transmute::<_, &'static Nexus>(nexus);
    r as *const Nexus as *mut Nexus
}

impl<'n> BdevOps for Nexus<'n> {
    type ChannelData = NexusChannel<'n>;
    type BdevData = Nexus<'n>;
    type IoDev = Nexus<'n>;

    /// TODO
    fn destruct(mut self: Pin<&mut Self>) {
        info!("{:?}: unregistering nexus bdev...", self);

        // A closed operation might already be in progress calling unregister
        // will trip an assertion within the external libraries
        if *self.state.lock() == NexusState::Closed {
            info!("{:?}: nexus already closed", self);
            return;
        }

        let self_ptr = unsafe { unsafe_static_ptr(&self) };

        Reactor::block_on(async move {
            let self_ref = unsafe { &mut *self_ptr };

            // TODO: double-check interaction with rebuild job logic
            // TODO: cancel rebuild jobs?
            let n = self_ref.children.iter().filter(|c| c.is_healthy()).count();

            if n > 0 {
                warn!(
                    "{:?}: {} open children remain(s), closing...",
                    self_ref, n
                );

                for child in self_ref.children.iter() {
                    if child.is_healthy() {
                        if let Err(e) = child.close().await {
                            error!(
                                "{:?}: child failed to close: {}",
                                child,
                                e.verbose()
                            );
                        }
                    }
                }
            }

            self_ref.children.clear();
        });

        self.as_mut().unregister_io_device();
        unsafe {
            self.as_mut().get_unchecked_mut().has_io_device = false;
        }

        self.as_mut().set_state(NexusState::Closed);

        info!("{:?}: nexus bdev unregistered", self);
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    /// This function is not called when the IO is re-submitted (see below).
    fn submit_request(
        &self,
        chan: IoChannel<NexusChannel<'n>>,
        bio: BdevIo<Nexus<'n>>,
    ) {
        let io = NexusBio::new(chan, bio);
        io.submit_request();
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        match io_type {
            // we always assume the device supports read/write commands
            // allow NVMe Admin as it is needed for local replicas
            IoType::Read | IoType::Write | IoType::NvmeAdmin => true,
            IoType::Flush
            | IoType::Reset
            | IoType::Unmap
            | IoType::WriteZeros
            | IoType::ZoneAppend
            | IoType::ZoneInfo
            | IoType::ZoneManagement
            | IoType::NvmeIo
            | IoType::ZeroCopy => {
                let supported = self.io_is_supported(io_type);
                if !supported {
                    warn!(
                        "{:?}: I/O type '{:?}' not supported by at least \
                        one of child devices",
                        self, io_type
                    );
                }
                supported
            }
            _ => {
                warn!(
                    "{:?}: I/O type '{:?}' support not implemented",
                    self, io_type
                );
                false
            }
        }
    }

    /// Called per core to create IO channels per Nexus instance.
    fn get_io_device(&self) -> &Self::IoDev {
        trace!("{:?}: getting IO channel", self);
        self
    }

    /// Device specific information which is returned by the get_bdevs RPC call.
    fn dump_info_json(&self, w: JsonWriteContext) {
        w.write_named_array_begin("children");
        if let Err(err) = w.write(&self.children) {
            error!("Failed to dump into JSON: {}", err.to_string());
        }
        w.write_array_end();
    }
}

/// Create a new nexus and bring it online.
/// If we fail to create any of the children, then we fail the whole operation.
/// On failure, we must cleanup by destroying any children that were
/// successfully created. Also, once the nexus is created, there still might
/// be a configuration mismatch that would prevent us from going online.
/// Currently, we can only determine this once we are already online,
/// and so we check the errors twice for now.
pub async fn nexus_create(
    name: &str,
    size: u64,
    uuid: Option<&str>,
    children: &[String],
) -> Result<(), Error> {
    nexus_create_internal(
        name,
        size,
        uuid,
        None,
        NexusNvmeParams::default(),
        children,
        None,
    )
    .await
}

/// As create_nexus with additional parameters:
/// min_cntlid, max_cntldi: NVMe controller ID range when sharing over NVMf
/// resv_key: NVMe reservation key for children
pub async fn nexus_create_v2(
    name: &str,
    size: u64,
    uuid: &str,
    nvme_params: NexusNvmeParams,
    children: &[String],
    nexus_info_key: Option<String>,
) -> Result<(), Error> {
    if nvme_params.min_cntlid < NVME_MIN_CNTLID
        || nvme_params.min_cntlid > nvme_params.max_cntlid
        || nvme_params.max_cntlid > NVME_MAX_CNTLID
    {
        let args = format!(
            "invalid NVMe controller ID range [{:x}h, {:x}h]",
            nvme_params.min_cntlid, nvme_params.max_cntlid
        );
        error!("failed to create nexus {}: {}", name, args);
        return Err(Error::InvalidArguments {
            name: name.to_owned(),
            args,
        });
    }
    if !nvme_params.reservations_enabled() {
        warn!(
            "Not using nvme reservations for nexus {}: {:?}",
            name, nvme_params
        );
    } else if !nvme_params.reservations_valid() {
        let args = "invalid NVMe reservation parameters";
        error!("failed to create nexus {}: {}", name, args);
        return Err(Error::InvalidArguments {
            name: name.to_owned(),
            args: args.to_string(),
        });
    }

    match uuid::Uuid::parse_str(name) {
        Ok(name_uuid) => {
            let bdev_uuid = name_uuid.to_string();
            let nexus_uuid = uuid::Uuid::parse_str(uuid).map_err(|_| {
                Error::InvalidUuid {
                    uuid: uuid.to_string(),
                }
            })?;
            nexus_create_internal(
                name,
                size,
                Some(bdev_uuid.as_str()),
                Some(nexus_uuid),
                nvme_params,
                children,
                nexus_info_key,
            )
            .await
        }
        Err(_) => {
            nexus_create_internal(
                name,
                size,
                Some(uuid),
                None,
                nvme_params,
                children,
                nexus_info_key,
            )
            .await
        }
    }
}

async fn get_nexus_zone_info_from_children(children_devices: &mut Vec<(String, String)>) -> Result<ZoneInfo, Error> {
    let mut zone_info = ZoneInfo::default();
    let mut conventional = false;
    let mut info_set = false;

    for (_uri, device_name) in &*children_devices {
        let dev = device_lookup(&device_name).unwrap();

        if dev.is_zoned() {
            zone_info.zoned = true;
            if !info_set {
                zone_info.num_zones = dev.get_num_zones();
                zone_info.zone_size = dev.get_zone_size();
                zone_info.max_zone_append_size = dev.get_max_zone_append_size();
                zone_info.max_open_zones = dev.get_max_open_zones();
                zone_info.max_active_zones = dev.get_max_active_zones();
                zone_info.optimal_open_zones = dev.get_optimal_open_zones();
                info_set = true;
            } else if zone_info.num_zones != dev.get_num_zones()
                    || zone_info.zone_size != dev.get_zone_size()
                    || zone_info.max_zone_append_size != dev.get_max_zone_append_size()
                    || zone_info.max_open_zones != dev.get_max_open_zones()
                    || zone_info.max_active_zones != dev.get_max_active_zones()
                    || zone_info.optimal_open_zones != dev.get_optimal_open_zones() {
                error!("Can not use ZBD's with different parameters as nexus children");
                return Err(Error::MixedZonedChild { child: device_name.to_string() });
           }
        } else {
            conventional = true;
        }

        if zone_info.zoned == conventional {
            error!("Can not handle conventional and zoned storage at the same time in a nexus");
            return Err(Error::MixedZonedChild { child: device_name.to_string() });
        }
    }

    Ok(zone_info)
}

async fn create_children_devices(
    children: &[String],
) -> Result<Vec<(String, String)>, Error> {
    let mut children_devices = Vec::new();
    for uri in children {
        let device_name = device_create(uri).await.unwrap();
        if device_lookup(&device_name).unwrap().is_zoned() && children.len() > 1 {
            return Err(Error::ZonedReplicationNotImplemented {});
        }
        children_devices.push((uri.to_string(), device_name));
    }
    Ok(children_devices)
}

async fn nexus_create_internal(
    name: &str,
    size: u64,
    bdev_uuid: Option<&str>,
    nexus_uuid: Option<Uuid>,
    nvme_params: NexusNvmeParams,
    children: &[String],
    nexus_info_key: Option<String>,
) -> Result<(), Error> {
    info!(
        "Creating new nexus '{}' ({} child(ren): {:?})...",
        name,
        children.len(),
        children
    );

    if let Some(nexus) = nexus_lookup_name_uuid(name, nexus_uuid) {
        // FIXME: Instead of error, we return Ok without checking
        // that the children match, which seems wrong.
        if *nexus.state.lock() == NexusState::Init {
            return Err(Error::NexusInitialising {
                name: name.to_owned(),
            });
        }
        if nexus.name != name
            || (nexus_uuid.is_some() && Some(nexus.nexus_uuid) != nexus_uuid)
        {
            return Err(Error::UuidExists {
                uuid: nexus.nexus_uuid.to_string(),
                nexus: name.to_string(),
            });
        }
        return Ok(());
    }

    let mut children_devices = create_children_devices(children).await?;

    let zone_info = get_nexus_zone_info_from_children(&mut children_devices).await?;

    if zone_info.zoned {
        info!("The Nexus will zoned with the properies {:?}", zone_info);
    }

    // Create a new Nexus object, and immediately add it to the global list.
    // This is necessary to ensure proper cleanup, as the code responsible for
    // closing a child assumes that the nexus to which it belongs will appear
    // in the global list of nexus instances. We must also ensure that the
    // nexus instance gets removed from the global list if an error occurs.
    let mut nexus_bdev = Nexus::new(
        name,
        size,
        bdev_uuid,
        nexus_uuid,
        nvme_params,
        nexus_info_key,
        zone_info,
    );


    for (uri, device_name) in children_devices {
        if let Err(error) = nexus_bdev.data_mut().new_child(&uri, &device_name).await {
            error!(
                "{n:?}: failed to add child '{uri}': {e}",
                n = nexus_bdev.data(),
                e = error.verbose()
            );
            nexus_bdev.data().close_children().await;

            error!(
                "{:?}: nexus creation failed: failed to create child '{}'",
                nexus_bdev.data(),
                uri
            );

            return Err(Error::CreateChild {
                source: error,
                name: name.to_owned(),
            });
        }
    }

    match Nexus::register_instance(&mut nexus_bdev).await {
        Err(Error::NexusIncomplete {
            name,
            reason,
            ..
        }) => {
            // We still have code that waits for children to come online,
            // although this currently only works for config files.
            // We need to explicitly clean up child devices
            // if we get this error.
            error!(
                "{:?}: not all child devices are available",
                nexus_bdev.data()
            );

            let uris = nexus_bdev
                .data()
                .children_iter()
                .map(|c| c.uri().to_owned())
                .collect::<Vec<_>>();

            for u in uris {
                // TODO: children may already be destroyed
                // TODO: mutability violation
                if let Err(e) = device_destroy(&u).await {
                    error!(
                        "{:?}: failed to destroy child device {}: {:?}",
                        nexus_bdev.data(),
                        u,
                        e,
                    );
                }
            }

            Err(Error::NexusCreate {
                name,
                reason,
            })
        }

        Err(error) => {
            error!(
                "{:?}: failed to open nexus: {}",
                nexus_bdev.data(),
                error.verbose()
            );
            nexus_bdev.data().close_children().await;
            Err(error)
        }
        Ok(_) => {
            info!("{:?}: nexus created ok", nexus_bdev.data());
            Ok(())
        }
    }
}
