use std::{
    cell::UnsafeCell,
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::Deref,
    rc::Rc,
};

use crate::{
    core::SegmentMap,
    rebuild::{RebuildMap, SEGMENT_SIZE},
};

use parking_lot::Mutex;
use spdk_rs::{Cores, IoType};

/// Per-core I/O log channel.
/// I/O log channel is enables lockless logging of I/O operations.
pub(crate) struct IOLogChannelInner {
    /// Channel's core.
    core: u32,
    /// Name of the underlying block device.
    device_name: String,
    /// Map of device segments.
    segments: UnsafeCell<Option<SegmentMap>>,
}

impl Debug for IOLogChannelInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "I/O log channel ({core}): '{dev}' ({seg:?})",
            core = self.core,
            dev = self.device_name,
            seg = self.segments()
        )
    }
}

impl IOLogChannelInner {
    /// Creates new I/O log channel for the given channel.
    fn new(
        core: u32,
        device_name: &str,
        num_blocks: u64,
        block_len: u64,
    ) -> Self {
        Self {
            core,
            segments: UnsafeCell::new(Some(SegmentMap::new(
                num_blocks,
                block_len,
                SEGMENT_SIZE,
            ))),
            device_name: device_name.to_owned(),
        }
    }

    /// Logs the given operation, marking the corresponding segment as modified
    /// in the case of a write operation.
    ///
    /// # Arguments
    ///
    /// * `io_type`: IoType of the operation to log.
    /// * `lbn`: Logical block number.
    /// * `lbn_cnt`: Number of logical blocks affected by the operation.
    pub(crate) fn log_io(&self, io_type: IoType, lbn: u64, lbn_cnt: u64) {
        assert_eq!(self.core, Cores::current());

        if matches!(io_type, IoType::Write | IoType::WriteZeros | IoType::Unmap)
        {
            unsafe { &mut *self.segments.get() }
                .as_mut()
                .expect("Accessing stopped I/O log channel")
                .set(lbn, lbn_cnt, true);
        }
    }

    /// Returns a reference to segments.
    #[inline]
    fn segments(&self) -> &SegmentMap {
        unsafe { &*self.segments.get() }
            .as_ref()
            .expect("Accessing stopped I/O log channel")
    }

    /// Takes segments from this channel.
    #[inline]
    fn take_segments(&self) -> SegmentMap {
        unsafe { &mut *self.segments.get() }
            .take()
            .expect("Accessing stopped I/O log channel")
    }
}

/// Reference to per-channel I/O log.
#[derive(Clone)]
pub(crate) struct IOLogChannel(Rc<IOLogChannelInner>);

impl From<IOLogChannelInner> for IOLogChannel {
    fn from(value: IOLogChannelInner) -> Self {
        Self(Rc::new(value))
    }
}

impl Deref for IOLogChannel {
    type Target = IOLogChannelInner;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl Debug for IOLogChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl IOLogChannel {
    /// Creates new I/O log channel for the given channel.
    fn new(
        core: u32,
        device_name: &str,
        num_blocks: u64,
        block_len: u64,
    ) -> Self {
        Self(Rc::new(IOLogChannelInner::new(
            core,
            device_name,
            num_blocks,
            block_len,
        )))
    }
}

/// I/O log.
pub struct IOLog {
    /// Name of the underlying block device.
    device_name: String,
    /// Per-core log channels.
    channels: Mutex<HashMap<u32, IOLogChannel>>,
}

impl Debug for IOLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "I/O log: '{dev}' ({seg:?})",
            dev = self.device_name,
            seg = self.channels.lock().iter().next().unwrap().1
        )
    }
}

impl IOLog {
    /// Creates a new I/O log instance for the given device.
    pub(crate) fn new(
        device_name: &str,
        num_blocks: u64,
        block_len: u64,
    ) -> Self {
        assert!(!device_name.is_empty() && num_blocks > 0 && block_len > 0);

        let mut channels = HashMap::new();

        for i in Cores::list_cores() {
            channels.insert(
                i,
                IOLogChannel::new(i, device_name, num_blocks, block_len),
            );
        }

        Self {
            device_name: device_name.to_owned(),
            channels: Mutex::new(channels),
        }
    }

    /// Returns I/O log channel for the current core.
    pub(crate) fn current_channel(&self) -> IOLogChannel {
        self.channels
            .lock()
            .get(&Cores::current())
            .expect("I/O log channel must exist")
            .clone()
    }

    /// Consumes an I/O log instance and returns the corresponding rebuild map.
    pub(crate) fn finalize(self) -> RebuildMap {
        let segments = self
            .channels
            .lock()
            .values_mut()
            .map(|x| x.take_segments())
            .reduce(|acc, e| acc.merge(&e))
            .expect("Should have at least 1 core");

        RebuildMap::new(&self.device_name, segments)
    }
}
