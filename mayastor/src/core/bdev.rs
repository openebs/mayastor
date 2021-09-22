use std::{
    convert::TryFrom,
    ffi::CString,
    fmt::{Debug, Display, Formatter},
    os::raw::c_void,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_first,
    spdk_bdev_get_by_name,
    spdk_bdev_get_device_stat,
    spdk_bdev_io_stat,
    spdk_bdev_next,
};

use crate::{
    bdev::SpdkBlockDevice,
    core::{
        share::{Protocol, Share},
        BlockDeviceIoStats,
        CoreError,
        Descriptor,
        DeviceEventType,
        IoType,
        ShareIscsi,
        ShareNvmf,
        UnshareIscsi,
        UnshareNvmf,
    },
    ffihelper::{cb_arg, FfiResult, IntoCString},
    subsys::NvmfSubsystem,
    target::{iscsi, nvmf, Side},
};

/// Newtype structure that represents a block device. The soundness of the API
/// is based on the fact that opening and finding of a bdev, returns a valid
/// bdev or None. Once the bdev is given, the operations on the bdev are safe.
/// It is not possible to remove a bdev through a core other than the management
/// core. This means that the structure is always valid for the lifetime of the
/// scope.
#[derive(Clone)]
pub struct Bdev(spdk::Bdev<()>);

#[async_trait(? Send)]
impl Share for Bdev {
    type Error = CoreError;
    type Output = String;

    /// share the bdev over iscsi
    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error> {
        iscsi::share(&self.name(), self, Side::Nexus).context(ShareIscsi {})
    }

    /// share the bdev over NVMe-OF TCP
    async fn share_nvmf(
        &self,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error> {
        let subsystem =
            NvmfSubsystem::try_from(self.clone()).context(ShareNvmf {})?;
        if let Some((cntlid_min, cntlid_max)) = cntlid_range {
            subsystem
                .set_cntlid_range(cntlid_min, cntlid_max)
                .context(ShareNvmf {})?;
        }
        subsystem.start().await.context(ShareNvmf {})
    }

    /// unshare the bdev regardless of current active share
    async fn unshare(&self) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Nvmf) => {
                if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name())
                {
                    subsystem.stop().await.context(UnshareNvmf {})?;
                    subsystem.destroy();
                }
            }
            Some(Protocol::Iscsi) => {
                iscsi::unshare(&self.name())
                    .await
                    .context(UnshareIscsi {})?;
            }
            Some(Protocol::Off) | None => {}
        }

        Ok(self.name().to_string())
    }

    /// returns if the bdev is currently shared
    /// TODO: we could do better here
    fn shared(&self) -> Option<Protocol> {
        match self.claimed_by() {
            Some(t) if t == "NVMe-oF Target" => Some(Protocol::Nvmf),
            Some(t) if t == "iSCSI Target" => Some(Protocol::Iscsi),
            _ => Some(Protocol::Off),
        }
    }

    /// return share URI for nvmf and iscsi (does "share path" not sound
    /// better?)
    fn share_uri(&self) -> Option<String> {
        match self.shared() {
            Some(Protocol::Nvmf) => nvmf::get_uri(&self.name()),
            Some(Protocol::Iscsi) => iscsi::get_uri(Side::Nexus, &self.name()),
            _ => Some(format!("bdev:///{}", self.name())),
        }
    }

    /// return the URI that was used to construct the bdev
    fn bdev_uri(&self) -> Option<String> {
        for alias in self.aliases().iter() {
            if let Ok(mut uri) = url::Url::parse(alias) {
                if self == uri {
                    if !uri.query_pairs().any(|e| e.0 == "uuid") {
                        uri.query_pairs_mut()
                            .append_pair("uuid", &self.uuid_as_string());
                    }
                    return Some(uri.to_string());
                }
            }
        }
        None
    }

    /// return the URI that was used to construct the bdev, without uuid
    fn bdev_uri_original(&self) -> Option<String> {
        for alias in self.aliases().iter() {
            if let Ok(uri) = url::Url::parse(alias) {
                if self == uri {
                    return Some(uri.to_string());
                }
            }
        }
        None
    }
}

impl Bdev {
    /// TODO
    pub(crate) fn new(b: spdk::Bdev<()>) -> Self {
        Self(b)
    }

    /// open a bdev by its name in read_write mode.
    pub fn open_by_name(
        name: &str,
        read_write: bool,
    ) -> Result<Descriptor, CoreError> {
        if let Some(bdev) = Self::lookup_by_name(name) {
            bdev.open(read_write)
        } else {
            Err(CoreError::OpenBdev {
                source: Errno::ENODEV,
            })
        }
    }

    /// Called by spdk when there is an asynchronous bdev event i.e. removal.
    fn event_cb(event: spdk::BdevEvent, bdev: spdk::Bdev<()>) {
        let name = bdev.name();

        // Translate SPDK events into common device events.
        let event = match event {
            spdk::BdevEvent::Remove => {
                info!("Received remove event for bdev {}", name);
                DeviceEventType::DeviceRemoved
            }
            spdk::BdevEvent::Resize => {
                warn!("Received resize event for bdev {}", name);
                DeviceEventType::DeviceResized
            }
            spdk::BdevEvent::MediaManagement => {
                warn!("Received media management event for bdev {}", name,);
                DeviceEventType::MediaManagement
            }
        };

        // Forward event to high-level handler.
        SpdkBlockDevice::process_device_event(event, &name);
    }

    /// Opens the current Bdev.
    /// A Bdev can be opened multiple times resulting in a new descriptor for
    /// each call.
    pub fn open(&self, read_write: bool) -> Result<Descriptor, CoreError> {
        match spdk::BdevDesc::<()>::open(
            self.name(),
            read_write,
            Self::event_cb,
        ) {
            Ok(d) => Ok(Descriptor::new(d)),
            Err(err) => Err(CoreError::OpenBdev {
                source: err,
            }),
        }
    }

    /// returns true if this bdev is claimed by some other component
    pub fn is_claimed(&self) -> bool {
        self.0.is_claimed()
    }

    /// returns by who the bdev is claimed
    pub fn claimed_by(&self) -> Option<String> {
        self.0.claimed_by().map(|m| m.name().to_string())
    }

    /// construct bdev from raw pointer
    pub fn from_ptr_abc(bdev: *mut spdk_bdev) -> Option<Bdev> {
        if bdev.is_null() {
            None
        } else {
            Some(Self(spdk::Bdev::<()>::legacy_from_ptr(bdev)))
        }
    }

    /// lookup a bdev by its name
    pub fn lookup_by_name(name: &str) -> Option<Bdev> {
        let name = CString::new(name).unwrap();
        Self::from_ptr_abc(unsafe { spdk_bdev_get_by_name(name.as_ptr()) })
    }

    /// returns the block_size of the underlying device
    pub fn block_len(&self) -> u32 {
        self.0.block_len()
    }

    /// set the block length of the device in bytes
    pub unsafe fn set_block_len(&mut self, len: u32) {
        self.0.set_block_len(len)
    }

    /// number of blocks for this device
    pub fn num_blocks(&self) -> u64 {
        self.0.num_blocks()
    }

    /// set the block count of this device
    pub unsafe fn set_num_blocks(&mut self, count: u64) {
        self.0.set_num_blocks(count)
    }

    /// return the bdev size in bytes
    pub fn size_in_bytes(&self) -> u64 {
        self.0.size_in_bytes()
    }

    /// returns the alignment of the bdev
    pub fn alignment(&self) -> u64 {
        self.0.alignment()
    }

    /// returns the configured product name
    pub fn product_name(&self) -> &str {
        self.0.product_name()
    }

    /// returns the name of driver module for the given bdev
    pub fn driver(&self) -> &str {
        self.0.module_name()
    }

    /// returns the bdev name
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// return the UUID of this bdev
    pub fn uuid(&self) -> uuid::Uuid {
        self.0.uuid().into()
    }

    /// return the UUID of this bdev as a hyphenated string
    pub fn uuid_as_string(&self) -> String {
        self.uuid().to_hyphenated().to_string()
    }

    /// Set a list of aliases on the bdev, used to find the bdev later
    pub fn add_aliases(&self, alias: &[String]) -> bool {
        alias
            .iter()
            .filter(|a| -> bool { !self.add_alias(a) })
            .count()
            == 0
    }

    /// Set an alias on the bdev, this alias can be used to find the bdev later.
    /// If the alias is already present we return true
    pub fn add_alias(&self, alias: &str) -> bool {
        let alias = alias.into_cstring();
        let ret = unsafe {
            spdk_sys::spdk_bdev_alias_add(self.as_ptr(), alias.as_ptr())
        }
        .to_result(Errno::from_i32);

        matches!(ret, Err(Errno::EEXIST) | Ok(_))
    }

    /// removes the given alias from the bdev
    pub fn remove_alias(&mut self, alias: &str) {
        self.0.remove_alias(alias)
    }

    /// Get list of bdev aliases.
    pub fn aliases(&self) -> Vec<String> {
        self.0.aliases()
    }

    /// returns whenever the bdev supports the requested IO type
    pub fn io_type_supported(&self, io_type: IoType) -> bool {
        self.0.io_type_supported(io_type)
    }

    /// returns the bdev as a ptr
    /// dont use please
    pub fn as_ptr(&self) -> *mut spdk_bdev {
        self.0.legacy_as_ptr()
    }

    /// set the UUID for this bdev
    pub unsafe fn set_uuid(&mut self, uuid: uuid::Uuid) {
        self.0.set_uuid(uuid.into());
    }

    extern "C" fn stat_cb(
        _bdev: *mut spdk_bdev,
        _stat: *mut spdk_bdev_io_stat,
        sender_ptr: *mut c_void,
        errno: i32,
    ) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("stat_cb receiver is gone");
    }

    /// Get bdev § or errno value in case of an error.
    pub async fn stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        let mut stat: spdk_bdev_io_stat = Default::default();
        let (sender, receiver) = oneshot::channel::<i32>();

        // this will iterate over io channels and call async cb when done
        unsafe {
            spdk_bdev_get_device_stat(
                self.as_ptr(),
                &mut stat as *mut _,
                Some(Self::stat_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(CoreError::DeviceStatisticsError {
                source: Errno::from_i32(errno),
            })
        } else {
            // stat is populated with the stats by now
            Ok(BlockDeviceIoStats {
                num_read_ops: stat.num_read_ops,
                num_write_ops: stat.num_write_ops,
                bytes_read: stat.bytes_read,
                bytes_written: stat.bytes_written,
                num_unmap_ops: stat.num_unmap_ops,
                bytes_unmapped: stat.bytes_unmapped,
            })
        }
    }

    /// returns the first bdev in the list
    pub fn bdev_first() -> Option<Bdev> {
        Self::from_ptr_abc(unsafe { spdk_bdev_first() })
    }

    /// TODO
    pub fn as_v2(&self) -> spdk::Bdev<()> {
        spdk::Bdev::<()>::legacy_from_ptr(self.as_ptr())
    }
}

pub struct BdevIter(*mut spdk_bdev);

impl IntoIterator for Bdev {
    type Item = Bdev;
    type IntoIter = BdevIter;
    fn into_iter(self) -> Self::IntoIter {
        BdevIter(unsafe { spdk_bdev_first() })
    }
}

/// iterator over the bdevs in the global bdev list
impl Iterator for BdevIter {
    type Item = Bdev;
    fn next(&mut self) -> Option<Bdev> {
        if self.0.is_null() {
            None
        } else {
            let current = self.0;
            self.0 = unsafe { spdk_bdev_next(current) };
            Bdev::from_ptr_abc(current)
        }
    }
}

impl From<*mut spdk_bdev> for Bdev {
    fn from(bdev: *mut spdk_bdev) -> Self {
        Self::from_ptr_abc(bdev)
            .expect("nullptr dereference while accessing a bdev")
    }
}

impl Display for Bdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "name: {}, driver: {}", self.name(), self.driver(),)
    }
}

impl Debug for Bdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "name: {}, driver: {}, product: {}, num_blocks: {}, block_len: {}, alignment: {}",
            self.name(),
            self.driver(),
            self.product_name(),
            self.num_blocks(),
            self.block_len(),
            self.alignment(),
        )
    }
}
