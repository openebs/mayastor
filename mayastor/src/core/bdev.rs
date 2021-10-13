use std::{
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
};

use async_trait::async_trait;
use nix::errno::Errno;
use snafu::ResultExt;

use spdk_rs::libspdk::spdk_bdev;

use crate::{
    bdev::SpdkBlockDevice,
    core::{
        share::{Protocol, Share},
        BlockDeviceIoStats,
        CoreError,
        Descriptor,
        IoType,
        ShareIscsi,
        ShareNvmf,
        UnshareIscsi,
        UnshareNvmf,
    },
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
pub struct Bdev(spdk_rs::DummyBdev);

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
        for alias in self.as_ref().aliases().iter() {
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
        for alias in self.as_ref().aliases().iter() {
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
    pub(crate) fn new(b: spdk_rs::DummyBdev) -> Self {
        Self(b)
    }

    /// construct bdev from raw pointer
    pub(crate) fn from_ptr(bdev: *mut spdk_bdev) -> Option<Bdev> {
        if bdev.is_null() {
            None
        } else {
            Some(Self(spdk_rs::DummyBdev::legacy_from_ptr(bdev)))
        }
    }

    /// returns the bdev as a ptr
    /// dont use please
    pub fn as_ptr(&self) -> *mut spdk_bdev {
        self.0.legacy_as_ptr()
    }

    /// TODO
    #[allow(dead_code)]
    pub(crate) fn as_ref(&self) -> &spdk_rs::DummyBdev {
        &self.0
    }

    /// TODO
    #[allow(dead_code)]
    pub(crate) fn as_mut(&mut self) -> &mut spdk_rs::DummyBdev {
        &mut self.0
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

    /// Opens the current Bdev.
    /// A Bdev can be opened multiple times resulting in a new descriptor for
    /// each call.
    pub fn open(&self, read_write: bool) -> Result<Descriptor, CoreError> {
        match spdk_rs::BdevDesc::<()>::open(
            self.name(),
            read_write,
            SpdkBlockDevice::bdev_event_callback,
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

    /// lookup a bdev by its name
    pub fn lookup_by_name(name: &str) -> Option<Bdev> {
        spdk_rs::DummyBdev::lookup_by_name(name).map(|bdev| Self::new(bdev))
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

    /// returns whenever the bdev supports the requested IO type
    pub fn io_type_supported(&self, io_type: IoType) -> bool {
        self.0.io_type_supported(io_type)
    }

    /// returns the first bdev in the list
    pub fn bdev_first() -> Option<Bdev> {
        BdevIter::new().next()
    }

    /// TODO
    pub async fn stats_async(&self) -> Result<BlockDeviceIoStats, CoreError> {
        match self.0.stats_async().await {
            Ok(stat) => Ok(BlockDeviceIoStats {
                num_read_ops: stat.num_read_ops,
                num_write_ops: stat.num_write_ops,
                bytes_read: stat.bytes_read,
                bytes_written: stat.bytes_written,
                num_unmap_ops: stat.num_unmap_ops,
                bytes_unmapped: stat.bytes_unmapped,
            }),
            Err(err) => Err(CoreError::DeviceStatisticsError {
                source: err,
            }),
        }
    }
}

pub struct BdevIter(spdk_rs::BdevGlobalIter<()>);

impl IntoIterator for Bdev {
    type Item = Bdev;
    type IntoIter = BdevIter;
    fn into_iter(self) -> Self::IntoIter {
        BdevIter::new()
    }
}

/// iterator over the bdevs in the global bdev list
impl Iterator for BdevIter {
    type Item = Bdev;
    fn next(&mut self) -> Option<Bdev> {
        self.0.next().map(|b| Self::Item::new(b))
    }
}

impl BdevIter {
    pub fn new() -> Self {
        BdevIter(spdk_rs::DummyBdev::iter_all())
    }
}

impl From<*mut spdk_bdev> for Bdev {
    fn from(bdev: *mut spdk_bdev) -> Self {
        Self::from_ptr(bdev)
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
