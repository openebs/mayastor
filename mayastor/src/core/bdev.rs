use std::{
    fmt::{Debug, Display, Formatter},
    ops::{Deref, DerefMut},
    pin::Pin,
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
        ShareNvmf,
        UnshareNvmf,
    },
    nexus_uri::bdev_uri_eq,
    subsys::NvmfSubsystem,
    target::nvmf,
};

/// Newtype structure that represents a block device. The soundness of the API
/// is based on the fact that opening and finding of a bdev, returns a valid
/// bdev or None. Once the bdev is given, the operations on the bdev are safe.
/// It is not possible to remove a bdev through a core other than the management
/// core. This means that the structure is always valid for the lifetime of the
/// scope.
pub struct Bdev<T: spdk_rs::BdevOps> {
    /// TODO
    inner: spdk_rs::Bdev<T>,
}

/// TODO
pub type UntypedBdev = Bdev<()>;

/// Allow transparent use of `spdk_rs` methods.
impl<T> Deref for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    type Target = spdk_rs::Bdev<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Allow transparent use of `spdk_rs` mutable methods.
impl<T> DerefMut for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Clone for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    /// TODO
    pub(crate) fn new(b: spdk_rs::Bdev<T>) -> Self {
        Self {
            inner: b,
        }
    }

    /// Constructs a Bdev from a raw SPDK pointer.
    pub(crate) unsafe fn checked_from_ptr(
        bdev: *mut spdk_bdev,
    ) -> Option<Self> {
        if bdev.is_null() {
            None
        } else {
            Some(Self::new(spdk_rs::Bdev::unsafe_from_inner_ptr(bdev)))
        }
    }

    /// Opens a Bdev by its name in read_write mode.
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

    /// Looks up a Bdev by its name.
    pub fn lookup_by_name(name: &str) -> Option<Self> {
        spdk_rs::Bdev::<T>::lookup_by_name(name).map(Self::new)
    }

    /// Looks up a Bdev by its uuid.
    pub fn lookup_by_uuid_str(uuid: &str) -> Option<Self> {
        match Self::bdev_first() {
            None => None,
            Some(bdev) => {
                let b: Vec<Self> = bdev
                    .into_iter()
                    .filter(|b| b.uuid_as_string() == uuid)
                    .collect();

                b.first().map(|b| Self {
                    inner: b.inner.clone(),
                })
            }
        }
    }

    /// Returns the name of driver module for the given Bdev.
    pub fn driver(&self) -> &str {
        self.inner.module_name()
    }

    /// Returns the first bdev in the list.
    pub fn bdev_first() -> Option<Self> {
        BdevIter::<T>::new().next()
    }

    /// TODO
    pub async fn stats_async(&self) -> Result<BlockDeviceIoStats, CoreError> {
        match self.inner.stats_async().await {
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

#[async_trait(? Send)]
impl<T> Share for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    type Error = CoreError;
    type Output = String;

    /// share the bdev over NVMe-OF TCP
    async fn share_nvmf(
        self: Pin<&mut Self>,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error> {
        let me = unsafe { self.get_unchecked_mut() };

        let subsystem = NvmfSubsystem::try_from(me).context(ShareNvmf {})?;
        if let Some((cntlid_min, cntlid_max)) = cntlid_range {
            subsystem
                .set_cntlid_range(cntlid_min, cntlid_max)
                .context(ShareNvmf {})?;
        }
        subsystem.start().await.context(ShareNvmf {})
    }

    /// unshare the bdev regardless of current active share
    async fn unshare(
        self: Pin<&mut Self>,
    ) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Nvmf) => {
                if let Some(subsystem) = NvmfSubsystem::nqn_lookup(self.name())
                {
                    subsystem.stop().await.context(UnshareNvmf {})?;
                    subsystem.destroy();
                }
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
            _ => Some(Protocol::Off),
        }
    }

    /// return share URI for nvmf (does "share path" not sound better?)
    fn share_uri(&self) -> Option<String> {
        match self.shared() {
            Some(Protocol::Nvmf) => nvmf::get_uri(self.name()),
            _ => Some(format!("bdev:///{}", self.name())),
        }
    }

    /// return the URI that was used to construct the bdev
    fn bdev_uri(&self) -> Option<String> {
        for alias in self.aliases().iter() {
            if let Ok(mut uri) = url::Url::parse(alias) {
                if bdev_uri_eq(self, &uri) {
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
                if bdev_uri_eq(self, &uri) {
                    return Some(uri.to_string());
                }
            }
        }
        None
    }
}

impl<T> Display for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "name: {}, driver: {}", self.name(), self.driver(),)
    }
}

impl<T> Debug for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
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

/// TODO
pub struct BdevIter<T: spdk_rs::BdevOps>(spdk_rs::BdevGlobalIter<T>);

impl<T> IntoIterator for Bdev<T>
where
    T: spdk_rs::BdevOps,
{
    type Item = Bdev<T>;
    type IntoIter = BdevIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        BdevIter::new()
    }
}

/// iterator over the bdevs in the global bdev list
impl<T> Iterator for BdevIter<T>
where
    T: spdk_rs::BdevOps,
{
    type Item = Bdev<T>;
    fn next(&mut self) -> Option<Bdev<T>> {
        self.0.next().map(Self::Item::new)
    }
}

impl<T> Default for BdevIter<T>
where
    T: spdk_rs::BdevOps,
{
    fn default() -> Self {
        BdevIter(spdk_rs::Bdev::iter_all())
    }
}

impl<T> BdevIter<T>
where
    T: spdk_rs::BdevOps,
{
    pub fn new() -> Self {
        Default::default()
    }
}
