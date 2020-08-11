use std::{
    convert::TryFrom,
    ffi::CStr,
    fmt::{Debug, Display, Formatter},
    os::raw::c_void,
    ptr::NonNull,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_first,
    spdk_bdev_get_aliases,
    spdk_bdev_get_block_size,
    spdk_bdev_get_by_name,
    spdk_bdev_get_device_stat,
    spdk_bdev_get_name,
    spdk_bdev_get_num_blocks,
    spdk_bdev_get_product_name,
    spdk_bdev_get_uuid,
    spdk_bdev_io_stat,
    spdk_bdev_io_type_supported,
    spdk_bdev_next,
    spdk_bdev_open,
    spdk_uuid_generate,
};

use crate::{
    bdev::nexus::instances,
    core::{
        share::{Protocol, Share},
        uuid::Uuid,
        CoreError,
        CoreError::{ShareIscsi, ShareNvmf},
        Descriptor,
    },
    ffihelper::{cb_arg, AsStr},
    subsys::NvmfSubsystem,
    target::{iscsi, nvmf, Side},
};

#[derive(Debug)]
pub struct Stat {
    pub num_read_ops: u64,
    pub num_write_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Newtype structure that represents a block device. The soundness of the API
/// is based on the fact that opening and finding of a bdev, returns a valid
/// bdev or None. Once the bdev is given, the operations on the bdev are safe.
/// It is not possible to remove a bdev through a core other than the management
/// core. This means that the structure is always valid for the lifetime of the
/// scope.
#[derive(Clone)]
pub struct Bdev(NonNull<spdk_bdev>);

#[async_trait(? Send)]
impl Share for Bdev {
    type Error = CoreError;
    type Output = String;

    /// share the bdev over iscsi
    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error> {
        iscsi::share(&self.name(), &self, Side::Nexus).map_err(|source| {
            ShareIscsi {
                source,
            }
        })
    }

    /// share the bdev over NVMe-OF TCP
    async fn share_nvmf(self) -> Result<Self::Output, Self::Error> {
        let ss = NvmfSubsystem::try_from(self).map_err(|source| ShareNvmf {
            source,
        })?;

        let shared_as = ss.start().await.map_err(|source| ShareNvmf {
            source,
        })?;

        info!("shared {}", shared_as);
        Ok(shared_as)
    }

    /// unshare the bdev regardless of current active share
    async fn unshare(&self) -> Result<Self::Output, Self::Error> {
        match self.shared() {
            Some(Protocol::Nvmf) => {
                let ss = NvmfSubsystem::nqn_lookup(&self.name()).unwrap();
                ss.stop().await.map_err(|source| ShareNvmf {
                    source,
                })?;
                ss.destroy();
            }
            Some(Protocol::Iscsi) => {
                iscsi::unshare(&self.name()).await.map_err(|source| {
                    ShareIscsi {
                        source,
                    }
                })?;
            }
            None => {}
        }

        Ok(self.name())
    }

    /// returns if the bdev is currently shared
    fn shared(&self) -> Option<Protocol> {
        match self.claimed_by() {
            Some(t) if t == "NVMe-oF Target" => Some(Protocol::Nvmf),
            Some(t) if t == "iSCSI Target" => Some(Protocol::Iscsi),
            _ => None,
        }
    }

    /// return share URI for nvmf and iscsi (does "share path" not sound
    /// better?)
    fn share_uri(&self) -> Option<String> {
        match self.shared() {
            Some(Protocol::Nvmf) => nvmf::get_uri(&self.name()),
            Some(Protocol::Iscsi) => iscsi::get_uri(Side::Nexus, &self.name()),
            None => None,
        }
    }

    /// return the URI that was used to construct the bdev
    fn bdev_uri(&self) -> Option<String> {
        for alias in self.aliases().iter() {
            if let Ok(mut uri) = url::Url::parse(alias) {
                if self == uri {
                    if uri.query_pairs().find(|e| e.0 == "uuid").is_none() {
                        uri.query_pairs_mut()
                            .append_pair("uuid", &self.uuid_as_string());
                    }
                    return Some(uri.to_string());
                }
            }
        }
        None
    }
}

impl Bdev {
    /// bdevs are created and destroyed in order, adding a bdev to the nexus
    /// does interferes with this order. There we traverse all nexuses
    /// looking for our a child and then close it when found.
    ///
    /// By default -- when opening the bdev through the ['Bdev'] module
    /// we by default, pass the context of the bdev being opened. If we
    /// need/want to optimize the performance (o^n) we can opt for passing
    /// a reference to the nexus instead avoiding the lookup.
    ///
    /// This does not handle any deep level of nesting
    extern "C" fn hot_remove(ctx: *mut c_void) {
        let bdev = Bdev(NonNull::new(ctx as *mut spdk_bdev).unwrap());
        instances().iter_mut().for_each(|n| {
            n.children.iter_mut().for_each(|b| {
                // note: it would perhaps be wise to close all children
                // here in one blow to avoid unneeded lookups
                if b.bdev.as_ref().unwrap().name() == bdev.name() {
                    info!("hot remove {} from {}", b.name, b.parent);
                    b.close();
                }
            })
        });
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

    /// open the current bdev, the bdev can be opened multiple times resulting
    /// in a new descriptor for each call.
    pub fn open(&self, read_write: bool) -> Result<Descriptor, CoreError> {
        let mut descriptor = std::ptr::null_mut();
        let rc = unsafe {
            spdk_bdev_open(
                self.as_ptr(),
                read_write,
                Some(Self::hot_remove),
                self.as_ptr() as *mut _,
                &mut descriptor,
            )
        };

        if rc != 0 {
            Err(CoreError::OpenBdev {
                source: Errno::from_i32(rc),
            })
        } else {
            Ok(Descriptor::from_null_checked(descriptor).unwrap())
        }
    }

    /// returns true if this bdev is claimed by some other component
    pub fn is_claimed(&self) -> bool {
        !unsafe { self.0.as_ref().internal.claim_module.is_null() }
    }

    /// returns by who the bdev is claimed
    pub fn claimed_by(&self) -> Option<String> {
        let ptr = unsafe { self.0.as_ref().internal.claim_module };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { (*ptr).name.as_str() }.to_string())
        }
    }

    /// construct bdev from raw pointer
    pub fn from_ptr(bdev: *mut spdk_bdev) -> Option<Bdev> {
        if let Some(ptr) = NonNull::new(bdev) {
            Some(Bdev(ptr))
        } else {
            None
        }
    }

    /// lookup a bdev by its name
    pub fn lookup_by_name(name: &str) -> Option<Bdev> {
        let name = std::ffi::CString::new(name).unwrap();
        if let Some(bdev) =
            NonNull::new(unsafe { spdk_bdev_get_by_name(name.as_ptr()) })
        {
            Some(Bdev(bdev))
        } else {
            None
        }
    }
    /// returns the block_size of the underlying device
    pub fn block_len(&self) -> u32 {
        unsafe { spdk_bdev_get_block_size(self.0.as_ptr()) }
    }

    /// number of blocks for this device
    pub fn num_blocks(&self) -> u64 {
        unsafe { spdk_bdev_get_num_blocks(self.0.as_ptr()) }
    }

    /// set the block count of this device
    pub fn set_block_count(&mut self, count: u64) {
        unsafe {
            self.0.as_mut().blockcnt = count;
        }
    }

    /// set the block length of the device in bytes
    pub fn set_block_len(&mut self, len: u32) {
        unsafe {
            self.0.as_mut().blocklen = len;
        }
    }

    /// return the bdev size in bytes
    pub fn size_in_bytes(&self) -> u64 {
        self.num_blocks() * self.block_len() as u64
    }

    /// returns the alignment of the bdev
    pub fn alignment(&self) -> u8 {
        unsafe { self.0.as_ref().required_alignment }
    }

    /// returns the configured product name
    pub fn product_name(&self) -> String {
        unsafe { CStr::from_ptr(spdk_bdev_get_product_name(self.0.as_ptr())) }
            .to_str()
            .unwrap()
            .to_string()
    }

    /// returns the name of driver module for the given bdev
    pub fn driver(&self) -> String {
        unsafe { CStr::from_ptr((*self.0.as_ref().module).name) }
            .to_str()
            .unwrap()
            .to_string()
    }

    /// returns the bdev name
    pub fn name(&self) -> String {
        unsafe { CStr::from_ptr(spdk_bdev_get_name(self.0.as_ptr())) }
            .to_str()
            .unwrap()
            .to_string()
    }

    /// the UUID that is set for this bdev, all bdevs should have a UUID set
    pub fn uuid(&self) -> Uuid {
        Uuid {
            0: unsafe { spdk_bdev_get_uuid(self.0.as_ptr()) },
        }
    }

    /// converts the UUID to a string
    pub fn uuid_as_string(&self) -> String {
        let u = Uuid(unsafe { spdk_bdev_get_uuid(self.0.as_ptr()) });
        let uuid = uuid::Uuid::from_bytes(u.as_bytes());
        uuid.to_hyphenated().to_string()
    }

    /// Set a list of aliases on the bdev, used to find the bdev later
    pub fn add_aliases(&self, alias: &[String]) -> bool {
        let r = alias
            .iter()
            .filter(|a| -> bool { !self.add_alias(a) })
            .collect::<Vec<&String>>();

        r.is_empty()
    }

    /// Set an alias on the bdev, this alias can be used to find the bdev later
    pub fn add_alias(&self, alias: &str) -> bool {
        let alias = std::ffi::CString::new(alias).unwrap();
        let ret = unsafe {
            spdk_sys::spdk_bdev_alias_add(self.0.as_ptr(), alias.as_ptr())
        };

        ret == 0
    }

    /// Get list of bdev aliases
    pub fn aliases(&self) -> Vec<String> {
        let mut aliases = Vec::new();
        let head = unsafe { &*spdk_bdev_get_aliases(self.0.as_ptr()) };
        let mut ent_ptr = head.tqh_first;
        while !ent_ptr.is_null() {
            let ent = unsafe { &*ent_ptr };
            let alias = unsafe { CStr::from_ptr(ent.alias) };
            aliases.push(alias.to_str().unwrap().to_string());
            ent_ptr = ent.tailq.tqe_next;
        }
        aliases
    }

    /// returns whenever the bdev supports the requested IO type
    pub fn io_type_supported(&self, io_type: u32) -> bool {
        unsafe { spdk_bdev_io_type_supported(self.0.as_ptr(), io_type) }
    }

    /// returns the bdev as a ptr
    pub fn as_ptr(&self) -> *mut spdk_bdev {
        self.0.as_ptr()
    }

    /// convert a given UUID into a spdk_bdev_uuid or otherwise, auto generate
    /// one when uuid is None
    pub fn set_uuid(&mut self, uuid: Option<String>) {
        if let Some(uuid) = uuid {
            if let Ok(this_uuid) = uuid::Uuid::parse_str(&uuid) {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        this_uuid.as_bytes().as_ptr() as *const _
                            as *mut c_void,
                        &mut self.0.as_mut().uuid.u.raw[0] as *const _
                            as *mut c_void,
                        self.0.as_ref().uuid.u.raw.len(),
                    );
                }
                return;
            }
        }
        unsafe { spdk_uuid_generate(&mut (*self.0.as_ptr()).uuid) };
        info!("No or invalid v4 UUID specified, using self generated one");
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

    /// Get bdev stats or errno value in case of an error.
    pub async fn stats(&self) -> Result<Stat, i32> {
        let mut stat: spdk_bdev_io_stat = Default::default();
        let (sender, receiver) = oneshot::channel::<i32>();

        // this will iterate over io channels and call async cb when done
        unsafe {
            spdk_bdev_get_device_stat(
                self.0.as_ptr(),
                &mut stat as *mut _,
                Some(Self::stat_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(errno)
        } else {
            // stat is populated with the stats by now
            Ok(Stat {
                num_read_ops: stat.num_read_ops,
                num_write_ops: stat.num_write_ops,
                bytes_read: stat.bytes_read,
                bytes_written: stat.bytes_written,
            })
        }
    }
    /// returns the first bdev in the list
    pub fn bdev_first() -> Option<Bdev> {
        let bdev = unsafe { spdk_bdev_first() };

        if bdev.is_null() {
            None
        } else {
            Some(Bdev::from(bdev))
        }
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
        if !self.0.is_null() {
            let current = self.0;
            self.0 = unsafe { spdk_bdev_next(current) };
            Some(Bdev::from(current))
        } else {
            None
        }
    }
}

impl From<*mut spdk_bdev> for Bdev {
    fn from(b: *mut spdk_bdev) -> Self {
        if let Some(b) = NonNull::new(b) {
            Bdev(b)
        } else {
            panic!("nullptr dereference while accessing a bdev");
        }
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
            "name: {}, driver: {}, product: {}, num_blocks: {}, block_len: {}",
            self.name(),
            self.driver(),
            self.product_name(),
            self.num_blocks(),
            self.block_len()
        )
    }
}
