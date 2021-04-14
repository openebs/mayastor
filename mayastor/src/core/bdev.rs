use std::{
    convert::TryFrom,
    ffi::{CStr, CString},
    fmt::{Debug, Display, Formatter},
    os::raw::c_void,
    ptr::NonNull,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::ResultExt;

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_event_type,
    spdk_bdev_first,
    spdk_bdev_get_aliases,
    spdk_bdev_get_block_size,
    spdk_bdev_get_buf_align,
    spdk_bdev_get_by_name,
    spdk_bdev_get_device_stat,
    spdk_bdev_get_name,
    spdk_bdev_get_num_blocks,
    spdk_bdev_get_product_name,
    spdk_bdev_get_uuid,
    spdk_bdev_io_stat,
    spdk_bdev_io_type_supported,
    spdk_bdev_next,
    spdk_bdev_open_ext,
    spdk_uuid,
    spdk_uuid_copy,
    spdk_uuid_generate,
};

use crate::{
    bdev::lookup_child_from_bdev,
    core::{
        share::{Protocol, Share},
        uuid::Uuid,
        CoreError,
        Descriptor,
        IoType,
        ShareIscsi,
        ShareNvmf,
        UnshareIscsi,
        UnshareNvmf,
    },
    ffihelper::{cb_arg, AsStr},
    subsys::NvmfSubsystem,
    target::{iscsi, nvmf, Side},
};

#[derive(Debug)]
pub struct BdevStats {
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
        iscsi::share(&self.name(), &self, Side::Nexus).context(ShareIscsi {})
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

        Ok(self.name())
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
    extern "C" fn event_cb(
        event: spdk_bdev_event_type,
        bdev: *mut spdk_bdev,
        _ctx: *mut c_void,
    ) {
        let bdev = Bdev::from_ptr(bdev).unwrap();
        // Take the appropriate action for the given event type
        match event {
            spdk_sys::SPDK_BDEV_EVENT_REMOVE => {
                info!("Received remove event for bdev {}", bdev.name());
                if let Some(child) = lookup_child_from_bdev(&bdev.name()) {
                    child.remove();
                }
            }
            spdk_sys::SPDK_BDEV_EVENT_RESIZE => {
                warn!("Received resize event for bdev {}", bdev.name())
            }
            spdk_sys::SPDK_BDEV_EVENT_MEDIA_MANAGEMENT => warn!(
                "Received media management event for bdev {}",
                bdev.name()
            ),
            _ => error!(
                "Received unknown event {} for bdev {}",
                event,
                bdev.name()
            ),
        }
    }

    /// open the current bdev, the bdev can be opened multiple times resulting
    /// in a new descriptor for each call.
    pub fn open(&self, read_write: bool) -> Result<Descriptor, CoreError> {
        let mut descriptor = std::ptr::null_mut();
        let cname = CString::new(self.name()).unwrap();
        let rc = unsafe {
            spdk_bdev_open_ext(
                cname.as_ptr(),
                read_write,
                Some(Self::event_cb),
                std::ptr::null_mut(),
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
        NonNull::new(bdev).map(Bdev)
    }

    /// lookup a bdev by its name
    pub fn lookup_by_name(name: &str) -> Option<Bdev> {
        let name = CString::new(name).unwrap();
        Self::from_ptr(unsafe { spdk_bdev_get_by_name(name.as_ptr()) })
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
    pub fn alignment(&self) -> u64 {
        unsafe { spdk_bdev_get_buf_align(self.0.as_ptr()) }
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

    /// return the UUID of this bdev
    pub fn uuid(&self) -> Uuid {
        Uuid(unsafe { spdk_bdev_get_uuid(self.0.as_ptr()) })
    }

    /// return the UUID of this bdev as a string
    pub fn uuid_as_string(&self) -> String {
        uuid::Uuid::from(self.uuid()).to_hyphenated().to_string()
    }

    /// Set a list of aliases on the bdev, used to find the bdev later
    pub fn add_aliases(&self, alias: &[String]) -> bool {
        alias
            .iter()
            .filter(|a| -> bool { !self.add_alias(a) })
            .count()
            == 0
    }

    /// Set an alias on the bdev, this alias can be used to find the bdev later
    pub fn add_alias(&self, alias: &str) -> bool {
        let alias = CString::new(alias).unwrap();
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
    pub fn io_type_supported(&self, io_type: IoType) -> bool {
        unsafe { spdk_bdev_io_type_supported(self.0.as_ptr(), io_type.into()) }
    }

    /// returns the bdev as a ptr
    /// dont use please
    pub fn as_ptr(&self) -> *mut spdk_bdev {
        self.0.as_ptr()
    }

    /// set the UUID for this bdev
    pub fn set_uuid(&mut self, uuid: uuid::Uuid) {
        unsafe {
            spdk_uuid_copy(
                &mut (*self.0.as_ptr()).uuid,
                uuid.as_bytes().as_ptr() as *const spdk_uuid,
            );
        }
    }

    /// generate a new random UUID for this bdev
    pub fn generate_uuid(&mut self) {
        unsafe {
            spdk_uuid_generate(&mut (*self.0.as_ptr()).uuid);
        }
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
    pub async fn stats(&self) -> Result<BdevStats, i32> {
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
            Ok(BdevStats {
                num_read_ops: stat.num_read_ops,
                num_write_ops: stat.num_write_ops,
                bytes_read: stat.bytes_read,
                bytes_written: stat.bytes_written,
            })
        }
    }

    /// returns the first bdev in the list
    pub fn bdev_first() -> Option<Bdev> {
        Self::from_ptr(unsafe { spdk_bdev_first() })
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
            Bdev::from_ptr(current)
        }
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
            "name: {}, driver: {}, product: {}, num_blocks: {}, block_len: {}",
            self.name(),
            self.driver(),
            self.product_name(),
            self.num_blocks(),
            self.block_len()
        )
    }
}
