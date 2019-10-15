use crate::executor::cb_arg;
use futures::channel::oneshot;
use libc::c_void;
use spdk_sys::{
    spdk_bdev,
    spdk_bdev_first,
    spdk_bdev_get_aliases,
    spdk_bdev_get_block_size,
    spdk_bdev_get_device_stat,
    spdk_bdev_get_name,
    spdk_bdev_get_num_blocks,
    spdk_bdev_get_product_name,
    spdk_bdev_get_uuid,
    spdk_bdev_io_stat,
    spdk_bdev_io_type_supported,
    spdk_bdev_next,
    spdk_conf_section,
    spdk_conf_section_get_nmval,
    spdk_uuid,
    spdk_uuid_generate,
};
use std::ffi::CStr;

/// Allocate C string and return pointer to it.
/// NOTE: you must explicitly free it, otherwise the memory is leaked!
macro_rules! c_str {
    ($lit:expr) => {
        std::ffi::CString::new($lit).unwrap().into_raw();
    };
}

pub mod nexus;

unsafe fn parse_config_param<T>(
    sp: *mut spdk_conf_section,
    dev_name: &str,
    dev_num: i32,
    position: i32,
) -> Result<T, String>
where
    T: std::str::FromStr,
{
    let dev_name_c = std::ffi::CString::new(dev_name).unwrap();
    let val =
        spdk_conf_section_get_nmval(sp, dev_name_c.as_ptr(), dev_num, position);
    if val.is_null() {
        return Err(format!(
            "Config value for {}{} at position {} not found",
            dev_name, dev_num, position
        ));
    }
    let val = CStr::from_ptr(val).to_str().unwrap().parse::<T>();
    match val {
        Err(_) => Err(format!(
            "Invalid config value for {}{} at position {}",
            dev_name, dev_num, position
        )),
        Ok(val) => Ok(val),
    }
}

/// Wrapper interface over raw bdev pointers, currently bdevs are
/// not held while processing.
#[derive(Debug)]
pub struct Bdev {
    pub inner: *mut spdk_bdev,
}

/// Muuid provides several From trait implementations for the raw spdk UUIDs
/// It depends largely, on the bdev, if you can set the uuid in a nice way
#[derive(Debug)]
pub struct Muuid(*const spdk_uuid);

impl Muuid {
    /// For some of reason the uuid is a union
    pub fn as_bytes(&self) -> [u8; 16] {
        unsafe { (*self.0).u.raw }
    }
}

impl From<Muuid> for *const spdk_sys::spdk_uuid {
    fn from(u: Muuid) -> Self {
        u.0
    }
}

impl From<Muuid> for uuid::Uuid {
    fn from(u: Muuid) -> Self {
        uuid::Uuid::from_bytes(u.as_bytes())
    }
}

impl From<Muuid> for String {
    fn from(u: Muuid) -> Self {
        uuid::Uuid::from(u).to_hyphenated().to_string()
    }
}

/// Bdev read/write statistics
#[derive(Debug)]
pub struct Stat {
    pub num_read_ops: u64,
    pub num_write_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl Bdev {
    /// # Safety
    /// we assume pointers passed in are valid.
    pub unsafe fn from_ptr(inner: *mut spdk_bdev) -> Self {
        Bdev {
            inner,
        }
    }

    /// returns the block_size of the underlying device
    pub fn block_size(&self) -> u32 {
        unsafe { spdk_bdev_get_block_size(self.inner) }
    }

    /// number of blocks for this device
    pub fn num_blocks(&self) -> u64 {
        unsafe { spdk_bdev_get_num_blocks(self.inner) }
    }

    pub fn set_num_blocks(&self, count: u64) {
        unsafe {
            (*self.inner).blockcnt = count;
        }
    }

    /// whenever the underlying device needs alignment to the page size
    /// this is typically the case with io_uring and AIO. The value represents
    /// as a shift/exponent
    pub fn alignment(&self) -> u8 {
        unsafe { (*self.inner).required_alignment }
    }

    /// returns the configured product name
    pub fn product_name(&self) -> String {
        unsafe {
            CStr::from_ptr(spdk_bdev_get_product_name(self.inner))
                .to_str()
                .unwrap()
                .to_string()
        }
    }

    /// returns the name of driver module for the given bdev
    pub fn driver(&self) -> String {
        unsafe {
            CStr::from_ptr((*(*self.inner).module).name)
                .to_str()
                .unwrap()
                .to_string()
        }
    }

    /// returns the bdev name
    pub fn name(&self) -> String {
        unsafe {
            CStr::from_ptr(spdk_bdev_get_name(self.inner))
                .to_str()
                .unwrap()
                .to_string()
        }
    }

    /// the uuid that is set for this bdev, all bdevs should have a UUID set
    pub fn uuid(&self) -> Muuid {
        Muuid {
            0: unsafe { spdk_bdev_get_uuid(self.inner) },
        }
    }

    pub fn uuid_as_string(&self) -> String {
        let u = Muuid {
            0: unsafe { spdk_bdev_get_uuid(self.inner) },
        };
        uuid::Uuid::from(u).to_string()
    }

    /// Set an alias on the bdev, this alias can be used to find the bdev later
    /// NOTE: using this before calling spdk_bdev_register() crashes the
    /// system
    pub fn add_alias(&self, alias: &str) -> bool {
        let alias = std::ffi::CString::new(alias).unwrap();
        let ret = unsafe {
            spdk_sys::spdk_bdev_alias_add(self.inner, alias.as_ptr())
        };

        ret == 0
    }

    /// Get list of bdev aliases
    pub fn aliases(&self) -> Vec<String> {
        let mut aliases = Vec::new();
        let head = unsafe { &*spdk_bdev_get_aliases(self.inner) };
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
        unsafe { spdk_bdev_io_type_supported(self.inner, io_type) }
    }

    /// returns the bdev als a ptr
    pub fn as_ptr(&self) -> *mut spdk_bdev {
        self.inner
    }

    /// convert a given UUID into a spdk_bdev_uuid or otherwise, auto generate
    /// one
    pub fn set_uuid(&mut self, uuid: Option<String>) {
        if let Some(uuid) = uuid {
            if let Ok(this_uuid) = uuid::Uuid::parse_str(&uuid) {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        this_uuid.as_bytes().as_ptr() as *const _
                            as *mut c_void,
                        &mut (*self.inner).uuid.u.raw[0] as *const _
                            as *mut c_void,
                        (*self.inner).uuid.u.raw.len(),
                    );
                }
                return;
            }
        }
        unsafe { spdk_uuid_generate(&mut (*self.inner).uuid) };
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
        sender.send(errno).expect("Receiver is gone");
    }

    /// Get bdev stats or errno value in case of an error.
    pub async fn stats(&self) -> Result<Stat, i32> {
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
}

impl From<*mut c_void> for Bdev {
    fn from(bdev: *mut c_void) -> Self {
        Bdev {
            inner: bdev as *mut _ as *mut spdk_bdev,
        }
    }
}

impl From<*mut spdk_bdev> for Bdev {
    fn from(bdev: *mut spdk_bdev) -> Self {
        Bdev {
            inner: bdev,
        }
    }
}

/// iterator over the bdevs in the global bdev list
impl Iterator for Bdev {
    type Item = Bdev;
    fn next(&mut self) -> Option<Bdev> {
        let bdev = unsafe { spdk_bdev_next(self.inner) };
        if !bdev.is_null() {
            self.inner = bdev;
            Some(Bdev {
                inner: bdev,
            })
        } else {
            None
        }
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

/// lookup a bdev by its name or one of its alias
pub fn bdev_lookup_by_name(name: &str) -> Option<Bdev> {
    let name = std::ffi::CString::new(name.to_string()).unwrap();
    unsafe {
        let b = spdk_sys::spdk_bdev_get_by_name(name.as_ptr());
        if b.is_null() {
            None
        } else {
            Some(Bdev::from(b))
        }
    }
}
