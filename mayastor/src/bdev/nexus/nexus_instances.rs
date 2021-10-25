use once_cell::sync::OnceCell;
use spdk_rs::Thread;
use std::{cell::RefCell, os::raw::c_void, ptr::null_mut};

/// TODO
pub struct NexusInstances {
    // pub(crate) nexuses: RefCell<Vec<*mut c_void>>,
    pub(crate) nexuses: RefCell<Vec<i32>>,
    // pub(crate) nexuses: RefCell<Vec<i64>>,
}

impl Default for NexusInstances {
    fn default() -> Self {
        Self {
            nexuses: RefCell::new(Vec::new()),
        }
    }
}

unsafe impl Sync for NexusInstances {}
unsafe impl Send for NexusInstances {}

impl NexusInstances {
    /// Returns instances, we ensure that this can only ever be called on a
    /// properly allocated thread.
    fn get_or_init() -> &'static Self {
        // if let None = Thread::current() {
        //     panic!("Nexus instances must be accessed from an SPDK thread")
        // }

        static INST: OnceCell<NexusInstances> = OnceCell::new();
        INST.get_or_init(|| NexusInstances::default())
    }

    /// TODO
    pub fn add() {
        let slf = Self::get_or_init();
        // slf.nexuses.borrow_mut().push(0 as *mut c_void);
        slf.nexuses.borrow_mut().push(123);
        slf.nexuses.borrow_mut().push(123);
        // slf.nexuses.borrow_mut().push(456);
        // slf.nexuses.borrow_mut().push(123);
    }

    // pub fn remove_by_name(name: &str) {
    // let slf = Self::get_or_init();
    // for (idx, p) in slf.nexuses.iter().enumerate() {
    //     if unsafe { p.as_ref() }.name != name {
    //         continue;
    //     }
    //
    //     // unsafe { Box::from_raw(p.as_ptr()) };
    //     // slf.nexuses.remove(idx);
    //     info!("%%%% no remove!");
    //     return;
    // }
    // }
}
