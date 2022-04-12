use spdk_rs::libspdk::{
    spdk_nvmf_poll_group,
    spdk_nvmf_poll_group_create,
    spdk_nvmf_tgt,
};

use crate::core::Mthread;

#[derive(Clone, Debug)]
struct Pg(*mut spdk_nvmf_poll_group);

#[repr(C)]
#[derive(Clone, Debug)]
pub(crate) struct PollGroup {
    pub thread: Mthread,
    group: Pg,
}

impl PollGroup {
    pub fn new(tgt: *mut spdk_nvmf_tgt, mt: Mthread) -> Self {
        Self {
            thread: mt,
            group: Pg(unsafe { spdk_nvmf_poll_group_create(tgt) }),
        }
    }

    pub fn group_ptr(&self) -> *mut spdk_nvmf_poll_group {
        self.group.0
    }
}
