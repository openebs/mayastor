use crate::bdev::{
    nexus::{self, nexus_module::NEXUS_MODULE},
    Bdev,
};

use crate::nexus_uri::{nexus_parse_uri, BdevType};
use serde::{export::Formatter, Serialize};
use spdk_sys::{
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_get_io_channel,
    spdk_bdev_module_release_bdev,
    spdk_io_channel,
};
use std::{fmt::Display, ops::Neg};

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ChildState {
    /// child has not been opened but we are in the process of opening it
    Init,
    /// can not add this bdev to the parent as its incompatible property wise
    ConfigInvalid,
    /// the child is open for RW
    Open,
    /// The child has been closed by its parent
    Closed,
    /// a fatal error (or errors) have occurred on this child
    Faulted,
}

impl ToString for ChildState {
    fn to_string(&self) -> String {
        match *self {
            ChildState::Init => "init",
            ChildState::ConfigInvalid => "configInvalid",
            ChildState::Open => "open",
            ChildState::Faulted => "faulted",
            ChildState::Closed => "closed",
        }
        .parse()
        .unwrap()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct NexusChild {
    /// name of the parent this child belongs too
    pub(crate) parent: String,
    ///  name of the child itself
    pub(crate) name: String,
    #[serde(skip_serializing)]
    /// the bdev wrapped in Bdev
    pub(crate) bdev: Option<Bdev>,
    #[serde(skip_serializing)]
    /// descriptor obtained after opening a device
    pub(crate) desc: *mut spdk_bdev_desc,
    #[serde(skip_serializing)]
    /// channel on which we submit the IO
    pub(crate) ch: *mut spdk_io_channel,
    /// current state of the child
    pub(crate) state: ChildState,
}

impl Display for NexusChild {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        if self.bdev.is_some() {
            let bdev = self.bdev.as_ref().unwrap();
            writeln!(
                f,
                "{}: {:?}, blk_cnt: {}, blk_size: {}",
                self.name,
                self.state,
                bdev.num_blocks(),
                bdev.block_size(),
            )
        } else {
            writeln!(f, "{}: state {:?}", self.name, self.state)
        }
    }
}

impl NexusChild {
    /// open the child bdev, assumes RW
    pub(crate) fn open(
        &mut self,
        num_blocks: u64,
        blk_size: u32,
    ) -> Result<String, nexus::Error> {
        debug!("{}: Opening child {}", self.parent, self.name);

        if self.bdev.as_ref()?.block_size() != blk_size {
            error!(
                "{}: Invalid block size for {}, wanted: {} got {}",
                self.parent,
                self.name,
                blk_size,
                self.bdev.as_ref()?.block_size(),
            );
            self.state = ChildState::ConfigInvalid;
            return Err(nexus::Error::Invalid);
        }

        if num_blocks > self.bdev.as_ref()?.num_blocks() {
            error!(
                "{}: child can not be larger then parent {} >= {}",
                self.parent,
                num_blocks,
                self.bdev.as_ref()?.num_blocks()
            );

            self.state = ChildState::ConfigInvalid;
            return Err(nexus::Error::Invalid);
        }

        let mut rc = unsafe {
            spdk_sys::spdk_bdev_open(
                self.bdev.as_ref()?.inner,
                true,
                None,
                std::ptr::null_mut(),
                &mut self.desc,
            )
        };

        if rc != 0 {
            error!("{}: Failed to open child {}", self.parent, self.name);
            self.state = ChildState::Faulted;
            self.desc = std::ptr::null_mut();
            return Err(match rc.neg() {
                libc::EPERM => nexus::Error::ReadOnly,
                libc::ENOTSUP => nexus::Error::InvalidThread,
                _ => nexus::Error::from(rc),
            });
        }

        rc = unsafe {
            spdk_sys::spdk_bdev_module_claim_bdev(
                self.bdev.as_ref()?.inner,
                std::ptr::null_mut(),
                &NEXUS_MODULE.module as *const _ as *mut _,
            )
        };

        if rc != 0 {
            self.state = ChildState::Faulted;
            error!("{}: Failed to claim device {}", self.parent, self.name);
            unsafe { spdk_bdev_close(self.desc) }
            self.desc = std::ptr::null_mut();
            return Err(match rc.neg() {
                libc::EPERM => nexus::Error::AlreadyClaimed,
                _ => nexus::Error::from(rc),
            });
        }

        self.state = ChildState::Open;
        Ok(self.name.clone())
    }

    /// close the bdev -- we have no means of determining if this succeeds
    pub(crate) fn close(&mut self) -> Result<ChildState, nexus::Error> {
        trace!("{}: Closing child {}", self.parent, self.name);
        if self.state == ChildState::Faulted {
            assert_eq!(self.desc, std::ptr::null_mut());
            self.clear();
            return Ok(self.state);
        }

        if let Some(bdev) = self.bdev.as_ref() {
            unsafe {
                spdk_bdev_module_release_bdev(bdev.inner);
                spdk_bdev_close(self.desc);
            }
        }

        // we leave the bdev structure around for when we want reopen it
        self.desc = std::ptr::null_mut();
        self.state = ChildState::Closed;
        Ok(self.state)
    }

    pub(crate) fn clear(&mut self) -> bool {
        self.state = ChildState::Init;
        true
    }

    pub(crate) fn get_io_channel(&self) -> *mut spdk_sys::spdk_io_channel {
        assert_eq!(self.state, ChildState::Open);
        unsafe { spdk_bdev_get_io_channel(self.desc) }
    }

    /// create a new nexus child
    pub fn new(name: String, parent: String, bdev: Option<Bdev>) -> Self {
        NexusChild {
            name,
            bdev,
            parent,
            desc: std::ptr::null_mut(),
            ch: std::ptr::null_mut(),
            state: ChildState::Init,
        }
    }

    /// destroy the child bdev
    pub async fn destroy(&self) -> Result<(), nexus::Error> {
        if let Ok(child_type) = nexus_parse_uri(&self.name) {
            match child_type {
                BdevType::Aio(args) => args.destroy().await,
                BdevType::Iscsi(args) => args.destroy().await,
                BdevType::Nvmf(args) => args.destroy(),
            }
        } else {
            // a bdev type we dont support is being used by the nexus
            Err(nexus::Error::Invalid)
        }
    }
}
