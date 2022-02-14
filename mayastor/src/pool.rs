//! High-level storage pool object methods.
//!
//! They provide abstraction on top of aio and uring bdev, lvol store, etc
//! and export simple-to-use json-rpc methods for managing pools.

use std::{
    convert::TryFrom,
    ffi::CStr,
    io::{Error as ioError, ErrorKind},
    os::raw::c_char,
};

use ::rpc::mayastor as rpc;
use spdk_rs::libspdk::{
    lvol_store_bdev,
    spdk_bs_free_cluster_count,
    spdk_bs_get_cluster_size,
    spdk_bs_total_data_cluster_count,
    spdk_lvol_store,
    vbdev_lvol_store_first,
    vbdev_lvol_store_next,
};

use crate::core::{Bdev, UntypedBdev};

/// Structure representing a pool which comprises lvol store and
/// underlying bdev.
///
/// Note about safety: The structure wraps raw C pointers from SPDK.
/// It is safe to use only in synchronous context. If you keep Pool for
/// longer than that then something else can run on reactor_0 in between,
/// which may destroy the pool and invalidate the pointers!
pub struct Pool {
    lvs_ptr: *mut spdk_lvol_store,
    lvs_bdev_ptr: *mut lvol_store_bdev,
}

impl Pool {
    /// An easy converter from a raw pointer to Pool object
    unsafe fn from_ptr(ptr: *mut lvol_store_bdev) -> Pool {
        Pool {
            lvs_ptr: (*ptr).lvs,
            lvs_bdev_ptr: ptr,
        }
    }

    /// Get name of the pool.
    pub fn get_name(&self) -> &str {
        unsafe {
            let lvs = &*self.lvs_ptr;
            CStr::from_ptr(&lvs.name as *const c_char).to_str().unwrap()
        }
    }

    /// Get base bdev for the pool (in our case AIO or uring bdev).
    pub fn get_base_bdev(&self) -> UntypedBdev {
        unsafe { Bdev::checked_from_ptr((*self.lvs_bdev_ptr).bdev).unwrap() }
    }

    /// Get capacity of the pool in bytes.
    pub fn get_capacity(&self) -> u64 {
        unsafe {
            let lvs = &*self.lvs_ptr;
            let cluster_size = spdk_bs_get_cluster_size(lvs.blobstore);
            let total_clusters =
                spdk_bs_total_data_cluster_count(lvs.blobstore);
            total_clusters * cluster_size
        }
    }

    /// Get free space in the pool in bytes.
    pub fn get_free(&self) -> u64 {
        unsafe {
            let lvs = &*self.lvs_ptr;
            let cluster_size = spdk_bs_get_cluster_size(lvs.blobstore);
            spdk_bs_free_cluster_count(lvs.blobstore) * cluster_size
        }
    }
}

/// Iterator over available storage pools.
#[derive(Default)]
pub struct PoolsIter {
    lvs_bdev_ptr: Option<*mut lvol_store_bdev>,
}

impl PoolsIter {
    pub fn new() -> Self {
        Self {
            lvs_bdev_ptr: None,
        }
    }
}

impl Iterator for PoolsIter {
    type Item = Pool;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lvs_bdev_ptr {
            Some(current) => {
                if current.is_null() {
                    return None;
                }
                self.lvs_bdev_ptr =
                    Some(unsafe { vbdev_lvol_store_next(current) });
                Some(unsafe { Pool::from_ptr(current) })
            }
            None => {
                let current = unsafe { vbdev_lvol_store_first() };
                if current.is_null() {
                    self.lvs_bdev_ptr = Some(current);
                    return None;
                }
                self.lvs_bdev_ptr =
                    Some(unsafe { vbdev_lvol_store_next(current) });
                Some(unsafe { Pool::from_ptr(current) })
            }
        }
    }
}

impl From<Pool> for rpc::Pool {
    fn from(pool: Pool) -> Self {
        rpc::Pool {
            name: pool.get_name().to_owned(),
            disks: vec![
                pool.get_base_bdev().driver().to_string()
                    + "://"
                    + pool.get_base_bdev().name(),
            ],
            // TODO: figure out how to detect state of pool
            state: rpc::PoolState::PoolOnline as i32,
            capacity: pool.get_capacity(),
            used: pool.get_capacity() - pool.get_free(),
        }
    }
}

/// PoolArgs is used to translate the input for the grpc
/// Create/Import requests which contains name, uuid & disks.
/// This help us avoid importing grpc structs in the actual lvs mod
#[derive(Clone, Debug)]
pub struct PoolArgs {
    pub name: String,
    pub disks: Vec<String>,
    pub uuid: Option<String>,
}

/// PoolBackend is the type of pool underneath Lvs, Lvm, etc
pub enum PoolBackend {
    Lvs,
}

impl TryFrom<i32> for PoolBackend {
    type Error = ioError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Lvs),
            _ => Err(ioError::new(
                ErrorKind::InvalidInput,
                format!("invalid pool type {}", value),
            )),
        }
    }
}
