use crate::{
    blkid::{
        blkid_partition,
        blkid_partition_get_name,
        blkid_partition_get_type_string,
        blkid_partition_get_uuid,
        blkid_partlist,
        blkid_partlist_get_partition,
        blkid_partlist_get_partition_by_partno,
        blkid_partlist_numof_partitions,
        to_result,
    },
    DevInfoError,
};
use std::{ffi::CStr, os::raw::c_int};
pub struct Partition(pub(crate) blkid_partition);

impl Partition {
    pub fn get_name(&self) -> Option<String> {
        let ptr = unsafe { blkid_partition_get_name(self.0) };
        if ptr.is_null() {
            return None;
        }

        Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
    }

    pub fn get_type_string(&self) -> Option<String> {
        let ptr = unsafe { blkid_partition_get_type_string(self.0) };
        if ptr.is_null() {
            return None;
        }

        Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
    }

    pub fn get_uuid(&self) -> Option<String> {
        let ptr = unsafe { blkid_partition_get_uuid(self.0) };
        if ptr.is_null() {
            return None;
        }

        Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
    }
}

pub struct PartList(pub(crate) blkid_partlist);

impl PartList {
    pub fn get_partition(&self, partition: i32) -> Option<Partition> {
        if let Ok(p) = to_result(unsafe {
            blkid_partlist_get_partition(self.0, partition as c_int)
        }) {
            return Some(Partition(p));
        }
        {
            None
        }
    }

    pub fn get_partition_by_partno(&self, partition: i32) -> Option<Partition> {
        if let Ok(p) = to_result(unsafe {
            blkid_partlist_get_partition_by_partno(self.0, partition as c_int)
        }) {
            return Some(Partition(p));
        }
        {
            None
        }
    }

    pub fn numof_partitions(&self) -> Result<u32, DevInfoError> {
        unsafe {
            to_result(blkid_partlist_numof_partitions(self.0)).map(|v| v as u32)
        }
    }
}
