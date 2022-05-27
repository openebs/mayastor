//!
//! Thread safe memory pool implemented by using DPDKs rte_ring constructs.
//! This is avoids doing memory allocations in the hot path.
//!
//! Borrowed buffers are accounted for and validated upon freeing.

use std::{marker::PhantomData, mem::size_of, os::raw::c_void, ptr::NonNull};

use spdk_rs::libspdk::{
    spdk_mempool,
    spdk_mempool_count,
    spdk_mempool_create,
    spdk_mempool_free,
    spdk_mempool_get,
    spdk_mempool_put,
};

use crate::ffihelper::IntoCString;

pub struct MemoryPool<T: Sized> {
    pool: NonNull<spdk_mempool>,
    name: String,
    capacity: u64,
    element_type: PhantomData<T>,
}

unsafe impl<T: Sized> Send for MemoryPool<T> {}
unsafe impl<T: Sized> Sync for MemoryPool<T> {}

impl<T: Sized> MemoryPool<T> {
    /// Create memory pool with given name and size.
    pub fn create(name: &str, size: u64) -> Option<Self> {
        let cname = name.into_cstring();

        let pool: *mut spdk_mempool = unsafe {
            spdk_mempool_create(
                cname.as_ptr(),
                size,
                size_of::<T>() as u64,
                0,
                -1,
            )
        };

        if pool.is_null() {
            error!("Failed to create memory pool '{}'", name);
            return None;
        }

        info!(
            "Memory pool '{}' with {} elements ({} bytes size) successfully created",
            name, size, size_of::<T>()
        );
        Some(Self {
            pool: NonNull::new(pool).unwrap(),
            name: String::from(name),
            capacity: size,
            element_type: PhantomData,
        })
    }

    /// Get free element from memory pool and initialize memory with target
    /// object.
    pub fn get(&self, val: T) -> Option<*mut T> {
        let ptr: *mut T =
            unsafe { spdk_mempool_get(self.pool.as_ptr()) } as *mut T;

        if ptr.is_null() {
            return None;
        }

        unsafe {
            ptr.write(val);
        }

        Some(ptr)
    }

    /// Return allocated element to memory pool.
    pub fn put(&self, ptr: *mut T) {
        unsafe {
            spdk_mempool_put(self.pool.as_ptr(), ptr as *mut c_void);
        }
    }
}

impl<T: Sized> Drop for MemoryPool<T> {
    fn drop(&mut self) {
        let available = unsafe { spdk_mempool_count(self.pool.as_ptr()) };
        debug!(
            "Dropping memory pool '{}', elements placement (t/u/f): {}/{}/{}",
            self.name,
            self.capacity,
            self.capacity - available,
            available
        );
        assert_eq!(available, self.capacity);
        unsafe { spdk_mempool_free(self.pool.as_ptr()) };
        info!(
            "Memory pool '{}' with {} elements successfully freed",
            self.name, self.capacity
        );
    }
}
