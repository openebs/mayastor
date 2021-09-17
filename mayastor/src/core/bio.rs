use core::fmt;
use std::{
    fmt::{Debug, Formatter},
    ptr::NonNull,
};

use libc::c_void;

use spdk_sys::spdk_bdev_io;

use crate::core::{Bdev, NvmeStatus};

use spdk::{IoStatus, IoType};

#[derive(Clone)]
#[repr(transparent)]
pub struct Bio(NonNull<spdk_bdev_io>);
