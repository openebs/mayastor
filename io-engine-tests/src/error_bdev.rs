use spdk_rs::libspdk::{
    create_aio_bdev,
    vbdev_error_create,
    vbdev_error_inject_error,
    vbdev_error_inject_opts,
};
pub use spdk_rs::libspdk::{SPDK_BDEV_IO_TYPE_READ, SPDK_BDEV_IO_TYPE_WRITE};

// constant used by the vbdev_error module but not exported
pub const VBDEV_IO_FAILURE: u32 = 1;

pub fn create_error_bdev(error_device: &str, backing_device: &str) {
    let mut retval: i32;
    let cname = std::ffi::CString::new(error_device).unwrap();
    let filename = std::ffi::CString::new(backing_device).unwrap();

    unsafe {
        // this allows us to create a bdev without its name being a uri
        retval = create_aio_bdev(
            cname.as_ptr(),
            filename.as_ptr(),
            512,
            false,
            false,
        )
    };
    assert_eq!(retval, 0);

    let err_bdev_name_str = std::ffi::CString::new(error_device.to_string())
        .expect("Failed to create name string");
    unsafe {
        // create the error bdev around it
        retval = vbdev_error_create(
            err_bdev_name_str.as_ptr(),
            std::ptr::null_mut(),
        );
    }
    assert_eq!(retval, 0);
}

pub fn inject_error(error_device: &str, op: u32, mode: u32, count: u32) {
    let retval: i32;
    let err_bdev_name_str = std::ffi::CString::new(error_device)
        .expect("Failed to create name string");
    let raw = err_bdev_name_str.into_raw();

    let opts = vbdev_error_inject_opts {
        io_type: op,
        error_type: mode,
        error_num: count,
        corrupt_offset: 0,
        corrupt_value: 0,
        error_qd: 0,
    };

    unsafe {
        retval = vbdev_error_inject_error(raw, &opts);
    }
    assert_eq!(retval, 0);
}
