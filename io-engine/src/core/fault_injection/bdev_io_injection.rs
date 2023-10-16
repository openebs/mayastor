use once_cell::sync::OnceCell;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;

use spdk_rs::{
    libspdk::{
        spdk_bdev_fn_table,
        spdk_bdev_io,
        spdk_bdev_io_complete_nvme_status,
        spdk_io_channel,
    },
    BdevIo,
    UntypedBdev,
};

use crate::core::IoCompletionStatus;

use super::{
    FaultDomain,
    FaultInjectionError,
    FaultIoStage,
    FaultMethod,
    InjectIoCtx,
    Injection,
};

/// TODO
struct BdevInfo {
    fn_table: *const spdk_bdev_fn_table,
    fn_table_orig: *const spdk_bdev_fn_table,
    inj: Injection,
}

unsafe impl Send for BdevInfo {}

/// TODO
type Bdevs = HashMap<usize, BdevInfo>;

/// TODO
fn get_bdevs<'a>() -> MutexGuard<'a, Bdevs> {
    static INJECTIONS: OnceCell<Mutex<Bdevs>> = OnceCell::new();

    INJECTIONS.get_or_init(|| Mutex::new(HashMap::new())).lock()
}

/// TODO
unsafe extern "C" fn inject_submit_request(
    chan: *mut spdk_io_channel,
    io_ptr: *mut spdk_bdev_io,
) {
    let mut g = get_bdevs();

    let io = BdevIo::<()>::legacy_from_ptr(io_ptr);
    let bdev = io.bdev();

    let hash = (*bdev.unsafe_inner_ptr()).fn_table as usize;
    let t = g.get_mut(&hash).expect("Bdev for injection not found");
    assert_eq!(t.fn_table, (*bdev.unsafe_inner_ptr()).fn_table);

    let inj = &mut t.inj;

    let ctx = InjectIoCtx::with_iovs(
        FaultDomain::BdevIo,
        inj.device_name.as_str(),
        io.io_type(),
        io.offset(),
        io.num_blocks(),
        io.iovs(),
    );

    match inj.inject(FaultIoStage::Submission, &ctx) {
        Some(s) => {
            error!("Injection {inj:?}: failing I/O: {io:?}");

            match s {
                IoCompletionStatus::NvmeError(err) => {
                    let (sct, sc) = err.as_sct_sc_codes();
                    unsafe {
                        spdk_bdev_io_complete_nvme_status(io_ptr, 0, sct, sc);
                    }
                }
                _ => panic!("Non-NVME error is not supported"),
            }
        }
        None => {
            ((*t.fn_table_orig).submit_request.unwrap())(chan, io_ptr);
        }
    }
}

/// TODO
pub(super) fn add_bdev_io_injection(
    inj: &Injection,
) -> Result<(), FaultInjectionError> {
    if inj.io_stage != FaultIoStage::Submission {
        return Err(FaultInjectionError::InvalidInjection {
            name: inj.device_name.clone(),
            msg: format!("bdev I/O supports only submission injections"),
        });
    }

    if !matches!(
        inj.method,
        FaultMethod::Status(IoCompletionStatus::NvmeError(_))
    ) {
        return Err(FaultInjectionError::InvalidInjection {
            name: inj.device_name.clone(),
            msg: format!("bdev I/O supports only NVME error injection"),
        });
    }

    let Some(mut bdev) = UntypedBdev::lookup_by_name(&inj.device_name) else {
        return Err(FaultInjectionError::DeviceNotFound {
            name: inj.device_name.clone(),
        });
    };

    let mut g = get_bdevs();

    // Check for double insertion.
    if g.iter().any(|(_, v)| v.inj.device_name == inj.device_name) {
        return Err(FaultInjectionError::InvalidInjection {
            name: inj.device_name.clone(),
            msg: format!(
                "bdev I/O does not support multiple injections per bdev"
            ),
        });
    }

    unsafe {
        let fn_table_orig = (*bdev.unsafe_inner_ptr()).fn_table;

        let fn_table = Box::into_raw(Box::new(spdk_bdev_fn_table {
            submit_request: Some(inject_submit_request),
            ..*fn_table_orig
        }));

        let bdev_inj = BdevInfo {
            fn_table,
            fn_table_orig,
            inj: inj.clone(),
        };

        (*bdev.unsafe_inner_mut_ptr()).fn_table = fn_table;

        g.insert(fn_table as usize, bdev_inj);

        info!("Added bdev I/O injection to bdev {bdev:?}: {inj:?}");
    }

    Ok(())
}
