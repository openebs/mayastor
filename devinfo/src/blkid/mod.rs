pub mod partition;
pub mod probe;
use crate::DevInfoError;

include!(concat!(env!("OUT_DIR"), "/libblkid.rs"));
pub(crate) trait CResult: Copy {
    fn is_error(self) -> bool;
}

impl CResult for i32 {
    fn is_error(self) -> bool {
        self < 0
    }
}

impl CResult for i64 {
    fn is_error(self) -> bool {
        self < 0
    }
}

impl<T> CResult for *const T {
    fn is_error(self) -> bool {
        self.is_null()
    }
}

impl<T> CResult for *mut T {
    fn is_error(self) -> bool {
        self.is_null()
    }
}

pub(crate) fn to_result<T: CResult>(result: T) -> Result<T, DevInfoError> {
    if result.is_error() {
        return Err(DevInfoError::Io {
            source: std::io::Error::last_os_error(),
        });
    }

    Ok(result)
}
