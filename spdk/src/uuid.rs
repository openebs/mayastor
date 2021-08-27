use spdk_sys::{spdk_uuid, spdk_uuid_copy, spdk_uuid_generate};

/// Wrapper for SPDK UUID.
pub struct Uuid {
    inner: spdk_uuid,
}

impl Uuid {
    /// Generats a new `Uuid` via SPDK.
    pub fn generate() -> Self {
        let mut inner = spdk_uuid::default();
        unsafe { spdk_uuid_generate(&mut inner as *mut _) };
        Self {
            inner,
        }
    }

    /// Consumes the `Uuid` and returns its SPDK internal representation.
    pub(crate) fn into_raw(self) -> spdk_uuid {
        self.inner
    }
}

impl From<uuid::Uuid> for Uuid {
    /// Converts a `uuid::Uuid` object into an SPDK `Uuid`.
    fn from(u: uuid::Uuid) -> Self {
        let mut inner = spdk_uuid::default();

        unsafe {
            spdk_uuid_copy(
                &mut inner as *mut _,
                u.as_bytes().as_ptr() as *const spdk_uuid,
            )
        };

        Self {
            inner,
        }
    }
}
