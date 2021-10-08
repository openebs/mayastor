use std::{
    fmt,
    fmt::{Display, Formatter},
};

use crate::libspdk::{spdk_uuid, spdk_uuid_copy, spdk_uuid_generate};

/// Wrapper for SPDK UUID.
pub struct Uuid {
    inner: spdk_uuid,
}

impl Uuid {
    /// Generats a new `Uuid` via SPDK.
    pub fn generate() -> Self {
        let mut r = Self::default();
        unsafe { spdk_uuid_generate(&mut r.inner) };
        r
    }

    /// TODO
    pub(crate) fn new(u: &spdk_uuid) -> Self {
        Self {
            inner: *u,
        }
    }

    /// Consumes the `Uuid` and returns its SPDK internal representation.
    pub(crate) fn into_raw(self) -> spdk_uuid {
        self.inner
    }

    /// TODO
    /// Note: for some reason the uuid is a union.
    fn as_bytes(&self) -> [u8; 16] {
        unsafe { self.inner.u.raw }
    }

    /// TODO
    pub fn legacy_from_ptr(p: *const spdk_uuid) -> Self {
        assert_eq!(p.is_null(), false);
        Self::new(unsafe { &*p })
    }
}

impl Default for Uuid {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
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

impl From<Uuid> for uuid::Uuid {
    fn from(u: Uuid) -> Self {
        Self::from_bytes(u.as_bytes())
    }
}

impl Clone for Uuid {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
        }
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        uuid::Uuid::from(self.clone()).fmt(f)
    }
}
