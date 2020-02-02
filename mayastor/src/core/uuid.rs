use spdk_sys::spdk_uuid;

pub struct Uuid(pub(crate) *const spdk_uuid);

impl Uuid {
    /// For some reason the uuid is a union
    pub fn as_bytes(&self) -> [u8; 16] {
        unsafe { (*self.0).u.raw }
    }
}
