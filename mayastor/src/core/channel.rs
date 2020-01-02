use spdk_sys::spdk_io_channel;

pub struct IoChannel(pub(crate) *mut spdk_io_channel);

impl Drop for IoChannel {
    fn drop(&mut self) {
        unimplemented!()
    }
}
