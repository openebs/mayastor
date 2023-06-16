use parking_lot::Mutex;
use std::{cell::RefCell, marker::PhantomData, pin::Pin, time::Duration};

use spdk_rs::{
    BdevIo,
    BdevModule,
    BdevModuleBuild,
    BdevOps,
    IoChannel,
    IoDevice,
    IoType,
    Poller,
    PollerBuilder,
    WithModuleInit,
};

const NULL_MODULE_NAME: &str = "NullNg";

/// Poller data for Null Bdev.
#[derive(Default)]
#[allow(clippy::non_send_fields_in_send_ty)]
struct NullIoPollerData<'a> {
    iovs: Mutex<Vec<BdevIo<NullIoDevice<'a>>>>,
    _my_num: f64,
}

// Required because `NullIoPollerData.iovs` contains `BdevIo`, which
// contains NonNull, which is not Send.
unsafe impl<'a> Send for NullIoPollerData<'a> {}

/// Per-core channel data.
struct NullIoChannelData<'a> {
    poller: Poller<'a, NullIoPollerData<'a>>,
    _some_value: i64,
}

impl NullIoChannelData<'_> {
    fn new(some_value: i64) -> Self {
        let poller = PollerBuilder::new()
            .with_interval(Duration::from_micros(1000))
            .with_data(NullIoPollerData {
                iovs: Mutex::new(Vec::new()),
                _my_num: 77.77 + some_value as f64,
            })
            .with_poll_fn(|dat| {
                let ready: Vec<_> = dat.iovs.lock().drain(..).collect();
                let cnt = ready.len();
                ready.iter().for_each(|io: &BdevIo<_>| io.ok());
                cnt as i32
            })
            .build();

        Self {
            poller,
            _some_value: some_value,
        }
    }
}

/// 'Null' I/O device structure.
struct NullIoDevice<'a> {
    _my_name: String,
    _smth: u64,
    next_chan_id: RefCell<i64>,
    _a: PhantomData<&'a ()>,
}

/// TODO
impl<'a> IoDevice for NullIoDevice<'a> {
    type ChannelData = NullIoChannelData<'a>;

    /// TODO
    fn io_channel_create(self: Pin<&mut Self>) -> Self::ChannelData {
        let mut x = self.next_chan_id.borrow_mut();
        *x += 1;
        self.get_io_device_id();

        Self::ChannelData::new(*x)
    }

    /// TODO
    fn io_channel_destroy(self: Pin<&mut Self>, _io_chan: Self::ChannelData) {}
}

/// TODO
impl<'a> BdevOps for NullIoDevice<'a> {
    type ChannelData = NullIoChannelData<'a>;
    type BdevData = Self;
    type IoDev = Self;

    /// TODO
    fn destruct(self: Pin<&mut Self>) {
        self.unregister_io_device();
    }

    /// TODO
    fn submit_request(
        &self,
        io_chan: IoChannel<Self::ChannelData>,
        bio: BdevIo<Self>,
    ) {
        let chan_data = io_chan.channel_data();

        match bio.io_type() {
            IoType::Read | IoType::Write => {
                chan_data.poller.data().iovs.lock().push(bio)
            }
            _ => bio.fail(),
        };
    }

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool {
        matches!(io_type, IoType::Read | IoType::Write)
    }

    /// TODO
    fn get_io_device(&self) -> &Self::IoDev {
        self
    }
}

/// TODO
impl<'a> NullIoDevice<'a> {
    /// TODO
    #[allow(dead_code)]
    fn create(name: &str) {
        let bm = BdevModule::find_by_name(NULL_MODULE_NAME).unwrap();

        let io_dev = NullIoDevice {
            _my_name: String::from(name),
            _smth: 789,
            next_chan_id: RefCell::new(10),
            _a: Default::default(),
        };

        let mut bdev = bm
            .bdev_builder()
            .with_data(io_dev)
            .with_name(name)
            .with_product_name("Null Device New Generation")
            .with_block_length(1 << 12)
            .with_block_count(1 << 20)
            .with_required_alignment(12)
            .with_zoned(false)
            .build();

        bdev.data().register_io_device(Some(name));

        match bdev.register_bdev() {
            Ok(_) => info!("NullNg Bdev regustered"),
            Err(err) => error!("Failed to register NullNg Bdev: {}", err),
        }
    }
}

/// Null Bdev module.
struct NullBdevModule {}

impl WithModuleInit for NullBdevModule {
    fn module_init() -> i32 {
        // NullIoDevice::create("nullng0");
        0
    }
}

impl BdevModuleBuild for NullBdevModule {}

pub fn register() {
    NullBdevModule::builder(NULL_MODULE_NAME)
        .with_module_init()
        .register();
}
