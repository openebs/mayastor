use spdk::{
    cpu_cores::Cores,
    BdevBuilder,
    BdevIo,
    BdevModule,
    BdevModuleBuild,
    BdevModuleInit,
    BdevOps,
    IoChannel,
    IoDevice,
    IoType,
    Poller,
    PollerBuilder,
};
use std::{cell::RefCell, marker::PhantomData};

const NULL_MODULE_NAME: &str = "NullNg";

/// Null Bdev module.
struct NullBdevModule {}

impl BdevModuleInit for NullBdevModule {
    fn module_init() -> i32 {
        NullIoDevice::create("nullng0");
        0
    }
}

impl BdevModuleBuild for NullBdevModule {}

pub fn register() {
    NullBdevModule::builder(NULL_MODULE_NAME)
        .with_module_init()
        .register();
}

/// Poller data for Null Bdev.
struct NullIoPollerData<'a> {
    iovs: RefCell<Vec<BdevIo<NullIoDevice<'a>>>>,
    my_num: f64,
}

impl Drop for NullIoPollerData<'_> {
    fn drop(&mut self) {
        dbgln!(NullIoPollerData, self.dbg(); "drop NullIoPollerData");
    }
}

impl<'a> NullIoPollerData<'a> {
    fn dbg(&self) -> String {
        format!("iovs cnt '{}' my_num '{}'",
                self.iovs.borrow().len(),
                self.my_num)
    }
}

/// Per-core channel data.
struct NullIoChannelData<'a> {
    poller: Poller<'a, NullIoPollerData<'a>>,
    some_value: i64,
}

impl NullIoChannelData<'_> {
    fn new(some_value: i64) -> Self {
        let poller = PollerBuilder::new()
            .with_interval(1000)
            .with_data(NullIoPollerData {
                iovs: RefCell::new(Vec::new()),
                my_num: 77.77 + some_value as f64
            })
            .with_poll_fn(|dat| {
                let ready: Vec<_> = dat.iovs.borrow_mut().drain(..).collect();
                let cnt = ready.len();
                if cnt > 0 {
                    dbgln!(NullIoChannelData, dat.dbg(); ">>>> polled something!");
                }
                ready.iter().for_each(|io: &BdevIo<_>| io.ok());
                cnt as i32
            })
            .build();

        let res = Self {
            poller,
            some_value,
        };
        dbgln!(NullIoPollerData, res.poller.data().dbg(); "new Null Poller");
        dbgln!(NullIoChannelData, res.dbg(); "new Null I/O Channel");
        res
    }

    fn dbg(&self) -> String {
        format!("some_value '{}'", self.some_value)
    }
}

impl Drop for NullIoChannelData<'_> {
    fn drop(&mut self) {
        dbgln!(NullIoChannelData, self.dbg(); "drop NullIoChannelData");
    }
}

/// 'Null' I/O device structure.
struct NullIoDevice<'a> {
    name: String,
    smth: u64,
    next_chan_id: RefCell<i64>,
    _a: PhantomData<&'a ()>,
}

/// TODO
impl Drop for NullIoDevice<'_> {
    fn drop(&mut self) {
        dbgln!(NullIoDevice, self.dbg(); "drop NullIoDevice");
    }
}

/// TODO
impl<'a> IoDevice for NullIoDevice<'a> {
    type ChannelData = NullIoChannelData<'a>;

    /// TODO
    fn io_channel_create(&self) -> Self::ChannelData {
        dbgln!(NullIoDevice, self.dbg(); "io_channel_create");

        let mut x = self.next_chan_id.borrow_mut();
        *x += 1;
        self.get_io_device_id();

        Self::ChannelData::new(*x)
    }

    /// TODO
    fn io_channel_destroy(&self, io_chan: Self::ChannelData) {
        dbgln!(NullIoDevice, self.dbg(); "io_channel_destroy: <{}>", io_chan.dbg());
    }
}

/// TODO
impl<'a> BdevOps for NullIoDevice<'a> {
    type ChannelData = NullIoChannelData<'a>;
    type BdevData = Self;
    type IoDev = Self;

    /// TODO
    fn destruct(&self) {
        dbgln!(NullIoDevice, self.dbg(); "destruct -> unregister i/o device");
        self.io_device_unregister();
    }

    /// TODO
    fn submit_request(
        &self,
        io_chan: IoChannel<Self::ChannelData>,
        bio: BdevIo<Self>,
    ) {
        let chan_data = io_chan.channel_data();

        dbgln!(NullIoDevice, self.dbg(); "submit req: my cd {}", chan_data.dbg());

        match bio.io_type() {
            IoType::Read | IoType::Write => {
                dbgln!(NullIoDevice, self.dbg(); ">>>> push BIO");
                chan_data.poller.data().iovs.borrow_mut().push(bio)
            }
            _ => bio.fail(),
        };
    }

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool {
        dbgln!(NullIoDevice, self.dbg(); "io_type_supported: type='{:?}' ?", io_type);
        matches!(io_type, IoType::Read | IoType::Write)
    }

    /// TODO
    fn get_io_device(&self) -> &Self::IoDev {
        self
    }
}

/// TODO
impl<'a> NullIoDevice<'a> {
    fn create(name: &str) {
        let bm = BdevModule::find_by_name(NULL_MODULE_NAME).unwrap();

        let io_dev = NullIoDevice {
            name: String::from(name),
            smth: 789,
            next_chan_id: RefCell::new(10),
            _a: Default::default(),
        };

        let bdev = BdevBuilder::new()
            .with_data(io_dev)
            .with_module(&bm)
            .with_name(name)
            .with_product_name("nullblk thing")
            .with_block_length(1 << 12)
            .with_block_count(1 << 20)
            .with_required_alignment(12)
            .build();

        bdev.data().io_device_register(Some(name));
        bdev.bdev_register();

        dbgln!(NullIoDevice, ""; "created '{}'", name);
    }

    fn dbg(&self) -> String {
        format!(
            "id '{:p}' name '{}' smth '{}'",
            self.get_io_device_id(),
            self.name,
            self.smth
        )
    }
}
