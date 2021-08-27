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

/// Poller context for Null Bdev.
struct NullIoPoller<'a> {
    iovs: RefCell<Vec<BdevIo<NullIoDevice<'a>>>>,
}

impl Drop for NullIoPoller<'_> {
    fn drop(&mut self) {
        dbgln!(NullIoPoller, self.iovs.borrow().len(); "drop");
    }
}

/// Per-core channel data.
struct NullIoChannelData<'a> {
    poller: Poller<'a, NullIoPoller<'a>>,
    my_data: i64,
}

impl NullIoChannelData<'_> {
    fn new(my_data: i64) -> Self {
        let poller = PollerBuilder::new()
            .with_interval(1000)
            .with_context(Box::new(NullIoPoller {
                iovs: RefCell::new(Vec::new()),
            }))
            .with_poll_fn(|ctx| {
                let ready: Vec<_> = ctx.iovs.borrow_mut().drain(..).collect();
                let cnt = ready.len();
                if cnt > 0 {
                    dbgln!(NullIo, "poller"; ">>>> poll: cnt={}", cnt);
                }
                ready.iter().for_each(|io: &BdevIo<_>| io.ok());
                cnt as i32
            })
            .build();

        let res = Self {
            poller,
            my_data,
        };
        dbgln!(NullIoChannelData, res.dbg(); "new");
        res
    }

    fn dbg(&self) -> String {
        format!("NIO.Dat[d '{}']", self.my_data)
    }
}

impl Drop for NullIoChannelData<'_> {
    fn drop(&mut self) {
        dbgln!(NullIoChannelData, self.dbg(); "drop");
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
        dbgln!(NullIoDevice, self.dbg(); "drop");
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

    /// TODO
    fn destruct(self: Box<Self>) {
        dbgln!(NullIoDevice, self.dbg(); "destruct");
        self.io_device_unregister();
    }

    fn submit_request(
        &self,
        io_chan: IoChannel<Self::ChannelData>,
        bio: BdevIo<NullIoDevice<'a>>,
    ) {
        let chan_data = io_chan.channel_data();

        dbgln!(NullIoDevice, self.dbg();
            "submit req: my cd {}", chan_data.dbg());

        match bio.io_type() {
            IoType::Read | IoType::Write => {
                dbgln!(NullIoDevice, self.dbg(); ">>>> push BIO");
                chan_data.poller.context().iovs.borrow_mut().push(bio)
            }
            _ => bio.fail(),
        };
    }

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool {
        dbgln!(NullIoDevice, self.dbg(); "io_type_supported?: {:?}", io_type);
        matches!(io_type, IoType::Read | IoType::Write)
    }
}

/// TODO
impl<'a> NullIoDevice<'a> {
    fn create(name: &str) {
        let bm = BdevModule::find_by_name(NULL_MODULE_NAME).unwrap();

        let io_dev = Box::new(NullIoDevice {
            name: String::from(name),
            smth: 789,
            next_chan_id: RefCell::new(10),
            _a: Default::default(),
        });

        let bdev = BdevBuilder::new()
            .with_context(io_dev.as_ref())
            .with_module(&bm)
            .with_name(name)
            .with_product_name("nullblk thing")
            .with_block_length(1 << 12)
            .with_block_count(1 << 20)
            .with_required_alignment(12)
            .build();

        io_dev.io_device_register(name);
        bdev.bdev_register();

        dbgln!(NullIoDevice, ""; "created '{}'", name);
    }

    fn dbg(&self) -> String {
        format!(
            "NIO.Dev[id '{:p}' name '{}' smth '{}']",
            self.get_io_device_id(),
            self.name,
            self.smth
        )
    }
}
