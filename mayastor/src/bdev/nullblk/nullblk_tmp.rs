#![allow(dead_code)]
#![allow(unused_imports)]

use std::{cell::RefCell, ptr::NonNull, rc::Rc, sync::Arc};

use libc::c_void;
use once_cell::sync::OnceCell;

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_fn_table,
    spdk_bdev_io,
    spdk_bdev_io_type,
    spdk_bdev_register,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_io_device_unregister,
};

use crate::core::{
    poller::{self, Poller},
    Bdev,
    Bio,
    Cores,
    IoDevice,
    IoType,
};

use spdk::{
    bdev::BdevBuilder,
    BdevModule,
    BdevModuleBuild,
    BdevModuleError,
    BdevModuleFini,
    BdevModuleInit,
};

//
// TODO: BDev NULL module.
//
struct BdevNullModule {}

const NULL_BLK_MODULE_NAME: &str = "NullBlk";

// unsafe impl Send for BdevNullModule {}
// unsafe impl Sync for BdevNullModule {}

impl BdevModuleInit for BdevNullModule {
    fn module_init() -> i32 {
        info!("init NullBlk");
        NullBlkBdev::create_new("nullblk0");
        0
    }
}

impl BdevModuleFini for BdevNullModule {
    fn module_fini() {
        info!("fini NullBlk");
    }
}

impl BdevModuleBuild for BdevNullModule {}

pub fn register() {
    BdevNullModule::builder(NULL_BLK_MODULE_NAME)
        .with_module_init()
        .with_module_fini()
        .register();
}

//
// TODO: NullBio
//
#[derive(Debug)]
pub struct NullBio(Bio);

//
// TODO: Channel
//
pub struct Channel(NonNull<spdk_io_channel>);

impl Channel {
    pub fn get_ctx(&mut self) -> &mut NullIoChannel {
        // get the offset to our ctx from the channel
        let offset = unsafe {
            use std::mem::size_of;
            (self.0.as_ptr() as *mut u8).add(size_of::<spdk_io_channel>())
                as *mut c_void
        };

        let wrapper = unsafe { &mut *(offset as *mut ChannelWrapper) };

        let l = wrapper.inner.unwrap();
        unsafe { &mut *(l) }
    }
}

//
// safe impls for bdev fn table
//
pub trait BdevFnTable {
    fn submit_request(io: NullBio, ch: Channel);
    fn io_type_supported(io: IoType) -> bool;
}

pub static NULLBLK_FN_TABLE: OnceCell<Arc<spdk_bdev_fn_table>> =
    OnceCell::new();

/// Wrapper for struct spdk_bdev_fn_table.
trait BdevRawFnTable: BdevFnTable {
    extern "C" fn raw_bdev_destruct(ctx: *mut c_void) -> i32 {
        unsafe {
            // let instance: Box<NullBlk> = Box::from_raw(ctx as *mut NullBlk);
            spdk_io_device_unregister(ctx, None);
            0
        }
    }

    extern "C" fn raw_bdev_submit_request(
        ch: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
    ) {
        unsafe {
            Self::submit_request(
                NullBio(Bio::from(io)),
                Channel(NonNull::new_unchecked(ch)),
            );
        }
    }

    extern "C" fn raw_bdev_io_type_supported(
        _ctx: *mut c_void,
        io_type: spdk_bdev_io_type,
    ) -> bool {
        Self::io_type_supported(IoType::from(io_type))
    }

    extern "C" fn raw_bdev_get_io_channel(
        ctx: *mut c_void,
    ) -> *mut spdk_io_channel {
        unsafe { spdk_get_io_channel(ctx) }
    }

    extern "C" fn raw_bdev_get_module_ctx(_ctx: *mut c_void) -> *mut c_void {
        todo!()
    }

    fn fn_table() -> Arc<spdk_bdev_fn_table> {
        // use box_leak?
        let f = NULLBLK_FN_TABLE.get_or_init(|| {
            Arc::new(spdk_bdev_fn_table {
                destruct: Some(Self::raw_bdev_destruct),
                submit_request: Some(Self::raw_bdev_submit_request),
                io_type_supported: Some(Self::raw_bdev_io_type_supported),
                get_io_channel: Some(Self::raw_bdev_get_io_channel),
                dump_info_json: None,
                write_config_json: None,
                get_spin_time: None,
                // module private data
                get_module_ctx: Some(Self::raw_bdev_get_module_ctx),
            })
        });

        Arc::clone(f)
    }
}

//
// safe fns for IoChannel operations
//
pub trait IoChannelFnTable {
    fn create_io_channel(bdev: &NullBlkBdev, ctx: &mut ChannelWrapper) -> i32;
    fn destroy_io_channel(bdev: &NullBlkBdev, ctx: Box<NullIoChannel>);
}

trait IoChannelRawFnTable: IoChannelFnTable {
    unsafe extern "C" fn create(device: *mut c_void, ctx: *mut c_void) -> i32 {
        let null_bdev: &NullBlkBdev = &*(device as *const NullBlkBdev);
        let ctx: &mut ChannelWrapper = &mut *(ctx as *mut ChannelWrapper);
        Self::create_io_channel(null_bdev, ctx)
    }

    unsafe extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        let null_bdev: &NullBlkBdev = &*(device as *const NullBlkBdev);
        let ctx: &mut ChannelWrapper = &mut *(ctx as *mut ChannelWrapper);

        if let Some(inner) = ctx.inner.take() {
            let inner = Box::from_raw(inner);
            Self::destroy_io_channel(null_bdev, inner)
        }
    }
}

//
// TODO: ChannelWrapper
//
#[derive(Debug)]
pub struct ChannelWrapper<'a> {
    inner: Option<*mut NullIoChannel<'a>>,
}

//
// TODO: NullIoChannel
//
#[repr(C)]
#[derive(Debug)]
pub struct NullIoChannel<'a> {
    iovs: Rc<RefCell<Vec<NullBio>>>,
    poller: Option<Poller<'a>>,
}

impl NullIoChannel<'_> {
    fn new() -> Self {
        Self::default()
    }
}

impl Default for NullIoChannel<'_> {
    fn default() -> Self {
        Self {
            iovs: Rc::new(RefCell::new(Vec::new())),
            poller: None,
        }
    }
}

//
// TODO: Null BDEV
//
#[derive(Debug)]
pub struct NullBlkBdev {
    name: String,
    bdev: Bdev,
}

impl NullBlkBdev {
    pub fn create_new(name: &str) {
        let mut bdev = Bdev::default();
        bdev.set_block_len(1 << 12);
        bdev.set_block_count(1 << 20);
        bdev.set_alignment(12);
        bdev.set_uuid(uuid::Uuid::new_v4());
        bdev.set_name(String::from(name));
        bdev.set_product(String::from("nullblk thing"));

        bdev.set_fntable(Self::fn_table());

        // Box is used to ensure the address remains valid
        let self_box = Box::new(Self {
            name: String::from(name),
            bdev,
        });

        // get a ptr to our boxed bdev, having it boxed means the ptr value
        // remains stable we will store a reference to ourselves in the
        // bdev ctxt pointer such that we can get our context when we
        // need to. Not sure how to do this any better at this point
        let bdev_ptr: *mut spdk_bdev = self_box.bdev.as_ptr();

        // "forget" about this memory and return a raw pointer.
        let self_ptr: *mut NullBlkBdev = Box::into_raw(self_box);

        let bdev_mod = BdevModule::find_by_name(NULL_BLK_MODULE_NAME)
            .expect("Nullblk module not found");

        unsafe {
            (*bdev_ptr).ctxt = self_ptr.cast();
            (*bdev_ptr).module = bdev_mod.as_ptr();
        };

        // >> spdk_io_device_register()
        let io_device = IoDevice::new::<ChannelWrapper>(
            // pass the ctxt raw pointer to our channel creation routines
            // from there we easily get access to everything we need
            NonNull::new(self_ptr.cast()).unwrap(),
            name,
            Some(Self::create),
            Some(Self::destroy),
        );

        // register the bdev with SPDK, its "online" now
        unsafe { spdk_bdev_register(bdev_ptr) };

        //dbg!(&io_device);

        // FIXME
        std::mem::forget(io_device);
    }
}

impl BdevFnTable for NullBlkBdev {
    // called per pthread/core
    fn submit_request(bdev_io: NullBio, mut ch: Channel) {
        info!(core = ?Cores::current(), "submit req");

        let ctx = ch.get_ctx();
        match bdev_io.0.io_type() {
            IoType::Read | IoType::Write => ctx.iovs.borrow_mut().push(bdev_io),
            _ => bdev_io.0.fail(),
        }
    }

    fn io_type_supported(io_type: IoType) -> bool {
        info!(core = ?Cores::current(), "io_type_supported? {:?}", io_type);

        matches!(io_type, IoType::Read | IoType::Write)
    }
}

impl IoChannelFnTable for NullBlkBdev {
    fn create_io_channel(bdev: &NullBlkBdev, ctx: &mut ChannelWrapper) -> i32 {
        info!(name = ?bdev.name, core = ?Cores::current(), "creating -NULL- IOraw_ channel");

        match ctx.inner {
            None => {
                let mut channel = Box::new(NullIoChannel::new());
                let iovs = Rc::clone(&channel.iovs);

                let poller = poller::Builder::new()
                    .with_interval(1000)
                    .with_poll_fn(move || {
                        let ready: Vec<_> =
                            iovs.borrow_mut().drain(..).collect();
                        let cnt = ready.len();
                        if cnt > 0 {
                            info!(core = ?Cores::current(), "-- poll: cnt={}", cnt);
                        }
                        ready.iter().for_each(|io| io.0.ok());
                        cnt as i32
                    })
                    .build();

                channel.poller = Some(poller);
                ctx.inner = Some(Box::into_raw(channel));
            }

            Some(_) => panic!("double channel create"),
        }
        0
    }

    fn destroy_io_channel(bdev: &NullBlkBdev, mut ctx: Box<NullIoChannel>) {
        info!(name = ?bdev.name, core = ?Cores::current(), "destroy -NULL- IO channel");

        ctx.poller.take();
        ctx.iovs.borrow_mut().iter_mut().for_each(|b| b.0.fail());
    }
}

// mark this trait as implemented the default will call into the safe trait
impl BdevRawFnTable for NullBlkBdev {}
impl IoChannelRawFnTable for NullBlkBdev {}
