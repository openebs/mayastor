use std::{ffi::CString, os::raw::c_void, ptr::NonNull};

use spdk_sys::{
    spdk_bdev,
    spdk_bdev___bdev_internal_fields,
    spdk_bdev_fn_table,
    spdk_bdev_io,
    spdk_bdev_io_type,
    spdk_bdev_register,
    spdk_get_io_channel,
    spdk_io_channel,
};

use crate::{
    cpu_cores::Cores,
    ffihelper::IntoCString,
    BdevIo,
    BdevModule,
    IoChannel,
    IoType,
    Uuid,
};
use std::marker::PhantomData;

/// Wrapper for SPDK `spdk_bdev` structure and related API.
pub struct Bdev<BdevContext: BdevOps> {
    inner: NonNull<spdk_bdev>,
    _ctx: PhantomData<BdevContext>,
}

impl<BdevContext: BdevOps> Bdev<BdevContext> {
    /// Consumes the Bdev and registers it in SPDK.
    pub fn bdev_register(self) {
        unsafe { spdk_bdev_register(self.inner.as_ptr()) };
    }

    /// Returns a context associated with this Bdev.
    pub fn context<'a>(&self) -> &'a BdevContext {
        unsafe { &*(self.inner.as_ref().ctxt as *mut BdevContext) }
    }

    /// Creates a new `Bdev` instance from SPDK structure pointer.
    pub(crate) fn new(ptr: *mut spdk_bdev) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
            _ctx: Default::default(),
        }
    }
}

/// TODO
pub trait BdevOps: Sized {
    /// Data type of Bdev I/O channel data.
    type ChannelData: Sized;

    /// TODO
    fn destruct(self: Box<Self>);

    /// TODO
    fn submit_request(
        &self,
        chan: IoChannel<Self::ChannelData>,
        bio: BdevIo<Self>,
    );

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool;

    /// TODO
    fn dbg(&self) -> String {
        format!("BdevOps[id '{:p}']", self as *const Self as *const c_void)
    }
}

/// TODO
unsafe extern "C" fn inner_bdev_destruct<BdevContext>(
    raw_ctx: *mut c_void,
) -> i32
where
    BdevContext: BdevOps,
{
    let ctx = Box::from_raw(raw_ctx as *mut BdevContext);
    dbgln!(BdevOps, ctx.dbg(); "inner_destruct");
    ctx.destruct();

    0
}

/// TODO
unsafe extern "C" fn inner_bdev_submit_request<BdevContext>(
    raw_chan: *mut spdk_io_channel,
    raw_bio: *mut spdk_bdev_io,
) where
    BdevContext: BdevOps,
{
    let chan = IoChannel::<BdevContext::ChannelData>::new(raw_chan);
    let bio = BdevIo::<BdevContext>::new(raw_bio);
    bio.bdev().context().submit_request(chan, bio);
}

/// TODO
unsafe extern "C" fn inner_bdev_get_io_channel<BdevContext>(
    raw_ctx: *mut c_void,
) -> *mut spdk_io_channel
where
    BdevContext: BdevOps,
{
    spdk_get_io_channel(raw_ctx)
}

/// TODO
unsafe extern "C" fn inner_bdev_get_module_ctx<BdevContext>(
    raw_ctx: *mut c_void,
) -> *mut c_void
where
    BdevContext: BdevOps,
{
    let ctx = &*(raw_ctx as *mut BdevContext);
    dbgln!(BdevOps, ctx.dbg(); "inner_get_module_ctx");
    std::ptr::null_mut::<c_void>()
}

/// TODO
unsafe extern "C" fn inner_bdev_io_type_supported<BdevContext>(
    raw_ctx: *mut c_void,
    io_type: spdk_bdev_io_type,
) -> bool
where
    BdevContext: BdevOps,
{
    (&*(raw_ctx as *mut BdevContext)).io_type_supported(IoType::from(io_type))
}

/// Builder for `Bdev` structure.
///
/// # Generic Parameters
///
/// * `'c` - Lifetime of the associated `BdevContext` instance.
/// * `'m` - Lifetime of the corresponding `BdevModule` instance.
/// * `BdevContext` - Type for the Bdev context associated with the `Bdev`.
pub struct BdevBuilder<'c, 'm, BdevContext>
where
    BdevContext: BdevOps,
{
    ctxt: Option<&'c BdevContext>,
    name: Option<CString>,
    // aliases: spdk_bdev_spdk_bdev_aliases_list,
    product_name: Option<CString>, // *mut ::std::os::raw::c_char,
    // write_cache: ::std::os::raw::c_int,
    blocklen: Option<u32>,
    //phys_blocklen: Option<u32>,
    blockcnt: Option<u64>,
    // write_unit_size: u32,
    // acwu: u16,
    required_alignment: Option<u8>,
    // split_on_optimal_io_boundary: bool,
    // optimal_io_boundary: u32,
    // max_segment_size: u32,
    // max_num_segments: u32,
    // max_unmap: u32,
    // max_unmap_segments: u32,
    // max_write_zeroes: u32,
    uuid: Option<Uuid>,
    // md_len: u32,
    // md_interleave: bool,
    // dif_type: spdk_dif_type,
    // dif_is_head_of_md: bool,
    // dif_check_flags: u32,
    // zoned: bool,
    // zone_size: u64,
    // max_zone_append_size: u32,
    // max_open_zones: u32,
    // max_active_zones: u32,
    // optimal_open_zones: u32,
    // media_events: bool,
    module: Option<&'m BdevModule>,
    fn_table: Option<Box<spdk_bdev_fn_table>>,
}

impl<'c, 'm, BdevContext> Default for BdevBuilder<'c, 'm, BdevContext>
where
    BdevContext: BdevOps,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'c, 'm, BdevContext> BdevBuilder<'c, 'm, BdevContext>
where
    BdevContext: BdevOps,
{
    /// Creates a new `BdevBuilder` instance.
    pub fn new() -> BdevBuilder<'c, 'm, BdevContext> {
        BdevBuilder {
            ctxt: None,
            name: None,
            product_name: None,
            required_alignment: None,
            blocklen: None,
            blockcnt: None,
            uuid: None,
            module: None,
            fn_table: None,
        }
    }

    /// Sets the context instance for this Bdev.
    /// This Bdev parameter is manadory.
    pub fn with_context(mut self, ctx: &'c BdevContext) -> Self {
        let fn_tab = Box::new(spdk_bdev_fn_table {
            destruct: Some(inner_bdev_destruct::<BdevContext>),
            submit_request: Some(inner_bdev_submit_request::<BdevContext>),
            io_type_supported: Some(
                inner_bdev_io_type_supported::<BdevContext>,
            ),
            get_io_channel: Some(inner_bdev_get_io_channel::<BdevContext>),
            dump_info_json: None,
            write_config_json: None,
            get_spin_time: None,
            get_module_ctx: Some(inner_bdev_get_module_ctx::<BdevContext>),
        });

        self.ctxt = Some(ctx);
        self.fn_table = Some(fn_tab);
        self
    }

    /// Sets the `BdevModule` instance for this Bdev.
    /// This Bdev parameter is manadory.
    pub fn with_module(mut self, m: &'m BdevModule) -> Self {
        self.module = Some(m);
        self
    }

    /// Sets Bdev name.
    /// This Bdev parameter is manadory.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(String::from(name).into_cstring());
        self
    }

    /// Sets Bdev product name.
    /// This Bdev parameter is manadory.
    pub fn with_product_name(mut self, prod_name: &str) -> Self {
        self.product_name = Some(String::from(prod_name).into_cstring());
        self
    }

    /// Sets Bdev block length.
    /// This Bdev parameter is manadory.
    pub fn with_block_length(mut self, val: u32) -> Self {
        self.blocklen = Some(val);
        self
    }

    /// Sets Bdev block count.
    /// This Bdev parameter is manadory.
    pub fn with_block_count(mut self, val: u64) -> Self {
        self.blockcnt = Some(val);
        self
    }

    /// Sets Bdev block required alignment.
    /// This Bdev parameter is manadory.
    pub fn with_required_alignment(mut self, val: u8) -> Self {
        self.required_alignment = Some(val);
        self
    }

    /// Consumes a `BdevBuilder` instance and produces a new `Bdev` instance.
    pub fn build(self) -> Bdev<BdevContext> {
        let ctxt = self.ctxt.expect("Bdev context must be set");
        let ctxt = &*ctxt as &BdevContext as *const BdevContext;

        let inner = Box::new(spdk_bdev {
            ctxt: ctxt as *mut c_void,
            name: self.name.expect("Bdev name must be set").into_raw(),
            aliases: Default::default(),
            product_name: self
                .product_name
                .expect("Bdev product name must be set")
                .into_raw(),
            write_cache: Default::default(),
            blocklen: self.blocklen.expect("Bdeb block length must be set"),
            phys_blocklen: Default::default(),
            blockcnt: self.blockcnt.expect("Bdeb block count must be set"),
            write_unit_size: Default::default(),
            acwu: Default::default(),
            required_alignment: self
                .required_alignment
                .expect("Bdev required alignment must be set"),
            split_on_optimal_io_boundary: Default::default(),
            optimal_io_boundary: Default::default(),
            max_segment_size: Default::default(),
            max_num_segments: Default::default(),
            max_unmap: Default::default(),
            max_unmap_segments: Default::default(),
            max_write_zeroes: Default::default(),
            uuid: self.uuid.unwrap_or_else(Uuid::generate).into_raw(),
            md_len: Default::default(),
            md_interleave: Default::default(),
            dif_type: Default::default(),
            dif_is_head_of_md: Default::default(),
            dif_check_flags: Default::default(),
            zoned: Default::default(),
            zone_size: Default::default(),
            max_zone_append_size: Default::default(),
            max_open_zones: Default::default(),
            max_active_zones: Default::default(),
            optimal_open_zones: Default::default(),
            media_events: Default::default(),
            module: self.module.expect("Bdev module must be set").as_ptr(),
            fn_table: Box::into_raw(
                self.fn_table.expect("Bdev function table must be set"),
            ),
            internal: spdk_bdev___bdev_internal_fields::default(),
        });

        Bdev::new(Box::into_raw(inner))
    }
}
