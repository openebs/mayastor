use std::{
    ffi::CString,
    marker::PhantomData,
    os::raw::c_void,
    ptr::{null_mut, NonNull},
};

use crate::{
    ffihelper::{AsStr, IntoCString},
    BdevIo,
    BdevModule,
    IoChannel,
    IoDevice,
    IoType,
    Uuid,
};

use spdk_sys::{
    spdk_bdev,
    spdk_bdev_fn_table,
    spdk_bdev_io,
    spdk_bdev_io_type,
    spdk_bdev_module_release_bdev,
    spdk_bdev_register,
    spdk_get_io_channel,
    spdk_io_channel,
};

/// Wrapper for SPDK `spdk_bdev` structure and related API.
pub struct Bdev<BdevData>
where
    BdevData: BdevOps,
{
    inner: NonNull<spdk_bdev>,
    _data: PhantomData<BdevData>,
}

impl<BdevData> Bdev<BdevData>
where
    BdevData: BdevOps,
{
    /// Consumes the Bdev and registers it in SPDK.
    pub fn bdev_register(self) {
        // TODO: error check.
        unsafe { spdk_bdev_register(self.as_ptr()) };
    }

    /// Returns Bdev name.
    pub fn name(&self) -> &str {
        self.as_ref().name.as_str()
    }

    /// Returns true if this Bdev is claimed by some other component.
    pub fn is_claimed(&self) -> bool {
        !self.as_ref().internal.claim_module.is_null()
    }

    /// Returns true if this Bdev is claimed by the given Bdev module.
    pub fn is_claimed_by_module(&self, module: &BdevModule) -> bool {
        self.as_ref().internal.claim_module == module.as_ptr()
    }

    /// Releases a write claim on a block device.
    pub fn release_claim(&self) {
        if self.is_claimed() {
            unsafe {
                spdk_bdev_module_release_bdev(self.as_ptr());
            }
        }
    }

    /// Returns a reference to a data object associated with this Bdev.
    pub fn data<'a>(&self) -> &'a BdevData {
        &self.container().data
    }

    /// Returns a reference to a container for with Bdev.
    fn container<'a>(&self) -> &'a Container<BdevData> {
        Container::<BdevData>::from_ptr(self.as_ref().ctxt)
    }

    /// Creates a new `Bdev` wrapper from an SPDK structure pointer.
    pub(crate) fn from_ptr(ptr: *mut spdk_bdev) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
            _data: Default::default(),
        }
    }

    /// `from_ptr()` for legacy use.
    /// TODO: remove me.
    pub fn legacy_from_ptr(ptr: *mut spdk_bdev) -> Self {
        Self::from_ptr(ptr)
    }

    /// Returns a pointer to the underlying `spdk_bdev` structure.
    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev {
        self.inner.as_ptr()
    }

    /// Returns a reference to the underlying `spdk_bdev` structure.
    pub(crate) fn as_ref(&self) -> &spdk_bdev {
        unsafe { self.inner.as_ref() }
    }
}

/// TODO
pub trait BdevOps {
    /// Data type of Bdev I/O channel data.
    type ChannelData;

    /// TODO
    type BdevData: BdevOps;

    /// TODO
    type IoDev: IoDevice;

    /// TODO
    fn destruct(&self);

    /// TODO
    fn submit_request(
        &self,
        chan: IoChannel<Self::ChannelData>,
        bio: BdevIo<Self::BdevData>,
    );

    /// TODO
    fn io_type_supported(&self, io_type: IoType) -> bool;

    /// TODO
    fn get_io_device(&self) -> &Self::IoDev;
}

/// TODO
impl BdevOps for () {
    type ChannelData = ();
    type BdevData = ();
    type IoDev = ();

    fn destruct(&self) {}

    fn submit_request(
        &self,
        _chan: IoChannel<Self::ChannelData>,
        _bio: BdevIo<Self::BdevData>,
    ) {
    }

    fn io_type_supported(&self, _io_type: IoType) -> bool {
        false
    }

    fn get_io_device(&self) -> &Self::IoDev {
        &self
    }
}

/// Container for the data associated with a `Bdev` instance.
/// This container stores the `spdk_bdev` structure itself,
/// its associated function table and user-defined data structure provided upon
/// Bdev creation.
///
/// When SPDK destructs a BDEV, this container is dropped,
/// automatically freeing all the resources allocated during BDEV creation.
struct Container<BdevData>
where
    BdevData: BdevOps,
{
    bdev: spdk_bdev,
    fn_table: spdk_bdev_fn_table,
    data: BdevData,
}

impl<BdevData> Drop for Container<BdevData>
where
    BdevData: BdevOps,
{
    fn drop(&mut self) {
        // Tell the Bdev data object to be cleaned up.
        self.data.destruct();

        // Drop the associated strings.
        unsafe {
            CString::from_raw(self.bdev.name);
            CString::from_raw(self.bdev.product_name);
        }
    }
}

impl<BdevData> Container<BdevData>
where
    BdevData: BdevOps,
{
    /// Creates a new mutable container reference from an SPDK Bdev context
    /// pointer.
    ///
    /// # Safety
    ///
    /// TODO
    fn from_ptr<'a>(ctx: *mut c_void) -> &'a mut Self {
        unsafe { &mut *(ctx as *mut Self) }
    }
}

/// Called by SPDK when a Bdev is being destroyed.
///
/// # Parameters
///
/// * `ctx` - Pointer to a Bdev context, which is a pointer to `Container<_>` in
///   our case.
unsafe extern "C" fn inner_bdev_destruct<BdevData>(ctx: *mut c_void) -> i32
where
    BdevData: BdevOps,
{
    // Dropping the container will drop all the associated resources:
    // the context, names, function table and `spdk_bdev` itself.
    Box::from_raw(ctx as *mut Container<BdevData>);
    0
}

/// TODO
unsafe extern "C" fn inner_bdev_submit_request<BdevData>(
    chan: *mut spdk_io_channel,
    bio: *mut spdk_bdev_io,
) where
    BdevData: BdevOps<BdevData = BdevData>,
{
    let c = IoChannel::<BdevData::ChannelData>::from_ptr(chan);
    let b = BdevIo::<BdevData::BdevData>::from_ptr(bio);
    b.bdev().data().submit_request(c, b);
}

/// Called by SPDK when it needs a new I/O channel for the given Bdev.
/// This function forwards the call to SPDK `spdk_get_io_channel`, which in turn
/// allocates a channel for a I/O device associated with this Bdev.
///
/// # Parameters
///
/// * `ctx` - Pointer to a Bdev context, which is a pointer to `Container<_>` in
///   our case.
unsafe extern "C" fn inner_bdev_get_io_channel<BdevData>(
    ctx: *mut c_void,
) -> *mut spdk_io_channel
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    let c = Container::<BdevData>::from_ptr(ctx);
    let io_dev = c.data.get_io_device();
    spdk_get_io_channel(io_dev.get_io_device_id())
}

/// TODO
unsafe extern "C" fn inner_bdev_get_module_ctx<BdevData>(
    _ctx: *mut c_void,
) -> *mut c_void
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    null_mut::<c_void>()
}

/// Called by SPDK to determine if a particular I/O channel for the given Bdev.
/// This function forwards the call to SPDK `spdk_get_io_channel`, which in turn
/// allocates a channel for a I/O device associated with this Bdev.
///
/// # Parameters
///
/// * `ctx` - Pointer to a Bdev context, which is a pointer to `Container<_>` in
///   our case.
unsafe extern "C" fn inner_bdev_io_type_supported<BdevData>(
    ctx: *mut c_void,
    io_type: spdk_bdev_io_type,
) -> bool
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    let c = Container::<BdevData>::from_ptr(ctx);
    c.data.io_type_supported(IoType::from(io_type))
}

/// Builder for `Bdev` structure.
///
/// # Generic Parameters
///
/// * `'m` - Lifetime of the corresponding `BdevModule` instance.
/// * `BdevData` - Type for the Bdev data structure associated with a `Bdev`.
pub struct BdevBuilder<'m, BdevData>
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    name: Option<CString>,
    product_name: Option<CString>,
    blocklen: Option<u32>,
    blockcnt: Option<u64>,
    required_alignment: Option<u8>,
    uuid: Option<Uuid>,
    module: Option<&'m BdevModule>,
    fn_table: Option<spdk_bdev_fn_table>,
    data: Option<BdevData>,
}

impl<'m, BdevData> Default for BdevBuilder<'m, BdevData>
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'m, BdevData> BdevBuilder<'m, BdevData>
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    /// Creates a new `BdevBuilder` instance.
    pub fn new() -> BdevBuilder<'m, BdevData> {
        BdevBuilder {
            name: None,
            product_name: None,
            required_alignment: None,
            blocklen: None,
            blockcnt: None,
            uuid: None,
            module: None,
            fn_table: None,
            data: None,
        }
    }

    /// Sets the Bdev data object for this Bdev.
    /// This Bdev parameter is manadory.
    pub fn with_data(mut self, ctx: BdevData) -> Self {
        self.fn_table = Some(spdk_bdev_fn_table {
            destruct: Some(inner_bdev_destruct::<BdevData>),
            submit_request: Some(inner_bdev_submit_request::<BdevData>),
            io_type_supported: Some(inner_bdev_io_type_supported::<BdevData>),
            get_io_channel: Some(inner_bdev_get_io_channel::<BdevData>),
            dump_info_json: None,
            write_config_json: None,
            get_spin_time: None,
            get_module_ctx: Some(inner_bdev_get_module_ctx::<BdevData>),
        });
        self.data = Some(ctx);
        self
    }

    /// Sets the `BdevModule` instance for this Bdev.
    /// This Bdev parameter is manadory.
    pub fn with_module(mut self, m: &'m BdevModule) -> Self {
        self.module = Some(m);
        self
    }

    /// Sets a name for this Bdev.
    /// This Bdev parameter is manadory.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(String::from(name).into_cstring());
        self
    }

    /// Sets a product name for this Bdev.
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
    pub fn build(self) -> Bdev<BdevData> {
        // Create a new container for the Bdev data, `spdk_bdev` itself and
        // the associated function table.
        // The context (pointer to the Container<> itself in our case) and
        // the function table are filled later, after cont is alloced.
        let cont = Box::new(Container {
            bdev: spdk_bdev {
                ctxt: null_mut::<c_void>(),
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
                fn_table: null_mut::<spdk_bdev_fn_table>(),
                internal: Default::default(),
            },
            fn_table: self.fn_table.expect("Bdev function table must be set"),
            data: self.data.expect("Bdev data must be set"),
        });

        // Consume the container and store pointer to it within the `spdk_bdev`
        // context field. It will be converted back into Box<> and
        // dropped later upon Bdev destruction.
        let p = Box::into_raw(cont);

        // Fill the context field (our Container<>) and the function table,
        // and construct a `Bdev` wrapper.
        unsafe {
            (*p).bdev.fn_table = &(*p).fn_table;
            (*p).bdev.ctxt = p as *mut c_void;
            Bdev::from_ptr(&mut (*p).bdev)
        }
    }
}
