use std::{
    ffi::CString,
    marker::PhantomData,
    os::raw::c_void,
    ptr::{null_mut, NonNull},
};

use nix::errno::Errno;

use crate::{
    ffihelper::{
        errno_result_from_i32,
        AsStr,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
    libspdk::{
        spdk_bdev,
        spdk_bdev_alias_add,
        spdk_bdev_alias_del,
        spdk_bdev_fn_table,
        spdk_bdev_get_aliases,
        spdk_bdev_get_buf_align,
        spdk_bdev_get_by_name,
        spdk_bdev_io,
        spdk_bdev_io_type,
        spdk_bdev_io_type_supported,
        spdk_bdev_module_release_bdev,
        spdk_bdev_register,
        spdk_bdev_unregister,
        spdk_get_io_channel,
        spdk_io_channel,
        spdk_json_write_ctx,
    },
    BdevIo,
    BdevModule,
    IoChannel,
    IoDevice,
    IoType,
    JsonWriteContext,
    Uuid,
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
    /// Registers this Bdev in SPDK.
    /// TODO: comment
    /// TODO: Error / result
    pub fn register_bdev(&mut self) -> ErrnoResult<()> {
        let errno = unsafe { spdk_bdev_register(self.as_ptr()) };
        errno_result_from_i32((), errno)
    }

    /// TODO
    pub fn unregister_bdev(&mut self) {
        unsafe {
            spdk_bdev_unregister(self.as_ptr(), None, null_mut::<c_void>());
        }
    }

    /// Returns a Bdev module for this Bdev.
    pub fn module(&self) -> BdevModule {
        BdevModule::from_ptr(self.as_ref().module)
    }

    /// Returns the name of the module for thos Bdev.
    pub fn module_name(&self) -> &str {
        unsafe { (*self.as_ref().module).name.as_str() }
    }

    /// TODO
    /// ... lookup a bdev by its name
    pub fn lookup_by_name(name: &str) -> Option<Self> {
        let name = String::from(name).into_cstring();
        let bdev = unsafe { spdk_bdev_get_by_name(name.as_ptr()) };
        if bdev.is_null() {
            None
        } else {
            Some(Self::from_ptr(bdev))
        }
    }

    /// Returns by a Bdev module who has claimed this Bdev.
    pub fn claimed_by(&self) -> Option<BdevModule> {
        let ptr = self.as_ref().internal.claim_module;
        if ptr.is_null() {
            None
        } else {
            Some(BdevModule::from_ptr(ptr))
        }
    }

    /// Returns Bdev name.
    pub fn name(&self) -> &str {
        self.as_ref().name.as_str()
    }
    /// Returns the configured product name.
    pub fn product_name(&self) -> &str {
        self.as_ref().product_name.as_str()
    }

    /// Returns Bdev's UUID.
    pub fn uuid(&self) -> Uuid {
        Uuid::new(&self.as_ref().uuid)
    }

    /// Sets Bdev's UUID.
    pub unsafe fn set_uuid(&mut self, uuid: Uuid) {
        self.as_mut().uuid = uuid.into_raw();
    }

    /// TODO
    /// Set a list of aliases on the bdev, used to find the bdev later
    pub fn add_aliases(&mut self, alias: &[String]) -> bool {
        alias
            .iter()
            .filter(|a| -> bool { !self.add_alias(a) })
            .count()
            == 0
    }

    /// TODO
    /// Set an alias on the bdev, this alias can be used to find the bdev later.
    /// If the alias is already present we return true
    pub fn add_alias(&mut self, alias: &str) -> bool {
        let alias = alias.into_cstring();
        let ret = unsafe { spdk_bdev_alias_add(self.as_ptr(), alias.as_ptr()) }
            .to_result(Errno::from_i32);

        matches!(ret, Err(Errno::EEXIST) | Ok(_))
    }

    /// Removes the given alias from the Bdev.
    pub fn remove_alias(&mut self, alias: &str) {
        unsafe {
            spdk_bdev_alias_del(self.as_ptr(), alias.into_cstring().as_ptr())
        };
    }

    /// Returns a list of Bdev aliases.
    pub fn aliases(&self) -> Vec<String> {
        let mut aliases = Vec::new();
        let head = unsafe { &*spdk_bdev_get_aliases(self.as_ptr()) };
        let mut ent_ptr = head.tqh_first;
        while !ent_ptr.is_null() {
            let ent = unsafe { &*ent_ptr };
            let alias = ent.alias.name.as_str();
            aliases.push(alias.to_string());
            ent_ptr = ent.tailq.tqe_next;
        }
        aliases
    }

    /// Returns the block size of the underlying device.
    pub fn block_len(&self) -> u32 {
        self.as_ref().blocklen
    }

    /// Sets the block size of the underlying device.
    pub unsafe fn set_block_len(&mut self, len: u32) {
        self.as_mut().blocklen = len;
    }

    /// Returns number of blocks for this device.
    pub fn num_blocks(&self) -> u64 {
        self.as_ref().blockcnt
    }

    /// Sets number of blocks for this device.
    pub unsafe fn set_num_blocks(&mut self, count: u64) {
        self.as_mut().blockcnt = count
    }

    /// Returns the Bdev size in bytes.
    pub fn size_in_bytes(&self) -> u64 {
        self.num_blocks() * (self.block_len() as u64)
    }

    /// Returns the alignment of the Bdev.
    pub fn alignment(&self) -> u64 {
        unsafe { spdk_bdev_get_buf_align(self.as_ptr()) }
    }

    /// Returns the required alignment of the Bdev.
    pub fn required_alignment(&self) -> u8 {
        self.as_ref().required_alignment
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

    /// Determines whenever the Bdev supports the requested I/O type.
    pub fn io_type_supported(&self, io_type: IoType) -> bool {
        unsafe { spdk_bdev_io_type_supported(self.as_ptr(), io_type.into()) }
    }

    /// Returns a reference to a data object associated with this Bdev.
    pub fn data<'a>(&self) -> &'a BdevData {
        &self.container().data
    }

    /// Returns a mutable reference to a data object associated with this Bdev.
    pub fn data_mut<'a>(&mut self) -> &'a mut BdevData {
        &mut self.container_mut().data
    }

    /// Returns a reference to a container for with Bdev.
    fn container<'a>(&self) -> &'a Container<BdevData> {
        Container::<BdevData>::from_ptr(self.as_ref().ctxt)
    }

    /// Returns a reference to a container for with Bdev.
    fn container_mut<'a>(&mut self) -> &'a mut Container<BdevData> {
        Container::<BdevData>::from_ptr(self.as_ref().ctxt)
    }

    /// Returns a pointer to the underlying `spdk_bdev` structure.
    pub(crate) fn as_ptr(&self) -> *mut spdk_bdev {
        self.inner.as_ptr()
    }

    /// Returns a reference to the underlying `spdk_bdev` structure.
    pub(crate) fn as_ref(&self) -> &spdk_bdev {
        unsafe { self.inner.as_ref() }
    }

    /// Returns a mutable reference to the underlying `spdk_bdev` structure.
    pub(crate) fn as_mut(&mut self) -> &mut spdk_bdev {
        unsafe { self.inner.as_mut() }
    }

    /// Public version of `as_ptr()`.
    /// TODO: remove me.
    pub fn legacy_as_ptr(&self) -> *mut spdk_bdev {
        self.inner.as_ptr()
    }

    /// Creates a new `Bdev` wrapper from an SPDK structure pointer.
    pub(crate) fn from_ptr(ptr: *mut spdk_bdev) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
            _data: Default::default(),
        }
    }

    /// Public version of `from_ptr()`.
    /// TODO: remove me.
    pub fn legacy_from_ptr(ptr: *mut spdk_bdev) -> Self {
        Self::from_ptr(ptr)
    }
}

impl<BdevData> Clone for Bdev<BdevData>
where
    BdevData: BdevOps,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            _data: Default::default(),
        }
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
    fn destruct(&mut self);

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

    /// TODO
    fn dump_info_json(&self, _w: JsonWriteContext) {}
}

/// TODO
impl BdevOps for () {
    type ChannelData = ();
    type BdevData = ();
    type IoDev = ();

    fn destruct(&mut self) {}

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
#[repr(C)]
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

/// TODO
///
/// # Parameters
///
/// * `ctx` - Pointer to a Bdev context, which is a pointer to `Container<_>` in
///   our case.
unsafe extern "C" fn inner_dump_info_json<BdevData>(
    ctx: *mut c_void,
    w: *mut spdk_json_write_ctx,
) -> i32
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    let c = Container::<BdevData>::from_ptr(ctx);
    c.data.dump_info_json(JsonWriteContext::from_ptr(w));

    // TODO: error processing?
    0
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
    module: &'m BdevModule,
    fn_table: Option<spdk_bdev_fn_table>,
    data: Option<BdevData>,
}

impl<'m, BdevData> BdevBuilder<'m, BdevData>
where
    BdevData: BdevOps<BdevData = BdevData>,
{
    /// Creates a new `BdevBuilder` instance.
    pub(crate) fn new(bdev_mod: &'m BdevModule) -> BdevBuilder<'m, BdevData> {
        BdevBuilder {
            name: None,
            product_name: None,
            required_alignment: None,
            blocklen: None,
            blockcnt: None,
            uuid: None,
            module: bdev_mod,
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
            dump_info_json: Some(inner_dump_info_json::<BdevData>),
            write_config_json: None,
            get_spin_time: None,
            get_module_ctx: Some(inner_bdev_get_module_ctx::<BdevData>),
        });
        self.data = Some(ctx);
        self
    }

    /// TODO
    pub fn with_uuid(mut self, u: Uuid) -> Self {
        self.uuid = Some(u);
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
                module: self.module.as_ptr(),
                fn_table: null_mut::<spdk_bdev_fn_table>(),
                internal: Default::default(),
            },
            fn_table: self.fn_table.expect("Bdev function table must be set"),
            data: self.data.expect("Bdev data must be set"),
        });

        // Consume the container and store a pointer to it within the
        // `spdk_bdev` context field. It will be converted back into
        // Box<> and dropped later upon Bdev destruction.
        let pcont = Box::into_raw(cont);

        // Fill the context field (our Container<>) and the function table,
        // and construct a `Bdev` wrapper.
        unsafe {
            (*pcont).bdev.fn_table = &(*pcont).fn_table;
            (*pcont).bdev.ctxt = pcont as *mut c_void;
            Bdev::from_ptr(&mut (*pcont).bdev)
        }
    }
}
