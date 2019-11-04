//! Methods for creating nvmf targets
//!
//! We create a default nvmf target when mayastor starts up. Then for each
//! replica which is to be exported, we create a subsystem in that default
//! target. Each subsystem has one namespace backed by the lvol.

use crate::{
    bdev::Bdev,
    executor::{cb_arg, done_cb},
};
use futures::channel::oneshot;
use spdk_sys::{
    spdk_nvme_transport_id,
    spdk_nvmf_poll_group,
    spdk_nvmf_poll_group_add,
    spdk_nvmf_poll_group_create,
    spdk_nvmf_poll_group_destroy,
    spdk_nvmf_qpair,
    spdk_nvmf_qpair_disconnect,
    spdk_nvmf_subsystem,
    spdk_nvmf_subsystem_add_listener,
    spdk_nvmf_subsystem_add_ns,
    spdk_nvmf_subsystem_create,
    spdk_nvmf_subsystem_destroy,
    spdk_nvmf_subsystem_get_first,
    spdk_nvmf_subsystem_get_next,
    spdk_nvmf_subsystem_get_nqn,
    spdk_nvmf_subsystem_set_allow_any_host,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_subsystem_set_sn,
    spdk_nvmf_subsystem_start,
    spdk_nvmf_subsystem_stop,
    spdk_nvmf_target_opts,
    spdk_nvmf_tgt,
    spdk_nvmf_tgt_accept,
    spdk_nvmf_tgt_add_transport,
    spdk_nvmf_tgt_create,
    spdk_nvmf_tgt_destroy,
    spdk_nvmf_tgt_find_subsystem,
    spdk_nvmf_tgt_listen,
    spdk_nvmf_transport_create,
    spdk_nvmf_transport_opts,
    spdk_nvmf_transport_opts_init,
    spdk_poller,
    spdk_poller_register,
    spdk_poller_unregister,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
    SPDK_NVMF_SUBTYPE_NVME,
    SPDK_NVMF_TRADDR_MAX_LEN,
    SPDK_NVMF_TRSVCID_MAX_LEN,
    NVMF_TGT_NAME_MAX_LENGTH,
};
use std::{
    cell::RefCell,
    ffi::{c_void, CStr, CString},
    fmt,
    os::raw::c_int,
    ptr::{self, copy_nonoverlapping},
};

thread_local! {
    /// nvmf target provides a scope for creating transports, namespaces etc.
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    static NVMF_TGT: RefCell<Option<Box<Target>>> = RefCell::new(None);
}

/// Given a bdev uuid return a NQN used to connect to the bdev from outside.
fn gen_nqn(id: &str) -> String {
    format!("nqn.2019-05.io.openebs:{}", id)
}

/// Wrapper around spdk nvme subsystem providing rust friendly api.
pub(crate) struct Subsystem {
    inner: *mut spdk_nvmf_subsystem,
    nqn: String,
}

impl Subsystem {
    /// Create a nvme subsystem identified by the id string (used for nqn
    /// creation).
    pub unsafe fn create(
        inner: *mut spdk_nvmf_subsystem,
        trid: *mut spdk_nvme_transport_id,
        nqn: String,
    ) -> Result<Self, String> {
        let sn = CString::new("MayaData Inc.").unwrap();
        if spdk_nvmf_subsystem_set_sn(inner, sn.as_ptr()) != 0 {
            return Err(
                "Failed to set nvmf subsystem's serial number".to_owned()
            );
        }
        let mn = CString::new("MayaStor NVMF controller").unwrap();
        if spdk_nvmf_subsystem_set_mn(inner, mn.as_ptr()) != 0 {
            return Err("Failed to set nvmf subsystem's model name".to_owned());
        }
        spdk_nvmf_subsystem_set_allow_any_host(inner, true);

        // make it listen on target's trid
        if spdk_nvmf_subsystem_add_listener(inner, trid) != 0 {
            return Err("listening on nvmf subsystem failed".to_owned());
        }

        Ok(Self {
            inner,
            nqn,
        })
    }

    /// Convert raw subsystem pointer to subsystem object.
    pub unsafe fn from_ptr(inner: *mut spdk_nvmf_subsystem) -> Self {
        let nqn = CStr::from_ptr(spdk_nvmf_subsystem_get_nqn(inner))
            .to_str()
            .unwrap()
            .to_string();
        Self {
            inner,
            nqn,
        }
    }

    /// Start the subsystem (it cannot be modified afterwards)
    pub async fn start(&mut self) -> Result<(), String> {
        let (sender, receiver) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_subsystem_start(
                self.inner,
                Some(Self::subsystem_start_stop_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(format!(
                "Failed to start nvmf subsystem {} (errno {})",
                self.nqn, errno
            ))
        } else {
            info!("Started nvmf subsystem {}", self.nqn);
            Ok(())
        }
    }

    /// Stop the subsystem (it cannot be modified afterwards)
    pub async fn stop(&mut self) -> Result<(), String> {
        let (sender, receiver) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_subsystem_stop(
                self.inner,
                Some(Self::subsystem_start_stop_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(format!(
                "Failed to stop nvmf subsystem {} (errno {})",
                self.nqn, errno
            ))
        } else {
            info!("Stopped nvmf subsystem {}", self.nqn);
            Ok(())
        }
    }

    /// Add nvme subsystem to the target
    pub fn add_namespace(&mut self, bdev: &Bdev) -> Result<(), String> {
        let ns_id = unsafe {
            spdk_nvmf_subsystem_add_ns(
                self.inner,
                bdev.as_ptr(),
                ptr::null_mut(),
                0,
                ptr::null_mut(),
            )
        };
        if ns_id == 0 {
            Err(format!(
                "Failed to add namespace to nvmf subsystem {}",
                self.nqn
            ))
        } else {
            Ok(())
        }
    }

    pub fn get_nqn(&mut self) -> String {
        unsafe {
            CStr::from_ptr(spdk_nvmf_subsystem_get_nqn(self.inner))
                .to_str()
                .unwrap()
                .to_string()
        }
    }

    /// Add nvme subsystem to the target and return it.
    pub fn destroy(self) {
        unsafe { spdk_nvmf_subsystem_destroy(self.inner) };
    }

    /// Callback for async nvmf subsystem start operation.
    extern "C" fn subsystem_start_stop_cb(
        _ss: *mut spdk_nvmf_subsystem,
        sender_ptr: *mut c_void,
        errno: i32,
    ) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("Receiver is gone");
    }
}

impl fmt::Debug for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.nqn)
    }
}

/// Iterator over nvmf subsystems of a nvmf target
struct SubsystemIter {
    ss_ptr: *mut spdk_nvmf_subsystem,
}

impl SubsystemIter {
    fn new(tgt_ptr: *mut spdk_nvmf_tgt) -> Self {
        Self {
            ss_ptr: unsafe { spdk_nvmf_subsystem_get_first(tgt_ptr) },
        }
    }
}

impl Iterator for SubsystemIter {
    type Item = Subsystem;

    fn next(&mut self) -> Option<Self::Item> {
        let ss_ptr = self.ss_ptr;
        if ss_ptr.is_null() {
            return None;
        }
        unsafe {
            self.ss_ptr = spdk_nvmf_subsystem_get_next(ss_ptr);
            Some(Subsystem::from_ptr(ss_ptr))
        }
    }
}
///
/// Some options can be passed into each target that gets created.
///
/// Currently the options are limited to the name of the target to be created
/// and the max number of subsystems this target supports. We set this number
/// equal to the number of pods that can get scheduled on a node which is, by
/// default 110.
pub(crate) struct TargetOpts {
    inner: spdk_nvmf_target_opts,
}

impl TargetOpts {
    fn new(name: &str, max_subsystems: u32) -> Self {
        let mut opts = spdk_nvmf_target_opts::default();

        unsafe {
            std::ptr::copy_nonoverlapping(
                name.as_ptr() as *const _ as *mut libc::c_void,
                &mut opts.name[0] as *const _ as *mut libc::c_void,
                NVMF_TGT_NAME_MAX_LENGTH as usize,
            );
        }

        // same as max pods by default
        opts.max_subsystems = max_subsystems;

        Self {
            inner: opts,
        }
    }
}

/// Wrapper around spdk nvmf target providing rust friendly api.
/// nvmf target binds listen addresses and nvmf subsystems with namespaces
/// together.
pub(crate) struct Target {
    inner: *mut spdk_nvmf_tgt,
    /// Endpoint where this nvmf target listens for incoming connections.
    trid: spdk_nvme_transport_id,
    opts: spdk_nvmf_transport_opts,
    acceptor_poll_rate: u64,
    acceptor_poller: *mut spdk_poller,
    /// TODO: One poll group per target does not scale
    pg: *mut spdk_nvmf_poll_group,
}

impl Target {
    /// Create preconfigured nvmf target with tcp transport and default options.
    pub fn create(addr: &str, port: u16) -> Result<Self, String> {
        let mut tgt_opts = TargetOpts::new("MayaStor", 110);

        let inner = unsafe { spdk_nvmf_tgt_create(&mut tgt_opts.inner) };
        if inner.is_null() {
            return Err("Failed to create nvmf target".to_owned());
        }

        let mut trid: spdk_nvme_transport_id = Default::default();
        trid.trtype = SPDK_NVME_TRANSPORT_TCP;
        trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;
        if addr.len() > SPDK_NVMF_TRADDR_MAX_LEN as usize {
            return Err("Invalid nvmf target address".to_owned());
        }
        let c_addr = CString::new(addr).unwrap();
        let port = format!("{}", port);
        assert!(port.len() < SPDK_NVMF_TRSVCID_MAX_LEN as usize);
        let c_port = CString::new(port.clone()).unwrap();

        unsafe {
            copy_nonoverlapping(
                c_addr.as_ptr(),
                &mut trid.traddr[0],
                addr.len() + 1,
            );
            copy_nonoverlapping(
                c_port.as_ptr(),
                &mut trid.trsvcid[0],
                port.len() + 1,
            );
        }
        info!("Created nvmf target at {}:{}", addr, port);

        Ok(Self {
            inner,
            trid,
            opts: spdk_nvmf_transport_opts::default(),
            acceptor_poll_rate: 1000, // 1ms
            acceptor_poller: ptr::null_mut(),
            pg: ptr::null_mut(),
        })
    }

    /// Add tcp transport to nvmf target
    pub async fn add_tcp_transport(&mut self) -> Result<(), String> {
        //#[allow(deprecated, invalid_value)] // TODO dont use uninitialized

        let ok = unsafe {
            spdk_nvmf_transport_opts_init(
                SPDK_NVME_TRANSPORT_TCP,
                &mut self.opts,
            )
        };
        if !ok {
            return Err("Failed to init opts for nvmf tcp transport".to_owned());
        }

        let transport = unsafe {
            spdk_nvmf_transport_create(SPDK_NVME_TRANSPORT_TCP, &mut self.opts)
        };
        if transport.is_null() {
            return Err("Failed to create nvmf tcp transport".to_owned());
        }

        let (sender, receiver) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_tgt_add_transport(
                self.inner,
                transport,
                Some(done_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(format!(
                "Failed to add nvmf tcp transport (errno {})",
                errno
            ))
        } else {
            info!("Added tcp nvmf transport {:?}", self);
            Ok(())
        }
    }

    /// Listen for incoming connections
    pub async fn listen(&mut self) -> Result<(), String> {
        let (sender, receiver) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_tgt_listen(
                self.inner,
                &mut self.trid as *mut _,
                Some(done_cb),
                cb_arg(sender),
            );
        }
        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(format!("Listen for nvmf target failed (errno {})", errno))
        } else {
            info!("nvmf target listens on {:?}", self);
            Ok(())
        }
    }

    /// A callback called by spdk when a new connection is accepted by nvmf
    /// transport. Assign new qpair to a poll group. We have just one poll
    /// group so we don't need fancy scheduling algorithm.
    extern "C" fn new_qpair(
        qpair: *mut spdk_nvmf_qpair,
        target_ptr: *mut c_void,
    ) {
        unsafe {
            let target = &*(target_ptr as *mut Self);
            if spdk_nvmf_poll_group_add(target.pg, qpair) != 0 {
                error!("Unable to add the qpair to a poll group");
                spdk_nvmf_qpair_disconnect(qpair, None, ptr::null_mut());
            }
        }
    }

    /// Called by SPDK poller to test if there is a new connection on
    /// nvmf transport.
    extern "C" fn acceptor_poll(target_ptr: *mut c_void) -> c_int {
        unsafe {
            let target = &mut *(target_ptr as *mut Self);
            spdk_nvmf_tgt_accept(
                target.inner,
                Some(Self::new_qpair),
                target as *mut Self as *mut c_void,
            );
        }
        -1
    }

    /// Create poll group and assign accepted connections (new qpairs) to
    /// the poll group.
    pub fn accept(&mut self) -> Result<(), String> {
        // create one poll group per target
        self.pg = unsafe { spdk_nvmf_poll_group_create(self.inner) };
        if self.pg.is_null() {
            return Err("Failed to create a poll group".to_owned());
        }

        self.acceptor_poller = unsafe {
            spdk_poller_register(
                Some(Self::acceptor_poll),
                self as *mut _ as *mut c_void,
                self.acceptor_poll_rate,
            )
        };
        info!("nvmf target {:?} accepts new connections", self);
        Ok(())
    }

    /// Add nvme subsystem to the target and return it.
    pub fn create_subsystem(&mut self, id: &str) -> Result<Subsystem, String> {
        let nqn = gen_nqn(id);
        let c_nqn = CString::new(nqn.clone()).unwrap();
        let ss = unsafe {
            spdk_nvmf_subsystem_create(
                self.inner,
                c_nqn.as_ptr(),
                SPDK_NVMF_SUBTYPE_NVME,
                1, // number of namespaces
            )
        };
        if ss.is_null() {
            return Err("Failed to create nvmf subsystem".to_owned());
        }
        unsafe { Subsystem::create(ss, &mut self.trid as *mut _, nqn) }
    }

    /// Lookup subsystem by NQN in given nvmf target.
    pub fn lookup_subsystem(&mut self, id: &str) -> Option<Subsystem> {
        let nqn = gen_nqn(id);
        let c_nqn = CString::new(nqn.clone()).unwrap();
        let inner =
            unsafe { spdk_nvmf_tgt_find_subsystem(self.inner, c_nqn.as_ptr()) };
        if inner.is_null() {
            None
        } else {
            Some(Subsystem {
                inner,
                nqn,
            })
        }
    }

    /// Callback for async destroy operation.
    extern "C" fn destroy_cb(sender_ptr: *mut c_void, errno: i32) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("Receiver is gone");
    }

    /// Stop nvmf target's subsystems and destroy it.
    ///
    /// NOTE: we cannot do this in drop because target destroy is asynchronous
    /// operation.
    pub async fn destroy(mut self) -> Result<(), String> {
        debug!("Destroying nvmf target {:?}", self);

        // stop accepting new connections
        if !self.acceptor_poller.is_null() {
            unsafe { spdk_poller_unregister(&mut self.acceptor_poller) };
        }

        // stop io processing
        if !self.pg.is_null() {
            unsafe { spdk_nvmf_poll_group_destroy(self.pg) };
        }

        // first we need to inactivate all subsystems of the target
        for mut ss in SubsystemIter::new(self.inner) {
            if let Err(msg) = ss.stop().await {
                return Err(format!("Failed to destroy a subsystem {:?} of nvmf target {:?}: {}", ss, self, msg));
            }
        }

        let (sender, receiver) = oneshot::channel::<i32>();
        unsafe {
            spdk_nvmf_tgt_destroy(
                self.inner,
                Some(Self::destroy_cb),
                cb_arg(sender),
            );
        }

        let errno = receiver.await.expect("Cancellation is not supported");
        if errno == 0 {
            info!("nvmf target was destroyed");
            Ok(())
        } else {
            Err(format!(
                "Failed to destroy nvmf target {:?} (errno {})",
                self, errno
            ))
        }
    }
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            write!(
                f,
                "{}:{}",
                CStr::from_ptr(&self.trid.traddr[0]).to_str().unwrap(),
                CStr::from_ptr(&self.trid.trsvcid[0]).to_str().unwrap(),
            )
        }
    }
}

/// Create nvmf target which will be used for exporting the replicas.
pub async fn init_nvmf() -> Result<(), String> {
    let mut boxed_tgt = match Target::create("127.0.0.1", 4401) {
        Ok(tgt) => Box::new(tgt),
        Err(msg) => return Err(msg),
    };
    if let Err(msg) = boxed_tgt.add_tcp_transport().await {
        return Err(msg);
    };
    if let Err(msg) = boxed_tgt.listen().await {
        return Err(msg);
    };
    if let Err(msg) = boxed_tgt.accept() {
        return Err(msg);
    };
    NVMF_TGT.with(move |nvmf_tgt| {
        if nvmf_tgt.borrow().is_some() {
            panic!("Double initialization of nvmf");
        }
        *nvmf_tgt.borrow_mut() = Some(boxed_tgt);
    });
    Ok(())
}

/// Destroy nvmf target with all its subsystems.
pub async fn fini_nvmf() -> Result<(), String> {
    let tgt = NVMF_TGT.with(move |nvmf_tgt| {
        nvmf_tgt
            .borrow_mut()
            .take()
            .expect("Called nvmf fini without init")
    });
    tgt.destroy().await
}

/// Export given bdev over nvmf target.
pub async fn share(uuid: &str, bdev: &Bdev) -> Result<(), String> {
    let mut ss = NVMF_TGT.with(move |maybe_tgt| {
        let mut maybe_tgt = maybe_tgt.borrow_mut();
        let tgt = maybe_tgt.as_mut().unwrap();
        tgt.create_subsystem(uuid)
    })?;
    ss.add_namespace(bdev)?;
    ss.start().await
}

/// Un-export given bdev from nvmf target.
/// Unsharing replica which is not shared is not an error.
pub async fn unshare(uuid: &str) -> Result<(), String> {
    let res = NVMF_TGT.with(move |maybe_tgt| {
        let mut maybe_tgt = maybe_tgt.borrow_mut();
        let tgt = maybe_tgt.as_mut().unwrap();
        tgt.lookup_subsystem(uuid)
    });

    match res {
        None => debug!("nvmf subsystem {} was not shared", uuid),
        Some(mut ss) => {
            ss.stop().await?;
            ss.destroy();
        }
    }
    Ok(())
}

pub fn get_nqn(uuid: &str) -> Option<String> {
    NVMF_TGT.with(move |maybe_tgt| {
        let mut maybe_tgt = maybe_tgt.borrow_mut();
        let tgt = maybe_tgt.as_mut().unwrap();
        match tgt.lookup_subsystem(uuid) {
            Some(mut ss) => Some(ss.get_nqn()),
            None => None,
        }
    })
}
