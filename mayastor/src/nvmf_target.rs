//! Methods for  creating nvmf targets
use crate::executor::{cb_arg, complete_callback_1};
use futures::channel::oneshot;
use spdk_sys::{
    spdk_bdev,
    spdk_nvme_transport_id,
    spdk_nvmf_subsystem,
    spdk_nvmf_subsystem_add_listener,
    spdk_nvmf_subsystem_add_ns,
    spdk_nvmf_subsystem_create,
    spdk_nvmf_subsystem_destroy,
    spdk_nvmf_subsystem_set_allow_any_host,
    spdk_nvmf_subsystem_set_mn,
    spdk_nvmf_subsystem_set_sn,
    spdk_nvmf_subsystem_start,
    spdk_nvmf_subsystem_stop,
    spdk_nvmf_tgt,
    spdk_nvmf_tgt_add_transport,
    spdk_nvmf_tgt_create,
    spdk_nvmf_tgt_destroy,
    spdk_nvmf_tgt_find_subsystem,
    spdk_nvmf_tgt_listen,
    spdk_nvmf_transport_create,
    spdk_nvmf_transport_opts,
    spdk_nvmf_transport_opts_init,
    SPDK_NVME_TRANSPORT_TCP,
    SPDK_NVMF_ADRFAM_IPV4,
    SPDK_NVMF_SUBTYPE_NVME,
    SPDK_NVMF_TRADDR_MAX_LEN,
    SPDK_NVMF_TRSVCID_MAX_LEN,
};
use std::{
    cell::RefCell,
    ffi::{c_void, CStr, CString},
    fmt,
    ptr::{self, copy_nonoverlapping},
};

thread_local! {
    /// nvmf target provides a scope for creating transports, namespaces etc.
    /// It is thread-local because TLS is safe to access in rust without any
    /// synchronization overhead. It should be accessed only from
    /// reactor_0 thread.
    pub (crate) static NVMF_TGT: RefCell<Option<Target>> = RefCell::new(None);
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
            return Err("listen on nvmf subsystem failed".to_owned());
        }

        Ok(Self {
            inner,
            nqn,
        })
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

    /// Start the subsystem (it cannot be modified afterwards)
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
    pub fn add_namespace(
        &mut self,
        bdev: *mut spdk_bdev,
    ) -> Result<(), String> {
        let ns_id = unsafe {
            spdk_nvmf_subsystem_add_ns(
                self.inner,
                bdev,
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

/// Wrapper around spdk nvmf target providing rust friendly api.
/// nvmf target binds listen addresses and nvmf subsystems with namespaces
/// together.
pub(crate) struct Target {
    inner: *mut spdk_nvmf_tgt,
    /// Endpoint where this nvmf target listens for incoming connections.
    trid: spdk_nvme_transport_id,
    opts: spdk_nvmf_transport_opts,
}

impl Target {
    /// Create preconfigured nvmf target with tcp transport and default options.
    pub fn create(addr: &str, port: u16) -> Result<Self, String> {
        let inner = unsafe { spdk_nvmf_tgt_create(0) };
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
                Some(complete_callback_1),
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
            info!("Added transport {:?}", self);
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
                Some(complete_callback_1),
                cb_arg(sender),
            );
        }
        let errno = receiver.await.expect("Cancellation is not supported");
        if errno != 0 {
            Err(format!("Listen for nvmf target failed (errno {})", errno))
        } else {
            info!("{:?} is accepting new connections", self);
            Ok(())
        }
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
    extern "C" fn destroy_cb(_ctx: *mut c_void, errno: i32) {
        if errno == 0 {
            info!("nvmf target was destroyed");
        } else {
            error!("Failed to destroy nvmf target (errno {})", errno);
        }
    }
}

impl Drop for Target {
    fn drop(&mut self) {
        debug!("Destroying {:?}", self);
        unsafe {
            spdk_nvmf_tgt_destroy(
                self.inner,
                Some(Self::destroy_cb),
                ptr::null_mut(),
            );
        }
    }
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            write!(
                f,
                "nvmf target {}:{}",
                CStr::from_ptr(&self.trid.traddr[0]).to_str().unwrap(),
                CStr::from_ptr(&self.trid.trsvcid[0]).to_str().unwrap(),
            )
        }
    }
}

/// Create nvmf target which will be used for exporting the replicas.
pub async fn init_nvmf() -> Result<(), String> {
    let mut tgt = match Target::create("127.0.0.1", 4401) {
        Ok(tgt) => tgt,
        Err(msg) => return Err(msg),
    };
    if let Err(msg) = tgt.add_tcp_transport().await {
        return Err(msg);
    };
    if let Err(msg) = tgt.listen().await {
        return Err(msg);
    };
    NVMF_TGT.with(move |nvmf_tgt| {
        *nvmf_tgt.borrow_mut() = Some(tgt);
    });
    Ok(())
}

/// Export given bdev over nvmf target.
pub async fn share(uuid: &str, bdev: *mut spdk_bdev) -> Result<(), String> {
    let mut ss = NVMF_TGT.with(move |maybe_tgt| {
        let mut maybe_tgt = maybe_tgt.borrow_mut();
        let tgt = maybe_tgt.as_mut().unwrap();
        tgt.create_subsystem(uuid)
    })?;
    ss.add_namespace(bdev)?;
    ss.start().await
}

/// Un-export given bdev from nvmf target.
pub async fn unshare(uuid: &str) -> Result<(), String> {
    let res = NVMF_TGT.with(move |maybe_tgt| {
        let mut maybe_tgt = maybe_tgt.borrow_mut();
        let tgt = maybe_tgt.as_mut().unwrap();
        tgt.lookup_subsystem(uuid)
    });

    match res {
        None => Err(format!("nvmf subsystem {} not found", uuid)),
        Some(mut ss) => {
            ss.stop().await?;
            ss.destroy();
            Ok(())
        }
    }
}
