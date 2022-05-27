use std::ffi::{c_void, CString};

use snafu::Snafu;
use spdk_rs::libspdk::{
    spdk_get_thread,
    spdk_set_thread,
    spdk_thread,
    spdk_thread_create,
    spdk_thread_destroy,
    spdk_thread_exit,
    spdk_thread_get_by_id,
    spdk_thread_get_id,
    spdk_thread_get_name,
    spdk_thread_is_exited,
    spdk_thread_poll,
    spdk_thread_send_msg,
};

use crate::core::{cpu_cores::CpuMask, CoreError, Cores, Reactors};
use futures::channel::oneshot::{channel, Receiver, Sender};
use nix::errno::Errno;
use std::{fmt::Debug, future::Future, ptr::NonNull};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Event spawned from a non-spdk thread"))]
    InvalidThread {},
}

#[derive(Debug, PartialEq, Clone, Copy)]
/// struct that wraps an SPDK thread. The name thread is chosen poorly and
/// should not be confused with an actual thread. Consider it more to be
/// analogous to a container to which you can submit work and poll it to drive
/// the submitted work to completion.
pub struct Mthread(NonNull<spdk_thread>);
unsafe impl Send for Mthread {}
impl From<*mut spdk_thread> for Mthread {
    fn from(t: *mut spdk_thread) -> Self {
        let t = NonNull::new(t).expect("thread may not be NULL");
        Mthread(t)
    }
}

impl Mthread {
    pub fn get_init() -> Mthread {
        Mthread(
            NonNull::new(unsafe { spdk_thread_get_by_id(1) })
                .expect("No init thread allocated"),
        )
    }

    ///
    /// With the given thread as context, execute the closure on that thread.
    ///
    /// Any function can be executed here however, this should typically be used
    /// to execute functions that reference any FFI to SPDK.

    pub fn new(name: String, core: u32) -> Option<Self> {
        let name = CString::new(name).unwrap();

        NonNull::new(unsafe {
            let mut mask = CpuMask::new();
            mask.set_cpu(core, true);
            spdk_thread_create(name.as_ptr(), mask.as_ptr())
        })
        .map(Mthread)
    }

    pub fn id(&self) -> u64 {
        unsafe { spdk_thread_get_id(self.0.as_ptr()) }
    }
    ///
    /// # Note
    ///
    /// Avoid any blocking calls as it will block the whole reactor. Also, avoid
    /// long-running functions. In general if you follow the nodejs event loop
    /// model, you should be good.
    pub fn with<T, F: FnOnce() -> T>(self, f: F) -> T {
        let th = Self::current();
        self.enter();
        let out = f();
        if let Some(t) = th {
            t.enter();
        }
        out
    }

    #[inline]
    pub fn poll(&self) {
        let _ = unsafe { spdk_thread_poll(self.0.as_ptr(), 0, 0) };
    }

    #[inline]
    pub fn enter(&self) {
        unsafe { spdk_set_thread(self.0.as_ptr()) };
    }

    #[inline]
    pub fn exit(&self) {
        unsafe { spdk_set_thread(std::ptr::null_mut()) };
    }

    pub fn current() -> Option<Mthread> {
        NonNull::new(unsafe { spdk_get_thread() }).map(Mthread)
    }

    pub fn name(&self) -> &str {
        unsafe {
            std::ffi::CStr::from_ptr(spdk_thread_get_name(self.0.as_ptr()))
                .to_str()
                .unwrap()
        }
    }

    pub fn into_raw(self) -> *mut spdk_thread {
        self.0.as_ptr()
    }

    /// destroy the given thread waiting for it to become ready to destroy
    pub fn destroy(self) {
        debug!("destroying thread {}...{:p}", self.name(), self.0);
        unsafe {
            spdk_set_thread(self.0.as_ptr());
            // set that we *want* to exit, but we have not exited yet
            spdk_thread_exit(self.0.as_ptr());

            // now wait until the thread is actually exited the internal
            // state is updated by spdk_thread_poll()
            while !spdk_thread_is_exited(self.0.as_ptr()) {
                spdk_thread_poll(self.0.as_ptr(), 0, 0);
            }
            spdk_thread_destroy(self.0.as_ptr());
        }

        debug!("thread {:p} destroyed", self.0);
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn send_msg(
        &self,
        f: extern "C" fn(ctx: *mut c_void),
        arg: *mut c_void,
    ) {
        let rc = unsafe { spdk_thread_send_msg(self.0.as_ptr(), Some(f), arg) };
        assert_eq!(rc, 0);
    }

    /// send the given thread 'msg' in xPDK speak.
    pub fn msg<F, T>(&self, t: T, f: F)
    where
        F: FnOnce(T),
        T: std::fmt::Debug + 'static,
    {
        // context structure which is passed to the callback as argument
        struct Ctx<F, T: std::fmt::Debug> {
            closure: F,
            args: T,
        }

        // helper routine to unpack the closure and its arguments
        extern "C" fn trampoline<F, T>(arg: *mut c_void)
        where
            F: FnOnce(T),
            T: 'static + std::fmt::Debug,
        {
            let ctx = unsafe { Box::from_raw(arg as *mut Ctx<F, T>) };
            (ctx.closure)(ctx.args);
        }

        let ctx = Box::new(Ctx {
            closure: f,
            args: t,
        });

        let rc = unsafe {
            spdk_thread_send_msg(
                self.0.as_ptr(),
                Some(trampoline::<F, T>),
                Box::into_raw(ctx).cast(),
            )
        };
        assert_eq!(rc, 0);
    }

    /// spawn a future on a core the current thread is running on returning a
    /// channel which can be awaited. This decouples the SPDK runtime from the
    /// future runtimes within rust.
    pub fn spawn_local<F>(&self, f: F) -> Result<Receiver<F::Output>, CoreError>
    where
        F: Future + 'static,
        F::Output: Send + Debug,
    {
        // context structure which is passed to the callback as argument
        struct Ctx<F>
        where
            F: Future,
            F::Output: Send + Debug,
        {
            future: F,
            sender: Option<Sender<F::Output>>,
        }

        // helper routine to unpack the closure and its arguments
        extern "C" fn trampoline<F>(arg: *mut c_void)
        where
            F: Future + 'static,
            F::Output: Send + Debug,
        {
            let mut ctx = unsafe { Box::from_raw(arg as *mut Ctx<F>) };
            Reactors::current()
                .spawn_local(async move {
                    let result = ctx.future.await;
                    if let Err(e) = ctx
                        .sender
                        .take()
                        .expect("sender already taken")
                        .send(result)
                    {
                        error!("Failed to send response future result {:?}", e);
                    }
                })
                .detach();
        }

        let (s, r) = channel::<F::Output>();

        let ctx = Box::new(Ctx {
            future: f,
            sender: Some(s),
        });

        let rc = unsafe {
            spdk_thread_send_msg(
                self.0.as_ptr(),
                Some(trampoline::<F>),
                Box::into_raw(ctx).cast(),
            )
        };
        if rc != 0 {
            Err(CoreError::NotSupported {
                source: Errno::UnknownErrno,
            })
        } else {
            Ok(r)
        }
    }

    /// spawns a thread and setting its affinity to the inverse cpu set of
    /// mayastor
    pub fn spawn_unaffinitized<F, T>(f: F) -> std::thread::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        std::thread::spawn(|| {
            Self::unaffinitize();
            f()
        })
    }

    pub fn unaffinitize() {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for i in 0 .. libc::sysconf(libc::_SC_NPROCESSORS_ONLN) {
                libc::CPU_SET(i as usize, &mut set)
            }

            Cores::count()
                .into_iter()
                .for_each(|i| libc::CPU_CLR(i as usize, &mut set));

            libc::sched_setaffinity(
                0,
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );

            debug!("pthread started on core {}", libc::sched_getcpu());
        }
    }
}
