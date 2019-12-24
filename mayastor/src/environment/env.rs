use std::{
    cell::Cell,
    env,
    ffi::CString,
    net::Ipv4Addr,
    os::raw::{c_char, c_void},
    sync::{Arc, Mutex},
};

use nix::sys::{
    signal,
    signal::{
        pthread_sigmask,
        SigHandler,
        SigSet,
        SigmaskHow,
        Signal::{SIGINT, SIGTERM},
    },
};
use snafu::{ResultExt, Snafu};

use spdk_sys::{
    maya_log,
    spdk_app_shutdown_cb,
    spdk_conf_allocate,
    spdk_conf_free,
    spdk_conf_read,
    spdk_conf_set_as_default,
    spdk_cpuset_alloc,
    spdk_cpuset_free,
    spdk_cpuset_set_cpu,
    spdk_cpuset_zero,
    spdk_env_get_core_count,
    spdk_env_get_current_core,
    spdk_log_level,
    spdk_log_open,
    spdk_log_set_level,
    spdk_log_set_print_level,
    spdk_pci_addr,
    spdk_rpc_set_state,
    spdk_thread_create,
    SPDK_LOG_DEBUG,
    SPDK_LOG_INFO,
    SPDK_RPC_RUNTIME,
};

use crate::{
    developer_delay,
    environment::args::MayastorCliArgs,
    event::Mthread,
    executor,
    iscsi_target,
    log_impl,
    mayastor_shutdown_cb,
    mayastor_stop,
    nvmf_target,
    pool,
    replica,
};

thread_local! {
    /// Only accessible from the master core for now
    static INIT_THREAD: Cell<Option<Mthread>> = Cell::new(None);
}

lazy_static! {
    /// global return code for the whole program. We must use ARC here to make proper use of
    /// lazy_static crate. It could have been a static mut all the same.
    pub static ref GLOBAL_RC: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
}

/// FFI functions that are needed to initialize the environment
extern "C" {
    pub fn rte_eal_init(argc: i32, argv: *mut *mut libc::c_char) -> i32;
    pub fn rte_log_set_level(_type: i32, level: i32) -> i32;
    pub fn spdk_app_json_config_load(
        file: *const c_char,
        addr: *const c_char,
        cb: Option<extern "C" fn(i32, *mut c_void)>,
        args: *mut c_void,
    );
    pub fn spdk_env_dpdk_post_init(legacy_mem: bool) -> i32;
    pub fn spdk_env_fini();
    pub fn spdk_log_close();
    pub fn spdk_log_set_flag(name: *const c_char, enable: bool) -> i32;
    pub fn spdk_reactors_fini();
    pub fn spdk_reactors_init() -> i32;
    pub fn spdk_reactors_start();
    pub fn spdk_reactors_stop(ctx: *mut c_void);
    pub fn spdk_rpc_finish();
    pub fn spdk_rpc_initialize(listen: *mut libc::c_char);
    pub fn spdk_subsystem_fini(
        f: Option<unsafe extern "C" fn(*mut c_void)>,
        ctx: *mut c_void,
    );
    pub fn spdk_subsystem_init(
        f: Option<extern "C" fn(i32, *mut c_void)>,
        ctx: *mut c_void,
    );
}

#[derive(Debug, Snafu)]
pub enum EnvError {
    #[snafu(display("Failed to install signal handler"))]
    SetSigHdl { source: nix::Error },
    #[snafu(display("Failed to read configuration file: {}", reason))]
    ParseConfig { reason: String },
    #[snafu(display("Failed to initialize logging subsystem"))]
    InitLog,
    #[snafu(display("Failed to initialize {} target", target))]
    InitTarget { target: String },
}

type Result<T, E = EnvError> = std::result::Result<T, E>;

/// Mayastor argument
#[derive(Debug)]
pub struct MayastorEnvironment {
    config: Option<String>,
    delay_subsystem_init: bool,
    enable_coredump: bool,
    env_context: String,
    hugedir: String,
    hugepage_single_segments: bool,
    json_config_file: Option<String>,
    master_core: i32,
    mem_channel: i32,
    mem_size: i32,
    name: String,
    no_pci: bool,
    num_entries: u64,
    num_pci_addr: usize,
    pci_blacklist: Vec<spdk_pci_addr>,
    pci_whitelist: Vec<spdk_pci_addr>,
    print_level: spdk_log_level,
    debug_level: spdk_log_level,
    reactor_mask: String,
    rpc_addr: String,
    shm_id: i32,
    shutdown_cb: spdk_app_shutdown_cb,
    tpoint_group_mask: String,
    unlink_hugepage: bool,
    log_component: Vec<String>,
}

impl Default for MayastorEnvironment {
    fn default() -> Self {
        Self {
            config: None,
            delay_subsystem_init: false,
            enable_coredump: true,
            env_context: String::new(),
            hugedir: String::new(),
            hugepage_single_segments: false,
            json_config_file: None,
            master_core: -1,
            mem_channel: -1,
            mem_size: -1,
            name: "mayastor".into(),
            no_pci: false,
            num_entries: 32 * 1024,
            num_pci_addr: 0,
            pci_blacklist: vec![],
            pci_whitelist: vec![],
            print_level: SPDK_LOG_INFO,
            debug_level: SPDK_LOG_INFO,
            reactor_mask: "0x1".into(),
            rpc_addr: "/var/tmp/mayastor.sock".into(),
            shm_id: -1,
            shutdown_cb: None,
            tpoint_group_mask: String::new(),
            unlink_hugepage: false,
            log_component: vec![],
        }
    }
}

/// main shutdown routine for mayastor
pub extern "C" fn mayastor_env_stop(rc: i32) {
    INIT_THREAD.with(|t| unsafe {
        use spdk_sys::*;
        let t = t.get().unwrap();
        spdk_set_thread(t.0);

        spdk_thread_send_msg(
            t.0,
            Some(mayastor_shutdown_cb),
            rc as *const c_void as *mut c_void,
        );
    });
}

/// called on SIGINT and SIGTERM
extern "C" fn mayastor_signal_handler(signo: i32) {
    warn!("Received SIGNO: {}", signo);
    // we don't differentiate between signal numbers for now, all signals will
    // cause a shutdown
    mayastor_env_stop(signo);
}

impl MayastorEnvironment {
    pub fn new(args: MayastorCliArgs) -> Self {
        Self {
            config: args.config,
            json_config_file: args.json,
            log_component: args.log_components,
            mem_size: args.mem_size,
            no_pci: args.no_pci,
            reactor_mask: args.reactor_mask,
            rpc_addr: args.rpc_address,
            ..Default::default()
        }
    }

    /// configure signal handling
    fn install_signal_handlers(&self) -> Result<()> {
        // first set that we ignore SIGPIPE
        let _ = unsafe { signal::signal(signal::SIGPIPE, SigHandler::SigIgn) }
            .context(SetSigHdl)?;

        // setup that we want mayastor_signal_handler to be invoked on SIGINT
        // and SIGTERM
        let handler = SigHandler::Handler(mayastor_signal_handler);

        unsafe {
            signal::signal(SIGINT, handler).context(SetSigHdl)?;
            signal::signal(SIGTERM, handler).context(SetSigHdl)?;
        }

        let mut mask = SigSet::empty();
        mask.add(SIGINT);
        mask.add(SIGTERM);

        pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&mask), None)
            .context(SetSigHdl)?;

        Ok(())
    }

    /// read the config file we use this mostly for testing
    fn read_config_file(&self) -> Result<()> {
        if self.config.is_none() {
            trace!("no configuration file specified");
            return Ok(());
        }

        let path =
            CString::new(self.config.as_ref().unwrap().as_str()).unwrap();
        let config = unsafe { spdk_conf_allocate() };

        assert_ne!(config, std::ptr::null_mut());

        if unsafe { spdk_conf_read(config, path.as_ptr()) } != 0 {
            return Err(EnvError::ParseConfig {
                reason: "Failed to read file from disk".into(),
            });
        }

        let rc = unsafe {
            if spdk_sys::spdk_conf_first_section(config).is_null() {
                Err(EnvError::ParseConfig {
                    reason: "failed to parse config file".into(),
                })
            } else {
                Ok(())
            }
        };

        if rc.is_ok() {
            trace!("Setting default config to {:p}", config);
            unsafe { spdk_conf_set_as_default(config) };
        } else {
            unsafe { spdk_conf_free(config) }
        }

        rc
    }

    /// construct an array of options to be passed to EAL and start it
    fn initialize_eal(&self) {
        let mut args: Vec<CString> = Vec::new();

        args.push(CString::new(self.name.clone()).unwrap());

        args.push(CString::new(format!("-c {}", self.reactor_mask)).unwrap());

        if self.mem_channel > 0 {
            args.push(
                CString::new(format!("-n {}", self.mem_channel)).unwrap(),
            );
        }

        if self.master_core > 0 {
            args.push(
                CString::new(format!("--master-lcore={}", self.master_core))
                    .unwrap(),
            );
        }

        if self.shm_id < 0 {
            args.push(CString::new("--no-shconf").unwrap());
        }

        if self.mem_size >= 0 {
            args.push(CString::new(format!("-m {}", self.mem_size)).unwrap());
        }

        if self.master_core > 0 {
            args.push(
                CString::new(format!("--master-lcore={}", self.master_core))
                    .unwrap(),
            );
        }

        if self.no_pci {
            args.push(CString::new("--no-pci").unwrap());
        }

        if self.hugepage_single_segments {
            args.push(CString::new("--single-file-segments").unwrap());
        }

        if !self.hugedir.is_empty() {
            args.push(
                CString::new(format!("--huge-dir={}", self.hugedir)).unwrap(),
            )
        }

        if cfg!(target_os = "linux") {
            // Ref: https://github.com/google/sanitizers/wiki/AddressSanitizerAlgorithm
            args.push(CString::new("--base-virtaddr=0x200000000000").unwrap());
        }

        if self.shm_id < 0 {
            args.push(
                CString::new(format!("--file-prefix=mayastor_pid{}", unsafe {
                    libc::getpid()
                }))
                .unwrap(),
            );
        } else {
            args.push(
                CString::new(format!(
                    "--file-prefix=mayastor_pid{}",
                    self.shm_id
                ))
                .unwrap(),
            );
            args.push(CString::new("--proc-type=auto").unwrap());
        }

        // set the log levels of the DPDK libs can be overridden by setting
        // env_context
        args.push(CString::new("--log-level=lib.eal:4").unwrap());
        args.push(CString::new("--log-level=lib.cryptodev:0").unwrap());
        args.push(CString::new("--log-level=user1:6").unwrap());
        args.push(CString::new("--match-allocations").unwrap());

        // any additional parameters we want to pass down to the eal. These
        // arguments are not checked or validated.
        if !self.env_context.is_empty() {
            args.push(CString::new(self.env_context.clone()).unwrap());
        }

        let mut cargs = args
            .iter()
            .map(|arg| arg.as_ptr())
            .collect::<Vec<*const i8>>();

        cargs.push(std::ptr::null());
        info!("EAL arguments {:?}", args);

        if unsafe {
            rte_eal_init(
                (cargs.len() as libc::c_int) - 1,
                cargs.as_ptr() as *mut *mut i8,
            )
        } < 0
        {
            panic!("Failed to init EAL");
        }
        if unsafe { spdk_env_dpdk_post_init(false) } != 0 {
            panic!("Failed execute post setup");
        }
    }

    /// Setup a "stackless thread", which will be our management thread. This
    /// thread is also used to initiate the shutdown.
    fn init_main_thread(&self) -> Result<()> {
        let rc = unsafe { spdk_reactors_init() };

        if rc != 0 {
            error!("Failed to initialize reactors, there is no point to continue, error code: {}", rc);
            std::process::exit(rc);
        }

        let cpu_mask = unsafe { spdk_cpuset_alloc() };

        if cpu_mask.is_null() {
            error!("CPU set allocation failed, aborting startup");
            std::process::exit(1);
        }

        unsafe {
            spdk_cpuset_zero(cpu_mask);
            spdk_cpuset_set_cpu(cpu_mask, spdk_env_get_current_core(), true);
            spdk_cpuset_free(cpu_mask);
        }

        // allocate the mayastor management thread (mm_thread)
        let thread = {
            let name = CString::new("mm_thread").unwrap();
            unsafe { spdk_thread_create(name.as_ptr(), cpu_mask) }
        };

        if thread.is_null() {
            error!(
                "Failed to allocate the management thread, aborting startup"
            );
            std::process::exit(1)
        }

        INIT_THREAD.with(|t| t.set(Some(Mthread(thread))));

        Ok(())
    }

    /// initialize the logging subsystem
    fn init_logger(&mut self) -> Result<()> {
        // if log flags are specified increase the loglevel and print level.
        if !self.log_component.is_empty() {
            warn!("Increasing debug and print level ...");
            self.debug_level = SPDK_LOG_DEBUG;
            self.print_level = SPDK_LOG_DEBUG;
        }

        unsafe {
            for flag in &self.log_component {
                let cflag = CString::new(flag.clone()).unwrap();
                if spdk_log_set_flag(cflag.as_ptr(), true) != 0 {
                    return Err(EnvError::InitLog);
                }
            }

            spdk_log_set_level(self.debug_level);
            spdk_log_set_print_level(self.print_level);
            // open our log implementation which is implemented in the wrapper
            spdk_log_open(Some(maya_log));
            // our callback called defined in rust called by our wrapper
            spdk_sys::logfn = Some(log_impl);
        }
        Ok(())
    }

    /// We implement our own default target init code here. Note that if there
    /// is an existing target we will fail the init process.
    extern "C" fn target_init() -> Result<(), EnvError> {
        let address = match env::var("MY_POD_IP") {
            Ok(val) => {
                let _ipv4: Ipv4Addr = match val.parse() {
                    Ok(val) => val,
                    Err(_) => {
                        error!("Invalid IP address: MY_POD_IP={}", val);
                        mayastor_stop(-1);
                        return Err(EnvError::InitLog);
                    }
                };
                val
            }
            Err(_) => "127.0.0.1".to_owned(),
        };

        if let Err(msg) = iscsi_target::init_iscsi(&address) {
            error!("Failed to initialize Mayastor iSCSI target: {}", msg);
            return Err(EnvError::InitTarget {
                target: "iscsi".into(),
            });
        }

        executor::spawn(async move {
            if let Err(msg) = nvmf_target::init_nvmf(&address).await {
                error!("Failed to initialize Mayastor nvmf target: {}", msg);
                mayastor_stop(-1);
            }
        });

        Ok(())
    }

    extern "C" fn start_rpc(rc: i32, arg: *mut c_void) {
        if arg.is_null() || rc != 0 {
            panic!("Failed to initialize subsystems: {}", rc);
        }

        let rpc = unsafe { CString::from_raw(arg as _) };

        info!("RPC server listening at: {}", rpc.to_str().unwrap());
        unsafe {
            spdk_rpc_initialize(arg as *mut i8);
            spdk_rpc_set_state(SPDK_RPC_RUNTIME);
        };

        Self::target_init().unwrap();
    }

    /// start mayastor and call f when all is setup.
    pub fn start<F>(&mut self, f: F) -> Result<i32>
    where
        F: FnOnce(),
    {
        self.read_config_file()?;
        self.initialize_eal();
        self.init_logger()?;

        if self.enable_coredump {
            //TODO
            warn!("rlimit configuration not implemented");
        }

        info!("Total number of cores available: {}", unsafe {
            spdk_env_get_core_count()
        });

        self.install_signal_handlers()?;
        self.init_main_thread()?;

        // init the subsystems and RPC server, this must be done in context of
        // the "stackless" threads.
        INIT_THREAD.with(|t| {
            let mt = t.get().unwrap();

            mt.with(|| unsafe {
                // all futures will be executed from the management thread
                // (mm_thread)
                executor::start();

                let rpc = CString::new(self.rpc_addr.as_str()).unwrap();

                if let Some(ref json) = self.json_config_file {
                    info!("Loading JSON configuration file");

                    let jsonfile = CString::new(json.as_str()).unwrap();
                    spdk_app_json_config_load(
                        jsonfile.as_ptr(),
                        rpc.as_ptr(),
                        Some(Self::start_rpc),
                        rpc.into_raw() as _,
                    );
                } else {
                    spdk_subsystem_init(
                        Some(Self::start_rpc),
                        rpc.into_raw() as _,
                    );
                }

                if let Some(_key) = env::var_os("DELAY") {
                    warn!("*** Delaying reactor every 1000us ***");
                    spdk_sys::spdk_poller_register(
                        Some(developer_delay),
                        std::ptr::null_mut(),
                        1000,
                    );
                }
            });
        });

        pool::register_pool_methods();
        replica::register_replica_methods();

        f();

        unsafe {
            // will block the main thread until we exit
            spdk_reactors_start();

            info!("Finalizing Mayastor shutdown...");
            spdk_reactors_fini();
            spdk_env_fini();
            spdk_log_close();
        }

        // return the global rc value

        Ok(*GLOBAL_RC.lock().unwrap())
    }
}
