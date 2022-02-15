use std::{
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_char, c_void},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
        Mutex,
    },
    time::Duration,
};

use byte_unit::{Byte, ByteUnit};
use futures::{channel::oneshot, future};
use git_version::git_version;
use http::Uri;
use once_cell::sync::{Lazy, OnceCell};
use snafu::Snafu;
use spdk_rs::{
    libspdk::{
        spdk_app_shutdown_cb,
        spdk_log_level,
        spdk_log_open,
        spdk_log_set_level,
        spdk_log_set_print_level,
        spdk_pci_addr,
        spdk_rpc_set_state,
        spdk_thread_lib_fini,
        spdk_thread_send_critical_msg,
        SPDK_LOG_DEBUG,
        SPDK_LOG_INFO,
        SPDK_RPC_RUNTIME,
    },
    spdk_rs_log,
};
use structopt::StructOpt;
use tokio::runtime::Builder;

use crate::{
    bdev::{bdev_io_ctx_pool_init, nexus, nvme_io_ctx_pool_init},
    core::{
        reactor::{Reactor, ReactorState, Reactors},
        Cores,
        MayastorFeatures,
        Mthread,
    },
    grpc,
    logger,
    persistent_store::PersistentStore,
    subsys::{self, Config, PoolConfig},
};

fn parse_mb(src: &str) -> Result<i32, String> {
    // For compatibility, we check to see if there are no alphabetic characters
    // passed in, if, so we interpret the value to be in MiB which is what the
    // EAL expects it to be in.

    let has_unit = src.trim_end().chars().any(|c| c.is_alphabetic());

    if let Ok(val) = Byte::from_str(src) {
        let value;
        if has_unit {
            value = val.get_adjusted_unit(ByteUnit::MiB).get_value() as i32
        } else {
            value = val.get_bytes() as i32
        }
        Ok(value)
    } else {
        Err(format!("Invalid argument {}", src))
    }
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(
    name = "Mayastor",
    about = "Containerized Attached Storage (CAS) for k8s",
    version = git_version!(args = ["--tags", "--abbrev=12"], fallback="unkown"),
    setting(structopt::clap::AppSettings::ColoredHelp)
)]
pub struct MayastorCliArgs {
    #[structopt(short = "g", default_value = grpc::default_endpoint_str())]
    /// IP address and port (optional) for the gRPC server to listen on.
    pub grpc_endpoint: String,
    #[structopt(short = "R", default_value = "https://core:50051")]
    /// Registration
    pub registration_endpoint: Uri,
    #[structopt(short = "L")]
    /// Enable logging for sub components.
    pub log_components: Vec<String>,
    #[structopt(short = "m", default_value = "0x1")]
    /// The reactor mask to be used for starting up the instance
    pub reactor_mask: String,
    #[structopt(short = "N")]
    /// Name of the node where mayastor is running (ID used by control plane)
    pub node_name: Option<String>,
    /// The maximum amount of hugepage memory we are allowed to allocate in
    /// MiB. A value of 0 means no limit.
    #[structopt(
    short = "s",
    parse(try_from_str = parse_mb),
    default_value = "0"
    )]
    pub mem_size: i32,
    #[structopt(short = "u")]
    /// Disable the use of PCIe devices.
    pub no_pci: bool,
    #[structopt(short = "r", default_value = "/var/tmp/mayastor.sock")]
    /// Path to create the rpc socket.
    pub rpc_address: String,
    #[structopt(short = "y")]
    /// Path to mayastor config YAML file.
    pub mayastor_config: Option<String>,
    #[structopt(short = "P")]
    /// Path to pool config file.
    pub pool_config: Option<String>,
    #[structopt(long = "huge-dir")]
    /// Path to hugedir.
    pub hugedir: Option<String>,
    #[structopt(long = "env-context")]
    /// Pass additional arguments to the EAL environment.
    pub env_context: Option<String>,
    #[structopt(short = "l")]
    /// List of cores to run on instead of using the core mask. When specified
    /// it supersedes the core mask (-m) argument.
    pub core_list: Option<String>,
    #[structopt(short = "p")]
    /// Endpoint of the persistent store.
    pub persistent_store_endpoint: Option<String>,
    #[structopt(long = "bdev-pool-size", default_value = "65535")]
    /// Number of entries in memory pool for bdev I/O contexts
    pub bdev_io_ctx_pool_size: u64,
    #[structopt(long = "nvme-ctl-pool-size", default_value = "65535")]
    /// Number of entries in memory pool for NVMe controller I/O contexts
    pub nvme_ctl_io_ctx_pool_size: u64,
}

/// Mayastor features.
impl MayastorFeatures {
    fn init_features() -> MayastorFeatures {
        let ana = match std::env::var("NEXUS_NVMF_ANA_ENABLE") {
            Ok(s) => s == "1",
            Err(_) => false,
        };

        MayastorFeatures {
            asymmetric_namespace_access: ana,
        }
    }

    pub fn get_features() -> Self {
        MAYASTOR_FEATURES.get_or_init(Self::init_features).clone()
    }
}

/// Defaults are redefined here in case of using it during tests
impl Default for MayastorCliArgs {
    fn default() -> Self {
        Self {
            grpc_endpoint: grpc::default_endpoint().to_string(),
            persistent_store_endpoint: None,
            node_name: None,
            env_context: None,
            reactor_mask: "0x1".into(),
            mem_size: 0,
            rpc_address: "/var/tmp/mayastor.sock".to_string(),
            no_pci: true,
            log_components: vec![],
            mayastor_config: None,
            pool_config: None,
            hugedir: None,
            core_list: None,
            bdev_io_ctx_pool_size: 65535,
            nvme_ctl_io_ctx_pool_size: 65535,
            registration_endpoint: Uri::try_from("https://core:50051").unwrap(),
        }
    }
}

/// Global exit code of the program, initially set to -1 to capture double
/// shutdown during test cases
pub static GLOBAL_RC: Lazy<Arc<Mutex<i32>>> =
    Lazy::new(|| Arc::new(Mutex::new(-1)));

/// keep track if we have received a signal already
pub static SIG_RECEIVED: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(false));

/// FFI functions that are needed to initialize the environment
extern "C" {
    pub fn rte_eal_init(argc: i32, argv: *mut *mut libc::c_char) -> i32;
    pub fn spdk_trace_cleanup();
    pub fn spdk_env_dpdk_post_init(legacy_mem: bool) -> i32;
    pub fn spdk_env_fini();
    pub fn spdk_log_close();
    pub fn spdk_log_set_flag(name: *const c_char, enable: bool) -> i32;
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
    #[snafu(display("Failed to initialize logging subsystem"))]
    InitLog,
    #[snafu(display("Failed to initialize {} target", target))]
    InitTarget { target: String },
}

type Result<T, E = EnvError> = std::result::Result<T, E>;

/// Mayastor argument
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MayastorEnvironment {
    pub node_name: String,
    pub grpc_endpoint: Option<std::net::SocketAddr>,
    pub registration_endpoint: Uri,
    persistent_store_endpoint: Option<String>,
    mayastor_config: Option<String>,
    pool_config: Option<String>,
    delay_subsystem_init: bool,
    enable_coredump: bool,
    env_context: Option<String>,
    hugedir: Option<String>,
    hugepage_single_segments: bool,
    json_config_file: Option<String>,
    master_core: i32,
    mem_channel: i32,
    pub mem_size: i32,
    pub name: String,
    no_pci: bool,
    num_entries: u64,
    num_pci_addr: usize,
    pci_blocklist: Vec<spdk_pci_addr>,
    pci_allowlist: Vec<spdk_pci_addr>,
    print_level: spdk_log_level,
    debug_level: spdk_log_level,
    reactor_mask: String,
    pub rpc_addr: String,
    shm_id: i32,
    shutdown_cb: spdk_app_shutdown_cb,
    tpoint_group_mask: String,
    unlink_hugepage: bool,
    log_component: Vec<String>,
    core_list: Option<String>,
    bdev_io_ctx_pool_size: u64,
    nvme_ctl_io_ctx_pool_size: u64,
}

impl Default for MayastorEnvironment {
    fn default() -> Self {
        Self {
            node_name: "mayastor-node".into(),
            grpc_endpoint: None,
            registration_endpoint: Uri::try_from("https://core:50051").unwrap(),
            persistent_store_endpoint: None,
            mayastor_config: None,
            pool_config: None,
            delay_subsystem_init: false,
            enable_coredump: true,
            env_context: None,
            hugedir: None,
            hugepage_single_segments: false,
            json_config_file: None,
            master_core: -1,
            mem_channel: -1,
            mem_size: -1,
            name: "mayastor".into(),
            no_pci: false,
            num_entries: 0,
            num_pci_addr: 0,
            pci_blocklist: vec![],
            pci_allowlist: vec![],
            print_level: SPDK_LOG_INFO,
            debug_level: SPDK_LOG_INFO,
            reactor_mask: "0x1".into(),
            rpc_addr: "/var/tmp/mayastor.sock".into(),
            shm_id: -1,
            shutdown_cb: None,
            tpoint_group_mask: String::new(),
            unlink_hugepage: true,
            log_component: vec![],
            core_list: None,
            bdev_io_ctx_pool_size: 65535,
            nvme_ctl_io_ctx_pool_size: 65535,
        }
    }
}

/// The actual routine which does the mayastor shutdown.
/// Must be called on the same thread which did the init.
async fn do_shutdown(arg: *mut c_void) {
    // we must enter the init thread explicitly here as this, typically, gets
    // called by the signal handler
    // callback for when the subsystems have shutdown
    extern "C" fn reactors_stop(arg: *mut c_void) {
        Reactors::iter().for_each(|r| r.shutdown());
        *GLOBAL_RC.lock().unwrap() = arg as i32;
    }

    let rc = arg as i32;

    if rc != 0 {
        warn!("Mayastor stopped non-zero: {}", rc);
    }

    nexus::nexus_children_to_destroying_state().await;
    crate::lvs::Lvs::export_all().await;
    unsafe {
        spdk_rpc_finish();
        spdk_subsystem_fini(Some(reactors_stop), arg);
    }
}

/// main shutdown routine for mayastor
pub fn mayastor_env_stop(rc: i32) {
    let r = Reactors::master();

    match r.get_state() {
        ReactorState::Running | ReactorState::Delayed | ReactorState::Init => {
            r.send_future(async move {
                do_shutdown(rc as *const i32 as *mut c_void).await;
            });
        }
        _ => {
            panic!("invalid reactor state during shutdown");
        }
    }
}

#[inline(always)]
unsafe extern "C" fn signal_trampoline(_: *mut c_void) {
    mayastor_env_stop(0);
}

/// called on SIGINT and SIGTERM
extern "C" fn mayastor_signal_handler(signo: i32) {
    if SIG_RECEIVED.load(SeqCst) {
        return;
    }

    warn!("Received SIGNO: {}", signo);
    SIG_RECEIVED.store(true, SeqCst);
    unsafe {
        spdk_thread_send_critical_msg(
            Mthread::get_init().into_raw(),
            Some(signal_trampoline),
        );
    };
}

#[derive(Debug)]
struct SubsystemCtx {
    rpc: CString,
    sender: futures::channel::oneshot::Sender<bool>,
}

static MAYASTOR_FEATURES: OnceCell<MayastorFeatures> = OnceCell::new();

static MAYASTOR_DEFAULT_ENV: OnceCell<MayastorEnvironment> = OnceCell::new();
impl MayastorEnvironment {
    pub fn new(args: MayastorCliArgs) -> Self {
        Self {
            grpc_endpoint: Some(grpc::endpoint(args.grpc_endpoint)),
            persistent_store_endpoint: args.persistent_store_endpoint,
            node_name: args.node_name.unwrap_or_else(|| "mayastor-node".into()),
            mayastor_config: args.mayastor_config,
            pool_config: args.pool_config,
            log_component: args.log_components,
            mem_size: args.mem_size,
            no_pci: args.no_pci,
            reactor_mask: args.reactor_mask,
            rpc_addr: args.rpc_address,
            hugedir: args.hugedir,
            env_context: args.env_context,
            core_list: args.core_list,
            bdev_io_ctx_pool_size: args.bdev_io_ctx_pool_size,
            nvme_ctl_io_ctx_pool_size: args.nvme_ctl_io_ctx_pool_size,
            ..Default::default()
        }
        .setup_static()
    }

    fn setup_static(self) -> Self {
        MAYASTOR_DEFAULT_ENV.get_or_init(|| self.clone());
        self
    }

    /// Get the global environment (first created on new)
    /// or otherwise the default one (used by the tests)
    pub fn global_or_default() -> Self {
        match MAYASTOR_DEFAULT_ENV.get() {
            Some(env) => env.clone(),
            None => MayastorEnvironment::default(),
        }
    }

    /// configure signal handling
    fn install_signal_handlers(&self) {
        unsafe {
            signal_hook::low_level::register(
                signal_hook::consts::SIGTERM,
                || mayastor_signal_handler(1),
            )
        }
        .unwrap();

        unsafe {
            signal_hook::low_level::register(
                signal_hook::consts::SIGINT,
                || mayastor_signal_handler(1),
            )
        }
        .unwrap();
    }

    /// construct an array of options to be passed to EAL and start it
    fn initialize_eal(&self) {
        let mut args = vec![CString::new(self.name.clone()).unwrap()];

        if self.mem_channel > 0 {
            args.push(
                CString::new(format!("-n {}", self.mem_channel)).unwrap(),
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

        if self.hugedir.is_some() {
            args.push(
                CString::new(format!(
                    "--huge-dir={}",
                    &self.hugedir.as_ref().unwrap().clone()
                ))
                .unwrap(),
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

        if self.unlink_hugepage {
            args.push(CString::new("--huge-unlink".to_string()).unwrap());
        }

        // set the log levels of the DPDK libs, this can be overridden by
        // setting env_context
        args.push(CString::new("--log-level=lib.eal:6").unwrap());
        args.push(CString::new("--log-level=lib.cryptodev:5").unwrap());
        args.push(CString::new("--log-level=user1:6").unwrap());
        args.push(CString::new("--match-allocations").unwrap());

        // any additional parameters we want to pass down to the eal. These
        // arguments are not checked or validated.
        if self.env_context.is_some() {
            args.extend(
                self.env_context
                    .as_ref()
                    .unwrap()
                    .split_ascii_whitespace()
                    .map(|s| CString::new(s.to_string()).unwrap())
                    .collect::<Vec<_>>(),
            );
        }

        // when -l is specified it overrules the core mask. The core mask still
        // carries our default of 0x1 such that existing testing code
        // does not require any changes.
        if let Some(list) = &self.core_list {
            args.push(CString::new(format!("-l {}", list)).unwrap());
        } else {
            args.push(
                CString::new(format!("-c {}", self.reactor_mask)).unwrap(),
            )
        }

        let mut cargs = args
            .iter()
            .map(|arg| arg.as_ptr())
            .collect::<Vec<*const c_char>>();

        cargs.push(std::ptr::null());
        debug!("EAL arguments {:?}", args);

        if unsafe {
            rte_eal_init(
                (cargs.len() as libc::c_int) - 1,
                cargs.as_ptr() as *mut *mut c_char,
            )
        } < 0
        {
            panic!("Failed to init EAL");
        }
        if unsafe { spdk_env_dpdk_post_init(false) } != 0 {
            panic!("Failed execute post setup");
        }
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
                let cflag = CString::new(flag.as_str()).unwrap();
                if spdk_log_set_flag(cflag.as_ptr(), true) != 0 {
                    return Err(EnvError::InitLog);
                }
            }

            spdk_log_set_level(self.debug_level);
            spdk_log_set_print_level(self.print_level);
            // open our log implementation which is implemented in the wrapper
            spdk_log_open(Some(spdk_rs_log));
            // our callback called defined in rust called by our wrapper
            spdk_rs::logfn = Some(logger::log_impl);
        }
        Ok(())
    }

    /// start the JSON rpc server which listens only to a local path
    extern "C" fn start_rpc(rc: i32, arg: *mut c_void) {
        let ctx = unsafe { Box::from_raw(arg as *mut SubsystemCtx) };

        if rc != 0 {
            ctx.sender.send(false).unwrap();
        } else {
            info!("RPC server listening at: {}", ctx.rpc.to_str().unwrap());
            unsafe {
                spdk_rpc_initialize(ctx.rpc.as_ptr() as *mut c_char);
                spdk_rpc_set_state(SPDK_RPC_RUNTIME);
            };

            let success = true;

            ctx.sender.send(success).unwrap();
        }
    }

    /// load the config and apply it before any subsystems have started.
    /// there is currently no run time check that enforces this.
    fn load_yaml_config(&self) {
        let cfg = if let Some(yaml) = &self.mayastor_config {
            info!("loading mayastor config YAML file {}", yaml);
            Config::get_or_init(|| {
                if let Ok(cfg) = Config::read(yaml) {
                    cfg
                } else {
                    // if the configuration is invalid exit early
                    panic!("Failed to load the mayastor configuration")
                }
            })
        } else {
            Config::get_or_init(Config::default)
        };
        cfg.apply();
    }

    /// load the pool config file.
    fn load_pool_config(&self) -> Option<PoolConfig> {
        if let Some(file) = &self.pool_config {
            info!("loading pool config file {}", file);
            match PoolConfig::load(file) {
                Ok(config) => {
                    return Some(config);
                }
                Err(error) => {
                    warn!("failed to load pool configuration: {}", error);
                }
            }
        }
        None
    }

    /// initialize the core, call this before all else
    pub fn init(mut self) -> Self {
        // setup the logger as soon as possible
        self.init_logger().unwrap();

        self.load_yaml_config();

        let pool_config = self.load_pool_config();

        // bootstrap DPDK and its magic
        self.initialize_eal();

        // initialize memory pool for allocating bdev I/O contexts
        bdev_io_ctx_pool_init(self.bdev_io_ctx_pool_size);

        // initialize memory pool for allocating NVMe controller I/O contexts
        nvme_io_ctx_pool_init(self.nvme_ctl_io_ctx_pool_size);

        info!(
            "Total number of cores available: {}",
            Cores::count().into_iter().count()
        );

        // setup our signal handlers
        self.install_signal_handlers();

        // allocate a Reactor per core
        Reactors::init();

        // launch the remote cores if any. note that during init these have to
        // be running as during setup cross call will take place.
        Cores::count()
            .into_iter()
            .for_each(|c| Reactors::launch_remote(c).unwrap());

        let rpc = CString::new(self.rpc_addr.as_str()).unwrap();

        // wait for all cores to be online, not sure if this is the right way
        // but when using more then 16 cores, I saw some "weirdness"
        // which could be related purely to logging.

        while Reactors::iter().any(|r| {
            r.get_state() == ReactorState::Init && r.core() != Cores::current()
        }) {
            std::thread::sleep(Duration::from_millis(1));
        }

        info!("All cores locked and loaded!");

        // ensure we are within the context of a spdk thread from here
        Mthread::get_init().enter();

        Reactor::block_on(async {
            let (sender, receiver) = oneshot::channel::<bool>();

            unsafe {
                spdk_subsystem_init(
                    Some(Self::start_rpc),
                    Box::into_raw(Box::new(SubsystemCtx {
                        rpc,
                        sender,
                    })) as *mut _,
                );
            }

            assert!(receiver.await.unwrap());
        });

        // load any pools that need to be created
        if let Some(config) = pool_config {
            config.import_pools();
        }

        self
    }

    // finalize our environment
    pub fn fini(&self) {
        unsafe {
            spdk_trace_cleanup();
            spdk_thread_lib_fini();
            spdk_env_fini();
            spdk_log_close();
        }
    }

    /// start mayastor and call f when all is setup.
    pub fn start<F>(self, f: F) -> Result<i32>
    where
        F: FnOnce() + 'static,
    {
        type FutureResult = Result<(), ()>;
        let grpc_endpoint = self.grpc_endpoint;
        let rpc_addr = self.rpc_addr.clone();
        let persistent_store_endpoint = self.persistent_store_endpoint.clone();
        let ms = self.init();

        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            PersistentStore::init(persistent_store_endpoint).await;
            let master = Reactors::current();
            master.send_future(async { f() });
            let mut futures: Vec<
                Pin<Box<dyn future::Future<Output = FutureResult>>>,
            > = Vec::new();
            if let Some(grpc_endpoint) = grpc_endpoint {
                futures.push(Box::pin(grpc::MayastorGrpcServer::run(
                    grpc_endpoint,
                    rpc_addr,
                )));
            }
            futures.push(Box::pin(subsys::Registration::run()));
            futures.push(Box::pin(master));
            let _out = future::try_join_all(futures).await;
            info!("reactors stopped");
            ms.fini();
        });

        Ok(*GLOBAL_RC.lock().unwrap())
    }
}
