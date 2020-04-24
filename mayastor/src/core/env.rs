use std::{
    env,
    ffi::CString,
    net::Ipv4Addr,
    os::raw::{c_char, c_void},
    sync::{Arc, Mutex},
};

use byte_unit::{Byte, ByteUnit};
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
use once_cell::sync::Lazy;
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;
use tokio::{runtime::Builder, task};

use spdk_sys::{
    maya_log,
    spdk_app_shutdown_cb,
    spdk_conf_allocate,
    spdk_conf_free,
    spdk_conf_read,
    spdk_conf_set_as_default,
    spdk_log_level,
    spdk_log_open,
    spdk_log_set_level,
    spdk_log_set_print_level,
    spdk_pci_addr,
    spdk_rpc_set_state,
    spdk_thread_lib_fini,
    SPDK_LOG_DEBUG,
    SPDK_LOG_INFO,
    SPDK_RPC_RUNTIME,
};

use crate::{
    core::{
        reactor,
        reactor::{Reactor, Reactors},
        Cores,
    },
    grpc::grpc_server_init,
    logger,
    subsys::Config,
    target,
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

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Mayastor",
    about = "Containerized Attached Storage (CAS) for k8s",
    version = "19.12.1",
    setting(structopt::clap::AppSettings::ColoredHelp)
)]
pub struct MayastorCliArgs {
    #[structopt(short = "a", default_value = "127.0.0.1", env = "MY_POD_IP")]
    /// IP address for gRPC
    pub addr: String,
    #[structopt(short = "p", default_value = "10125")]
    /// Port for gRPC
    pub port: String,
    #[structopt(short = "c")]
    /// Path to the configuration file if any
    pub config: Option<String>,
    #[structopt(short = "L")]
    /// Enable logging for sub components
    pub log_components: Vec<String>,
    #[structopt(short = "m", default_value = "0x1")]
    /// The reactor mask to be used for starting up the instance
    pub reactor_mask: String,
    /// The maximum amount of hugepage memory we are allowed to allocate in MiB
    /// (default: all)
    #[structopt(
    short = "s",
    parse(try_from_str = parse_mb),
    default_value = "0"
    )]
    pub mem_size: i32,
    #[structopt(short = "u")]
    /// Disable the use of PCIe devices
    pub no_pci: bool,
    #[structopt(short = "r", default_value = "/var/tmp/mayastor.sock")]
    /// Path to create the rpc socket
    pub rpc_address: String,
    #[structopt(short = "y")]
    /// path to mayastor config file
    pub mayastor_config: Option<String>,
    #[structopt(long = "huge-dir")]
    /// path to hugedir
    pub hugedir: Option<String>,
    #[structopt(long = "env-context")]
    /// pass additional arguments to the EAL environment
    pub env_context: Option<String>,
}

/// Defaults are redefined here in case of using it during tests
impl Default for MayastorCliArgs {
    fn default() -> Self {
        Self {
            addr: "None".into(),
            env_context: None,
            port: "None".into(),
            reactor_mask: "0x1".into(),
            mem_size: 0,
            rpc_address: "/var/tmp/mayastor.sock".to_string(),
            no_pci: true,
            log_components: vec![],
            config: None,
            mayastor_config: None,
            hugedir: None,
        }
    }
}

/// Global exit code of the program, initially set to -1 to capture double
/// shutdown during test cases
pub static GLOBAL_RC: Lazy<Arc<Mutex<i32>>> =
    Lazy::new(|| Arc::new(Mutex::new(-1)));

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
    #[snafu(display("Failed to read configuration file: {}", reason))]
    ParseConfig { reason: String },
    #[snafu(display("Failed to initialize logging subsystem"))]
    InitLog,
    #[snafu(display("Failed to initialize {} target", target))]
    InitTarget { target: String },
}

type Result<T, E = EnvError> = std::result::Result<T, E>;

/// Mayastor argument
#[derive(Debug, Clone)]
pub struct MayastorEnvironment {
    pub config: Option<String>,
    pub enable_grpc: bool,
    grpc_addr: Option<String>,
    grpc_port: Option<String>,
    mayastor_config: Option<String>,
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
    pci_blacklist: Vec<spdk_pci_addr>,
    pci_whitelist: Vec<spdk_pci_addr>,
    print_level: spdk_log_level,
    debug_level: spdk_log_level,
    reactor_mask: String,
    pub rpc_addr: String,
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
            enable_grpc: false,
            grpc_addr: None,
            grpc_port: None,
            mayastor_config: None,
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
            pci_blacklist: vec![],
            pci_whitelist: vec![],
            print_level: SPDK_LOG_INFO,
            debug_level: SPDK_LOG_INFO,
            reactor_mask: "0x1".into(),
            rpc_addr: "/var/tmp/mayastor.sock".into(),
            shm_id: -1,
            shutdown_cb: None,
            tpoint_group_mask: String::new(),
            unlink_hugepage: true,
            log_component: vec![],
        }
    }
}

/// The actual routine which does the mayastor shutdown.
/// Must be called on the same thread which did the init.
async fn do_shutdown(arg: *mut c_void) {
    // callback for when the subsystems have shutdown
    extern "C" fn reactors_stop(_arg: *mut c_void) {
        Reactors::iter().for_each(|r| r.shutdown());
    }

    let rc = arg as i32;

    if rc != 0 {
        warn!("Mayastor stopped non-zero: {}", rc);
    }

    let rc_current = *GLOBAL_RC.lock().unwrap();

    if rc_current != -1 {
        // to avoid double shutdown when we are running with in the native test
        // framework
        std::process::exit(rc_current);
    }

    *GLOBAL_RC.lock().unwrap() = rc;

    let cfg = Config::by_ref();
    if cfg.nexus_opts.iscsi_enable {
        target::iscsi::fini();
        debug!("iSCSI target down");
    };

    if cfg.nexus_opts.nvmf_enable {
        let f = async move {
            if let Err(msg) = target::nvmf::fini().await {
                error!("Failed to finalize nvmf target: {}", msg);
            }
            debug!("nvmf target down");
        };
        f.await;
    }

    unsafe {
        spdk_rpc_finish();
        spdk_subsystem_fini(Some(reactors_stop), std::ptr::null_mut());
    }
}

/// main shutdown routine for mayastor
pub fn mayastor_env_stop(rc: i32) {
    let r = Reactors::get_by_core(Cores::first()).unwrap();

    match r.get_sate() {
        reactor::INIT => {
            Reactor::block_on(async move {
                do_shutdown(rc as *const i32 as *mut c_void).await;
            });
        }
        reactor::RUNNING | reactor::DEVELOPER_DELAY => {
            r.send_future(async move {
                do_shutdown(rc as *const i32 as *mut c_void).await;
            });
        }
        _ => {
            panic!("invalid state reactor state during shutdown");
        }
    }
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
            grpc_addr: Some(args.addr),
            grpc_port: Some(args.port),
            config: args.config,
            mayastor_config: args.mayastor_config,
            log_component: args.log_components,
            mem_size: args.mem_size,
            no_pci: args.no_pci,
            reactor_mask: args.reactor_mask,
            rpc_addr: args.rpc_address,
            hugedir: args.hugedir,
            env_context: args.env_context,
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
        args.push(CString::new("--log-level=lib.cryptodev:0").unwrap());
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
            spdk_sys::logfn = Some(logger::log_impl);
        }
        Ok(())
    }

    /// We implement our own default target init code here. Note that if there
    /// is an existing target we will fail the init process.
    extern "C" fn target_init() -> Result<(), EnvError> {
        let address = MayastorEnvironment::get_pod_ip().map_err(|e| {
            error!("Invalid IP address: MY_POD_IP={}", e);
            mayastor_env_stop(-1);
            EnvError::InitLog
        })?;

        let cfg = Config::by_ref();

        if cfg.nexus_opts.iscsi_enable {
            if let Err(msg) = target::iscsi::init(&address) {
                error!("Failed to initialize Mayastor iSCSI target: {}", msg);
                return Err(EnvError::InitTarget {
                    target: "iscsi".into(),
                });
            }
        }

        if cfg.nexus_opts.nvmf_enable {
            let f = async move {
                if let Err(msg) = target::nvmf::init(&address).await {
                    error!(
                        "Failed to initialize Mayastor nvmf target: {}",
                        msg
                    );
                    mayastor_env_stop(-1);
                }
            };

            Reactor::block_on(f);
        }

        Ok(())
    }

    fn get_pod_ip() -> Result<String, String> {
        match env::var("MY_POD_IP") {
            Ok(val) => {
                if val.parse::<Ipv4Addr>().is_ok() {
                    Ok(val)
                } else {
                    Err(val)
                }
            }
            Err(_) => Ok("127.0.0.1".to_owned()),
        }
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

    fn load_yaml_config(&self) {
        // load the config and apply it before any subsystems have started.
        // there is currently no run time check that enforces this.
        let cfg = if let Some(yaml) = &self.mayastor_config {
            info!("loading YAML config file {}", yaml);
            Config::get_or_init(|| {
                if let Ok(cfg) = Config::read(yaml) {
                    cfg
                } else {
                    // if the configuration is invalid exit early
                    std::process::exit(-1);
                }
            })
        } else {
            Config::get_or_init(Config::default)
        };
        cfg.apply();
    }
    /// initialize the core, call this before all else
    pub fn init(mut self) -> Self {
        self.load_yaml_config();
        // load the .ini format file, still here to allow CI passing. There is
        // no real harm of loading this ini file as long as there are no
        // conflicting bdev definitions
        self.read_config_file().unwrap();

        // bootstrap DPDK and its magic
        self.initialize_eal();

        // setup the logger as soon as possible
        self.init_logger().unwrap();
        if self.enable_coredump {
            //TODO
            warn!("rlimit configuration not implemented");
        }

        info!(
            "Total number of cores available: {}",
            Cores::count().into_iter().count()
        );

        // setup our signal handlers
        self.install_signal_handlers().unwrap();

        // allocate a Reactor per core
        Reactors::init();

        // launch the remote cores if any. note that during init these have to
        // be running as during setup cross call will take place.

        Cores::count()
            .into_iter()
            .for_each(|c| Reactors::launch_remote(c).unwrap());

        let rpc = CString::new(self.rpc_addr.as_str()).unwrap();

        // register our RPC methods
        crate::pool::register_pool_methods();
        crate::replica::register_replica_methods();

        // init the subsystems
        Reactor::block_on(async {
            unsafe {
                spdk_subsystem_init(Some(Self::start_rpc), rpc.into_raw() as _);
            }
        });

        // load any bdevs that need to be created
        Config::by_ref().import_bdevs();

        self
    }

    // finalize our environment
    fn fini() {
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
        let grpc = self.enable_grpc;

        let grpc_addr = self.grpc_addr.clone();
        let grpc_port = self.grpc_port.clone();
        self.init();

        let mut rt = Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap();

        let local = task::LocalSet::new();
        rt.block_on(async {
            local
                .run_until(async {
                    let master = Reactors::current();
                    master.send_future(async { f() });
                    if grpc {
                        let _out = tokio::try_join!(
                            grpc_server_init(
                                &grpc_addr.as_ref().unwrap(),
                                &grpc_port.as_ref().unwrap()
                            ),
                            master
                        );
                    } else {
                        let _out = master.await;
                    };
                    info!("reactors stopped....");
                    Self::fini();
                })
                .await
        });

        Ok(*GLOBAL_RC.lock().unwrap())
    }
}
