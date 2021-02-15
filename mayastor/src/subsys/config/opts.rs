//! The structure copying is needed because we cannot impl things for foreign
//! types. Naturally this is a good reason, but it means we have to copy things
//! around. If the structures change, we will know about it because we use the
//! from trait, and we are not allowed to skip or use different types.
use std::ptr::copy_nonoverlapping;

use serde::{Deserialize, Serialize};

use spdk_sys::{
    bdev_nvme_get_opts,
    bdev_nvme_set_opts,
    iscsi_opts_copy,
    spdk_bdev_nvme_opts,
    spdk_bdev_opts,
    spdk_bdev_set_opts,
    spdk_iscsi_opts,
    spdk_nvmf_target_opts,
    spdk_nvmf_transport_opts,
    spdk_sock_impl_get_opts,
    spdk_sock_impl_opts,
    spdk_sock_impl_set_opts,
    SPDK_BDEV_NVME_TIMEOUT_ACTION_ABORT,
};

use crate::bdev::ActionType;
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

pub trait GetOpts {
    fn get(&self) -> Self;
    fn set(&self) -> bool {
        true
    }
}

#[serde(default, deny_unknown_fields)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NexusOpts {
    /// enable nvmf target
    pub nvmf_enable: bool,
    /// enable the nvmf discovery subsystem
    pub nvmf_discovery_enable: bool,
    /// nvmf port over which we export
    pub nvmf_nexus_port: u16,
    /// NOTE: we do not (yet) differentiate between
    /// the nexus and replica nvmf target
    pub nvmf_replica_port: u16,
    /// enable iSCSI support
    pub iscsi_enable: bool,
    /// Port for nexus target portal
    pub iscsi_nexus_port: u16,
    /// Port for replica target portal
    pub iscsi_replica_port: u16,
}

/// Default nvmf port used for replicas.
/// It's different from the standard nvmf port 4420 because we don't want
/// to conflict with nexus exported over nvmf running on the same node.
const NVMF_PORT_REPLICA: u16 = 8420;
const NVMF_PORT_NEXUS: u16 = 4421;

/// Default iSCSI target (portal) port numbers
const ISCSI_PORT_NEXUS: u16 = 3260;
const ISCSI_PORT_REPLICA: u16 = 3262;

impl Default for NexusOpts {
    fn default() -> Self {
        Self {
            nvmf_enable: true,
            nvmf_discovery_enable: true,
            nvmf_nexus_port: NVMF_PORT_NEXUS,
            nvmf_replica_port: NVMF_PORT_REPLICA,
            iscsi_enable: true,
            iscsi_nexus_port: ISCSI_PORT_NEXUS,
            iscsi_replica_port: ISCSI_PORT_REPLICA,
        }
    }
}

impl GetOpts for NexusOpts {
    fn get(&self) -> Self {
        self.clone()
    }
}

#[serde(default, deny_unknown_fields)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NvmfTgtConfig {
    /// name of the target to be created
    pub name: String,
    /// the max number of namespaces this target should allow for
    pub max_namespaces: u32,
    /// TCP transport options
    pub opts: NvmfTcpTransportOpts,
}

impl From<NvmfTgtConfig> for Box<spdk_nvmf_target_opts> {
    fn from(o: NvmfTgtConfig) -> Self {
        let mut out = Self::default();
        unsafe {
            copy_nonoverlapping(
                o.name.as_ptr(),
                &mut out.name[0] as *const _ as *mut _,
                256,
            )
        };
        out.max_subsystems = o.max_namespaces;
        out
    }
}

impl Default for NvmfTgtConfig {
    fn default() -> Self {
        Self {
            name: "mayastor_target".to_string(),
            max_namespaces: 110,
            opts: NvmfTcpTransportOpts::default(),
        }
    }
}

impl GetOpts for NvmfTgtConfig {
    fn get(&self) -> Self {
        self.clone()
    }
}

/// Settings for the TCP transport
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NvmfTcpTransportOpts {
    /// max queue depth
    max_queue_depth: u16,
    /// max qpairs per controller
    max_qpairs_per_ctrl: u16,
    /// encapsulated data size
    in_capsule_data_size: u32,
    /// max IO size
    max_io_size: u32,
    /// IO unit size
    io_unit_size: u32,
    /// max admin queue depth per admin queue
    max_aq_depth: u32,
    /// num of shared buffers
    num_shared_buf: u32,
    /// cache size
    buf_cache_size: u32,
    /// dif
    dif_insert_or_strip: bool,
    /// abort execution timeout
    abort_timeout_sec: u32,
}

/// try to read an env variable or returns the default when not found
fn try_from_env<T>(name: &str, default: T) -> T
where
    T: FromStr + Display + Copy,
    <T as FromStr>::Err: Debug + Display,
{
    std::env::var(name).map_or_else(
        |_| default,
        |v| {
            match v.parse::<T>() {
               Ok(val) => {
                   info!("Overriding {} value to '{}'", name, val);
                   val
               },
               Err(e) => {
                   error!("Invalid value: {} (error {}) specified for {}. Reverting to default ({})", v, e, name, default);
                   default
               }
            }
        },
    )
}

impl Default for NvmfTcpTransportOpts {
    fn default() -> Self {
        Self {
            max_queue_depth: try_from_env("NVMF_TCP_MAX_QUEUE_DEPTH", 64),
            in_capsule_data_size: 4096,
            max_io_size: 131_072,
            io_unit_size: 131_072,
            max_qpairs_per_ctrl: 128,
            num_shared_buf: try_from_env("NVMF_TCP_NUM_SHARED_BUF", 2048),
            buf_cache_size: try_from_env("NVMF_TCP_BUF_CACHE_SIZE", 64),
            dif_insert_or_strip: false,
            max_aq_depth: 128,
            abort_timeout_sec: 1,
        }
    }
}

/// we cannot add derives for YAML to these structs directly, so we need to
/// copy them. The upside though, is that if the FFI structures change, we will
/// know about it during compile time.
impl From<NvmfTcpTransportOpts> for spdk_nvmf_transport_opts {
    fn from(o: NvmfTcpTransportOpts) -> Self {
        Self {
            max_queue_depth: o.max_queue_depth,
            max_qpairs_per_ctrlr: o.max_qpairs_per_ctrl,
            in_capsule_data_size: o.in_capsule_data_size,
            max_io_size: o.max_io_size,
            io_unit_size: o.io_unit_size,
            max_aq_depth: o.max_aq_depth,
            num_shared_buffers: o.num_shared_buf,
            buf_cache_size: o.buf_cache_size,
            dif_insert_or_strip: o.dif_insert_or_strip,
            abort_timeout_sec: o.abort_timeout_sec,
            association_timeout: 120000,
            transport_specific: std::ptr::null(),
            opts_size: std::mem::size_of::<spdk_nvmf_transport_opts>() as u64,
        }
    }
}

/// generic settings for the NVMe bdev (all our replicas)
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NvmeBdevOpts {
    /// action take on timeout
    pub action_on_timeout: u32,
    /// timeout for each command
    pub timeout_us: u64,
    /// keep-alive timeout
    pub keep_alive_timeout_ms: u32,
    /// retry count
    pub retry_count: u32,
    /// TODO
    pub arbitration_burst: u32,
    /// max number of low priority cmds a controller may launch at one time
    pub low_priority_weight: u32,
    /// max number of medium priority cmds a controller may launch at one time
    pub medium_priority_weight: u32,
    /// max number of high priority cmds a controller may launch at one time
    pub high_priority_weight: u32,
    /// admin queue polling period
    pub nvme_adminq_poll_period_us: u64,
    /// ioq polling period
    pub nvme_ioq_poll_period_us: u64,
    /// number of requests per nvme IO queue
    pub io_queue_requests: u32,
    /// allow for batching of commands
    pub delay_cmd_submit: bool,
}

impl GetOpts for NvmeBdevOpts {
    fn get(&self) -> Self {
        let opts = spdk_bdev_nvme_opts::default();
        unsafe {
            bdev_nvme_get_opts(&opts as *const _ as *mut spdk_bdev_nvme_opts)
        };
        opts.into()
    }

    fn set(&self) -> bool {
        let opts = Box::new(self.into());
        debug!("{:?}", &opts);
        if unsafe { bdev_nvme_set_opts(Box::into_raw(opts)) } != 0 {
            return false;
        }
        true
    }
}

impl Default for NvmeBdevOpts {
    fn default() -> Self {
        Self {
            action_on_timeout: SPDK_BDEV_NVME_TIMEOUT_ACTION_ABORT,
            timeout_us: try_from_env("NVME_TIMEOUT_US", 30_000_000),
            keep_alive_timeout_ms: try_from_env("NVME_KATO_MS", 10_000),
            retry_count: try_from_env("NVME_RETRY_COUNT", 3),
            arbitration_burst: 0,
            low_priority_weight: 0,
            medium_priority_weight: 0,
            high_priority_weight: 0,
            nvme_adminq_poll_period_us: try_from_env(
                "NVME_ADMINQ_POLL_PERIOD_US",
                0,
            ),
            nvme_ioq_poll_period_us: try_from_env("NVME_IOQ_POLL_PERIOD_US", 0),
            io_queue_requests: 0,
            delay_cmd_submit: true,
        }
    }
}

impl From<spdk_bdev_nvme_opts> for NvmeBdevOpts {
    fn from(o: spdk_bdev_nvme_opts) -> Self {
        Self {
            action_on_timeout: o.action_on_timeout,
            timeout_us: o.timeout_us,
            keep_alive_timeout_ms: o.keep_alive_timeout_ms,
            retry_count: o.retry_count,
            arbitration_burst: o.arbitration_burst,
            low_priority_weight: o.low_priority_weight,
            medium_priority_weight: o.medium_priority_weight,
            high_priority_weight: o.high_priority_weight,
            nvme_adminq_poll_period_us: o.nvme_adminq_poll_period_us,
            nvme_ioq_poll_period_us: o.nvme_ioq_poll_period_us,
            io_queue_requests: o.io_queue_requests,
            delay_cmd_submit: o.delay_cmd_submit,
        }
    }
}

impl From<&NvmeBdevOpts> for spdk_bdev_nvme_opts {
    fn from(o: &NvmeBdevOpts) -> Self {
        Self {
            action_on_timeout: o.action_on_timeout,
            timeout_us: o.timeout_us,
            keep_alive_timeout_ms: o.keep_alive_timeout_ms,
            retry_count: o.retry_count,
            arbitration_burst: o.arbitration_burst,
            low_priority_weight: o.low_priority_weight,
            medium_priority_weight: o.medium_priority_weight,
            high_priority_weight: o.high_priority_weight,
            nvme_adminq_poll_period_us: o.nvme_adminq_poll_period_us,
            nvme_ioq_poll_period_us: o.nvme_ioq_poll_period_us,
            io_queue_requests: o.io_queue_requests,
            delay_cmd_submit: o.delay_cmd_submit,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BdevOpts {
    /// number of bdev IO structures in the shared mempool
    bdev_io_pool_size: u32,
    /// number of bdev IO structures cached per thread
    bdev_io_cache_size: u32,
    /// small buffer pool size
    small_buf_pool_size: u32,
    /// large buffer pool size
    large_buf_pool_size: u32,
}

impl GetOpts for BdevOpts {
    fn get(&self) -> Self {
        let opts = spdk_bdev_opts::default();
        unsafe {
            spdk_sys::spdk_bdev_get_opts(
                &opts as *const _ as *mut spdk_bdev_opts,
                std::mem::size_of::<spdk_bdev_opts>() as u64,
            )
        };
        opts.into()
    }

    fn set(&self) -> bool {
        let opts = Box::new(self.into());
        if unsafe { spdk_bdev_set_opts(Box::into_raw(opts)) } != 0 {
            return false;
        }
        true
    }
}

impl Default for BdevOpts {
    fn default() -> Self {
        Self {
            bdev_io_pool_size: try_from_env("BDEV_IO_POOL_SIZE", 65535),
            bdev_io_cache_size: try_from_env("BDEV_IO_CACHE_SIZE", 512),
            small_buf_pool_size: try_from_env("BDEV_SMALL_BUF_POOL_SIZE", 8191),
            large_buf_pool_size: try_from_env("BDEV_LARGE_BUF_POOL_SIZE", 1023),
        }
    }
}

impl From<spdk_bdev_opts> for BdevOpts {
    fn from(o: spdk_bdev_opts) -> Self {
        Self {
            bdev_io_pool_size: o.bdev_io_pool_size,
            bdev_io_cache_size: o.bdev_io_cache_size,
            small_buf_pool_size: o.small_buf_pool_size,
            large_buf_pool_size: o.large_buf_pool_size,
        }
    }
}

impl From<&BdevOpts> for spdk_bdev_opts {
    fn from(o: &BdevOpts) -> Self {
        Self {
            bdev_io_pool_size: o.bdev_io_pool_size,
            bdev_io_cache_size: o.bdev_io_cache_size,
            bdev_auto_examine: false,
            opts_size: std::mem::size_of::<spdk_bdev_opts>() as u64,
            small_buf_pool_size: o.small_buf_pool_size,
            large_buf_pool_size: o.large_buf_pool_size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IscsiTgtOpts {
    authfile: String,
    /// none iqn name
    nodebase: String,
    /// timeout in seconds
    timeout: i32,
    /// nop interval in seconds
    nop_ininterval: i32,
    /// chap enabled
    disable_chap: bool,
    /// chap is required
    require_chap: bool,
    /// mutual chap
    mutual_chap: bool,
    /// chap group
    chap_group: i32,
    /// max number of sessions in the host
    max_sessions: u32,
    /// max connections per session
    max_connections_per_session: u32,
    /// max connections
    max_connections: u32,
    /// max queue depth
    max_queue_depth: u32,
    /// default
    default_time2wait: u32,
    /// todo
    default_time2retain: u32,
    /// todo
    first_burst_length: u32,
    ///todo
    immediate_data: bool,
    /// todo
    error_recovery_level: u32,
    /// todo
    allow_duplicate_isid: bool,
    /// todo
    max_large_data_in_per_connection: u32,
    /// todo
    max_r2t_per_connection: u32,
}

impl Default for IscsiTgtOpts {
    fn default() -> Self {
        Self {
            authfile: "".to_string(),
            nodebase: "iqn.2019-05.io.openebs".to_string(),
            timeout: try_from_env("ISCSI_TIMEOUT_SEC", 30),
            nop_ininterval: 1,
            disable_chap: false,
            require_chap: false,
            mutual_chap: false,
            chap_group: 0,
            max_sessions: 128,
            max_connections_per_session: 2,
            max_connections: 1024,
            max_queue_depth: 128,
            default_time2wait: 2,
            default_time2retain: 20,
            first_burst_length: 8192,
            immediate_data: true,
            error_recovery_level: 0,
            allow_duplicate_isid: false,
            max_large_data_in_per_connection: 64,
            max_r2t_per_connection: 64,
        }
    }
}

impl From<&IscsiTgtOpts> for spdk_iscsi_opts {
    fn from(o: &IscsiTgtOpts) -> Self {
        Self {
            authfile: std::ffi::CString::new(o.authfile.clone())
                .unwrap()
                .into_raw(),
            nodebase: std::ffi::CString::new(o.nodebase.clone())
                .unwrap()
                .into_raw(),
            timeout: o.timeout,
            nopininterval: o.nop_ininterval,
            disable_chap: o.disable_chap,
            require_chap: o.require_chap,
            mutual_chap: o.mutual_chap,
            chap_group: o.chap_group,
            MaxSessions: o.max_sessions,
            MaxConnectionsPerSession: o.max_connections_per_session,
            MaxConnections: o.max_connections,
            MaxQueueDepth: o.max_queue_depth,
            DefaultTime2Wait: o.default_time2wait,
            DefaultTime2Retain: o.default_time2wait,
            FirstBurstLength: o.first_burst_length,
            ImmediateData: o.immediate_data,
            ErrorRecoveryLevel: o.error_recovery_level,
            AllowDuplicateIsid: o.allow_duplicate_isid,
            MaxLargeDataInPerConnection: o.max_large_data_in_per_connection,
            MaxR2TPerConnection: o.max_r2t_per_connection,
        }
    }
}

extern "C" {
    /// global shared variable defined by tgt implementation
    static mut g_spdk_iscsi_opts: *mut spdk_iscsi_opts;
}

impl GetOpts for IscsiTgtOpts {
    fn get(&self) -> Self {
        // as per the set method, g_spdk_iscsi_opts is not set to NULL. We can
        // not get the information back unless we read and parse g_spdk_iscsi.
        // as the options cannot change, we do not bother.
        self.clone()
    }

    fn set(&self) -> bool {
        unsafe {
            // spdk_iscsi_opts_copy copies our struct to a new portion of
            // memory and returns a pointer to it which we store into the
            // defined global. Later on, when iscsi initializes, those options
            // are verified and then -- copied to g_spdk_iscsi. Once they
            // are copied g_spdk_iscsi_opts is freed.
            g_spdk_iscsi_opts = iscsi_opts_copy(&mut self.into());

            if g_spdk_iscsi_opts.is_null() {
                panic!("iSCSI_init failed");
            }
        }

        true
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PosixSocketOpts {
    recv_buf_size: u32,
    send_buf_size: u32,
    enable_recv_pipe: bool,
    enable_zero_copy_send: bool,
    enable_quickack: bool,
    enable_placement_id: bool,
}

impl Default for PosixSocketOpts {
    fn default() -> Self {
        Self {
            recv_buf_size: try_from_env("SOCK_RECV_BUF_SIZE", 2097152),
            send_buf_size: try_from_env("SOCK_SEND_BUF_SIZE", 2097152),
            enable_recv_pipe: try_from_env("SOCK_ENABLE_RECV_PIPE", true),
            enable_zero_copy_send: try_from_env("SOCK_ZERO_COPY_SEND", true),
            enable_quickack: try_from_env("SOCK_ENABLE_QUICKACK", true),
            enable_placement_id: try_from_env("SOCK_ENABLE_PLACEMENT_ID", true),
        }
    }
}

impl GetOpts for PosixSocketOpts {
    fn get(&self) -> Self {
        let opts = spdk_sock_impl_opts::default();

        unsafe {
            let name = std::ffi::CString::new("posix").unwrap();
            let mut size = std::mem::size_of::<spdk_sock_impl_opts>() as u64;
            let rc = spdk_sock_impl_get_opts(
                name.as_ptr(),
                &opts as *const _ as *mut spdk_sock_impl_opts,
                &mut size,
            );
            assert_eq!(rc, 0);
        };

        Self {
            recv_buf_size: opts.recv_buf_size,
            send_buf_size: opts.send_buf_size,
            enable_recv_pipe: opts.enable_recv_pipe,
            enable_zero_copy_send: opts.enable_zerocopy_send,
            enable_quickack: opts.enable_quickack,
            enable_placement_id: opts.enable_placement_id,
        }
    }

    fn set(&self) -> bool {
        let opts = spdk_sock_impl_opts {
            recv_buf_size: self.recv_buf_size,
            send_buf_size: self.send_buf_size,
            enable_recv_pipe: self.enable_recv_pipe,
            enable_zerocopy_send: self.enable_zero_copy_send,
            enable_quickack: self.enable_quickack,
            enable_placement_id: self.enable_placement_id,
        };

        let size = std::mem::size_of::<spdk_sock_impl_opts>() as u64;
        unsafe {
            let name = std::ffi::CString::new("posix").unwrap();
            let rc = spdk_sock_impl_set_opts(
                name.as_ptr(),
                &opts as *const _ as *mut spdk_sock_impl_opts,
                size,
            );
            rc == 0
        }
    }
}

#[serde(default, deny_unknown_fields)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrStoreOpts {
    /// ring buffer size
    pub err_store_size: usize,

    /// NexusErrStore enabled
    pub enable_err_store: bool,

    /// whether to fault the child due to the total number of failed IOs
    pub action: ActionType,

    /// the maximum number of errors in total
    pub max_errors: u32,

    /// errors older than this are ignored
    pub retention_ns: u64,

    /// the maximum number of IO attempts per IO
    pub max_io_attempts: i32,
}

impl Default for ErrStoreOpts {
    fn default() -> Self {
        Self {
            err_store_size: 256,
            enable_err_store: true,
            action: ActionType::Fault,
            max_errors: 64,
            retention_ns: 10_000_000_000,
            max_io_attempts: 1,
        }
    }
}

impl GetOpts for ErrStoreOpts {
    fn get(&self) -> Self {
        self.clone()
    }
}
