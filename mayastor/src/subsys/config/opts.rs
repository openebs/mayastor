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
};

use crate::bdev::ActionType;

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
    pub opts: TcpTransportOpts,
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
            opts: TcpTransportOpts::default(),
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
pub struct TcpTransportOpts {
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
    /// max admin queue depth (?)
    max_aq_depth: u32,
    /// num of shared buffers
    num_shared_buf: u32,
    /// cache size
    buf_cache_size: u32,
    /// RDMA only
    max_srq_depth: u32,
    /// RDMA only
    no_srq: bool,
    /// optimize success
    ch2_success: bool,
    /// dif
    dif_insert_or_strip: bool,
    /// no idea
    sock_priority: u32,
}

impl Default for TcpTransportOpts {
    fn default() -> Self {
        Self {
            max_queue_depth: 64,
            in_capsule_data_size: 4096,
            max_io_size: 131_072,
            io_unit_size: 131_072,
            ch2_success: true,
            max_qpairs_per_ctrl: 128,
            num_shared_buf: 511,
            // reduce when we have a single target
            buf_cache_size: 64,
            dif_insert_or_strip: false,
            max_aq_depth: 128,
            max_srq_depth: 0, // RDMA
            no_srq: false,    // RDMA
            sock_priority: 0,
        }
    }
}

/// we cannot add derives for YAML to these structs directly, so we need to
/// copy them. The upside though, is that if the FFI structures change, we will
/// know about it during compile time.
impl From<TcpTransportOpts> for spdk_nvmf_transport_opts {
    fn from(o: TcpTransportOpts) -> Self {
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
            abort_timeout_sec: 0,
            association_timeout: 120000,
            transport_specific: std::ptr::null(),
        }
    }
}

/// generic settings for the NVMe bdev (all our replicas)
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NvmeBdevOpts {
    /// action take on timeout
    action_on_timeout: u32,
    /// timeout for each command
    timeout_us: u64,
    /// retry count
    retry_count: u32,
    /// TODO
    arbitration_burst: u32,
    /// max number of low priority cmds a controller may launch at one time
    low_priority_weight: u32,
    /// max number of medium priority cmds a controller may launch at one time
    medium_priority_weight: u32,
    /// max number of high priority cmds a controller may launch at one time
    high_priority_weight: u32,
    /// admin queue polling period
    nvme_adminq_poll_period_us: u64,
    /// ioq polling period
    nvme_ioq_poll_period_us: u64,
    /// number of requests per nvme IO queue
    io_queue_requests: u32,
    /// allow for batching of commands
    delay_cmd_submit: bool,
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
        if unsafe { bdev_nvme_set_opts(Box::into_raw(opts)) } != 0 {
            return false;
        }
        true
    }
}

impl Default for NvmeBdevOpts {
    fn default() -> Self {
        Self {
            action_on_timeout: 1,
            timeout_us: 2_000_000,
            retry_count: 5,
            arbitration_burst: 0,
            low_priority_weight: 0,
            medium_priority_weight: 0,
            high_priority_weight: 0,
            nvme_adminq_poll_period_us: 10_000,
            nvme_ioq_poll_period_us: 0,
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
}

impl GetOpts for BdevOpts {
    fn get(&self) -> Self {
        let opts = spdk_bdev_opts::default();
        unsafe {
            spdk_sys::spdk_bdev_get_opts(
                &opts as *const _ as *mut spdk_bdev_opts,
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
            bdev_io_pool_size: 65535,
            bdev_io_cache_size: 512,
        }
    }
}

impl From<spdk_bdev_opts> for BdevOpts {
    fn from(o: spdk_bdev_opts) -> Self {
        Self {
            bdev_io_pool_size: o.bdev_io_pool_size,
            bdev_io_cache_size: o.bdev_io_cache_size,
        }
    }
}

impl From<&BdevOpts> for spdk_bdev_opts {
    fn from(o: &BdevOpts) -> Self {
        Self {
            bdev_io_pool_size: o.bdev_io_pool_size,
            bdev_io_cache_size: o.bdev_io_cache_size,
            bdev_auto_examine: false,
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
            timeout: 5,
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
