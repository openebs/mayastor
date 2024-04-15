use crate::{
    bdev::{nexus, NvmeControllerState},
    core::{BlockDeviceIoStats, CoreError, MayastorFeatures},
    grpc::{
        controller_grpc::{
            controller_stats,
            list_controllers,
            NvmeControllerInfo,
        },
        rpc_submit,
        GrpcClientContext,
        GrpcResult,
        Serializer,
    },
    host::{blk_device, resource},
    subsys::{registration::registration_grpc::ApiVersion, Registration},
};
use ::function_name::named;
use futures::FutureExt;
use io_engine_api::v1::{host as host_rpc, registration::RegisterRequest};
use std::panic::AssertUnwindSafe;
use tonic::{Request, Response, Status};
use version_info::raw_version_string;

/// RPC service for generic host machine and mayastor instance related
/// operations
#[derive(Debug)]
pub struct HostService {
    #[allow(dead_code)]
    name: String,
    node_name: String,
    node_nqn: Option<String>,
    grpc_socket: std::net::SocketAddr,
    api_versions: Vec<ApiVersion>,
    client_context: tokio::sync::Mutex<Option<GrpcClientContext>>,
}

#[async_trait::async_trait]
impl<F, T> Serializer<F, T> for HostService
where
    T: Send + 'static,
    F: core::future::Future<Output = Result<T, Status>> + Send + 'static,
{
    async fn locked(&self, ctx: GrpcClientContext, f: F) -> Result<T, Status> {
        let mut context_guard = self.client_context.lock().await;

        // Store context as a marker of to detect abnormal termination of the
        // request. Even though AssertUnwindSafe() allows us to
        // intercept asserts in underlying method strategies, such a
        // situation can still happen when the high-level future that
        // represents gRPC call at the highest level (i.e. the one created
        // by gRPC server) gets cancelled (due to timeout or somehow else).
        // This can't be properly intercepted by 'locked' function itself in the
        // first place, so the state needs to be cleaned up properly
        // upon subsequent gRPC calls.
        if let Some(c) = context_guard.replace(ctx) {
            warn!("{}: gRPC method timed out, args: {}", c.id, c.args);
        }

        let fut = AssertUnwindSafe(f).catch_unwind();
        let r = fut.await;

        // Request completed, remove the marker.
        let ctx = context_guard.take().expect("gRPC context disappeared");

        match r {
            Ok(r) => r,
            Err(_e) => {
                warn!("{}: gRPC method panicked, args: {}", ctx.id, ctx.args);
                Err(Status::cancelled(format!(
                    "{}: gRPC method panicked",
                    ctx.id
                )))
            }
        }
    }
}

impl HostService {
    /// Return a new `Self` with the given host parameters.
    pub fn new(
        node_name: &str,
        node_nqn: &Option<String>,
        grpc_socket: std::net::SocketAddr,
        api_versions: Vec<ApiVersion>,
    ) -> Self {
        Self {
            name: String::from("MayastorService"),
            node_name: node_name.to_string(),
            node_nqn: node_nqn.clone(),
            grpc_socket,
            api_versions,
            client_context: tokio::sync::Mutex::new(None),
        }
    }
}

impl From<MayastorFeatures> for host_rpc::MayastorFeatures {
    fn from(f: MayastorFeatures) -> Self {
        Self {
            asymmetric_namespace_access: f.asymmetric_namespace_access,
        }
    }
}

impl From<blk_device::BlockDevice> for host_rpc::BlockDevice {
    fn from(b: blk_device::BlockDevice) -> Self {
        Self {
            devname: b.devname,
            devtype: b.devtype,
            devmajor: b.devmaj,
            devminor: b.devmin,
            model: b.model,
            devpath: b.devpath,
            devlinks: b.devlinks,
            size: b.size,
            partition: b.partition.map(host_rpc::Partition::from),
            filesystem: b.filesystem.map(host_rpc::Filesystem::from),
            available: b.available,
            connection_type: b.connection_type,
            is_rotational: b.is_rotational,
        }
    }
}

impl From<blk_device::FileSystem> for host_rpc::Filesystem {
    fn from(fs: blk_device::FileSystem) -> Self {
        Self {
            fstype: fs.fstype,
            label: fs.label,
            uuid: fs.uuid,
            mountpoints: fs.mountpoints,
        }
    }
}

impl From<blk_device::Partition> for host_rpc::Partition {
    fn from(p: blk_device::Partition) -> Self {
        Self {
            parent: p.parent,
            number: p.number,
            name: p.name,
            scheme: p.scheme,
            typeid: p.typeid,
            uuid: p.uuid,
        }
    }
}

impl From<resource::Usage> for host_rpc::ResourceUsage {
    fn from(usage: resource::Usage) -> Self {
        let rusage = usage.0;
        Self {
            soft_faults: rusage.ru_minflt,
            hard_faults: rusage.ru_majflt,
            swaps: rusage.ru_nswap,
            in_block_ops: rusage.ru_inblock,
            out_block_ops: rusage.ru_oublock,
            ipc_msg_send: rusage.ru_msgsnd,
            ipc_msg_rcv: rusage.ru_msgrcv,
            signals: rusage.ru_nsignals,
            vol_csw: rusage.ru_nvcsw,
            invol_csw: rusage.ru_nivcsw,
        }
    }
}

impl From<NvmeControllerInfo> for host_rpc::NvmeController {
    fn from(n: NvmeControllerInfo) -> Self {
        Self {
            name: n.name,
            state: host_rpc::NvmeControllerState::from(n.state) as i32,
            size: n.size,
            blk_size: n.blk_size,
        }
    }
}

impl From<NvmeControllerState> for host_rpc::NvmeControllerState {
    fn from(state: NvmeControllerState) -> Self {
        match state {
            NvmeControllerState::New => host_rpc::NvmeControllerState::New,
            NvmeControllerState::Initializing => {
                host_rpc::NvmeControllerState::Initializing
            }
            NvmeControllerState::Running => {
                host_rpc::NvmeControllerState::Running
            }
            NvmeControllerState::Faulted(_) => {
                host_rpc::NvmeControllerState::Faulted
            }
            NvmeControllerState::Unconfiguring => {
                host_rpc::NvmeControllerState::Unconfiguring
            }
            NvmeControllerState::Unconfigured => {
                host_rpc::NvmeControllerState::Unconfigured
            }
        }
    }
}

impl From<BlockDeviceIoStats> for host_rpc::NvmeControllerIoStats {
    fn from(b: BlockDeviceIoStats) -> Self {
        Self {
            num_read_ops: b.num_read_ops,
            num_write_ops: b.num_write_ops,
            bytes_read: b.bytes_read,
            bytes_written: b.bytes_written,
            num_unmap_ops: b.num_unmap_ops,
            bytes_unmapped: b.bytes_unmapped,
        }
    }
}

#[tonic::async_trait]
impl host_rpc::HostRpc for HostService {
    async fn get_mayastor_info(
        &self,
        _request: Request<()>,
    ) -> GrpcResult<host_rpc::MayastorInfoResponse> {
        let features = MayastorFeatures::get_features().into();
        let api_versions = self
            .api_versions
            .iter()
            .map(|v| {
                let api_version: io_engine_api::v1::registration::ApiVersion =
                    v.clone().into();
                api_version as i32
            })
            .collect();

        let response = host_rpc::MayastorInfoResponse {
            version: raw_version_string(),
            supported_features: Some(features),
            registration_info: Some(RegisterRequest {
                id: self.node_name.clone(),
                grpc_endpoint: self.grpc_socket.to_string(),
                instance_uuid: Registration::get()
                    .map(|r| r.instance_uuid().to_string()),
                api_version: api_versions,
                hostnqn: self.node_nqn.clone(),
            }),
        };

        Ok(Response::new(response))
    }

    async fn list_block_devices(
        &self,
        request: Request<host_rpc::ListBlockDevicesRequest>,
    ) -> GrpcResult<host_rpc::ListBlockDevicesResponse> {
        let args = request.into_inner();
        let block_devices = blk_device::list_block_devices(args.all).await?;

        let response = host_rpc::ListBlockDevicesResponse {
            devices: block_devices
                .into_iter()
                .map(host_rpc::BlockDevice::from)
                .collect(),
        };
        trace!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn get_mayastor_resource_usage(
        &self,
        _request: Request<()>,
    ) -> GrpcResult<host_rpc::GetMayastorResourceUsageResponse> {
        let usage = resource::get_resource_usage().await?;
        let response = host_rpc::GetMayastorResourceUsageResponse {
            usage: Some(usage.into()),
        };
        trace!("{:?}", response);
        Ok(Response::new(response))
    }

    #[named]
    async fn list_nvme_controllers(
        &self,
        request: Request<()>,
    ) -> GrpcResult<host_rpc::ListNvmeControllersResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let rx = rpc_submit::<_, _, nexus::Error>(async move {
                    let controllers = list_controllers()
                        .await
                        .into_iter()
                        .map(host_rpc::NvmeController::from)
                        .collect();
                    Ok(host_rpc::ListNvmeControllersResponse {
                        controllers,
                    })
                })?;

                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(Response::new)
            },
        )
        .await
    }

    #[named]
    async fn stat_nvme_controller(
        &self,
        request: Request<host_rpc::StatNvmeControllerRequest>,
    ) -> GrpcResult<host_rpc::StatNvmeControllerResponse> {
        self.locked(
            GrpcClientContext::new(&request, function_name!()),
            async move {
                let args = request.into_inner();
                let rx = rpc_submit::<_, _, CoreError>(async move {
                    controller_stats(&args.name)
                        .await
                        .map(|blk_stat| {
                            Some(host_rpc::NvmeControllerIoStats::from(
                                blk_stat,
                            ))
                        })
                        .map(|ctrl_stat| host_rpc::StatNvmeControllerResponse {
                            stats: ctrl_stat,
                        })
                })?;
                rx.await
                    .map_err(|_| Status::cancelled("cancelled"))?
                    .map_err(Status::from)
                    .map(Response::new)
            },
        )
        .await
    }
}
