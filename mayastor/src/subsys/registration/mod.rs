//! A Registration subsystem is used to keep control-plane in the loop
//! about the lifecycle of mayastor instances.

pub mod registration_grpc;

pub mod misc {
    use strum_macros::{AsRefStr, ToString};

    /// Error type which is returned for any operation
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct ReplyError {
        /// error kind
        pub kind: ReplyErrorKind,
        /// resource kind
        pub resource: ResourceKind,
        /// last source of this error
        pub source: String,
        /// extra information
        pub extra: String,
    }

    impl ReplyError {
        /// useful when the grpc server is dropped due to panic
        pub fn tonic_reply_error(source: String) -> Self {
            Self {
                kind: ReplyErrorKind::Aborted,
                resource: ResourceKind::Node,
                source,
                extra: "".to_string(),
            }
        }
    }

    impl From<tonic::Status> for ReplyError {
        fn from(status: tonic::Status) -> Self {
            Self::tonic_reply_error(status.to_string())
        }
    }

    impl From<tonic::transport::Error> for ReplyError {
        fn from(e: tonic::transport::Error) -> Self {
            Self::tonic_reply_error(e.to_string())
        }
    }

    impl std::fmt::Display for ReplyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "'{}' Error on '{}' resources, from Error '{}', extra: '{}'",
                self.kind.as_ref(),
                self.resource.as_ref(),
                self.source,
                self.extra
            )
        }
    }

    /// All the different variants of `ReplyError`
    #[derive(Serialize, Deserialize, Debug, Clone, strum_macros::AsRefStr)]
    #[allow(missing_docs)]
    pub enum ReplyErrorKind {
        WithMessage,
        DeserializeReq,
        Internal,
        Timeout,
        InvalidArgument,
        DeadlineExceeded,
        NotFound,
        AlreadyExists,
        PermissionDenied,
        ResourceExhausted,
        FailedPrecondition,
        Aborted,
        OutOfRange,
        Unimplemented,
        Unavailable,
        Unauthenticated,
    }

    /// All the different variants of Resources
    #[derive(Serialize, Deserialize, Debug, Clone, AsRefStr, ToString)]
    pub enum ResourceKind {
        /// Unknown or unspecified resource
        Unknown,
        /// Node resource
        Node,
        /// Pool resource
        Pool,
        /// Replica resource
        Replica,
        /// Nexus resource
        Nexus,
        /// Child resource
        Child,
        /// Volume resource
        Volume,
        /// Json Grpc methods
        JsonGrpc,
        /// Block devices
        Block,
    }
}

use crate::core::MayastorEnvironment;
use registration_grpc::Registration;
use spdk_rs::libspdk::{
    spdk_add_subsystem,
    spdk_subsystem,
    spdk_subsystem_fini_next,
    spdk_subsystem_init_next,
};

// wrapper around our Registration subsystem used for registration
pub struct RegistrationSubsystem(*mut spdk_subsystem);

impl Default for RegistrationSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistrationSubsystem {
    /// initialise a new subsystem that handles the control plane
    /// registration process
    extern "C" fn init() {
        unsafe { spdk_subsystem_init_next(0) }
    }

    extern "C" fn fini() {
        debug!("mayastor registration subsystem fini");
        let args = MayastorEnvironment::global_or_default();
        if args.grpc_endpoint.is_some() {
            if let Some(registration) = Registration::get() {
                registration.fini();
            }
        }
        unsafe { spdk_subsystem_fini_next() }
    }

    fn new() -> Self {
        info!("creating Mayastor registration subsystem...");
        let mut ss = Box::new(spdk_subsystem::default());
        ss.name = b"mayastor_grpc_registration\x00" as *const u8
            as *const libc::c_char;
        ss.init = Some(Self::init);
        ss.fini = Some(Self::fini);
        ss.write_config_json = None;
        Self(Box::into_raw(ss))
    }

    /// register the subsystem with spdk
    pub(super) fn register() {
        unsafe { spdk_add_subsystem(RegistrationSubsystem::new().0) }
    }
}
