fn print_error_chain(err: &dyn std::error::Error) -> String {
    let mut msg = format!("{}", err);
    let mut opt_source = err.source();
    while let Some(source) = opt_source {
        msg = format!("{}: {}", msg, source);
        opt_source = source.source();
    }
    msg
}

/// Macro locally is used to spawn a future on the same thread that executes
/// the macro. That is needed to guarantee that the execution context does
/// not leave the mgmt core (core0) that is a basic assumption in spdk. Also
/// the input/output parameters don't have to be Send and Sync in that case,
/// which simplifies the code. The value of the macro is Ok() variant of the
/// expression in the macro. Err() variant returns from the function.
#[macro_export]
macro_rules! locally {
    ($body:expr) => {{
        let hdl = crate::core::Reactors::current().spawn_local($body);
        match hdl.await.unwrap() {
            Ok(res) => res,
            Err(err) => {
                error!("{}", crate::grpc::print_error_chain(&err));
                return Err(err.into());
            }
        }
    }};
}

mod bdev_grpc;
mod mayastor_grpc;
mod server;

pub use server::MayastorGrpcServer;
use tonic::{Response, Status};
pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
