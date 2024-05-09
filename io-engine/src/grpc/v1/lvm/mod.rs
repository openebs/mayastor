pub mod pool;
pub mod replica;

#[macro_export]
macro_rules! lvm_run {
    ($fut:expr) => {{
        $crate::core::runtime::spawn_blocking(|| {
            tokio::runtime::Handle::current().block_on($fut)
        })
        .await
        .map_err(|_| Status::cancelled("cancelled"))?
        .map(Response::new)
    }};
}
