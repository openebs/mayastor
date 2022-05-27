use criterion::{criterion_group, criterion_main, Criterion};
use io_engine::{
    bdev::nexus::nexus_create,
    core::MayastorCliArgs,
    grpc::v1::nexus::nexus_destroy,
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};
use std::sync::Arc;

#[allow(unused)]
mod common;
use common::compose::MayastorTest;
use composer::{Binary, Builder, ComposeTest};

/// Infer the build type from the `OUT_DIR` and `SRCDIR`.
fn build_type() -> String {
    let out_dir = env!("OUT_DIR");
    let src_dir = env!("SRCDIR");
    let prefix = format!("{}/target/", src_dir);
    let target = out_dir.replace(&prefix, "");
    let splits = target.split('/').take(1).collect::<Vec<_>>();
    let build = splits.first().expect("build type not found");
    assert!(!build.is_empty());
    build.to_string()
}

/// Create a new compose test cluster.
async fn new_compose() -> Arc<ComposeTest> {
    let binary = Binary::from_target(&build_type(), "io-engine");

    let builder = Builder::new()
        .name("cargo-bench")
        .network("10.1.0.0/16")
        .add_container_bin("io-engine-1", binary.clone())
        .add_container_bin("io-engine-2", binary.clone())
        .add_container_bin("io-engine-3", binary.clone())
        .add_container_bin("io-engine-4", binary)
        .with_clean(true)
        .build()
        .await
        .unwrap();
    Arc::new(builder)
}
/// Create a new in-binary environment.
fn new_environment<'a>() -> Arc<MayastorTest<'a>> {
    Arc::new(MayastorTest::new(MayastorCliArgs::default()))
}

/// Get remote nvmf targets to use as nexus children.
async fn get_children(compose: Arc<ComposeTest>) -> &'static Vec<String> {
    static STATIC_TARGETS: tokio::sync::OnceCell<Vec<String>> =
        tokio::sync::OnceCell::const_new();

    STATIC_TARGETS
        .get_or_init(|| async move {
            // get the handles if needed, to invoke methods to the containers
            let mut hdls = compose.grpc_handles().await.unwrap();
            let mut children = Vec::with_capacity(hdls.len());

            let disk_index = 0;
            // create and share a bdev on each container
            for h in &mut hdls {
                h.bdev.list(Null {}).await.unwrap();
                h.bdev
                    .create(BdevUri {
                        uri: format!("malloc:///disk{}?size_mb=20", disk_index),
                    })
                    .await
                    .unwrap();
                h.bdev
                    .share(BdevShareRequest {
                        name: format!("disk{}", disk_index),
                        proto: "nvmf".into(),
                    })
                    .await
                    .unwrap();

                // create a nexus with the remote replica as its child
                let child_uri = format!(
                    "nvmf://{}:8420/nqn.2019-05.io.openebs:disk{}",
                    h.endpoint.ip(),
                    disk_index
                );
                children.push(child_uri);
            }
            children
        })
        .await
}

/// Created Nexus that is destroyed on drop.
struct DirectNexus(Arc<MayastorTest<'static>>, String);
impl Drop for DirectNexus {
    fn drop(&mut self) {
        let name = self.1.clone();
        let io_engine = self.0.clone();
        std::thread::spawn(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    io_engine
                        .spawn(async move {
                            nexus_destroy(name.as_str()).await.unwrap();
                        })
                        .await;
                });
        })
        .join()
        .unwrap();
    }
}
/// Create a new nexus in-binary and return it as droppable to be destroyed.
async fn nexus_create_direct(
    ms_environment: &Arc<MayastorTest<'static>>,
    compose: &Arc<ComposeTest>,
    nr_children: usize,
) -> DirectNexus {
    let uuid = uuid::Uuid::new_v4();
    let nexus_name = format!("nexus-{}", uuid);
    let name = nexus_name.clone();
    let uuid_str = uuid.to_string();

    let children = get_children(compose.clone())
        .await
        .iter()
        .take(nr_children)
        .cloned();

    let name = ms_environment
        .spawn(async move {
            nexus_create(
                &name,
                10 * 1024 * 1024,
                Some(uuid_str.as_str()),
                &children.collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            uuid_str
        })
        .await;
    DirectNexus(ms_environment.clone(), name)
}

/// Created Grpc Nexus that is destroyed on drop.
struct GrpcNexus(Arc<ComposeTest>, rpc::mayastor::Nexus);
impl Drop for GrpcNexus {
    fn drop(&mut self) {
        let uuid = self.1.uuid.clone();
        let compose = self.0.clone();
        std::thread::spawn(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let mut hdls = compose.grpc_handles().await.unwrap();
                    let nexus_hdl = &mut hdls.last_mut().unwrap();
                    nexus_hdl
                        .mayastor
                        .destroy_nexus(rpc::mayastor::DestroyNexusRequest {
                            uuid,
                        })
                        .await
                        .unwrap();
                });
        })
        .join()
        .unwrap()
    }
}
/// Create a new nexus via grpc and return it as droppable to be destroyed.
async fn nexus_create_grpc(
    compose: &Arc<ComposeTest>,
    nr_children: usize,
) -> GrpcNexus {
    let children = get_children(compose.clone())
        .await
        .iter()
        .take(nr_children)
        .cloned();
    let mut hdls = compose.grpc_handles().await.unwrap();

    let nexus_hdl = &mut hdls.last_mut().unwrap();
    let nexus = nexus_hdl
        .mayastor
        .create_nexus(rpc::mayastor::CreateNexusRequest {
            uuid: uuid::Uuid::new_v4().to_string(),
            size: 10 * 1024 * 1024,
            children: children.collect::<Vec<_>>(),
        })
        .await
        .unwrap();
    GrpcNexus(compose.clone(), nexus.into_inner())
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let compose = runtime.block_on(async move { new_compose().await });
    let ms_environment = new_environment();

    let mut group = c.benchmark_group(format!("{}/nexus/create", build_type()));
    group
        // Benchmark nexus create in-binary
        .bench_function("direct", |b| {
            b.to_async(&runtime).iter_with_large_drop(|| {
                nexus_create_direct(&ms_environment, &compose, 3)
            })
        })
        // Benchmark nexus create via gRPC
        .bench_function("grpc", |b| {
            b.to_async(&runtime)
                .iter_with_large_drop(|| nexus_create_grpc(&compose, 3))
        });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
