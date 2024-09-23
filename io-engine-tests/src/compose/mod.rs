pub use composer::*;

use crossbeam::channel::bounded;
use std::future::Future;
use tokio::sync::oneshot::channel;

use crate::mayastor_test_init_ex;
use io_engine::{
    core::{
        device_monitor_loop,
        mayastor_env_stop,
        runtime,
        MayastorCliArgs,
        MayastorEnvironment,
        ProtectedSubsystems,
        Reactor,
        Reactors,
        ResourceLockManager,
        ResourceLockManagerConfig,
        GLOBAL_RC,
    },
    grpc,
};
use std::time::Duration;

pub mod rpc;

/// Mayastor test structure that simplifies sending futures. Mayastor has
/// its own reactor, which is not tokio based, so we need to handle properly
#[derive(Debug)]
pub struct MayastorTest<'a> {
    reactor: &'a Reactor,
    thdl: Option<std::thread::JoinHandle<()>>,
}

impl<'a> MayastorTest<'a> {
    /// spawn a future on this mayastor instance collecting the output
    /// notice that rust will not allow sending Bdevs as they are !Send
    pub async fn spawn<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = channel::<T>();
        self.reactor.send_future(async move {
            let arg = future.await;
            let _ = tx.send(arg);
        });

        rx.await.unwrap()
    }

    pub fn send<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.reactor.send_future(future);
    }

    /// starts mayastor, notice we start mayastor on a thread and return a
    /// handle to the management core. This handle is used to spawn futures,
    /// and the thread handle can be used to synchronize the shutdown
    async fn _new(args: MayastorCliArgs) -> MayastorTest<'static> {
        let (tx, rx) = channel::<&'static Reactor>();

        let thdl = std::thread::spawn(|| {
            MayastorEnvironment::new(args).init();
            tx.send(Reactors::master()).unwrap();
            Reactors::master().running();
            Reactors::master().poll_reactor();
        });

        let reactor = rx.await.unwrap();
        MayastorTest {
            reactor,
            thdl: Some(thdl),
        }
    }

    pub fn new(args: MayastorCliArgs) -> MayastorTest<'static> {
        Self::new_ex(args, None)
    }

    pub fn new_ex(
        args: MayastorCliArgs,
        log_level: Option<&str>,
    ) -> MayastorTest<'static> {
        let (tx, rx) = bounded(1);
        mayastor_test_init_ex(args.log_format.unwrap_or_default(), log_level);
        let thdl = std::thread::Builder::new()
            .name("mayastor_master".into())
            .spawn(move || {
                MayastorEnvironment::new(args).init();
                tx.send(Reactors::master()).unwrap();
                Reactors::master().running();
                Reactors::master().poll_reactor();
            })
            .unwrap();

        let reactor = rx.recv().unwrap();
        MayastorTest {
            reactor,
            thdl: Some(thdl),
        }
    }

    /// explicitly stop mayastor
    pub async fn stop(&self) {
        self.spawn(async { mayastor_env_stop(0) }).await;
        loop {
            if *GLOBAL_RC.lock().unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Starts the device monitor loop which is required to fully
    /// remove devices when they are not in use.
    pub fn start_device_monitor(&self) {
        runtime::spawn(device_monitor_loop());
    }

    /// Start the gRPC server which can be useful to debug tests.
    pub fn start_grpc(&self) {
        let cfg = ResourceLockManagerConfig::default()
            .with_subsystem(ProtectedSubsystems::POOL, 32)
            .with_subsystem(ProtectedSubsystems::NEXUS, 512)
            .with_subsystem(ProtectedSubsystems::REPLICA, 1024);
        ResourceLockManager::initialize(cfg);

        let env = MayastorEnvironment::global_or_default();
        runtime::spawn(async {
            grpc::MayastorGrpcServer::run(
                &env.node_name,
                &env.node_nqn,
                env.grpc_endpoint.unwrap(),
                env.rpc_addr,
                env.api_versions,
            )
            .await
            .ok();
        });
    }
}

impl<'a> Drop for MayastorTest<'a> {
    fn drop(&mut self) {
        self.reactor.send_future(async { mayastor_env_stop(0) });
        // wait for mayastor to stop
        let hdl = self.thdl.take().unwrap();
        hdl.join().unwrap()
    }
}
