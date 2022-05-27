pub use composer::*;

use crossbeam::channel::bounded;
use std::future::Future;
use tokio::sync::oneshot::channel;

use crate::common::mayastor_test_init;
use io_engine::core::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    Reactor,
    Reactors,
    GLOBAL_RC,
};
use std::time::Duration;

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
        let (tx, rx) = bounded(1);
        mayastor_test_init();
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
}

impl<'a> Drop for MayastorTest<'a> {
    fn drop(&mut self) {
        self.reactor.send_future(async { mayastor_env_stop(0) });
        // wait for mayastor to stop
        let hdl = self.thdl.take().unwrap();
        hdl.join().unwrap()
    }
}
