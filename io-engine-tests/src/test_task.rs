use crossbeam::channel::{bounded, Receiver, Sender};
use once_cell::sync::OnceCell;
use tokio::sync::oneshot;

pub use io_engine_tests_macros::spdk_test;

struct TestTask {
    sx: oneshot::Sender<()>,
    f: Box<dyn FnOnce() + Send + 'static>,
}

fn main_loop(receiver: Receiver<TestTask>) {
    loop {
        let task = receiver.recv().unwrap();
        (task.f)();
        task.sx.send(()).unwrap();
    }
}

static MAIN_THREAD: OnceCell<Sender<TestTask>> = OnceCell::new();

/// Executes a test task in a special designated thread.
pub async fn run_single_thread_test_task<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (sx, rx) = oneshot::channel();

    let sc = MAIN_THREAD.get_or_init(|| {
        let (sc, rc) = bounded(1);
        std::thread::spawn(move || {
            main_loop(rc);
        });
        sc
    });

    let task = TestTask {
        sx,
        f: Box::new(f),
    };
    sc.send(task).unwrap();

    rx.await.unwrap();
}
