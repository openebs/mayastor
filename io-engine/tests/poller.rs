use crossbeam::atomic::AtomicCell;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use io_engine::core::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    Reactors,
};

use spdk_rs::{Cores, PollerBuilder};

pub mod common;

static COUNT: Lazy<AtomicCell<u32>> = Lazy::new(|| AtomicCell::new(0));

fn test_fn(a: u32, b: u32) -> u32 {
    a + b
}

#[common::spdk_test]
fn poller() {
    common::mayastor_test_init();
    MayastorEnvironment::new(MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    })
    .init();

    let args = (1, 2);
    let poller = PollerBuilder::<()>::new()
        .with_poll_fn(move |_| {
            println!("and a {} and {}", args.0, args.1);
            let mut count = COUNT.load();
            count += 1;
            COUNT.store(count);
            0
        })
        .build();

    drop(poller);
    Reactors::master().poll_once();

    // we dropped the poller before we polled, the value should still be 0
    assert_eq!(COUNT.load(), 0);

    let args = (1, 2);
    let poller = PollerBuilder::<()>::new()
        .with_poll_fn(move |_| {
            let count = COUNT.load();
            println!("and a {} and {} (count: {}) ", args.0, args.1, count);
            COUNT.store(count + 1);
            0
        })
        .build();

    Reactors::master().poll_times(64);
    assert_eq!(COUNT.load(), 64);

    poller.pause();
    Reactors::master().poll_times(64);
    assert_eq!(COUNT.load(), 64);

    poller.resume();
    Reactors::master().poll_times(64);
    assert_eq!(COUNT.load(), 128);

    // poller stop consumes self
    poller.stop();

    Reactors::master().poll_times(64);
    assert_eq!(COUNT.load(), 128);

    // demonstrate we keep state during callbacks
    let mut ctx_state = 0;
    let poller = PollerBuilder::<()>::new()
        .with_poll_fn(move |_| {
            ctx_state += test_fn(1, 2);
            dbg!(ctx_state);
            0
        })
        .build();

    Reactors::master().poll_times(64);
    drop(poller);

    // Running a poller on another core.
    #[derive(Default)]
    struct Data {
        cnt: Mutex<u32>,
    }

    let poller = PollerBuilder::new()
        .with_core(1)
        .with_data(Data {
            cnt: Mutex::new(0),
        })
        .with_poll_fn(|d| {
            *d.cnt.lock() += 1;
            assert_eq!(Cores::current(), 1);
            0
        })
        .build();

    std::thread::sleep(std::time::Duration::from_millis(1));

    let res = *poller.data().cnt.lock();
    assert!(res > 0);
    poller.stop();

    mayastor_env_stop(0);
}
