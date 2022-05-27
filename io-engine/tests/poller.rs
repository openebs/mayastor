use crossbeam::atomic::AtomicCell;
use once_cell::sync::Lazy;

use io_engine::core::{
    mayastor_env_stop,
    poller,
    MayastorCliArgs,
    MayastorEnvironment,
    Reactors,
};

pub mod common;

static COUNT: Lazy<AtomicCell<u32>> = Lazy::new(|| AtomicCell::new(0));

fn test_fn(a: u32, b: u32) -> u32 {
    a + b
}

#[test]
fn poller() {
    common::mayastor_test_init();
    MayastorEnvironment::new(MayastorCliArgs::default()).init();

    let args = (1, 2);
    let poller = poller::Builder::new()
        .with_interval(0)
        .with_poll_fn(move || {
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
    let mut poller = poller::Builder::new()
        .with_interval(0)
        .with_poll_fn(move || {
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
    let poller = poller::Builder::new()
        .with_interval(0)
        .with_poll_fn(move || {
            ctx_state += test_fn(1, 2);
            dbg!(ctx_state);
            0
        })
        .build();

    Reactors::master().poll_times(64);
    drop(poller);

    mayastor_env_stop(0);
}
