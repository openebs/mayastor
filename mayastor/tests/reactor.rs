use mayastor::core::{
    mayastor_env_stop,
    Cores,
    MayastorCliArgs,
    MayastorEnvironment,
    Mthread,
    ReactorState,
    Reactors,
};
use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::unsync::Lazy;
use pin_utils::core_reexport::time::Duration;

pub mod common;

// This test requires the system to have at least 2 cpus
#[test]
fn reactor_start_stop() {
    common::mayastor_test_init();
    let mut args = MayastorCliArgs::default();
    args.reactor_mask = "0x1".to_string();
    let ms = MayastorEnvironment::new(args);

    static mut WAIT_FOR: Lazy<AtomicUsize> =
        Lazy::new(|| AtomicUsize::new(Cores::count().into_iter().count()));

    ms.start(|| {
        Reactors::iter().for_each(|r| {
            assert_eq!(r.get_state(), ReactorState::Delayed);
        });

        // verify that each core is running/pinned on the right CPU
        // things up correctly in our reactors. NOTE: changing the core
        // mask may result in different values
        Reactors::iter().for_each(|r| {
            r.send_future(async {
                let core = Cores::current();
                assert_eq!(core, unsafe { libc::sched_getcpu() as u32 });

                // global mutable state is unsafe
                unsafe { WAIT_FOR.fetch_sub(1, Ordering::SeqCst) };
            });
        });

        while unsafe { WAIT_FOR.load(Ordering::SeqCst) } != 0 {
            Reactors::master().poll_once();
        }

        Cores::count().into_iter().for_each(|_| {
            Mthread::spawn_unaffinitized(|| {
                std::thread::sleep(Duration::from_secs(1));

                let cpu = unsafe { libc::sched_getcpu() };
                // TODO: The main thread does not know when this assertion
                // triggers and the test will hapilly pass.
                assert_eq!(cpu as u32 > Cores::last().id(), true)
            });
        });
        std::thread::sleep(Duration::from_secs(3));

        mayastor_env_stop(0);
    })
    .unwrap();
}
