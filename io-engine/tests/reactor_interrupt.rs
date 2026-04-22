//! Regression test: `mayastor_env_stop` must accept the `Interrupt`
//! reactor state. Under `--enable-interrupt-mode` the master reactor
//! is in `ReactorState::Interrupt`, so any SIGTERM after the
//! transition used to hit the `_ => panic!` arm at env.rs:649.

use io_engine::core::{
    mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, ReactorState, Reactors,
};

pub mod common;

#[common::spdk_test]
fn reactor_interrupt_mode_shutdown() {
    common::mayastor_test_init();
    MayastorEnvironment::new(MayastorCliArgs {
        reactor_mask: "0x1".into(),
        interrupt_mode: true,
        ..Default::default()
    })
    .init();

    // Drive the master reactor into Interrupt state — same transition
    // `.start()` does at env.rs:1263.
    Reactors::master().enter_interrupt_mode();
    assert_eq!(Reactors::master().get_state(), ReactorState::Interrupt);

    // Must not panic with "invalid reactor state during shutdown".
    mayastor_env_stop(0);
}
