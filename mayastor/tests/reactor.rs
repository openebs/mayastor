use mayastor::core::{
    mayastor_env_stop,
    MayastorCliArgs,
    MayastorEnvironment,
    ReactorState,
    Reactors,
};

pub mod common;

#[test]
fn reactor_start_stop() {
    common::mayastor_test_init();
    let ms = MayastorEnvironment::new(MayastorCliArgs::default());
    ms.start(|| {
        Reactors::iter().for_each(|r| {
            assert_eq!(r.get_state(), ReactorState::Delayed);
        });
        mayastor_env_stop(0);
    })
    .unwrap();
}
