extern crate bytes;
extern crate prost;
extern crate prost_derive;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate tonic;
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
pub mod mayastor {

    impl From<()> for Null {
        fn from(_: ()) -> Self {
            Self {}
        }
    }

    include!(concat!(env!("OUT_DIR"), "/mayastor.rs"));
}
