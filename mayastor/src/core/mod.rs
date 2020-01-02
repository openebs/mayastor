//!
//! core contains the primary abstractions around the SPDK primitives.

mod bdev;
mod channel;
mod descriptor;
mod uuid;

pub use ::uuid::Uuid;
pub use bdev::Bdev;
pub use channel::IoChannel;
pub use descriptor::Descriptor;
