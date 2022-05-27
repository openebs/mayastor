pub mod nvmf;

// Which kind of target interface to use for a bdev
pub enum Side {
    Nexus,
    Replica,
}
