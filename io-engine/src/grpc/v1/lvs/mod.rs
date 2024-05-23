use crate::{lvs::Lvol, replica_backend::ReplicaOps};
use io_engine_api::v1::replica::Replica;

impl From<Lvol> for Replica {
    fn from(lvol: Lvol) -> Self {
        let lvol = &lvol as &dyn ReplicaOps;
        lvol.into()
    }
}
