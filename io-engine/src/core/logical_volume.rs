use crate::lvs::LvolSpaceUsage;

///  LogicalVolume Trait Provide all the Generic Interface for Volume
pub trait LogicalVolume {
    /// Returns the name of the Logical Volume
    fn name(&self) -> String;

    /// Returns the UUID of the Logical Volume
    fn uuid(&self) -> String;

    /// Returns the pool name of the Logical Volume
    fn pool_name(&self) -> String;

    /// Returns the pool uuid of the Logical Volume
    fn pool_uuid(&self) -> String;

    /// Returns a boolean indicating if the Logical Volume is thin provisioned
    fn is_thin(&self) -> bool;

    /// Returns a boolean indicating if the Logical Volume is read-only
    fn is_read_only(&self) -> bool;

    /// Return the size of the Logical Volume in bytes
    fn size(&self) -> u64;

    /// Returns Lvol disk space usage
    fn usage(&self) -> LvolSpaceUsage;
}
