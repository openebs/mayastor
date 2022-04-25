pub struct NvmeDevice {
    /// Namespace index
    pub namespace: i32,
    /// Device name
    pub device: String,
    /// Firmware
    pub firmware: String,
    /// Model
    pub model: String,
    /// Serial
    pub serial: String,
    /// Utilisation
    pub utilisation: u64,
    /// Maximum LBA
    pub max_lba: u64,
    /// Capacity
    pub capacity: u64,
    /// Sector size
    pub sector_size: u32,
}
