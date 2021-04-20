//!
//! This module implements the list_block_devices() gRPC method
//! for listing available disk devices on the current host.
//!
//! The relevant information is obtained via udev.
//! The method works by iterating through udev records and selecting block
//! (ie. SUBSYSTEM=block) devices that represent either disks or disk
//! partitions. For each such device, it is then determined as to whether the
//! device is available for use.
//!
//! A device is currently deemed to be "available" if it satisfies the following
//! criteria:
//!  - the device has a non-zero size
//!  - the device is of an acceptable type as determined by well known device
//!    numbers (eg. SCSI disks)
//!  - the device represents either a disk with no partitions or a disk
//!    partition of an acceptable type (Linux filesystem partitions only at
//!    present)
//!  - the device currently contains no filesystem or volume id (although this
//!    logically implies that the device is not currently mounted, for the sake
//!    of consistency, the mount table is also checked to ENSURE that the device
//!    is not mounted)

use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    io::Error,
};

use proc_mounts::{MountInfo, MountIter};
use rpc::mayastor::{
    block_device::{Filesystem, Partition},
    BlockDevice,
};
use udev::{Device, Enumerator};

// Struct representing a property value in a udev::Device struct (and possibly
// elsewhere). It is used to provide conversions via various "From" trait
// implementations below.
struct Property<'a>(Option<&'a OsStr>);

impl From<Property<'_>> for String {
    fn from(property: Property) -> Self {
        String::from(property.0.map(|s| s.to_str()).flatten().unwrap_or(""))
    }
}

impl From<Property<'_>> for Option<String> {
    fn from(property: Property) -> Self {
        property.0.map(|s| s.to_str()).flatten().map(String::from)
    }
}

impl From<Property<'_>> for Option<u32> {
    fn from(property: Property) -> Self {
        Option::<String>::from(property)
            .map(|s| s.parse().ok())
            .flatten()
    }
}

impl From<Property<'_>> for u32 {
    fn from(property: Property) -> Self {
        Option::<Self>::from(property).unwrap_or(0)
    }
}

impl From<Property<'_>> for Option<u64> {
    fn from(property: Property) -> Self {
        Option::<String>::from(property)
            .map(|s| s.parse().ok())
            .flatten()
    }
}

impl From<Property<'_>> for u64 {
    fn from(property: Property) -> Self {
        Option::<Self>::from(property).unwrap_or(0)
    }
}

// Determine the type of devices which may be potentially presented
// as "available" for use.
fn usable_device(devmajor: &u32) -> bool {
    const DEVICE_TYPES: [u32; 4] = [
        7,   // Loopback devices
        8,   // SCSI disk devices
        43,  // Network block devices
        259, // Block Extended Major
    ];

    if DEVICE_TYPES.iter().any(|m| m == devmajor) {
        return true;
    }

    // TODO: add extra logic here as needed for devices with dynamically
    // allocated major numbers

    false
}

// Determine the type of partitions which may be potentially presented
// as "available" for use
fn usable_partition(partition: &Option<Partition>) -> bool {
    const GPT_PARTITION_TYPES: [&str; 1] = [
        "0fc63daf-8483-4772-8e79-3d69d8477de4", // Linux
    ];

    const MBR_PARTITION_TYPES: [&str; 1] = [
        "0x83", // Linux
    ];

    if let Some(part) = partition {
        if part.scheme == "gpt" {
            return GPT_PARTITION_TYPES.iter().any(|&s| s == part.typeid);
        }
        if part.scheme == "dos" {
            return MBR_PARTITION_TYPES.iter().any(|&s| s == part.typeid);
        }
        return false;
    }

    true
}

// Determine if device is provided internally via mayastor.
// At present this simply involves examining the value of
// the udev "ID_MODEL" property.
fn mayastor_device(device: &Device) -> bool {
    matches!(
        device
            .property_value("ID_MODEL")
            .map(|s| s.to_str())
            .flatten(),
        Some("Mayastor NVMe controller") | Some("Nexus_CAS_Driver")
    )
}

// Create a new Partition object from udev::Device properties
fn new_partition(parent: Option<&str>, device: &Device) -> Option<Partition> {
    if let Some(devtype) = device.property_value("DEVTYPE") {
        if devtype.to_str() == Some("partition") {
            return Some(Partition {
                parent: String::from(parent.unwrap_or("")),
                number: Property(device.property_value("PARTN")).into(),
                name: Property(device.property_value("PARTNAME")).into(),
                scheme: Property(device.property_value("ID_PART_ENTRY_SCHEME"))
                    .into(),
                typeid: Property(device.property_value("ID_PART_ENTRY_TYPE"))
                    .into(),
                uuid: Property(device.property_value("ID_PART_ENTRY_UUID"))
                    .into(),
            });
        }
    }
    None
}

// Create a new Filesystem object from udev::Device properties
// and the list of current filesystem mounts.
// Note that the result can be None if there is no filesystem
// associated with this Device.
fn new_filesystem(
    device: &Device,
    mountinfo: Option<&MountInfo>,
) -> Option<Filesystem> {
    let mut fstype: Option<String> =
        Property(device.property_value("ID_FS_TYPE")).into();

    if fstype.is_none() {
        fstype = mountinfo.map(|m| m.fstype.clone());
    }

    let label: Option<String> =
        Property(device.property_value("ID_FS_LABEL")).into();

    let uuid: Option<String> =
        Property(device.property_value("ID_FS_UUID")).into();

    // Do no return an actual object if none of the fields therein have actual
    // values.
    if fstype.is_none()
        && label.is_none()
        && uuid.is_none()
        && mountinfo.is_none()
    {
        return None;
    }

    Some(Filesystem {
        fstype: fstype.unwrap_or_else(|| String::from("")),
        label: label.unwrap_or_else(|| String::from("")),
        uuid: uuid.unwrap_or_else(|| String::from("")),
        mountpoint: mountinfo
            .map(|m| String::from(m.dest.to_string_lossy()))
            .unwrap_or_else(|| String::from("")),
    })
}

// Create a new BlockDevice object from collected information.
// This function also contains the logic for determining whether
// or not the device that this represents is "available" for use.
fn new_device(
    parent: Option<&str>,
    include: bool,
    device: &Device,
    mounts: &HashMap<OsString, MountInfo>,
) -> Option<BlockDevice> {
    if let Some(devname) = device.property_value("DEVNAME") {
        let partition = new_partition(parent, device);
        let filesystem = new_filesystem(device, mounts.get(devname));
        let devmajor: u32 = Property(device.property_value("MAJOR")).into();
        let size: u64 = Property(device.attribute_value("size")).into();

        let available = include
            && size > 0
            && !mayastor_device(device)
            && usable_device(&devmajor)
            && (partition.is_none() || usable_partition(&partition))
            && filesystem.is_none();

        return Some(BlockDevice {
            devname: String::from(devname.to_str().unwrap_or("")),
            devtype: Property(device.property_value("DEVTYPE")).into(),
            devmajor,
            devminor: Property(device.property_value("MINOR")).into(),
            model: Property(device.property_value("ID_MODEL")).into(),
            devpath: Property(device.property_value("DEVPATH")).into(),
            devlinks: device
                .property_value("DEVLINKS")
                .map(|s| s.to_str())
                .flatten()
                .unwrap_or("")
                .split(' ')
                .filter(|&s| !s.is_empty())
                .map(String::from)
                .collect(),
            size,
            partition,
            filesystem,
            available,
        });
    }
    None
}

// Get the list of current filesystem mounts.
fn get_mounts() -> Result<HashMap<OsString, MountInfo>, Error> {
    let mut table: HashMap<OsString, MountInfo> = HashMap::new();

    for mount in (MountIter::new()?).flatten() {
        table.insert(OsString::from(mount.source.clone()), mount);
    }

    Ok(table)
}

// Iterate through udev to generate a list of all (block) devices
// with DEVTYPE == "disk"
fn get_disks(
    all: bool,
    mounts: &HashMap<OsString, MountInfo>,
) -> Result<Vec<BlockDevice>, Error> {
    let mut list: Vec<BlockDevice> = Vec::new();

    let mut enumerator = Enumerator::new()?;

    enumerator.match_subsystem("block")?;
    enumerator.match_property("DEVTYPE", "disk")?;

    for entry in enumerator.scan_devices()? {
        if let Some(devname) = entry.property_value("DEVNAME") {
            let partitions = get_partitions(devname.to_str(), &entry, mounts)?;

            if let Some(device) =
                new_device(None, partitions.is_empty(), &entry, &mounts)
            {
                if all || device.available {
                    list.push(device);
                }
            }

            for device in partitions {
                if all || device.available {
                    list.push(device);
                }
            }
        }
    }

    Ok(list)
}

// Iterate through udev to generate a list of all (block) devices
// associated with parent device <disk>
fn get_partitions(
    parent: Option<&str>,
    disk: &Device,
    mounts: &HashMap<OsString, MountInfo>,
) -> Result<Vec<BlockDevice>, Error> {
    let mut list: Vec<BlockDevice> = Vec::new();

    let mut enumerator = Enumerator::new()?;

    enumerator.match_parent(disk)?;
    enumerator.match_property("DEVTYPE", "partition")?;

    for entry in enumerator.scan_devices()? {
        if let Some(device) = new_device(parent, true, &entry, &mounts) {
            list.push(device);
        }
    }

    Ok(list)
}

/// Return a list of block devices on the current host.
/// The <all> parameter controls whether to return list containing
/// all matching devices, or just those deemed to be available.
pub async fn list_block_devices(all: bool) -> Result<Vec<BlockDevice>, Error> {
    let mounts = get_mounts()?;
    get_disks(all, &mounts)
}
