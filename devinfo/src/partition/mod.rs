use self::PartitionSource::{Path as SourcePath, *};
use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
    fs,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    str::FromStr,
};

/// Describes a partition identity.
///
/// A device path may be recovered from this.
///
/// # Notes
///
/// This is a struct instead of an enum to make access to the `id` string
/// easier for situations where the variant does not need to be checked.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct PartitionID {
    pub variant: PartitionSource,
    pub id: String,
}

impl PartitionID {
    /// Construct a new `PartitionID` as the given source.
    pub fn new(variant: PartitionSource, id: String) -> Self {
        Self {
            variant,
            id,
        }
    }

    /// Construct a new `PartitionID` as a `ID` source.
    pub fn new_id(id: String) -> Self {
        Self::new(ID, id)
    }

    /// Construct a new `PartitionID` as a `Label` source.
    pub fn new_label(id: String) -> Self {
        Self::new(Label, id)
    }

    /// Construct a new `PartitionID` as a `UUID` source.
    pub fn new_uuid(id: String) -> Self {
        Self::new(UUID, id)
    }

    /// Construct a new `PartitionID` as a `PartLabel` source.
    pub fn new_partlabel(id: String) -> Self {
        Self::new(PartLabel, id)
    }

    /// Construct a new `PartitionID` as a `PartUUID` source.
    pub fn new_partuuid(id: String) -> Self {
        Self::new(PartUUID, id)
    }

    /// Construct a new `PartitionID` as a `Path` source.
    pub fn new_path(id: String) -> Self {
        Self::new(SourcePath, id)
    }

    /// Find the device path of this ID.
    pub fn get_device_path(&self) -> Option<PathBuf> {
        if self.variant == PartitionSource::Path && self.id.starts_with('/') {
            Some(PathBuf::from(&self.id))
        } else {
            from_id(&self.id, &self.variant.disk_by_path())
        }
    }

    /// Find the given source ID of the device at the given path.
    pub fn get_source<P: AsRef<Path>>(
        variant: PartitionSource,
        path: P,
    ) -> Option<Self> {
        Some(Self {
            variant,
            id: find_id(path.as_ref(), &variant.disk_by_path())?,
        })
    }

    /// Find the UUID of the device at the given path.
    pub fn get_uuid<P: AsRef<Path>>(path: P) -> Option<Self> {
        Self::get_source(UUID, path)
    }

    /// Find the PARTUUID of the device at the given path.
    pub fn get_partuuid<P: AsRef<Path>>(path: P) -> Option<Self> {
        Self::get_source(PartUUID, path)
    }

    /// Fetch a partition ID by a `/dev/disk/by-` path.
    pub fn from_disk_by_path<S: AsRef<str>>(path: S) -> Result<Self, Error> {
        let path = path.as_ref();

        let path = if let Some(path) = path.strip_prefix("/dev/disk/by-") {
            path
        } else {
            return Err(Error::new(ErrorKind::NotFound, path));
        };

        let id = if let Some(id) = path.strip_prefix("id/") {
            Self::new(ID, id.into())
        } else if let Some(path) = path.strip_prefix("label/") {
            Self::new(Label, path.into())
        } else if let Some(path) = path.strip_prefix("partlabel/") {
            Self::new(PartLabel, path.into())
        } else if let Some(path) = path.strip_prefix("partuuid/") {
            Self::new(PartUUID, path.into())
        } else if let Some(path) = path.strip_prefix("path/") {
            Self::new(PartUUID, path.into())
        } else if let Some(path) = path.strip_prefix("uuid/") {
            Self::new(PartUUID, path.into())
        } else {
            return Err(Error::new(ErrorKind::InvalidData, path));
        };

        Ok(id)
    }
}

impl Display for PartitionID {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        if let PartitionSource::Path = self.variant {
            write!(fmt, "{}", self.id)
        } else {
            write!(fmt, "{}={}", <&'static str>::from(self.variant), self.id)
        }
    }
}

impl FromStr for PartitionID {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Some(s) = input.strip_prefix('/') {
            Ok(PartitionID {
                variant: SourcePath,
                id: s.to_owned(),
            })
        } else if let Some(s) = input.strip_prefix("ID=") {
            Ok(PartitionID {
                variant: ID,
                id: s.to_owned(),
            })
        } else if let Some(s) = input.strip_prefix("LABEL=") {
            Ok(PartitionID {
                variant: Label,
                id: s.to_owned(),
            })
        } else if let Some(s) = input.strip_prefix("PARTLABEL=") {
            Ok(PartitionID {
                variant: PartLabel,
                id: s.to_owned(),
            })
        } else if let Some(s) = input.strip_prefix("PARTUUID=") {
            Ok(PartitionID {
                variant: PartUUID,
                id: s.to_owned(),
            })
        } else if let Some(s) = input.strip_prefix("UUID=") {
            Ok(PartitionID {
                variant: UUID,
                id: s.to_owned(),
            })
        } else {
            Err(Error::new(ErrorKind::InvalidData, input))
        }
    }
}

/// Describes the type of partition identity.
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub enum PartitionSource {
    ID,
    Label,
    PartLabel,
    PartUUID,
    Path,
    UUID,
}

impl Display for PartitionSource {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(fmt, "{}", <&'static str>::from(*self))
    }
}

impl From<PartitionSource> for &'static str {
    fn from(pid: PartitionSource) -> &'static str {
        match pid {
            PartitionSource::ID => "ID",
            PartitionSource::Label => "LABEL",
            PartitionSource::PartLabel => "PARTLABEL",
            PartitionSource::PartUUID => "PARTUUID",
            PartitionSource::Path => "PATH",
            PartitionSource::UUID => "UUID",
        }
    }
}

impl PartitionSource {
    fn disk_by_path(self) -> PathBuf {
        PathBuf::from(
            ["/dev/disk/by-", &<&'static str>::from(self).to_lowercase()]
                .concat(),
        )
    }
}

/// A collection of all discoverable identifiers for a partition.
#[derive(Debug, Default, Clone, Hash, PartialEq)]
pub struct PartitionIdentifiers {
    pub id: Option<String>,
    pub label: Option<String>,
    pub part_label: Option<String>,
    pub part_uuid: Option<String>,
    pub path: Option<String>,
    pub uuid: Option<String>,
}

impl PartitionIdentifiers {
    /// Fetches all discoverable identifiers for a partition by the path to that
    /// partition.
    pub fn from_path<P: AsRef<Path>>(path: P) -> PartitionIdentifiers {
        let path = path.as_ref();

        PartitionIdentifiers {
            path: PartitionID::get_source(SourcePath, path).map(|id| id.id),
            id: PartitionID::get_source(ID, path).map(|id| id.id),
            label: PartitionID::get_source(Label, path).map(|id| id.id),
            part_label: PartitionID::get_source(PartLabel, path)
                .map(|id| id.id),
            part_uuid: PartitionID::get_source(PartUUID, path).map(|id| id.id),
            uuid: PartitionID::get_source(UUID, path).map(|id| id.id),
        }
    }

    /// Checks if the given identity matches one of the available identifiers.
    pub fn matches(&self, id: &PartitionID) -> bool {
        match id.variant {
            ID => self.id.as_ref().map_or(false, |s| &id.id == s),
            Label => self.label.as_ref().map_or(false, |s| &id.id == s),
            PartLabel => {
                self.part_label.as_ref().map_or(false, |s| &id.id == s)
            }
            PartUUID => self.part_uuid.as_ref().map_or(false, |s| &id.id == s),
            SourcePath => self.path.as_ref().map_or(false, |s| &id.id == s),
            UUID => self.uuid.as_ref().map_or(false, |s| &id.id == s),
        }
    }
}

fn attempt<T, F: FnMut() -> Option<T>>(
    attempts: u8,
    wait: u64,
    mut func: F,
) -> Option<T> {
    let mut tried = 0;
    let mut result;

    loop {
        result = func();
        if result.is_none() && tried != attempts {
            ::std::thread::sleep(::std::time::Duration::from_millis(wait));
            tried += 1;
        } else {
            return result;
        }
    }
}

fn canonicalize(path: &Path) -> Cow<'_, Path> {
    // NOTE: It seems that the kernel may intermittently error.
    match attempt::<PathBuf, _>(10, 1, || path.canonicalize().ok()) {
        Some(path) => Cow::Owned(path),
        None => Cow::Borrowed(path),
    }
}

/// Attempts to find the ID from the given path.
fn find_id(path: &Path, uuid_dir: &Path) -> Option<String> {
    // NOTE: It seems that the kernel may sometimes intermittently skip
    // directories.
    attempt(10, 1, move || {
        let dir = uuid_dir.read_dir().ok()?;
        find_id_(path, dir)
    })
}

fn from_id(uuid: &str, uuid_dir: &Path) -> Option<PathBuf> {
    // NOTE: It seems that the kernel may sometimes intermittently skip
    // directories.
    attempt(10, 1, move || {
        let dir = uuid_dir.read_dir().ok()?;
        from_id_(uuid, dir)
    })
}

fn find_id_(path: &Path, uuid_dir: fs::ReadDir) -> Option<String> {
    let path = canonicalize(path);
    for uuid_entry in uuid_dir.filter_map(|entry| entry.ok()) {
        let uuid_path = uuid_entry.path();
        let uuid_path = canonicalize(&uuid_path);
        if uuid_path == path {
            if let Some(uuid_entry) = uuid_entry.file_name().to_str() {
                return Some(uuid_entry.into());
            }
        }
    }

    None
}

fn from_id_(uuid: &str, uuid_dir: fs::ReadDir) -> Option<PathBuf> {
    for uuid_entry in uuid_dir.filter_map(|entry| entry.ok()) {
        let uuid_entry = uuid_entry.path();
        if let Some(name) = uuid_entry.file_name() {
            if name == uuid {
                if let Ok(uuid_entry) = uuid_entry.canonicalize() {
                    return Some(uuid_entry);
                }
            }
        }
    }

    None
}
