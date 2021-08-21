use crate::partition::PartitionID;
use std::{
    ffi::OsString,
    fmt::{self, Display, Formatter},
    fs::File,
    io::{self, BufRead, BufReader, Error, ErrorKind},
    os::unix::prelude::OsStringExt,
    path::{Path, PathBuf},
    str::FromStr,
};
#[derive(Debug, Default, Clone, Hash, Eq, PartialEq)]
pub struct MountInfo {
    pub source: PathBuf,
    pub dest: PathBuf,
    pub fstype: String,
    pub options: Vec<String>,
}

impl Display for MountInfo {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(
            fmt,
            "{} {} {} {}",
            self.source.display(),
            self.dest.display(),
            self.fstype,
            if self.options.is_empty() {
                "defaults".into()
            } else {
                self.options.join(",")
            },
        )
    }
}

impl FromStr for MountInfo {
    type Err = io::Error;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let mut parts = line.split_whitespace();

        fn map_err(why: &'static str) -> io::Error {
            Error::new(ErrorKind::InvalidData, why)
        }

        let source = parts.next().ok_or_else(|| map_err("missing source"))?;
        let dest = parts.next().ok_or_else(|| map_err("missing dest"))?;
        let fstype = parts.next().ok_or_else(|| map_err("missing type"))?;
        let options = parts.next().ok_or_else(|| map_err("missing options"))?;

        let _dump = parts.next().map_or(Ok(0), |value| {
            value
                .parse::<i32>()
                .map_err(|_| map_err("dump value is not a number"))
        })?;

        let _pass = parts.next().map_or(Ok(0), |value| {
            value
                .parse::<i32>()
                .map_err(|_| map_err("pass value is not a number"))
        })?;

        let path = Self::parse_value(source)?;
        let path = path
            .to_str()
            .ok_or_else(|| map_err("non-utf8 paths are unsupported"))?;

        let source = if path.starts_with("/dev/disk/by-") {
            Self::fetch_from_disk_by_path(path)?
        } else {
            PathBuf::from(path)
        };

        let path = Self::parse_value(dest)?;
        let path = path
            .to_str()
            .ok_or_else(|| map_err("non-utf8 paths are unsupported"))?;

        let dest = PathBuf::from(path);

        Ok(MountInfo {
            source,
            dest,
            fstype: fstype.to_owned(),
            options: options.split(',').map(String::from).collect(),
        })
    }
}

impl MountInfo {
    /// Attempt to parse a `/proc/mounts`-like line.

    fn fetch_from_disk_by_path(path: &str) -> io::Result<PathBuf> {
        PartitionID::from_disk_by_path(path)
            .map_err(|why| {
                Error::new(ErrorKind::InvalidData, format!("{}: {}", path, why))
            })?
            .get_device_path()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("device path for {} was not found", path),
                )
            })
    }

    fn parse_value(value: &str) -> io::Result<OsString> {
        let mut ret = Vec::new();

        let mut bytes = value.bytes();
        while let Some(b) = bytes.next() {
            match b {
                b'\\' => {
                    let mut code = 0;
                    for _i in 0..3 {
                        if let Some(b) = bytes.next() {
                            code *= 8;
                            code += u32::from_str_radix(
                                &(b as char).to_string(),
                                8,
                            )
                            .map_err(|err| Error::new(ErrorKind::Other, err))?;
                        } else {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "truncated octal code",
                            ));
                        }
                    }
                    ret.push(code as u8);
                }
                _ => {
                    ret.push(b);
                }
            }
        }

        Ok(OsString::from_vec(ret))
    }
}

/// Iteratively parse the `/proc/mounts` file.
pub struct MountIter<R> {
    file: R,
    buffer: String,
}

impl MountIter<BufReader<File>> {
    pub fn new() -> io::Result<Self> {
        Self::new_from_file("/proc/mounts")
    }

    /// Read mounts from any mount-tab-like file.
    pub fn new_from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self::new_from_reader(BufReader::new(File::open(path)?)))
    }
}

impl<R: BufRead> MountIter<R> {
    /// Read mounts from any in-memory buffer.
    pub fn new_from_reader(readable: R) -> Self {
        Self {
            file: readable,
            buffer: String::with_capacity(512),
        }
    }

    /// Iterator-based variant of `source_mounted_at`.
    ///
    /// Returns true if the `source` is mounted at the given `dest`.
    ///
    /// Due to iterative parsing of the mount file, an error may be returned.
    pub fn source_mounted_at<D: AsRef<Path>, P: AsRef<Path>>(
        source: D,
        path: P,
    ) -> io::Result<bool> {
        let source = source.as_ref();
        let path = path.as_ref();

        let mut is_found = false;

        let mounts = MountIter::new()?;
        for mount in mounts {
            let mount = mount?;
            if mount.source == source {
                is_found = mount.dest == path;
                break;
            }
        }

        Ok(is_found)
    }
}

impl<R: BufRead> Iterator for MountIter<R> {
    type Item = io::Result<MountInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buffer.clear();
            match self.file.read_line(&mut self.buffer) {
                Ok(read) if read == 0 => return None,
                Ok(_) => {
                    let line = self.buffer.trim_start();
                    if !(line.starts_with('#') || line.is_empty()) {
                        return Some(MountInfo::from_str(line));
                    }
                }
                Err(why) => return Some(Err(why)),
            }
        }
    }
}
