use crate::lvm::{error, error::Error, property::Property};

use serde::de::Deserialize;
use snafu::ResultExt;
use std::ffi::OsStr;
use strum_macros::{AsRefStr, Display, EnumString};
use tokio::process::Command;

/// Common set of query options for a volume group or logical volume.
/// If the name is present then the name will be used to query.
/// Otherwise, the tag is present, then it will be used to query.
#[derive(Default, Debug)]
pub(crate) struct CmnQueryArgs {
    /// Find entries with the given name.
    pub(super) name: Option<String>,
    /// Find entries with the given uuid.
    pub(super) uuid: Option<String>,
    /// Find entries containing at least the given tag.
    pub(super) tag: Option<String>,
}
impl CmnQueryArgs {
    /// Find any and all entries in the system.
    pub(crate) fn any() -> Self {
        Self::default()
    }
    /// Find only our entries (ie, with our tag).
    pub(crate) fn ours() -> Self {
        Self {
            tag: Some(Property::Lvm.tag()),
            ..Default::default()
        }
    }
    /// Find entries with the given name.
    pub(crate) fn named_opt(self, name: &Option<String>) -> Self {
        let Some(name) = name else {
            return self;
        };
        Self {
            name: Some(name.to_string()),
            ..self
        }
    }
    /// Find the entry with the given uuid.
    pub(crate) fn uuid_opt(self, uuid: &Option<String>) -> Self {
        let Some(uuid) = uuid else {
            return self;
        };
        Self {
            uuid: Some(uuid.to_string()),
            ..self
        }
    }
    /// Find entries with the given name.
    pub(crate) fn named(self, name: &str) -> Self {
        Self {
            name: Some(name.to_string()),
            ..self
        }
    }
    /// Find the entry with the given uuid.
    pub(crate) fn uuid(self, uuid: &str) -> Self {
        Self {
            uuid: Some(uuid.to_string()),
            ..self
        }
    }

    /// Find the entries containing at least the given tag.
    #[allow(unused)]
    pub(crate) fn tagged(self, tag: &str) -> Self {
        Self {
            tag: Some(tag.to_string()),
            ..self
        }
    }
}

/// The following commands implement the core LVM functionality.
#[derive(AsRefStr, EnumString, Display)]
enum LvmSubCmd {
    /// Initialize physical volume(s) for use by LVM.
    #[strum(serialize = "pvcreate")]
    PVCreate,
    /// Remove LVM label(s) from physical volume(s).
    #[strum(serialize = "pvremove")]
    PVRemove,
    /// Display information about volume groups.
    #[strum(serialize = "vgs")]
    VGList,
    /// Create a volume group.
    #[strum(serialize = "vgcreate")]
    VGCreate,
    /// Change volume group attributes.
    #[strum(serialize = "vgchange")]
    VGChange,
    /// Remove volume group(s).
    #[strum(serialize = "vgremove")]
    VGRemove,
    /// Create a logical volume.
    #[strum(serialize = "lvcreate")]
    LVCreate,
    /// Change the attributes of logical volume(s).
    #[strum(serialize = "lvchange")]
    LVChange,
    /// Resize the logical volume.
    #[strum(serialize = "lvresize")]
    LVResize,
    /// Remove logical volume(s) from the system.
    #[strum(serialize = "lvremove")]
    LVRemove,
    /// Display information about logical volumes.
    #[strum(serialize = "lvs")]
    LVList,
}

/// LVM wrapper over `Command` with added qol such as error mapping and
/// decoding of json output reports.
pub(super) struct LvmCmd {
    cmd: &'static str,
    cmder: Command,
}

/// Used to decode the json output for lvm commands, example
/// sudo vgs --options=vg_size,vg_free --units=b --nosuffix --report-format=json
///   {
///       "report": [
///           {
///               "vg": [
///                   {"vg_name": "pool", "vg_size":"15372124160",
/// "vg_free":"15372124160"}                ]
///           }
///       ]
///   }
#[derive(Debug, Serialize, Deserialize)]
struct LvReport<T> {
    report: Vec<T>,
}

impl LvmCmd {
    /// See `Command` Help.
    pub(super) fn new(cmd: &'static str) -> Self {
        Self {
            cmd,
            cmder: Command::new(cmd),
        }
    }
    /// Prepare a `Command` for `LvmSubCmd::PVCreate`.
    pub(super) fn pv_create() -> Self {
        Self::new(LvmSubCmd::PVCreate.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::PVRemove`.
    pub(super) fn pv_remove() -> Self {
        Self::new(LvmSubCmd::PVRemove.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::VGCreate`.
    pub(super) fn vg_create() -> Self {
        Self::new(LvmSubCmd::VGCreate.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::VGList`.
    pub(super) fn vg_list() -> Self {
        Self::new(LvmSubCmd::VGList.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::VGChange`.
    pub(super) fn vg_change(vg_name: &str) -> Self {
        Self::new(LvmSubCmd::VGChange.as_ref()).arg(vg_name)
    }
    /// Prepare a `Command` for `LvmSubCmd::VGRemove`.
    pub(super) fn vg_remove() -> Self {
        Self::new(LvmSubCmd::VGRemove.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::LVCreate`.
    pub(super) fn lv_create() -> Self {
        Self::new(LvmSubCmd::LVCreate.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::LVChange`.
    pub(super) fn lv_change() -> Self {
        Self::new(LvmSubCmd::LVChange.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::LVResize`.
    pub(super) fn lv_resize() -> Self {
        Self::new(LvmSubCmd::LVResize.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::LVRemove`.
    pub(super) fn lv_remove() -> Self {
        Self::new(LvmSubCmd::LVRemove.as_ref())
    }
    /// Prepare a `Command` for `LvmSubCmd::LVList`.
    pub(super) fn lv_list() -> Self {
        Self::new(LvmSubCmd::LVList.as_ref())
    }
    /// Runs the LVM command with the provided `Command` arguments et all and
    /// returns an LVM specific report containing an output type `T`.
    /// >> Note: This requires the json output to be specified in args.
    ///
    /// # Errors
    ///
    /// `Error::LvmBinSpawnErr` => Failed to execute or await for completion.
    /// `Error::LvmBinErr` => Completed with an exit code.
    /// `Error::JsonParsing` => StdOut output is not a valid json for `T`.
    /// `Error::ReportMissing` => Output does not contain a report for `T`.
    pub(super) async fn report<T: for<'a> Deserialize<'a>>(
        self,
    ) -> Result<T, Error> {
        let cmd = self.cmd;
        let json_output: LvReport<T> = self.output_json().await?;

        let report: T = json_output.report.into_iter().next().ok_or(
            Error::ReportMissing {
                command: cmd.to_string(),
            },
        )?;

        Ok(report)
    }

    /// Runs the LVM command with the provided `Command` arguments et all and
    /// returns the type `T` object decoded from the output json format.
    /// >> Note: This requires the json output to be specified in args.
    ///
    /// # Errors
    ///
    /// `Error::LvmBinSpawnErr` => Failed to execute or await for completion.
    /// `Error::LvmBinErr` => Completed with an exit code.
    /// `Error::JsonParsing` => StdOut output is not a valid json for `T`.
    pub(super) async fn output_json<T: for<'a> Deserialize<'a>>(
        self,
    ) -> Result<T, Error> {
        let cmd = self.cmd;
        let output = self.output().await?;
        let json_output: T = serde_json::from_slice(output.stdout.as_slice())
            .map_err(|error| Error::JsonParsing {
            command: cmd.to_string(),
            error: error.to_string(),
        })?;

        Ok(json_output)
    }
    /// Tag the given `Property`.
    pub(super) fn tag(self, property: Property) -> Self {
        self.arg(property.add())
    }
    /// Tag the given `Property`.
    pub(super) fn tag_if(self, tag: bool, property: Property) -> Self {
        if tag {
            self.arg(property.add())
        } else {
            self
        }
    }
    /// Untag the given `Property`.
    pub(super) fn untag(self, property: Property) -> Self {
        self.arg(property.del())
    }
    /// See help for `Command::arg`.
    pub(super) fn arg<S: AsRef<OsStr>>(mut self, arg: S) -> Self {
        self.cmder.arg(arg);
        self
    }
    /// See help for `Command::args`.
    pub(super) fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.cmder.args(args);
        self
    }
    /// Runs the LVM command with the provided `Command` arguments et al.
    ///
    /// # Errors
    ///
    /// # Errors
    ///
    /// `Error::LvmBinSpawnErr` => Failed to execute or await for completion.
    /// `Error::LvmBinErr` => Completed with an exit code.
    pub(super) async fn run(self) -> Result<(), Error> {
        self.output().await.map(|_| ())
    }
    /// Runs the LVM command with the provided `Command` arguments et all and
    /// returns the `std::process::Output` in case of success.
    ///
    /// # Errors
    ///
    /// `Error::LvmBinSpawnErr` => Failed to execute or await for completion.
    /// `Error::LvmBinErr` => Completed with an exit code.
    pub(super) async fn output(
        mut self,
    ) -> Result<std::process::Output, Error> {
        tracing::trace!("{:?}", self.cmder);

        crate::tokio_run!(async move {
            let output = self.cmder.output().await.context(
                error::LvmBinSpawnErrSnafu {
                    command: self.cmd.to_string(),
                },
            )?;
            if !output.status.success() {
                let error = String::from_utf8_lossy(&output.stderr).to_string();
                return Err(Error::LvmBinErr {
                    command: self.cmd.to_string(),
                    error: error.trim_start().to_string(),
                });
            }
            Ok(output)
        })
    }
}

/// Serde deserializer helpers to help decode LVM json output from the cli.
pub(super) mod de {
    use serde::de::{self, Deserialize, Deserializer, Visitor};
    use std::{
        fmt::Display,
        iter::FromIterator,
        marker::PhantomData,
        str::FromStr,
    };

    /// Decode a number from a number as a string, example: "10".
    pub(crate) fn number_from_string<'de, T, D>(
        deserializer: D,
    ) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        T::from_str(&s).map_err(de::Error::custom)
    }

    /// Decode a comma-separated string into a vector of strings.
    pub(crate) fn comma_separated<'de, V, T, D>(
        deserializer: D,
    ) -> Result<V, D::Error>
    where
        V: FromIterator<T>,
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        struct CommaSeparated<V, T>(PhantomData<V>, PhantomData<T>);

        impl<'de, V, T> Visitor<'de> for CommaSeparated<V, T>
        where
            V: FromIterator<T>,
            T: FromStr,
            T::Err: Display,
        {
            type Value = V;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter
                    .write_str("string containing comma-separated elements")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let iter = s
                    .split(',')
                    .skip_while(|&x| x.is_empty())
                    .map(FromStr::from_str);
                Result::from_iter(iter).map_err(de::Error::custom)
            }
        }

        let visitor = CommaSeparated(PhantomData, PhantomData);
        deserializer.deserialize_str(visitor)
    }
}
