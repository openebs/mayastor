//! Properties are attributes which are persisted in a given LVM resource and
//! which can be used to identify or retrieve specific information from a
//! resource, even across reboots.

use crate::core::Protocol;
use std::str::FromStr;

#[macro_export]
macro_rules! impl_properties {
    ($tag:ident, $tag_key:literal, $($name:ident,$value:ty,$key:literal,)+) => {
        /// Various types of properties which we persist with LVM.
        #[derive(Eq, PartialEq, Debug, Clone)]
        pub(crate) enum Property {
            $tag,
            Unknown(String,String),
            $(
                $name($value),
            )+
        }
        impl Property {
            /// Get this properties type.
            pub(super) fn type_(&self) -> PropertyType {
                match self {
                    Self::$tag => PropertyType::$tag,
                    Self::Unknown(_, _) => PropertyType::Unknown,
                    $(Self::$name(_) => PropertyType::$name,)+
                }
            }
            $(
                /// If the property is $name then get its value.
                #[allow(non_snake_case)]
                pub(super) fn $name(self) -> Option<$value> {
                    match self {
                        Self::$name(value) => Some(value),
                        _ => None,
                    }
                }
            )+
            /// The type of this property a string.
            pub(super) fn key(&self) -> &str {
                match self {
                    Self::Unknown(key, _) => key,
                     types => types.type_().value(),
                }
            }
        }
        /// All types of properties.
        #[derive(Eq, PartialEq, Debug, Clone)]
        pub enum PropertyType {
            $tag,
            Unknown,
            $(
                $name,
            )+
        }
        impl PropertyType {
            /// The type of this property a string.
            pub(super) fn value(&self) -> &'static str {
                match self {
                    Self::$tag => $tag_key,
                    Self::Unknown => "",
                    $(
                        Self::$name => $key,
                    )+
                }
            }
        }
        impl FromStr for Property {
            type Err = &'static str;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self::new(s))
            }
        }
        impl FromStr for PropertyType {
            type Err = &'static str;

            fn from_str(key: &str) -> Result<Self, Self::Err> {
                match key {
                    $tag_key => Ok(Self::$tag),
                    $(
                        $key => Ok(Self::$name),
                    )+
                    _ => Err(""),
                }
            }
        }
    };
}

impl_properties! {
    Lvm,                                        "mayastor",
    LvName,            String,                  "mayastor.lv.name",
    LvShare,           crate::core::Protocol,   "mayastor.lv.share",
    LvAllowedHosts,    Vec<String>,             "mayastor.lv.allowed_hosts",
    LvEntityId,        String,                  "mayastor.lv.entity_id",
    LvSnapshotUuid,    String,                  "mayastor.lv.snapshot_uuid",
    VgOpts,            String,                  "mayastor.vg.opts",
    SnapName,          String,                  "mayastor.snap.name",
    SnapEntityId,      String,                  "mayastor.snap.entity_id",
    SnapParentId,      String,                  "mayastor.snap.parent_id",
    SnapTxnId,         String,                  "mayastor.snap.txn_id",
    SnapCreateTime,    String,                  "mayastor.snap.create_time",
}

impl Property {
    /// The value of this property as a string.
    pub(super) fn value(&self) -> Option<String> {
        match self {
            Property::Lvm => None,
            Property::LvName(name) => Some(name.to_owned()),
            Property::LvShare(protocol) => Some(protocol.value_str().to_owned()),
            Property::LvAllowedHosts(hosts) => Some(hosts.join(",").to_owned()),
            Property::LvEntityId(entity_id) => Some(entity_id.to_owned()),
            Property::LvSnapshotUuid(uuid) => Some(uuid.to_owned()),
            Property::VgOpts(opts) => Some(opts.to_owned()),
            Property::SnapName(name) => Some(name.to_owned()),
            Property::SnapEntityId(entity_id) => Some(entity_id.to_owned()),
            Property::SnapParentId(parent_id) => Some(parent_id.to_owned()),
            Property::SnapTxnId(txn_id) => Some(txn_id.to_owned()),
            Property::SnapCreateTime(time) => Some(time.to_owned()),
            Property::Unknown(_, value) => Some(value.to_owned()),
        }
    }
    /// Format this property as a key value tag: key=value.
    pub(super) fn tag(&self) -> String {
        let key = self.key();
        match self.value() {
            None => key.to_string(),
            Some(value) => format!("{key}={value}"),
        }
    }
    /// Add the property.
    pub(super) fn add(&self) -> String {
        let key = self.key();
        match self.value() {
            None => format!("--addtag={key}"),
            Some(value) => format!("--addtag={key}={value}"),
        }
    }
    /// Remove the property.
    pub(super) fn del(&self) -> String {
        let key = self.key();
        match self.value() {
            None => format!("--deltag={key}"),
            Some(value) => format!("--deltag={key}={value}"),
        }
    }

    /// Builds a property from the given key and value.
    /// If the pair is not valid then nothing is returned.
    fn new_known(key: &str, value: &str) -> Option<Self> {
        match PropertyType::from_str(key).ok()? {
            PropertyType::Lvm => Some(Self::Lvm),
            PropertyType::LvName => Some(Self::LvName(value.to_owned())),
            PropertyType::LvShare => Some(Self::LvShare(Protocol::from_value(value))),
            PropertyType::LvAllowedHosts => Some(Self::LvAllowedHosts(
                value
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>(),
            )),
            PropertyType::LvEntityId => Some(Self::LvEntityId(value.to_owned())),
            PropertyType::LvSnapshotUuid => Some(Self::LvSnapshotUuid(value.to_owned())),
            PropertyType::VgOpts => Some(Self::VgOpts(value.to_owned())),
            PropertyType::SnapName => Some(Self::SnapName(value.to_owned())),
            PropertyType::SnapEntityId => Some(Self::SnapEntityId(value.to_owned())),
            PropertyType::SnapParentId => Some(Self::SnapParentId(value.to_owned())),
            PropertyType::SnapTxnId => Some(Self::SnapTxnId(value.to_owned())),
            PropertyType::SnapCreateTime => Some(Self::SnapCreateTime(value.to_owned())),
            _ => None,
        }
    }

    /// Builds a property from the given tag, which should be in
    /// the following format: key=value
    /// The value may itself contain '=', as pool options do, so the tag is
    /// only split on the first one.
    /// If the pair is not valid then nothing is returned.
    pub(super) fn new(tag: &str) -> Self {
        match tag.split_once('=') {
            Some((key, value)) => Self::new_known(key, value)
                .unwrap_or_else(|| Property::Unknown(key.to_string(), value.to_string())),
            None => Self::new_known(tag, "")
                .unwrap_or_else(|| Property::Unknown(tag.to_string(), "".to_string())),
        }
    }
}

impl Protocol {
    fn value_str(&self) -> &str {
        match self {
            Protocol::Off => "off",
            Protocol::Nvmf => "nvmf",
        }
    }
    fn from_value(value: &str) -> Self {
        match value {
            "nvmf" => Self::Nvmf,
            _ => Self::Off,
        }
    }
}
