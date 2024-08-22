use serde::Deserialize;

use crate::{
    bdev::PtplFileOps,
    core::Protocol,
    lvm::{property::Property, LogicalVolume},
    pool_backend::PoolArgs,
};

use super::{
    cli::{de, CmnQueryArgs, LvmCmd},
    error::Error,
};

/// VG query arguments, allowing filtering via --select.
/// It's essentially a new-type wrapper over the common arguments
/// which can't be used to build the query because... it's common
/// between VG and LV..
#[derive(Default, Debug)]
pub(crate) struct QueryArgs(CmnQueryArgs);
impl QueryArgs {
    /// Get a comma-separated list of query selection args.
    /// todo: should be Display trait?
    pub(super) fn query(&self) -> Result<String, Error> {
        Self::query_args(&self.0)
    }
    /// Get a comma-separated list of query selection args.
    pub(super) fn query_args(args: &CmnQueryArgs) -> Result<String, Error> {
        let mut select = String::new();
        if let Some(vg_name) = &args.name {
            super::is_alphanumeric("vg_name", vg_name)?;
            select.push_str(&format!("vg_name={vg_name},"));
        }
        if let Some(vg_uuid) = &args.uuid {
            super::is_alphanumeric("vg_uuid", vg_uuid)?;
            // todo: validate more...
            select.push_str(&format!("vg_uuid={vg_uuid},"));
        }
        if let Some(vg_tag) = &args.tag {
            super::is_alphanumeric("vg_tag", vg_tag)?;
            select.push_str(&format!("vg_tags={vg_tag},"));
        }
        Ok(select)
    }
}
impl From<CmnQueryArgs> for QueryArgs {
    fn from(value: CmnQueryArgs) -> Self {
        Self(value)
    }
}

/// Used to decode the json output for vgs command to get
/// the capacity and free size of a given volume group
/// sudo vgs --options=vg_size,vg_free --units=b --nosuffix --reportformat=json
///   {
///       "report": [
///           {
///               "vg": [
///                   {"vg_name": "pool", "vg_size":"15372124160",
/// "vg_free":"15372124160"}                ]
///           }
///       ]
///   }
#[derive(Debug, Deserialize)]
struct VolGroups {
    /// Corresponds to the vg field in json output.
    vg: Vec<VolumeGroup>,
}

/// An LVM Volume Group.
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeGroup {
    /// Corresponds to the vg_name field in json output, the name of the
    /// volume group.
    #[serde(rename = "vg_name")]
    name: String,
    /// Corresponds to the vg_uuid field in json output, the uuid of the
    /// volume group.
    #[serde(rename = "vg_uuid")]
    uuid: String,
    /// Corresponds to the vg_size field in json output, the total capacity of
    /// volume group in bytes.
    #[serde(deserialize_with = "de::number_from_string", rename = "vg_size")]
    size: u64,
    /// Corresponds to the vg_free field in json output, the free space on
    /// volume group in bytes.
    #[serde(deserialize_with = "de::number_from_string", rename = "vg_free")]
    free: u64,
    /// Corresponds to the vg_tags field in json output, the tags set in the
    /// volume group.
    #[serde(deserialize_with = "de::comma_separated", rename = "vg_tags")]
    tags: Vec<Property>,
    /// The physical vol disks used by the volume group.
    #[serde(deserialize_with = "de::comma_separated", rename = "pv_name")]
    disks: Vec<String>,
}

impl VolumeGroup {
    /// Lookup a single volume group.
    pub(crate) async fn lookup(args: CmnQueryArgs) -> Result<Self, Error> {
        let vgs = Self::list(&args).await?;
        vgs.into_iter().next().ok_or(Error::NotFound {
            query: QueryArgs(args).query().unwrap_or_else(|e| e.to_string()),
        })
    }

    /// List all the volume groups using the provided list options.
    pub(crate) async fn list(
        opts: &CmnQueryArgs,
    ) -> Result<Vec<VolumeGroup>, Error> {
        let mut args = vec![
            "--units=b",
            "--nosuffix",
            "-q",
            "--options=vg_name,vg_uuid,vg_size,vg_free,vg_tags,pv_name",
            "--report-format=json",
        ];
        let select = QueryArgs::query_args(opts)?;
        let select_query = format!("--select={select}");
        if !select.is_empty() {
            args.push(select_query.trim_end_matches(','));
        }
        let report: VolGroups =
            LvmCmd::vg_list().args(args.as_slice()).report().await?;

        let vgs = report
            .vg
            .into_iter()
            // todo: not needed as we did the select?
            .filter(|vg| vg.matches(opts))
            .fold(Vec::<VolumeGroup>::new(), |mut acc, vg| {
                match acc.iter_mut().find(|e_vg| e_vg.name == vg.name) {
                    None => acc.push(vg),
                    Some(e_vg) => {
                        e_vg.disks.extend(vg.disks);
                    }
                }
                acc
            });

        Ok(vgs)
    }

    /// Import a volume group with the name provided or create one with the name
    /// and disks provided currently only import is supported.
    pub(crate) async fn create(args: PoolArgs) -> Result<VolumeGroup, Error> {
        let vg =
            match VolumeGroup::lookup(CmnQueryArgs::any().named(&args.name))
                .await
            {
                Ok(_) => Self::import_inner(args).await,
                Err(Error::NotFound {
                    ..
                }) => {
                    LvmCmd::pv_create().args(&args.disks).run().await?;

                    LvmCmd::vg_create()
                        .arg(&args.name)
                        .tag(Property::Lvm)
                        .args(args.disks)
                        .run()
                        .await?;
                    let lookup = CmnQueryArgs::ours()
                        .named(&args.name)
                        .uuid_opt(&args.uuid);
                    VolumeGroup::lookup(lookup).await
                }
                Err(error) => Err(error),
            }?;

        info!("The lvm vg pool '{}' has been created", vg.name());
        Ok(vg)
    }

    async fn import_lvols(&self) -> Result<(), Error> {
        self.list_lvs().await?;
        Ok(())
    }
    pub async fn list_lvs(&self) -> Result<Vec<LogicalVolume>, Error> {
        let query = super::QueryArgs::new()
            .with_lv(CmnQueryArgs::ours())
            .with_vg(CmnQueryArgs::ours().uuid(self.uuid()).named(self.name()));
        LogicalVolume::list(&query).await
    }
    async fn list_foreign_lvs(&self) -> Result<Vec<LogicalVolume>, Error> {
        let query = super::QueryArgs::new()
            .with_lv(CmnQueryArgs::any())
            .with_vg(CmnQueryArgs::any().uuid(self.uuid()).named(self.name()));
        LogicalVolume::list(&query)
            .await
            .map(|lvs| lvs.into_iter().filter(|lv| !lv.ours()).collect())
    }

    /// Import a volume group by its name, match the disks on the volume group
    /// and if true add our tag to the volume group to make it available
    /// as a Pool.
    pub(crate) async fn import(args: PoolArgs) -> Result<VolumeGroup, Error> {
        let vg = Self::import_inner(args).await?;
        vg.import_lvols().await?;
        Ok(vg)
    }

    /// Import a volume group by its name, match the disks on the volume group
    /// and if true add our tag to the volume group to make it available
    /// as a Pool.
    async fn import_inner(args: PoolArgs) -> Result<VolumeGroup, Error> {
        let name = &args.name;
        let mut vg = Self::lookup(CmnQueryArgs::any().named(name)).await?;

        if args.uuid.is_some() {
            return Err(Error::VgUuidSet {});
        }

        if vg.disks != args.disks {
            return Err(Error::DisksMismatch {
                args: args.disks,
                vg: vg.disks,
            });
        }

        if vg.tags.contains(&Property::Lvm) {
            return Ok(vg);
        }

        LvmCmd::vg_change(name)
            .arg("-q")
            .tag(Property::Lvm)
            .run()
            .await?;
        vg.tags.push(Property::Lvm);
        Ok(vg)
    }

    /// Delete the volume group.
    /// > Note: The Vg is first exported and then destroyed.
    pub(crate) async fn destroy(mut self) -> Result<(), Error> {
        self.export().await?;

        let foreign_lvs = self.list_foreign_lvs().await?;
        let name = self.name().to_string();

        if foreign_lvs.is_empty() {
            LvmCmd::vg_remove()
                .arg(format!("--select=vg_name={name}"))
                .arg("-y")
                .run()
                .await?;

            LvmCmd::pv_remove().args(&self.disks).run().await?;

            info!("LVM pool '{}' has been destroyed successfully", self.name());
        } else {
            warn!("LVM pool '{}' is not destroyed as it contains foreign lvs: {foreign_lvs:?}", self.name());
        }
        self.ptpl().destroy().ok();
        Ok(())
    }

    /// Exports the volume group by unloading all logical volumes and finally
    /// removing our tag from it.
    pub(crate) async fn export(&mut self) -> Result<(), Error> {
        let lvs = self.list_lvs().await?;
        for mut lv in lvs {
            lv.export_bdev().await?;
        }

        LvmCmd::vg_change(self.name())
            .untag(Property::Lvm)
            .run()
            .await?;

        info!("LVM pool '{}' has been exported successfully", self.name);
        Ok(())
    }

    /// Export all VG instances.
    pub(crate) async fn export_all() {
        let Ok(pools) = VolumeGroup::list(&CmnQueryArgs::ours()).await else {
            return;
        };

        for mut pool in pools {
            pool.export().await.ok();
        }
    }

    /// Create a logical volume in this volume group.
    pub(super) async fn create_lvol(
        &self,
        name: &str,
        size: u64,
        uuid: &str,
        thin: bool,
        entity_id: &Option<String>,
        share: Protocol,
    ) -> Result<(), Error> {
        let vg_name = self.name();
        let ins_space =
            format!("Volume group \"{vg_name}\" has insufficient free space");

        if thin {
            return Err(Error::ThinProv {});
        } else if size > self.free {
            return Err(Error::NoSpace {
                error: ins_space,
            });
        }

        let ins_space =
            format!("Volume group \"{vg_name}\" has insufficient free space");
        let entity_id = entity_id.clone().unwrap_or_default();
        match LvmCmd::lv_create()
            .arg(format!("-L{size}b"))
            .args(["-n", uuid])
            .tag(Property::LvName(name.to_string()))
            .tag(Property::LvShare(share))
            .tag_if(!entity_id.is_empty(), Property::LvEntityId(entity_id))
            .tag(Property::Lvm)
            .arg(self.name())
            .run()
            .await
        {
            // not great, but not sure how else to map the error otherwise...
            Err(Error::LvmBinErr {
                error, ..
            }) if error.starts_with(&ins_space) => Err(Error::NoSpace {
                error,
            }),
            _else => _else,
        }?;

        info!("lvm volume {name} created");

        Ok(())
    }

    /// Get the volume group name.
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Get the volume group uuid.
    pub(crate) fn uuid(&self) -> &str {
        &self.uuid
    }

    /// Get the volume group disks.
    pub(crate) fn disks(&self) -> Vec<String> {
        self.disks.clone()
    }

    /// Get the volume group capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.size
    }

    /// Get the volume group committed bytes.
    pub(crate) fn committed(&self) -> u64 {
        self.size
    }

    /// Get the volume group cluster size.
    pub(crate) fn cluster_size(&self) -> u64 {
        4 * 1024 * 1024
    }

    /// Get the volume group available capacity.
    pub(crate) fn available(&self) -> u64 {
        self.free
    }

    /// Get the volume group used capacity.
    pub(crate) fn used(&self) -> u64 {
        self.capacity() - self.available()
    }

    /// Check if the volume group matches the list options.
    fn matches(&self, opts: &CmnQueryArgs) -> bool {
        self.named(opts.name.as_ref())
            && self.tagged(opts.tag.as_ref())
            && self.uuided(opts.uuid.as_ref())
    }

    /// Check if the volume group name matches.
    fn named(&self, name: Option<&String>) -> bool {
        let eq = name.map(|name| &self.name == name).unwrap_or(true);
        tracing::trace!("{name:?} == {} ? {eq}", self.name);
        eq
    }
    /// Check if the volume group contains the given tag.
    fn tagged(&self, tag: Option<&String>) -> bool {
        let eq = tag
            .map(|tag| self.tags.iter().any(|ttag| ttag.key() == tag.as_str()))
            .unwrap_or(true);
        tracing::trace!("{tag:?} == {:?} ? {eq}", self.tags);
        eq
    }
    /// Check if the volume group uuid matches.
    fn uuided(&self, uuid: Option<&String>) -> bool {
        let eq = uuid.map(|uuid| &self.uuid == uuid).unwrap_or(true);
        tracing::trace!("{uuid:?} == {} ? {eq}", self.uuid);
        eq
    }

    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        VgPtpl::from(self.name())
    }
    /// Get a `PtplFileOps` from a VG name.
    pub(super) fn vg_ptpl(name: &str) -> VgPtpl {
        VgPtpl::from(name)
    }
}

/// Persist through power loss implementation for a VG (pool).
pub(super) struct VgPtpl {
    name: String,
}

impl From<&str> for VgPtpl {
    fn from(vg: &str) -> Self {
        Self {
            name: vg.to_string(),
        }
    }
}
impl PtplFileOps for VgPtpl {
    fn destroy(&self) -> Result<(), std::io::Error> {
        if let Some(path) = self.path() {
            if path.exists() {
                std::fs::remove_dir_all(path)?;
            }
        }
        Ok(())
    }

    fn subpath(&self) -> std::path::PathBuf {
        std::path::PathBuf::from("pool/vg/").join(&self.name)
    }
}
