use super::{
    cli::{de, CmnQueryArgs, LvmCmd},
    error::Error,
    options::{LvmPoolOpts, THIN_POOL_LV},
};
use crate::{
    bdev::PtplFileOps,
    core::Protocol,
    lvm::{
        dm_setup::DmSetup,
        property::{Property, PropertyType},
        LogicalVolume,
    },
    pool_backend::PoolArgs,
};
use serde::Deserialize;

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
    /// Corresponds to the vg_extent_size field in json output, the size of
    /// the physical extents in bytes.
    #[serde(deserialize_with = "de::number_from_string", rename = "vg_extent_size")]
    extent_size: u64,
    /// Corresponds to the vg_tags field in json output, the tags set in the
    /// volume group.
    #[serde(deserialize_with = "de::comma_separated", rename = "vg_tags")]
    tags: Vec<Property>,
    /// The physical vol disks used by the volume group.
    #[serde(deserialize_with = "de::comma_separated", rename = "pv_name")]
    disks: Vec<String>,
    /// Our thin pool, if the volume group has one. Loaded at list time.
    #[serde(skip)]
    thinpool: Option<ThinPool>,
    /// The total size of our replicas. Loaded at list time.
    #[serde(skip)]
    committed: u64,
}

/// A row of the lvs report used to load pool level state.
#[derive(Debug, Deserialize)]
pub(super) struct PoolLv {
    vg_name: String,
    lv_name: String,
    #[serde(deserialize_with = "de::number_from_string")]
    lv_size: u64,
    /// The first character is the volume type, 't' for a thin pool.
    lv_attr: String,
    #[serde(deserialize_with = "de::comma_separated")]
    lv_tags: Vec<Property>,
    #[serde(deserialize_with = "de::opt_number")]
    chunk_size: Option<u64>,
    #[serde(deserialize_with = "de::opt_percent")]
    data_percent: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PoolLvs {
    lv: Vec<PoolLv>,
}

impl PoolLv {
    /// List the logical volumes of the given volume groups, optionally
    /// filtered by an lvs selection.
    pub(super) async fn list(
        vg_names: &[String],
        select: Option<&str>,
    ) -> Result<Vec<Self>, Error> {
        if vg_names.is_empty() {
            return Ok(vec![]);
        }
        let mut cmd = LvmCmd::lv_list().args([
            "--units=b",
            "--nosuffix",
            "-q",
            "--report-format=json",
            "--options=vg_name,lv_name,lv_size,lv_attr,lv_tags,chunk_size,data_percent",
        ]);
        if let Some(select) = select {
            cmd = cmd.arg(format!("--select={select}"));
        }
        let report: PoolLvs = cmd.args(vg_names).report().await?;
        Ok(report.lv)
    }
}

/// Our thin pool, as reported by lvs.
#[derive(Debug, Clone)]
pub(super) struct ThinPool {
    /// Size of the thin pool data in bytes.
    size: u64,
    /// The chunk size in bytes.
    chunk: u64,
    /// Percentage of the data which is mapped, None while inactive.
    data_percent: Option<f64>,
}

impl ThinPool {
    /// Find our thin pool of the given volume group in the lvs rows.
    pub(super) fn find(lvs: &[PoolLv], vg_name: &str) -> Option<Self> {
        lvs.iter()
            .find(|lv| {
                lv.vg_name == vg_name && lv.lv_name == THIN_POOL_LV && lv.lv_attr.starts_with('t')
            })
            .map(|lv| Self {
                size: lv.lv_size,
                chunk: lv.chunk_size.unwrap_or_default(),
                data_percent: lv.data_percent,
            })
    }

    /// The chunk size in bytes.
    pub(super) fn chunk(&self) -> u64 {
        self.chunk
    }

    /// Bytes of the thin pool data which are mapped. An inactive thin pool
    /// reports no usage, so all of it is counted.
    fn used(&self) -> u64 {
        match self.data_percent {
            Some(percent) => ((self.size as f64) * percent / 100.0) as u64,
            None => self.size,
        }
    }

    /// Check this thin pool against the requested options. lvcreate rounds
    /// the size up to whole extents, and possibly to whole chunks.
    fn check(&self, opts: &LvmPoolOpts, extent_size: u64) -> Result<(), Error> {
        let Some(size) = opts.thinpool() else {
            return Ok(());
        };
        let min = round_up(size, extent_size);
        let max = round_up(size, extent_size.max(self.chunk));
        let size_ok = (min..=max).contains(&self.size);
        let chunk_ok = opts.chunk().is_none_or(|chunk| chunk == self.chunk);
        if size_ok && chunk_ok {
            return Ok(());
        }
        Err(Error::InvalidOption {
            error: format!(
                "'{}' does not match the existing thin pool of {}b with {}b chunks",
                opts.query(),
                self.size,
                self.chunk
            ),
        })
    }
}

/// Round the value up to a multiple of the unit.
fn round_up(value: u64, unit: u64) -> u64 {
    if unit == 0 {
        return value;
    }
    value.div_ceil(unit).saturating_mul(unit)
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
    pub(crate) async fn list(opts: &CmnQueryArgs) -> Result<Vec<VolumeGroup>, Error> {
        let mut args = vec![
            "--units=b",
            "--nosuffix",
            "-q",
            "--options=vg_name,vg_uuid,vg_size,vg_free,vg_extent_size,vg_tags,pv_name",
            "--report-format=json",
        ];
        let select = QueryArgs::query_args(opts)?;
        let select_query = format!("--select={select}");
        if !select.is_empty() {
            args.push(select_query.trim_end_matches(','));
        }
        let report: VolGroups = LvmCmd::vg_list().args(args.as_slice()).report().await?;

        let mut vgs = report
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

        let names: Vec<String> = vgs.iter().map(|vg| vg.name.clone()).collect();
        let lvs = PoolLv::list(&names, None).await?;
        vgs.iter_mut().for_each(|vg| vg.load_lvs(&lvs));
        Ok(vgs)
    }

    /// Load the thin pool and the size of our replicas from the lvs rows.
    fn load_lvs(&mut self, lvs: &[PoolLv]) {
        self.thinpool = ThinPool::find(lvs, &self.name);
        // Replicas and clones carry their name as a tag, snapshots do not.
        self.committed = lvs
            .iter()
            .filter(|lv| lv.vg_name == self.name)
            .filter(|lv| {
                lv.lv_tags
                    .iter()
                    .any(|tag| tag.type_() == PropertyType::LvName)
            })
            .map(|lv| lv.lv_size)
            .sum();
    }

    /// Import a volume group with the name provided or create one with the name
    /// and disks provided currently only import is supported.
    pub async fn create(args: PoolArgs) -> Result<VolumeGroup, Error> {
        tracing::info!(?args, "Creating/Importing LVM Volume Group");
        let opts = LvmPoolOpts::try_from_disks(&args.disks)?;
        match VolumeGroup::lookup(CmnQueryArgs::any().named(&args.name)).await {
            Ok(_) => {
                let vg = Self::import_inner(args).await?;
                info!(name = vg.name(), "LVM Volume Group imported successfully");
                Ok(vg)
            }
            Err(Error::NotFound { .. }) => {
                LvmCmd::pv_create().args(opts.devices()).run().await?;

                // A broken vg as a result of improper cleanup during tests
                let edm_e = format!("/dev/{}: already exists in filesystem", args.name);
                let cmd = || {
                    LvmCmd::vg_create()
                        .arg(&args.name)
                        .tag_if(!args.no_spdk, Property::Lvm)
                        .tag_if(
                            !opts.query().is_empty(),
                            Property::VgOpts(opts.query().to_string()),
                        )
                        .args(opts.devices())
                };
                match cmd().run().await {
                    Err(Error::LvmBinErr { error, command }) if error.starts_with(&edm_e) => {
                        // cleanup
                        let name = &args.name;
                        let Ok(dir) = std::fs::read_dir(format!("/dev/{name}")) else {
                            return Err(Error::LvmBinErr { error, command });
                        };
                        for entry in dir.flatten() {
                            DmSetup::remove(&entry.path().display().to_string()).await?;
                        }
                        cmd().run().await
                    }
                    _else => _else,
                }?;
                if let Err(error) = Self::create_thinpool(&args.name, &opts).await {
                    // The volume group was created here, so do not leave a pool
                    // without the thin pool it was asked for.
                    Self::rollback_create(&args.name, opts.devices()).await;
                    return Err(error);
                }
                info!(name = args.name, "LVM VolumeGroup created successfully");
                let lookup = CmnQueryArgs::ours_if(!args.no_spdk)
                    .named(&args.name)
                    .uuid_opt(&args.uuid);
                VolumeGroup::lookup(lookup).await
            }
            Err(error) => Err(error),
        }
    }

    /// Undo a partially created volume group, best effort.
    async fn rollback_create(vg_name: &str, devices: &[String]) {
        if let Err(error) = LvmCmd::vg_remove()
            .arg(format!("--select=vg_name={vg_name}"))
            .arg("-y")
            .run()
            .await
        {
            warn!(vg_name, %error, "Failed to remove partially created volume group");
            return;
        }
        if let Err(error) = LvmCmd::pv_remove().args(devices).run().await {
            warn!(vg_name, %error, "Failed to remove physical volumes");
        }
    }

    /// Create the volume group's thin pool if requested and not present.
    /// A thin pool which is present must match the requested options.
    async fn ensure_thinpool(&self, opts: &LvmPoolOpts) -> Result<(), Error> {
        match &self.thinpool {
            Some(thinpool) => thinpool.check(opts, self.extent_size),
            None => Self::create_thinpool(self.name(), opts).await,
        }
    }

    /// Create the volume group's thin pool, if requested.
    async fn create_thinpool(vg_name: &str, opts: &LvmPoolOpts) -> Result<(), Error> {
        let Some(size) = opts.thinpool() else {
            return Ok(());
        };
        // Without a metadata spare lvcreate never has to deactivate one, which
        // intermittently fails and asks for manual intervention.
        let cmd = || {
            let cmd = LvmCmd::lv_create()
                .arg(format!("-L{size}b"))
                .args(["-T", &format!("{vg_name}/{THIN_POOL_LV}")])
                .args(["--poolmetadataspare", "n"]);
            match opts.chunk() {
                Some(chunk) => cmd.arg(format!("--chunksize={chunk}b")),
                None => cmd,
            }
        };
        let no_target = "Required device-mapper target";
        let mut result = cmd().run().await;
        if matches!(&result, Err(Error::LvmBinErr { error, .. }) if error.contains(no_target)) {
            // When a table names a target, the kernel loads its module with the
            // host's modprobe, which also works from inside the container.
            DmSetup::load_target("thin-pool").await;
            result = cmd().run().await;
        }
        match result {
            Err(Error::LvmBinErr { error, .. }) if error.contains("insufficient free space") => {
                Err(Error::NoSpace { error })
            }
            Err(Error::LvmBinErr { error, .. }) if error.contains(no_target) => {
                Err(Error::NoThinPoolTarget {})
            }
            _else => _else,
        }?;
        info!(vg_name, "LVM thin pool created");
        Ok(())
    }

    /// Creates a [`LogicalVolume`] from this [`VolumeGroup`].
    pub async fn create_lvol(
        &self,
        args: crate::pool_backend::ReplicaArgs,
    ) -> Result<LogicalVolume, Error> {
        LogicalVolume::create(self, args, Protocol::Off, self.ours()).await
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
        info!(name = vg.name(), "LVM Volume Group imported successfully");
        vg.import_lvols().await?;
        Ok(vg)
    }

    /// Import a volume group by its name, match the disks on the volume group
    /// and if true add our tag to the volume group to make it available
    /// as a Pool.
    async fn import_inner(args: PoolArgs) -> Result<VolumeGroup, Error> {
        tracing::info!(?args, "Importing LVM Volume Group");
        let opts = LvmPoolOpts::try_from_disks(&args.disks)?;
        let name = &args.name;
        let mut vg = Self::lookup(CmnQueryArgs::any().named(name)).await?;

        if args.uuid.is_some() {
            return Err(Error::VgUuidSet {});
        }

        if &vg.disks != opts.devices() {
            return Err(Error::DisksMismatch {
                args: args.disks,
                vg: vg.disks,
            });
        }
        vg.ensure_thinpool(&opts).await?;
        if !opts.query().is_empty() {
            let opts_tag = Property::VgOpts(opts.query().to_string());
            if !vg.tags.contains(&opts_tag) {
                // Replace the tag instead of adding a second one, which would
                // leave the persisted options ambiguous.
                let stale: Vec<_> = vg
                    .tags
                    .iter()
                    .filter(|tag| tag.type_() == PropertyType::VgOpts)
                    .cloned()
                    .collect();
                let mut cmd = LvmCmd::vg_change(name);
                for tag in stale {
                    cmd = cmd.untag(tag);
                }
                cmd.tag(opts_tag.clone()).run().await?;
                vg.tags.retain(|tag| tag.type_() != PropertyType::VgOpts);
                vg.tags.push(opts_tag);
            }
        }

        if args.no_spdk {
            if !vg.tags.contains(&Property::Lvm) {
                return Ok(vg);
            }
            LvmCmd::vg_change(name).untag(Property::Lvm).run().await?;
            vg.tags.retain(|t| t != &Property::Lvm);
            return Ok(vg);
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
    /// > Note: The Vg is kept if it contains foreign LVs.
    pub async fn destroy(self) -> Result<(), Error> {
        self.destroy_(false).await
    }

    /// Delete the volume group.
    /// > Note: The Vg is first exported and then destroyed.
    /// > Warning: The Vg is destroyed even if containing foreign LVs.
    pub async fn purge(self) -> Result<(), Error> {
        self.destroy_(true).await
    }

    /// Delete the volume group.
    /// > Note: The Vg is first exported and then destroyed.
    pub async fn destroy_(mut self, purge_always: bool) -> Result<(), Error> {
        self.export().await?;

        let foreign_lvs = self.list_foreign_lvs().await?;
        let name = self.name().to_string();

        if purge_always || foreign_lvs.is_empty() {
            LvmCmd::vg_remove()
                .arg(format!("--select=vg_name={name}"))
                .arg("-y")
                .run()
                .await?;

            LvmCmd::pv_remove().args(&self.disks).run().await?;

            info!("LVM pool '{}' has been destroyed successfully", self.name());
        } else {
            warn!(
                "LVM pool '{}' is not destroyed as it contains foreign lvs: {foreign_lvs:?}",
                self.name()
            );
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
    /// This is an internal method that should not be used outside the LVM module.
    pub(super) async fn create_lvoli(
        &self,
        args: &crate::pool_backend::ReplicaArgs,
        share: Protocol,
        spdk: bool,
    ) -> Result<(), Error> {
        let vg_name = self.name();
        let uuid = &args.uuid;
        let ins_space = format!("Volume group \"{vg_name}\" has insufficient free space");
        let eexists =
            format!("Logical Volume \"{uuid}\" already exists in volume group \"{vg_name}\"");

        let cmd = if args.thin {
            if !self.has_thinpool() {
                return Err(Error::NoThinPool {});
            }
            LvmCmd::lv_create()
                .arg(format!("-V{}b", args.size))
                .args(["--thinpool", THIN_POOL_LV])
        } else {
            LvmCmd::lv_create().arg(format!("-L{}b", args.size))
        };

        let entity_id = args.entity_id.clone().unwrap_or_default();
        match cmd
            .args(["-n", uuid])
            .tag(Property::LvName(args.name.to_string()))
            .tag(Property::LvShare(share))
            .tag_if(!entity_id.is_empty(), Property::LvEntityId(entity_id))
            .tag_if(spdk, Property::Lvm)
            .arg(self.name())
            .run()
            .await
        {
            // not great, but not sure how else to map the error otherwise...
            Err(Error::LvmBinErr { error, .. }) if error.starts_with(&ins_space) => {
                Err(Error::NoSpace { error })
            }
            Err(Error::LvmBinErr { error, .. }) if error.starts_with(&eexists) => {
                Err(Error::Exists { error })
            }
            _else => _else,
        }?;

        info!(
            name = args.name,
            uuid, vg_name, "LVM Logical Volume created successfully"
        );

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
        match self.opts() {
            // Echo back the disks entries as given at create time, with the
            // persisted options query restored, so spec/actual comparisons
            // stay stable.
            Ok(opts) => opts.disks(),
            Err(_) => self.disks.clone(),
        }
    }

    /// The pool options, rebuilt from the persisted volume group tag.
    pub(super) fn opts(&self) -> Result<LvmPoolOpts, Error> {
        let query = self
            .tags
            .iter()
            .find(|tag| tag.type_() == PropertyType::VgOpts)
            .and_then(|tag| tag.clone().VgOpts())
            .unwrap_or_default();
        LvmPoolOpts::try_from_query(self.disks.clone(), &query)
    }

    /// Whether this volume group carries our thin pool.
    pub(super) fn has_thinpool(&self) -> bool {
        self.thinpool.is_some()
    }

    /// Get the volume group capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.size
    }

    /// Get the volume group committed bytes, the total size of our replicas.
    pub(crate) fn committed(&self) -> u64 {
        self.committed
    }

    /// Get the volume group cluster size.
    pub(crate) fn cluster_size(&self) -> u64 {
        4 * 1024 * 1024
    }

    /// Get the volume group used capacity.
    /// The thin pool takes its full size out of the volume group, but only
    /// its mapped data is in use.
    pub(crate) fn used(&self) -> u64 {
        let allocated = self.size.saturating_sub(self.free);
        match &self.thinpool {
            None => allocated,
            Some(thinpool) => allocated.saturating_sub(thinpool.size) + thinpool.used(),
        }
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

    /// Check if the volume group is owned by mayastor.
    fn ours(&self) -> bool {
        self.tags.iter().any(|t| t == &Property::Lvm)
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

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn opts(query: &str) -> LvmPoolOpts {
        LvmPoolOpts::try_from_disks(&[format!("disk0?{query}")]).unwrap()
    }

    fn thinpool(size: u64, chunk: u64) -> ThinPool {
        ThinPool {
            size,
            chunk,
            data_percent: Some(0.0),
        }
    }

    fn pool_lv(name: &str, size: u64, attr: &str, tags: &str, percent: &str) -> PoolLv {
        serde_json::from_value(serde_json::json!({
            "vg_name": "vg",
            "lv_name": name,
            "lv_size": size.to_string(),
            "lv_attr": attr,
            "lv_tags": tags,
            "chunk_size": if attr.starts_with('t') { "131072" } else { "0" },
            "data_percent": percent,
        }))
        .unwrap()
    }

    fn volume_group(size: u64, free: u64) -> VolumeGroup {
        serde_json::from_value(serde_json::json!({
            "vg_name": "vg",
            "vg_uuid": "vg-uuid",
            "vg_size": size.to_string(),
            "vg_free": free.to_string(),
            "vg_extent_size": (4 * MIB).to_string(),
            "vg_tags": "mayastor",
            "pv_name": "disk0",
        }))
        .unwrap()
    }

    #[test]
    fn round_up_to_unit() {
        assert_eq!(round_up(0, 4), 0);
        assert_eq!(round_up(1, 4), 4);
        assert_eq!(round_up(8, 4), 8);
        assert_eq!(round_up(9, 0), 9);
    }

    #[test]
    fn thinpool_check() {
        let extent = 4 * MIB;
        let pool = thinpool(64 * MIB, 128 * 1024);
        assert!(pool
            .check(&opts("thinpool=64m&thinpoolchunk=128k"), extent)
            .is_ok());
        assert!(pool.check(&opts("thinpool=64m"), extent).is_ok());
        // lvcreate rounds the size up to whole extents
        assert!(pool.check(&opts("thinpool=62m"), extent).is_ok());
        assert!(pool.check(&opts("thinpool=60m"), extent).is_err());
        assert!(pool.check(&opts("thinpool=65m"), extent).is_err());
        assert!(pool
            .check(&opts("thinpool=64m&thinpoolchunk=64k"), extent)
            .is_err());
        // no thin pool requested, so nothing to compare
        let plain = LvmPoolOpts::try_from_disks(&["disk0".to_string()]).unwrap();
        assert!(pool.check(&plain, extent).is_ok());
        // a chunk larger than the extent may round the size up further
        let pool = thinpool(16 * MIB, 8 * MIB);
        assert!(pool.check(&opts("thinpool=10m"), extent).is_ok());
        assert!(pool.check(&opts("thinpool=12m"), extent).is_ok());
        assert!(pool.check(&opts("thinpool=4m"), extent).is_err());
    }

    #[test]
    fn thinpool_accounting() {
        let lvs = vec![
            pool_lv(THIN_POOL_LV, 512 * MIB, "twi-aotz--", "", "25.00"),
            pool_lv(
                "r1",
                64 * MIB,
                "Vwi-aotz--",
                "mayastor,mayastor.lv.name=r1",
                "10.00",
            ),
            pool_lv(
                "r2",
                32 * MIB,
                "-wi-ao----",
                "mayastor,mayastor.lv.name=r2",
                "",
            ),
            pool_lv(
                "c1",
                64 * MIB,
                "Vwi-aotz--",
                "mayastor,mayastor.lv.name=c1,mayastor.lv.snapshot_uuid=s1",
                "0.00",
            ),
            pool_lv(
                "s1",
                64 * MIB,
                "Vwi---tz-k",
                "mayastor,mayastor.snap.name=s1,mayastor.snap.parent_id=r1",
                "",
            ),
            pool_lv("user", 16 * MIB, "-wi-a-----", "", ""),
        ];
        let mut vg = volume_group(1024 * MIB, 256 * MIB);
        vg.load_lvs(&lvs);
        assert!(vg.has_thinpool());
        assert_eq!(vg.thinpool.as_ref().map(ThinPool::chunk), Some(128 * 1024));
        // replicas and clones only
        assert_eq!(vg.committed(), 160 * MIB);
        // 768m allocated, of which the thin pool's 512m is only 25% mapped
        assert_eq!(vg.used(), 256 * MIB + 128 * MIB);

        // an inactive thin pool is counted as fully used
        let lvs = vec![pool_lv(THIN_POOL_LV, 512 * MIB, "twi---tz--", "", "")];
        vg.load_lvs(&lvs);
        assert_eq!(vg.used(), 768 * MIB);

        // not a thin pool, whatever its name
        let lvs = vec![pool_lv(THIN_POOL_LV, 512 * MIB, "-wi-a-----", "", "")];
        vg.load_lvs(&lvs);
        assert!(!vg.has_thinpool());
        assert_eq!(vg.committed(), 0);
        assert_eq!(vg.used(), 768 * MIB);
    }
}
