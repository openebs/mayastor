//! Versioned GPT label: a [`GptLabel`] tagged with a layout version.
//!
//! The underlying label is a standard GPT label so the partitions remain
//! accessible even when read directly via NVMF/iSCSI or as local block
//! devices.
//!
//! Each version owns its layout in [`super::maya`] (`v1` and `v2`
//! submodules).
//! Versions implement [`LabelVariant`] for detection/validation and
//! expose their own `generate` constructor — there is no generic
//! generator here because each version's parameters differ.

use super::{
    maya::{self, V1, V2},
    primitives::{GptDiskOps, GptDiskProps, GptGuid, GptLabel, LabelError, ProbeError},
};

/// Position and length of the data partition on a child device.
///
/// All values are expressed in blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataExtent {
    /// First block of the data partition (inclusive).
    pub start: u64,
    /// Last block of the data partition (inclusive).
    pub end: u64,
}
impl DataExtent {
    fn size(&self) -> u64 {
        self.end - self.start + 1
    }

    /// Check if this partition size is multiple of the given number of blocks.
    pub fn is_multiple_of(&self, blocks: u64) -> bool {
        self.size().is_multiple_of(blocks)
    }
}

impl std::fmt::Display for DataExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "Start: {}, End: {}, Size: {}",
            self.start,
            self.end,
            self.size()
        )?;
        Ok(())
    }
}

/// Behaviour each label version must provide.
///
/// Kept deliberately small so it doesn't leak how many partitions a
/// layout has, what they are called or how they are addressed; those
/// details are private to each variant's implementation.
pub trait LabelVariant {
    /// Verify that `label` matches this variant's expected layout
    /// (offsets, sizes, names, etc.). Used both to probe an unknown disk
    /// (via [`LabelVersion::detect`]) and to validate a label that claims
    /// to be of this version.
    fn check(&self, label: &GptLabel) -> bool;

    /// Compute the placement of the data partition on a device with the
    /// given geometry for the given requested size.
    fn data_extent(
        &self,
        req_size: u64,
        num_blocks: u64,
        block_size: u64,
    ) -> Result<DataExtent, LabelError>;
}

/// Version of the on-disk label layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelVersion {
    /// Original layout. See [`super::maya::v1`].
    V1,
    /// 4 MiB cluster-aligned layout. See [`super::maya::v2`].
    V2,
}

impl std::fmt::Display for LabelVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::V1 => write!(f, "v1"),
            Self::V2 => write!(f, "v2"),
        }
    }
}

impl LabelVersion {
    /// All known versions, in any order. Used by [`Self::detect`] to walk
    /// the list when probing an existing label.
    const ALL: &'static [LabelVersion] = &[LabelVersion::V1, LabelVersion::V2];

    /// Return the [`LabelVariant`] backing this version.
    pub fn variant(self) -> &'static dyn LabelVariant {
        match self {
            Self::V1 => &V1,
            Self::V2 => &V2,
        }
    }

    /// Identify which label version, if any, wrote the partition table in
    /// `label`. Each variant decides what counts as a match. Returns
    /// `None` when no version recognises the layout.
    pub fn detect(label: &GptLabel) -> Option<Self> {
        Self::ALL.iter().copied().find(|v| v.variant().check(label))
    }

    /// Compute the data partition placement for this version.
    ///
    /// Thin forwarder to [`LabelVariant::data_extent`] so callers don't need
    /// to import the trait.
    pub fn data_extent(
        self,
        req_size: u64,
        num_blocks: u64,
        block_size: u64,
    ) -> Result<DataExtent, LabelError> {
        self.variant().data_extent(req_size, num_blocks, block_size)
    }

    /// Generate a fresh label of this version for the given disk, sized
    /// for `req_size` bytes of data (clamped to what the device can fit).
    pub fn generate(
        self,
        guid: GptGuid,
        disk: GptDiskProps,
        req_size: u64,
    ) -> Result<VersionedLabel, LabelError> {
        match self {
            Self::V1 => maya::generate::<V1>(guid, disk, req_size),
            Self::V2 => maya::generate::<V2>(guid, disk, req_size),
        }
    }

    /// Data start offset in blocks for this version.
    /// Useful for testing only.
    pub fn data_start_blks(&self, block_size: u64) -> u64 {
        use maya::Layout;
        match self {
            Self::V1 => V1::DATA_START_OFFSET / block_size,
            Self::V2 => V2::DATA_START_OFFSET / block_size,
        }
    }
}

/// A versioned on-disk label: a [`GptLabel`] paired with the
/// [`LabelVersion`] that produced it.
///
/// Construct via [`LabelVersion::generate`] or by probing an existing disk
/// with [`Self::probe`].
#[derive(Debug, Clone)]
pub struct VersionedLabel {
    /// Layout version this label was written with.
    pub version: LabelVersion,
    /// The underlying GPT label.
    pub gpt: GptLabel,
}

impl VersionedLabel {
    /// Probe the disk for a versioned label. Fails with
    /// [`ProbeError::IncorrectPartitions`] if a valid GPT is present but
    /// doesn't match any known layout version.
    pub async fn probe<T: GptDiskOps>(handle: &T) -> Result<VersionedLabel, LabelError> {
        let gpt = GptLabel::probe_label(handle).await?;
        let version = LabelVersion::detect(&gpt).ok_or(LabelError::InvalidLabel {
            source: ProbeError::IncorrectPartitions {},
        })?;
        Ok(VersionedLabel { version, gpt })
    }

    /// Verify the partition layout matches the recorded version.
    pub fn check(&self) -> bool {
        self.version.variant().check(&self.gpt)
    }
}

impl std::fmt::Display for VersionedLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Version: {}", self.version)?;
        writeln!(f, "{}", self.gpt)?;
        Ok(())
    }
}
