//! Versioned GPT label: a [`GptLabel`] tagged with a layout version.
//!
//! The underlying label is a standard GPT label so the partitions remain
//! accessible even when read directly via NVMF/iSCSI or as local block
//! devices.
//!
//! Each version owns its layout in its own module (see [`super::v1`]).
//! Versions implement [`LabelVariant`] for detection/validation and
//! expose their own `generate` constructor — there is no generic
//! generator here because each version's parameters differ.

use super::{
    primitives::{GptDiskOps, GptLabel, LabelError, ProbeError},
    v1::V1,
};

/// Behaviour each label version must provide.
///
/// Kept deliberately small so it doesn't leak how many partitions a
/// layout has, what they are called or how they are addressed; those
/// details are private to each variant's implementation.
pub trait LabelVariant {
    /// Return `true` if `label`'s partition table matches the signature
    /// of this variant (typically by inspecting partition type GUIDs).
    fn detect(&self, label: &GptLabel) -> bool;

    /// Verify that `label` matches this variant's expected layout
    /// (offsets, sizes, names, etc.).
    fn check(&self, label: &GptLabel) -> bool;
}

/// Version of the on-disk label layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelVersion {
    /// First (and currently the only) layout. See [`super::v1`].
    V1,
}

impl std::fmt::Display for LabelVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::V1 => write!(f, "v1"),
        }
    }
}

impl LabelVersion {
    /// All known versions, in any order. Used by [`Self::detect`] to walk
    /// the list when probing an existing label.
    const ALL: &'static [LabelVersion] = &[LabelVersion::V1];

    /// Return the [`LabelVariant`] backing this version.
    pub fn variant(self) -> &'static dyn LabelVariant {
        match self {
            Self::V1 => &V1::INSTANCE,
        }
    }

    /// Identify which label version, if any, wrote the partition table in
    /// `label`. Each variant decides what counts as a match. Returns
    /// `None` when no version recognises the layout.
    pub fn detect(label: &GptLabel) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|v| v.variant().detect(label))
    }
}

/// A versioned on-disk label: a [`GptLabel`] paired with the
/// [`LabelVersion`] that produced it.
///
/// Construct via a version-specific generator (e.g. [`V1::generate`]) or
/// by probing an existing disk with [`Self::probe`].
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
