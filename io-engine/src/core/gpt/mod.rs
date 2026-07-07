//! GPT (GUID Partition Table) handling.
//!
//! - [`primitives`] — standards-based GPT primitives (headers, partition
//!   entries, protective MBR, label container, probing, serialisation,
//!   validation), with no knowledge of any particular partition layout.
//! - [`label`] — versioned on-disk label: a [`VersionedLabel`] built on
//!   top of [`primitives::GptLabel`] with a [`LabelVariant`] per version.
//! - [`maya`] — the dual MayaMeta/MayaData layout shared by all versions;
//!   per-version knobs are the inline [`maya::v1`] and [`maya::v2`] modules.

pub mod label;
pub mod maya;
pub mod primitives;

pub use label::{DataExtent, LabelVariant, LabelVersion, VersionedLabel};
pub use maya::{V1, V2};
pub use primitives::{
    Aligned, GptBuffer, GptDiskOps, GptDiskProps, GptEntry, GptGuid, GptHeader, GptLabel, GptName,
    LabelData, LabelError, LabelStatus, MbrEntry, Pmbr, ProbeError,
};
