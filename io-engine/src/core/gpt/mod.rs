//! GPT (GUID Partition Table) handling.
//!
//! - [`primitives`] — standards-based GPT primitives (headers, partition
//!   entries, protective MBR, label container, probing, serialisation,
//!   validation), with no knowledge of any particular partition layout.
//! - [`label`] — versioned on-disk label: a [`VersionedLabel`] built on
//!   top of [`primitives::GptLabel`] with a [`LabelVariant`] per version.
//! - Per-version layout modules ([`v1`], ...) implement [`LabelVariant`].

pub mod label;
pub mod primitives;
pub mod v1;

pub use label::{LabelVariant, LabelVersion, VersionedLabel};
pub use primitives::{
    Aligned, GptBuffer, GptDiskOps, GptDiskProps, GptEntry, GptGuid, GptHeader, GptLabel, GptName,
    LabelData, LabelError, LabelStatus, MbrEntry, Pmbr, ProbeError,
};
pub use v1::V1;
