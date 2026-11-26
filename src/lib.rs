//! Metadata-only concatenation of local Lance datasets using Lance 14.0.1.
//!
//! The output references the original files. Keep those files alive for as long
//! as the output is needed; source cleanup does not track these references.

mod merger;

pub use merger::Merger;
