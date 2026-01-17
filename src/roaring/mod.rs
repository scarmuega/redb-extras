//! Roaring bitmap handling module.
//!
//! This module provides roaring-specific value handling including encoding,
//! decoding, and operations that require bitmap knowledge.

pub mod traits;
pub mod value;

// Re-export main types for public API
pub use traits::RoaringTableTrait;
pub use value::RoaringValue;
