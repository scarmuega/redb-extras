//! Roaring bitmap handling module.
//! 
//! This module provides roaring-specific value handling including encoding,
//! decoding, and operations that require bitmap knowledge.

pub mod value;
pub mod traits;

// Re-export main types for public API
pub use value::RoaringValue;
pub use traits::RoaringTableTrait;