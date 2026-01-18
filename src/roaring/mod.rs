//! Roaring bitmap handling module.
//!
//! This module provides roaring-specific value handling including encoding,
//! decoding, and operations that require bitmap knowledge.

use crate::Result;
use roaring::RoaringTreemap;
use std::fmt;

/// Errors specific to the roaring layer.
/// These are concerned with bitmap operations and value-specific semantics.
#[derive(Debug)]
pub enum RoaringError {
    /// Failed to serialize/deserialize RoaringTreemap
    SerializationFailed(String),

    /// Compaction operation failed
    CompactionFailed(String),

    /// Invalid roaring bitmap data
    InvalidBitmap(String),

    /// Size query failed
    SizeQueryFailed(String),
}

impl fmt::Display for RoaringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoaringError::SerializationFailed(msg) => {
                write!(f, "Roaring serialization failed: {}", msg)
            }
            RoaringError::CompactionFailed(msg) => {
                write!(f, "Compaction failed: {}", msg)
            }
            RoaringError::InvalidBitmap(msg) => {
                write!(f, "Invalid roaring bitmap: {}", msg)
            }
            RoaringError::SizeQueryFailed(msg) => {
                write!(f, "Size query failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for RoaringError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub trait RoaringValueReadOnlyTable<'txn, K> {
    /// Gets complete roaring bitmap for the given base key.
    ///
    /// # Arguments
    /// * `base_key` - The base key to retrieve (any type that implements redb::Key)
    ///
    /// # Returns
    /// The complete RoaringTreemap or empty if not found
    fn get_bitmap(&self, base_key: K) -> Result<RoaringTreemap>;

    fn iter_members(&self, base_key: K) -> Result<impl Iterator<Item = u64> + '_> {
        // Get complete bitmap and return iterator
        let bitmap = self.get_bitmap(base_key)?;
        Ok(bitmap.into_iter())
    }
}

pub trait RoaringValueTable<'txn, K> {
    /// Inserts a single member ID into the bitmap for the given base key.
    ///
    /// This method handles shard selection, head segment discovery, segment rolling,
    /// and bitmap serialization automatically.
    ///
    /// # Arguments
    /// * `base_key` - The base key to modify (any type that implements redb::Key)
    /// * `member` - The member to insert
    ///
    /// # Returns
    /// Result indicating success or failure
    fn insert_member(&mut self, base_key: K, member: u64) -> Result<()>;
}

mod facade;
mod value;

// Re-export main types for public API
pub use value::RoaringValue;
