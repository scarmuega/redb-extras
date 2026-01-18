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
    /// Gets complete roaring bitmap for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve (any type that implements redb::Key)
    ///
    /// # Returns
    /// The complete RoaringTreemap or empty if not found
    fn get_bitmap(&self, key: K) -> Result<RoaringTreemap>;

    /// Checks if a member exists in the bitmap for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to check
    /// * `member` - The member to check for
    ///
    /// # Returns
    /// True if the member exists, false otherwise
    fn contains_member(&self, key: K, member: u64) -> Result<bool> {
        let bitmap = self.get_bitmap(key)?;
        Ok(bitmap.contains(member))
    }

    /// Gets the number of members in the bitmap for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to query
    ///
    /// # Returns
    /// The number of members in the bitmap
    fn get_member_count(&self, key: K) -> Result<u64> {
        let bitmap = self.get_bitmap(key)?;
        Ok(bitmap.len())
    }

    fn iter_members(&self, key: K) -> Result<impl Iterator<Item = u64> + '_> {
        // Get complete bitmap and return iterator
        let bitmap = self.get_bitmap(key)?;
        Ok(bitmap.into_iter())
    }
}

pub trait RoaringValueTable<'txn, K>: RoaringValueReadOnlyTable<'txn, K> {
    /// Inserts a single member ID into the bitmap for the given key.
    ///
    /// This method handles shard selection, head segment discovery, segment rolling,
    /// and bitmap serialization automatically.
    ///
    /// # Arguments
    /// * `key` - The key to modify (any type that implements redb::Key)
    /// * `member` - The member to insert
    ///
    /// # Returns
    /// Result indicating success or failure
    fn insert_member(&mut self, key: K, member: u64) -> Result<()>;

    /// Removes a single member ID from the bitmap for the given key.
    ///
    /// This method handles shard selection, head segment discovery, segment rolling,
    /// and bitmap serialization automatically.
    ///
    /// # Arguments
    /// * `key` - The key to modify (any type that implements redb::Key)
    /// * `member` - The member to remove
    ///
    /// # Returns
    /// Result indicating success or failure
    fn remove_member(&mut self, key: K, member: u64) -> Result<()>;

    /// Inserts multiple members into the bitmap for the given key.
    ///
    /// This is a batch operation that is more efficient than individual inserts
    /// for large numbers of members.
    ///
    /// # Arguments
    /// * `key` - The key to modify
    /// * `members` - Iterator of members to insert
    ///
    /// # Returns
    /// Result indicating success or failure
    fn insert_members<I>(&mut self, key: K, members: I) -> Result<()>
    where
        K: Clone,
        I: IntoIterator<Item = u64>,
    {
        let mut current_bitmap = self.get_bitmap(key.clone())?;
        current_bitmap.extend(members);
        self.replace_bitmap(key, current_bitmap)
    }

    /// Removes multiple members from the bitmap for the given key.
    ///
    /// This is a batch operation that is more efficient than individual removals
    /// for large numbers of members.
    ///
    /// # Arguments
    /// * `key` - The key to modify
    /// * `members` - Iterator of members to remove
    ///
    /// # Returns
    /// Result indicating success or failure
    fn remove_members<I>(&mut self, key: K, members: I) -> Result<()>
    where
        K: Clone,
        I: IntoIterator<Item = u64>,
    {
        let mut current_bitmap = self.get_bitmap(key.clone())?;
        for member in members {
            current_bitmap.remove(member);
        }
        self.replace_bitmap(key, current_bitmap)
    }

    /// Clears all members from the bitmap for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to clear
    ///
    /// # Returns
    /// Result indicating success or failure
    fn clear_bitmap(&mut self, key: K) -> Result<()> {
        self.remove_key(key)
    }

    // Helper methods for internal implementation
    fn replace_bitmap(&mut self, key: K, bitmap: RoaringTreemap) -> Result<()>;
    fn remove_key(&mut self, key: K) -> Result<()>;
}

mod facade;
mod value;

// Re-export main types for public API
pub use value::RoaringValue;
