//! Trait defining roaring-specific table operations.
//!
//! This trait abstracts operations that require roaring bitmap knowledge
//! from the generic partitioned storage layer.

use crate::Result;
use roaring::RoaringTreemap;

/// Trait for table-level operations to support segmentation.
pub trait SegmentedTableTrait {
    /// Gets the serialized size of a value for segment rolling decisions.
    ///
    /// This is used by the partition layer to determine when a segment
    /// has exceeded its maximum size and should be rolled.
    ///
    /// # Arguments
    /// * `value` - The roaring bitmap value to measure
    ///
    /// # Returns
    /// Serialized size in bytes including any version prefixes
    fn get_value_size(&self, value: &RoaringTreemap) -> Result<usize>;
}
