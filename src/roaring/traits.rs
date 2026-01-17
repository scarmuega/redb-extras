//! Trait defining roaring-specific table operations.
//!
//! This trait abstracts operations that require roaring bitmap knowledge
//! from the generic partitioned storage layer.

use crate::error::Result;
use crate::roaring::value::RoaringValue;
use roaring::RoaringTreemap;

/// Trait for table-level operations that require roaring bitmap knowledge.
/// 
/// This trait provides the interface between the generic partitioned storage
/// layer and roaring-specific operations. It allows the `PartitionedTable<V>`
/// to work with any value type while preserving value-specific optimizations.
pub trait RoaringTableTrait {
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
    
    /// Compacts all segments for a given base key.
    /// 
    /// This operation merges multiple segments into fewer, larger segments
    /// to reduce read fanout and improve performance.
    /// 
    /// # Arguments
    /// * `base_key` - The base key whose segments should be compacted
    /// 
    /// # Returns
    /// Ok on successful compaction
    fn compact_segments(&self, base_key: &[u8]) -> Result<()>;
    
    /// Performs union operation across segments (scaffold for future implementation).
    /// 
    /// This method provides a hook for implementing efficient union operations
    /// across multiple segments of a partitioned bitmap.
    /// 
    /// # Arguments
    /// * `base_key` - The base key whose segments should be unioned
    /// 
    /// # Returns
    /// Unioned bitmap result
    fn union_segments(&self, base_key: &[u8]) -> Result<RoaringTreemap> {
        todo!("Union operation not yet implemented")
    }
    
    /// Performs intersection operation across segments (scaffold for future implementation).
    /// 
    /// This method provides a hook for implementing efficient intersection
    /// operations across multiple segments of a partitioned bitmap.
    /// 
    /// # Arguments
    /// * `base_key` - The base key whose segments should be intersected
    /// 
    /// # Returns
    /// Intersected bitmap result
    fn intersect_segments(&self, base_key: &[u8]) -> Result<RoaringTreemap> {
        todo!("Intersection operation not yet implemented")
    }
}

impl RoaringTableTrait for RoaringValue {
    fn get_value_size(&self, value: &RoaringTreemap) -> Result<usize> {
        self.get_serialized_size(value)
    }
    
    fn compact_segments(&self, base_key: &[u8]) -> Result<()> {
        todo!("Compaction not yet implemented - will be implemented in compact.rs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roaring::value::RoaringValue;
    
    #[test]
    fn test_roaring_value_implements_trait() {
        let handler = RoaringValue::new();
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        
        let size = handler.get_value_size(&bitmap).unwrap();
        assert!(size > 0);
    }
    
    #[test]
    fn test_empty_bitmap_size() {
        let handler = RoaringValue::new();
        let bitmap = RoaringTreemap::new();
        
        let size = handler.get_value_size(&bitmap).unwrap();
        // Should include at least the version byte
        assert_eq!(size, 1);
    }
}