//! Shard selection logic for partitioned storage.
//!
//! Provides deterministic shard selection using fast hashing to distribute
//! writes across multiple shards while maintaining consistent placement.

use crate::error::{PartitionError};
use crate::error::Result;
use xxhash_rust::xxh3::xxh3_64;

/// Selects a shard for a given base key and element id.
/// 
/// This uses a deterministic hash to ensure consistent shard placement
/// for the same (base_key, element_id) pair across different runs.
/// 
/// # Arguments
/// * `base_key` - The opaque base key
/// * `element_id` - The element identifier (e.g., bitmap member id)
/// * `shard_count` - Total number of available shards
/// 
/// # Returns
/// Shard index in range [0, shard_count)
pub fn select_shard(base_key: &[u8], element_id: u64, shard_count: u16) -> Result<u16> {
    if shard_count == 0 {
        return Err(PartitionError::InvalidShardCount(shard_count).into());
    }
    
    // Combine base_key and element_id for hashing
    let mut hasher = xxh3_64(base_key);
    hasher = xxh3_64(&element_id.to_be_bytes()) ^ hasher;
    
    // Convert hash to shard index
    let shard = (hasher % shard_count as u64) as u16;
    Ok(shard)
}

/// Selects a shard for operations that don't involve a specific element.
/// 
/// Used for operations like compaction or scanning where we need to iterate
/// through shards for a given base key.
/// 
/// # Arguments
/// * `base_key` - The opaque base key
/// * `shard_index` - Which shard to work with
/// * `shard_count` - Total number of available shards
/// 
/// # Returns
/// Shard index if valid, error if out of range
pub fn validate_shard_index(shard_index: u16, shard_count: u16) -> Result<u16> {
    if shard_index >= shard_count {
        return Err(PartitionError::InvalidShardCount(shard_count).into());
    }
    Ok(shard_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shard_selection_deterministic() {
        let base_key = b"test_key";
        let element_id = 12345;
        let shard_count = 16;
        
        let shard1 = select_shard(base_key, element_id, shard_count).unwrap();
        let shard2 = select_shard(base_key, element_id, shard_count).unwrap();
        
        assert_eq!(shard1, shard2);
    }
    
    #[test]
    fn test_shard_selection_distribution() {
        let base_key = b"test_key";
        let shard_count = 16;
        
        // Test that different element IDs distribute across shards
        let mut shards = std::collections::HashSet::new();
        for i in 0..100 {
            let shard = select_shard(base_key, i, shard_count).unwrap();
            shards.insert(shard);
        }
        
        // Should distribute across multiple shards (not all same)
        assert!(shards.len() > 1);
        assert!(shards.len() <= shard_count as usize);
    }
    
    #[test]
    fn test_different_keys_different_shards() {
        let key1 = b"key1";
        let key2 = b"key2";
        let element_id = 42;
        let shard_count = 16;
        
        let shard1 = select_shard(key1, element_id, shard_count).unwrap();
        let shard2 = select_shard(key2, element_id, shard_count).unwrap();
        
        // Different keys should likely go to different shards (not guaranteed, but probable)
        assert_ne!(shard1, shard2);
    }
    
    #[test]
    fn test_invalid_shard_count() {
        let base_key = b"test_key";
        let element_id = 123;
        
        let result = select_shard(base_key, element_id, 0);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_validate_shard_index() {
        let valid = validate_shard_index(5, 16);
        assert!(valid.is_ok());
        assert_eq!(valid.unwrap(), 5);
        
        let invalid = validate_shard_index(16, 16);
        assert!(invalid.is_err());
    }
}