//! Configuration for partitioned tables.
//!
//! Contains the configuration structure for generic partitioned storage.

/// Configuration for partitioned tables.
/// 
/// This structure defines how data is distributed across shards and segments,
/// providing control over write amplification and read performance.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Number of shards to distribute writes across
    /// 
    /// Higher values spread writes better for hot keys but increase read fanout.
    /// Must be between 1 and 65535.
    pub shard_count: u16,
    
    /// Maximum size in bytes for a single segment before rolling
    /// 
    /// When a segment exceeds this size, a new segment is created.
    /// This controls write amplification - smaller segments rewrite less data
    /// but increase read overhead.
    pub segment_max_bytes: usize,
    
    /// Whether to use a meta table for O(1) head segment discovery
    /// 
    /// With meta: Faster writes, additional storage overhead
    /// Without meta: Simpler, but requires scanning to find writable segment
    pub use_meta: bool,
}

impl PartitionConfig {
    /// Creates a new partition configuration with sensible defaults.
    /// 
    /// # Arguments
    /// * `shard_count` - Number of shards (1-65535)
    /// * `segment_max_bytes` - Maximum segment size in bytes
    /// * `use_meta` - Whether to use meta table
    /// 
    /// # Returns
    /// Validated configuration or error
    pub fn new(shard_count: u16, segment_max_bytes: usize, use_meta: bool) -> crate::error::Result<Self> {
        if shard_count == 0 {
            return Err(crate::error::PartitionError::InvalidShardCount(shard_count).into());
        }
        
        if segment_max_bytes == 0 {
            return Err(crate::error::PartitionError::InvalidSegmentSize(segment_max_bytes).into());
        }
        
        Ok(Self {
            shard_count,
            segment_max_bytes,
            use_meta,
        })
    }
    
    /// Creates a default configuration suitable for most use cases.
    pub fn default() -> Self {
        Self {
            shard_count: 16,           // Good balance for most workloads
            segment_max_bytes: 64 * 1024, // 64KB segments match roaring compression
            use_meta: true,             // Faster writes worth the overhead
        }
    }
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_valid_config() {
        let config = PartitionConfig::new(8, 1024, true);
        assert!(config.is_ok());
        
        let config = config.unwrap();
        assert_eq!(config.shard_count, 8);
        assert_eq!(config.segment_max_bytes, 1024);
        assert!(config.use_meta);
    }
    
    #[test]
    fn test_invalid_shard_count() {
        let config = PartitionConfig::new(0, 1024, true);
        assert!(config.is_err());
    }
    
    #[test]
    fn test_invalid_segment_size() {
        let config = PartitionConfig::new(8, 0, true);
        assert!(config.is_err());
    }
    
    #[test]
    fn test_default_config() {
        let config = PartitionConfig::default();
        assert_eq!(config.shard_count, 16);
        assert_eq!(config.segment_max_bytes, 64 * 1024);
        assert!(config.use_meta);
    }
}