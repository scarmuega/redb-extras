//! Generic partitioned table implementation.
//!
//! Provides the core storage infrastructure for sharded and segmented data
//! that can work with any value type.

use crate::error::{PartitionError, Result};
use crate::partition::config::PartitionConfig;
use crate::partition::shard::select_shard;
use redb::{ReadableTable, WriteTransaction, ReadTransaction};

/// Generic partitioned table that stores values in sharded segments.
/// 
/// This type provides the core storage infrastructure without knowing anything
/// about specific value types. It handles the mechanics of:
/// - Sharding writes across multiple partitions
/// - Segmenting large values to control write amplification
/// - Optional meta table for O(1) head segment discovery
/// 
/// The `V` parameter represents the value handler type that knows how to
/// encode/decode and manipulate specific value types.
pub struct PartitionedTable<V> {
    name: &'static str,
    config: PartitionConfig,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> PartitionedTable<V> {
    /// Creates a new partitioned table with the given configuration.
    /// 
    /// # Arguments
    /// * `name` - Table name for database storage
    /// * `config` - Partitioning configuration
    /// 
    /// # Returns
    /// New partitioned table instance
    pub fn new(name: &'static str, config: PartitionConfig) -> Self {
        Self {
            name,
            config,
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Returns the table name.
    pub fn name(&self) -> &'static str {
        self.name
    }
    
    /// Returns the configuration.
    pub fn config(&self) -> &PartitionConfig {
        &self.config
    }
    
    /// Selects the appropriate shard for a given base key and element.
    pub fn select_shard(&self, base_key: &[u8], element_id: u64) -> Result<u16> {
        select_shard(base_key, element_id, self.config.shard_count)
    }
}

/// Read operations for partitioned tables.
/// 
/// Provides read-only access to partitioned data without the ability to modify.
pub struct PartitionedRead<'a, V> {
    table: &'a PartitionedTable<V>,
    txn: &'a ReadTransaction,
}

impl<'a, V> PartitionedRead<'a, V> {
    /// Creates a new read handle.
    pub fn new(table: &'a PartitionedTable<V>, txn: &'a ReadTransaction) -> Self {
        Self { table, txn }
    }
    
    /// Gets the table reference.
    pub fn table(&self) -> &PartitionedTable<V> {
        self.table
    }
}

/// Write operations for partitioned tables.
/// 
/// Provides read-write access to partitioned data with the ability to modify values.
pub struct PartitionedWrite<'a, V> {
    table: &'a PartitionedTable<V>,
    txn: &'a mut WriteTransaction,
}

impl<'a, V> PartitionedWrite<'a, V> {
    /// Creates a new write handle.
    pub fn new(table: &'a PartitionedTable<V>, txn: &'a mut WriteTransaction) -> Self {
        Self { table, txn }
    }
    
    /// Gets the table reference.
    pub fn table(&self) -> &PartitionedTable<V> {
        self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::config::PartitionConfig;
    
    #[test]
    fn test_partitioned_table_creation() {
        let config = PartitionConfig::default();
        let table: PartitionedTable<()> = PartitionedTable::new("test_table", config);
        
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.config().shard_count, 16);
        assert!(table.config().use_meta);
    }
    
    #[test]
    fn test_shard_selection() {
        let config = PartitionConfig::new(8, 1024, true).unwrap();
        let table: PartitionedTable<()> = PartitionedTable::new("test", config);
        
        let base_key = b"test_key";
        let element_id = 12345;
        
        let shard = table.select_shard(base_key, element_id).unwrap();
        assert!(shard < 8);
        
        // Should be deterministic
        let shard2 = table.select_shard(base_key, element_id).unwrap();
        assert_eq!(shard, shard2);
    }
}