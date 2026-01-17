//! redb-extras: Use-case agnostic utilities for redb.
//!
//! This crate provides a collection of focused utilities built on top of redb
//! for solving common low-level storage problems while maintaining explicit,
//! synchronous behavior and integrating naturally with redb's transaction model.
//!
//! The first utility provided is **Partitioned Roaring Bitmap Tables**, which offer
//! a key-value-like interface where values are Roaring bitmaps that are automatically
//! sharded and segmented to control write amplification.

// Re-export main public types
pub use error::Error;
pub use partition::{PartitionConfig, PartitionedTable};
pub use roaring::{RoaringTableTrait, RoaringValue};

// Re-export internal utilities for advanced users
pub mod encoding;
pub mod partition;
pub mod roaring;

// Error handling for public API
pub mod error;

use redb::{ReadTransaction, WriteTransaction};
use std::marker::PhantomData;

/// Configuration for PartitionedRoaringTable.
///
/// Combines generic partitioning configuration with roaring-specific settings.
#[derive(Debug, Clone)]
pub struct RoaringConfig {
    /// Generic partitioning configuration
    pub partition: PartitionConfig,
}

impl RoaringConfig {
    /// Creates a new roaring configuration.
    ///
    /// # Arguments
    /// * `partition` - Partition configuration
    ///
    /// # Returns
    /// New roaring configuration
    pub fn new(partition: crate::partition::PartitionConfig) -> Self {
        Self { partition }
    }

    /// Creates a default configuration suitable for most use cases.
    pub fn default() -> Self {
        Self {
            partition: crate::partition::PartitionConfig::default(),
        }
    }
}

impl Default for RoaringConfig {
    fn default() -> Self {
        Self::default()
    }
}

/// Opinionated facade combining partitioned storage with roaring bitmap semantics.
///
/// This type provides a high-level key-value interface where:
/// - Keys are opaque byte slices
/// - Values are Roaring bitmaps (RoaringTreemap<u64>)
/// - Large values are automatically sharded and segmented
/// - Write amplification is controlled via segment size limits
///
/// The facade is opinionated and handles transaction management internally.
/// For more flexibility, use the lower-level `PartitionedTable` and `RoaringValue`
/// utilities directly.
pub struct PartitionedRoaringTable {
    inner: crate::partition::PartitionedTable<crate::roaring::RoaringValue>,
    value_handler: crate::roaring::RoaringValue,
}

impl PartitionedRoaringTable {
    /// Creates a new partitioned roaring table.
    ///
    /// # Arguments
    /// * `name` - Table name for database storage
    /// * `config` - Configuration for the table
    ///
    /// # Returns
    /// New table instance
    pub fn new(name: &'static str, config: RoaringConfig) -> Self {
        Self {
            inner: crate::partition::PartitionedTable::new(name, config.partition),
            value_handler: crate::roaring::RoaringValue::new(),
        }
    }

    /// Gets the table name.
    pub fn name(&self) -> &'static str {
        self.inner.name()
    }

    /// Gets the configuration.
    pub fn config(&self) -> &PartitionConfig {
        self.inner.config()
    }

    /// Opens a read handle for the table.
    ///
    /// # Arguments
    /// * `txn` - Database read transaction
    ///
    /// # Returns
    /// Read handle for the table
    pub fn read<'txn>(&'txn self, txn: &'txn ReadTransaction) -> PartitionedRoaringRead<'txn> {
        PartitionedRoaringRead {
            table: &self.inner,
            value_handler: &self.value_handler,
            txn,
        }
    }

    /// Opens a write handle for the table.
    ///
    /// # Arguments
    /// * `txn` - Database write transaction
    ///
    /// # Returns
    /// Write handle for the table
    pub fn write<'txn>(
        &'txn self,
        txn: &'txn mut WriteTransaction,
    ) -> PartitionedRoaringWrite<'txn> {
        PartitionedRoaringWrite {
            table: &self.inner,
            value_handler: &self.value_handler,
            txn,
            _phantom: PhantomData,
        }
    }
}

/// Read operations for PartitionedRoaringTable.
pub struct PartitionedRoaringRead<'a> {
    table: &'a crate::partition::PartitionedTable<crate::roaring::RoaringValue>,
    value_handler: &'a crate::roaring::RoaringValue,
    txn: &'a ReadTransaction,
}

/// Write operations for PartitionedRoaringTable.
pub struct PartitionedRoaringWrite<'a> {
    table: &'a crate::partition::PartitionedTable<crate::roaring::RoaringValue>,
    value_handler: &'a crate::roaring::RoaringValue,
    txn: &'a mut WriteTransaction,
    _phantom: PhantomData<()>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::config::PartitionConfig;

    #[test]
    fn test_roaring_config_creation() {
        let partition_config = crate::partition::PartitionConfig::new(16, 64 * 1024, true).unwrap();
        let config = RoaringConfig::new(partition_config);

        assert_eq!(config.partition.shard_count, 16);
        assert_eq!(config.partition.segment_max_bytes, 64 * 1024);
        assert!(config.partition.use_meta);
    }

    #[test]
    fn test_default_config() {
        let config = RoaringConfig::default();
        assert_eq!(config.partition.shard_count, 16);
        assert_eq!(config.partition.segment_max_bytes, 64 * 1024);
        assert!(config.partition.use_meta);
    }

    #[test]
    fn test_partitioned_roaring_table_creation() {
        let config = RoaringConfig::default();
        let table = PartitionedRoaringTable::new("test_table", config);

        assert_eq!(table.name(), "test_table");
        assert_eq!(table.config().shard_count, 16);
    }
}
