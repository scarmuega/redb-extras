//! Table bucket storage utility module.
//!
//! This module provides bucket-based table grouping for sequence data by
//! mapping each bucket to its own redb table. It mirrors the bucketed key
//! approach but uses table-per-bucket instead of key prefixes.

use redb::{Key, MultimapTableDefinition, TableDefinition, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub mod iterator;

pub use crate::key_buckets::BucketError;
pub use iterator::{
    TableBucketIterExt, TableBucketMultimapIterExt, TableBucketRangeIterator,
    TableBucketRangeMultimapIterator,
};

/// Builder for table bucket configuration and name resolution.
#[derive(Debug, Clone)]
pub struct TableBucketBuilder {
    bucket_size: u64,
    table_prefix: String,
    table_names: Arc<Mutex<HashMap<u64, &'static str>>>,
}

impl TableBucketBuilder {
    /// Create a new builder with the specified bucket size and table prefix.
    ///
    /// # Arguments
    /// * `bucket_size` - Size of each bucket for integer division (must be > 0)
    /// * `table_prefix` - Prefix for bucket table names
    pub fn new(bucket_size: u64, table_prefix: impl Into<String>) -> Result<Self, BucketError> {
        if bucket_size == 0 {
            return Err(BucketError::InvalidBucketSize(bucket_size));
        }

        Ok(Self {
            bucket_size,
            table_prefix: table_prefix.into(),
            table_names: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Get the configured bucket size.
    pub fn bucket_size(&self) -> u64 {
        self.bucket_size
    }

    /// Get the configured table prefix.
    pub fn table_prefix(&self) -> &str {
        &self.table_prefix
    }

    /// Compute the bucket for the given sequence.
    pub fn bucket_for_sequence(&self, sequence: u64) -> u64 {
        sequence / self.bucket_size
    }

    /// Resolve the bucket table name, caching and leaking the name string.
    pub fn bucket_table_name(&self, bucket: u64) -> &'static str {
        let mut table_names = self
            .table_names
            .lock()
            .unwrap_or_else(|err| err.into_inner());

        if let Some(name) = table_names.get(&bucket) {
            return name;
        }

        let name = format!("{}_{}", self.table_prefix, bucket);
        let leaked = Box::leak(name.into_boxed_str());
        table_names.insert(bucket, leaked);
        leaked
    }

    /// Create a table definition for the given bucket.
    pub fn table_definition<K: Key + 'static, V: Value + 'static>(
        &self,
        bucket: u64,
    ) -> TableDefinition<'static, K, V> {
        TableDefinition::new(self.bucket_table_name(bucket))
    }

    /// Create a multimap table definition for the given bucket.
    pub fn multimap_table_definition<K: Key + 'static, V: Key + 'static>(
        &self,
        bucket: u64,
    ) -> MultimapTableDefinition<'static, K, V> {
        MultimapTableDefinition::new(self.bucket_table_name(bucket))
    }
}
