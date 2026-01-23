//! Table bucket storage utility module.
//!
//! This module provides bucket-based table grouping for sequence data by
//! mapping each bucket to its own redb table. It mirrors the bucketed key
//! approach but uses table-per-bucket instead of key prefixes.

use crate::MergeableValue;
use redb::{
    Key, MultimapTableDefinition, ReadableTable, TableDefinition, TableHandle, Value,
    WriteTransaction,
};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
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

#[cfg(test)]
mod tests {
    use super::TableBucketBuilder;
    use crate::MergeableValue;
    use redb::{Database, ReadableDatabase, TableDefinition, TableError};
    use tempfile::NamedTempFile;

    impl MergeableValue for String {
        fn merge(existing: Option<Self>, incoming: Self) -> Self {
            match existing {
                Some(existing) => format!("{}+{}", existing, incoming),
                None => incoming,
            }
        }
    }

    #[test]
    fn merge_bucket_tables_into_target() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let builder = TableBucketBuilder::new(100, "merge_test")?;
        let target: TableDefinition<u64, String> = TableDefinition::new("merged");

        {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(builder.table_definition::<u64, String>(0))?;
                table.insert(1u64, "a".to_string())?;
                table.insert(2u64, "x".to_string())?;
            }
            {
                let mut table = write_txn.open_table(builder.table_definition::<u64, String>(1))?;
                table.insert(1u64, "b".to_string())?;
                table.insert(3u64, "y".to_string())?;
            }
            {
                let mut table = write_txn.open_table(builder.table_definition::<u64, String>(2))?;
                table.insert(1u64, "c".to_string())?;
            }
            write_txn.commit()?;
        }

        {
            let mut write_txn = db.begin_write()?;
            builder.merge(&mut write_txn, target, 0, 1)?;
            write_txn.commit()?;
        }

        let read_txn = db.begin_read()?;
        let target_read: TableDefinition<u64, String> = TableDefinition::new("merged");
        let table = read_txn.open_table(target_read)?;
        assert_eq!(table.get(1u64)?.unwrap().value(), "a+b");
        assert_eq!(table.get(2u64)?.unwrap().value(), "x");
        assert_eq!(table.get(3u64)?.unwrap().value(), "y");

        match read_txn.open_table(builder.table_definition::<u64, String>(0)) {
            Err(TableError::TableDoesNotExist(_)) => {}
            _ => panic!("bucket 0 table should be deleted"),
        }

        match read_txn.open_table(builder.table_definition::<u64, String>(1)) {
            Err(TableError::TableDoesNotExist(_)) => {}
            _ => panic!("bucket 1 table should be deleted"),
        }

        let bucket_two = read_txn.open_table(builder.table_definition::<u64, String>(2))?;
        assert_eq!(bucket_two.get(1u64)?.unwrap().value(), "c");

        Ok(())
    }

    #[test]
    fn merge_all_bucket_tables_into_target() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let builder = TableBucketBuilder::new(100, "merge_all")?;
        let target: TableDefinition<u64, String> = TableDefinition::new("merged_all");

        {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(builder.table_definition::<u64, String>(0))?;
                table.insert(1u64, "a".to_string())?;
            }
            {
                let mut table = write_txn.open_table(builder.table_definition::<u64, String>(2))?;
                table.insert(1u64, "c".to_string())?;
            }
            write_txn.commit()?;
        }

        {
            let mut write_txn = db.begin_write()?;
            builder.merge_all(&mut write_txn, target)?;
            write_txn.commit()?;
        }

        let read_txn = db.begin_read()?;
        let target_read: TableDefinition<u64, String> = TableDefinition::new("merged_all");
        let table = read_txn.open_table(target_read)?;
        assert_eq!(table.get(1u64)?.unwrap().value(), "a+c");

        match read_txn.open_table(builder.table_definition::<u64, String>(0)) {
            Err(TableError::TableDoesNotExist(_)) => {}
            _ => panic!("bucket 0 table should be deleted"),
        }

        match read_txn.open_table(builder.table_definition::<u64, String>(2)) {
            Err(TableError::TableDoesNotExist(_)) => {}
            _ => panic!("bucket 2 table should be deleted"),
        }

        Ok(())
    }
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

    /// Merge bucket tables into a single non-bucketed target table and delete the originals.
    pub fn merge<K, V>(
        &self,
        txn: &mut WriteTransaction,
        target: TableDefinition<'static, K, V>,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<(), BucketError>
    where
        K: Key + 'static,
        V: Value + MergeableValue + 'static,
        for<'b> V: From<V::SelfType<'b>>,
        for<'b> V: Borrow<V::SelfType<'b>>,
    {
        if start_bucket > end_bucket {
            return Err(BucketError::InvalidRange {
                start: start_bucket,
                end: end_bucket,
            });
        }

        let mut existing_tables = HashSet::new();
        let tables = txn.list_tables().map_err(|err| {
            BucketError::IterationError(format!("Failed to list tables: {}", err))
        })?;
        for table in tables {
            existing_tables.insert(table.name().to_string());
        }

        let mut target_table = txn.open_table(target).map_err(|err| {
            BucketError::IterationError(format!("Failed to open target table: {}", err))
        })?;

        for bucket in start_bucket..=end_bucket {
            let bucket_name = self.bucket_table_name(bucket);
            if !existing_tables.contains(bucket_name) {
                continue;
            }

            let definition = self.table_definition::<K, V>(bucket);
            let bucket_table = txn.open_table(definition).map_err(|err| {
                BucketError::IterationError(format!(
                    "Failed to open bucket table {}: {}",
                    bucket, err
                ))
            })?;

            let iter = bucket_table.iter().map_err(|err| {
                BucketError::IterationError(format!(
                    "Failed to iterate bucket table {}: {}",
                    bucket, err
                ))
            })?;

            for entry in iter {
                let (key_guard, value_guard) = entry.map_err(|err| {
                    BucketError::IterationError(format!(
                        "Failed to read bucket table {}: {}",
                        bucket, err
                    ))
                })?;

                let incoming = V::from(value_guard.value());
                let existing_value = match target_table.get(key_guard.value()) {
                    Ok(Some(existing_guard)) => Some(V::from(existing_guard.value())),
                    Ok(None) => None,
                    Err(err) => {
                        return Err(BucketError::IterationError(format!(
                            "Failed to read target table: {}",
                            err
                        )))
                    }
                };
                let merged = V::merge(existing_value, incoming);
                target_table
                    .insert(key_guard.value(), merged)
                    .map_err(|err| {
                        BucketError::IterationError(format!(
                            "Failed to write merged value: {}",
                            err
                        ))
                    })?;
            }

            drop(bucket_table);
            txn.delete_table(definition).map_err(|err| {
                BucketError::IterationError(format!(
                    "Failed to delete bucket table {}: {}",
                    bucket, err
                ))
            })?;
        }

        Ok(())
    }

    /// Merge all bucket tables discovered in the database into the target table.
    pub fn merge_all<K, V>(
        &self,
        txn: &mut WriteTransaction,
        target: TableDefinition<'static, K, V>,
    ) -> Result<(), BucketError>
    where
        K: Key + 'static,
        V: Value + MergeableValue + 'static,
        for<'b> V: From<V::SelfType<'b>>,
        for<'b> V: Borrow<V::SelfType<'b>>,
    {
        let Some((min_bucket, max_bucket)) = self.bucket_range_from_tables(txn)? else {
            return Ok(());
        };

        self.merge(txn, target, min_bucket, max_bucket)
    }

    fn bucket_range_from_tables(
        &self,
        txn: &WriteTransaction,
    ) -> Result<Option<(u64, u64)>, BucketError> {
        let mut min_bucket: Option<u64> = None;
        let mut max_bucket: Option<u64> = None;
        let prefix = format!("{}_", self.table_prefix);

        let tables = txn.list_tables().map_err(|err| {
            BucketError::IterationError(format!("Failed to list tables: {}", err))
        })?;

        for table in tables {
            let name = table.name();
            let Some(bucket_suffix) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Ok(bucket) = bucket_suffix.parse::<u64>() else {
                continue;
            };

            min_bucket = Some(min_bucket.map_or(bucket, |current| current.min(bucket)));
            max_bucket = Some(max_bucket.map_or(bucket, |current| current.max(bucket)));
        }

        Ok(min_bucket.zip(max_bucket))
    }
}
