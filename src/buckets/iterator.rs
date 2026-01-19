//! Bucket range iterator implementation.
//!
//! Provides efficient iteration over bucket ranges for specific base keys.

use crate::buckets::key::{BucketedKey, KeyBuilder};
use crate::buckets::BucketError;
use redb::ReadOnlyTable;

/// Iterator over a range of buckets for a specific base key.
///
/// BucketRangeIterator enables efficient traversal of all values for a given
/// base key across a specified range of bucket numbers.
pub struct BucketRangeIterator<K, V>
where
    K: redb::Key + 'static,
    BucketedKey<K>: redb::Key + redb::Value,
    V: redb::Value + 'static,
{
    table: ReadOnlyTable<BucketedKey<K>, V>,
    key_builder: KeyBuilder,
    base_key: K,
    start_bucket: u64,
    end_bucket: u64,
}

impl<K, V> BucketRangeIterator<K, V>
where
    K: redb::Key,
    BucketedKey<K>: redb::Key + redb::Value,
    V: redb::Value + 'static,
{
    /// Create a new bucket range iterator.
    pub fn new(
        table: ReadOnlyTable<BucketedKey<K>, V>,
        key_builder: &KeyBuilder,
        base_key: K,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<Self, BucketError> {
        if start_bucket > end_bucket {
            return Err(BucketError::InvalidRange {
                start: start_bucket,
                end: end_bucket,
            });
        }

        Ok(Self {
            table,
            key_builder: key_builder.clone(),
            base_key,
            start_bucket,
            end_bucket,
        })
    }

    /// Get the bucket range.
    pub fn bucket_range(&self) -> (u64, u64) {
        (self.start_bucket, self.end_bucket)
    }
}

/// Extension trait for convenient bucket iteration on read-only tables.
pub trait BucketIterExt<K, V>
where
    K: redb::Key,
    BucketedKey<K>: redb::Key + redb::Value,
    V: redb::Value + 'static,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: K,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<BucketRangeIterator<K, V>, BucketError>;
}

impl<K, V> BucketIterExt<K, V> for ReadOnlyTable<BucketedKey<K>, V>
where
    K: redb::Key,
    BucketedKey<K>: redb::Key + redb::Value,
    V: redb::Value + 'static,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: K,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<BucketRangeIterator<K, V>, BucketError> {
        BucketRangeIterator::new(self, key_builder, base_key, start_bucket, end_bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{Database, ReadableDatabase, TableDefinition};
    use tempfile::NamedTempFile;

    const TEST_TABLE: TableDefinition<'static, BucketedKey<u64>, String> =
        TableDefinition::new("test_table");

    #[test]
    fn test_basic_functionality() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let key_builder = KeyBuilder::new(100)?;

        // Insert test data
        {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(TEST_TABLE)?;

                // Insert values for user123 in different buckets
                table.insert(key_builder.bucketed_key(123u64, 50), "value_50".to_string())?;
                table.insert(
                    key_builder.bucketed_key(123u64, 150),
                    "value_150".to_string(),
                )?;
                table.insert(
                    key_builder.bucketed_key(123u64, 250),
                    "value_250".to_string(),
                )?;

                // Insert values for user456 in same buckets (should not appear in iteration)
                table.insert(key_builder.bucketed_key(456u64, 50), "other_50".to_string())?;
                table.insert(
                    key_builder.bucketed_key(456u64, 150),
                    "other_150".to_string(),
                )?;
            }
            write_txn.commit()?;
        }

        // Test that we can create bucket range iterators
        {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TEST_TABLE)?;
            let iter = BucketRangeIterator::new(table, &key_builder, 123u64, 0, 1)?;
            assert_eq!(iter.bucket_range(), (0, 1));

            // Test invalid range
            let table = read_txn.open_table(TEST_TABLE)?;
            let invalid_iter = BucketRangeIterator::new(table, &key_builder, 123u64, 2, 1);
            assert!(invalid_iter.is_err());
        }

        Ok(())
    }
}
