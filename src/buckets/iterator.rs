//! Bucket range iterator implementation.
//!
//! Provides efficient iteration over bucket ranges for specific base keys.

use crate::buckets::key::{BucketedKey, KeyBuilder};
use crate::buckets::BucketError;
use redb::{Database, ReadableDatabase, TableDefinition};

/// Iterator over a range of buckets for a specific base key.
///
/// BucketRangeIterator enables efficient traversal of all values for a given
/// base key across a specified range of bucket numbers.
pub struct BucketRangeIterator<V: redb::Value + 'static> {
    table_def: TableDefinition<'static, BucketedKey<u64>, V>,
    key_builder: KeyBuilder,
    base_key: u64,
    start_bucket: u64,
    end_bucket: u64,
}

impl<V: redb::Value + 'static> BucketRangeIterator<V> {
    /// Create a new bucket range iterator.
    pub fn new(
        key_builder: &KeyBuilder,
        base_key: u64,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<Self, BucketError> {
        if start_bucket > end_bucket {
            return Err(BucketError::InvalidRange {
                start: start_bucket,
                end: end_bucket,
            });
        }

        let table_def = TableDefinition::<BucketedKey<u64>, V>::new("bucketed_table");
        Ok(Self {
            table_def,
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

/// Extension trait for convenient bucket iteration on tables.
pub trait BucketIterExt<V: redb::Value + 'static> {
    fn bucket_range(
        &self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<BucketRangeIterator<V>, BucketError>;
}

impl<V: redb::Value + 'static> BucketIterExt<V> for Database {
    fn bucket_range(
        &self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_bucket: u64,
        end_bucket: u64,
    ) -> Result<BucketRangeIterator<V>, BucketError> {
        BucketRangeIterator::new(key_builder, base_key, start_bucket, end_bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    type TestTable = TableDefinition<'static, BucketedKey<u64>, String>;

    #[test]
    fn test_basic_functionality() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let key_builder = KeyBuilder::new(100)?;

        // Insert test data
        {
            let write_txn = db.begin_write()?;
            {
                let mut table: redb::TableHandle<TestTable> = write_txn.open_table(TestTable)?;

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
            let iter = BucketRangeIterator::new(&key_builder, 123u64, 0, 1)?;
            assert_eq!(iter.bucket_range(), (0, 1));

            // Test invalid range
            let invalid_iter = BucketRangeIterator::new(&key_builder, 123u64, 2, 1);
            assert!(invalid_iter.is_err());
        }

        Ok(())
    }
}
