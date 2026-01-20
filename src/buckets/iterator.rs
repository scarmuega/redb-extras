//! Bucket range iterator implementation.
//!
//! Provides efficient iteration over bucket ranges for specific base keys.

use crate::buckets::key::{BucketedKey, KeyBuilder};
use crate::buckets::BucketError;
use redb::{ReadOnlyMultimapTable, ReadOnlyTable};
use std::collections::VecDeque;

/// Iterator over a range of buckets for a specific base key.
///
/// BucketRangeIterator performs point lookups for each bucket in the
/// requested sequence range, yielding only values that match the base key.
///
/// Implements `DoubleEndedIterator` for reverse iteration.
pub struct BucketRangeIterator<V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    table: ReadOnlyTable<BucketedKey<u64>, V>,
    base_key: u64,
    start_bucket: u64,
    end_bucket: u64,
    front_bucket: i64,
    back_bucket: i64,
    finished: bool,
}

impl<V> BucketRangeIterator<V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    /// Create a new bucket range iterator.
    pub fn new(
        table: ReadOnlyTable<BucketedKey<u64>, V>,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Self, BucketError> {
        if start_sequence > end_sequence {
            return Err(BucketError::InvalidRange {
                start: start_sequence,
                end: end_sequence,
            });
        }

        let bucket_size = key_builder.bucket_size();
        let start_bucket = start_sequence / bucket_size;
        let end_bucket = end_sequence / bucket_size;

        Ok(Self {
            table,
            base_key,
            start_bucket,
            end_bucket,
            front_bucket: start_bucket as i64,
            back_bucket: end_bucket as i64,
            finished: false,
        })
    }

    /// Get the bucket range.
    pub fn bucket_range(&self) -> (u64, u64) {
        (self.start_bucket, self.end_bucket)
    }
}

impl<V> Iterator for BucketRangeIterator<V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    type Item = Result<V, BucketError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        while self.front_bucket <= self.back_bucket {
            let bucket = self.front_bucket as u64;
            self.front_bucket += 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(Some(value_guard)) => {
                    return Some(Ok(V::from(value_guard.value())));
                }
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }

        self.finished = true;
        None
    }
}

impl<V> DoubleEndedIterator for BucketRangeIterator<V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        while self.front_bucket <= self.back_bucket {
            let bucket = self.back_bucket as u64;
            self.back_bucket -= 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(Some(value_guard)) => {
                    return Some(Ok(V::from(value_guard.value())));
                }
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }

        self.finished = true;
        None
    }
}

/// Iterator over a range of buckets for a specific base key in multimap tables.
///
/// This iterator flattens the multimap values, yielding each value in order
/// across the requested bucket range via per-bucket point lookups.
///
/// Implements `DoubleEndedIterator` to iterate buckets and values in reverse.
///
/// ```
/// use redb::{Database, MultimapTableDefinition, ReadableDatabase};
/// use redb_extras::buckets::{BucketMultimapIterExt, BucketedKey, KeyBuilder};
///
/// const TABLE: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
///     MultimapTableDefinition::new("bucketed_values");
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Database::create("example.redb")?;
/// let key_builder = KeyBuilder::new(100)?;
///
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_multimap_table(TABLE)?;
///     table.insert(key_builder.bucketed_key(42u64, 10), 1u64)?;
///     table.insert(key_builder.bucketed_key(42u64, 110), 2u64)?;
/// }
/// write_txn.commit()?;
///
/// let read_txn = db.begin_read()?;
/// let table = read_txn.open_multimap_table(TABLE)?;
/// let values: Vec<u64> = table
///     .bucket_range(&key_builder, 42u64, 0, 199)?
///     .collect::<Result<_, _>>()?;
/// assert_eq!(values, vec![1u64, 2u64]);
/// # Ok(())
/// # }
/// ```
pub struct BucketRangeMultimapIterator<V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    table: ReadOnlyMultimapTable<BucketedKey<u64>, V>,
    base_key: u64,
    start_bucket: u64,
    end_bucket: u64,
    front_bucket: i64,
    back_bucket: i64,
    finished: bool,
    front_values: Option<VecDeque<V>>,
    back_values: Option<VecDeque<V>>,
}

impl<V> BucketRangeMultimapIterator<V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    /// Create a new bucket range iterator for a multimap table.
    pub fn new(
        table: ReadOnlyMultimapTable<BucketedKey<u64>, V>,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Self, BucketError> {
        if start_sequence > end_sequence {
            return Err(BucketError::InvalidRange {
                start: start_sequence,
                end: end_sequence,
            });
        }

        let bucket_size = key_builder.bucket_size();
        let start_bucket = start_sequence / bucket_size;
        let end_bucket = end_sequence / bucket_size;

        Ok(Self {
            table,
            base_key,
            start_bucket,
            end_bucket,
            front_bucket: start_bucket as i64,
            back_bucket: end_bucket as i64,
            finished: false,
            front_values: None,
            back_values: None,
        })
    }

    /// Get the bucket range.
    pub fn bucket_range(&self) -> (u64, u64) {
        (self.start_bucket, self.end_bucket)
    }
}

impl<V> Iterator for BucketRangeMultimapIterator<V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    type Item = Result<V, BucketError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(values) = self.front_values.as_mut() {
                if let Some(value) = values.pop_front() {
                    return Some(Ok(value));
                }
                self.front_values = None;
            }

            if self.front_bucket > self.back_bucket {
                self.finished = true;
                return None;
            }

            let bucket = self.front_bucket as u64;
            self.front_bucket += 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(values) => {
                    let mut collected = VecDeque::new();
                    for value_result in values {
                        match value_result {
                            Ok(value_guard) => {
                                collected.push_back(V::from(value_guard.value()));
                            }
                            Err(err) => {
                                self.finished = true;
                                return Some(Err(BucketError::IterationError(format!(
                                    "Database error during point lookup: {}",
                                    err
                                ))));
                            }
                        }
                    }
                    if collected.is_empty() {
                        continue;
                    }
                    self.front_values = Some(collected);
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }
    }
}

impl<V> DoubleEndedIterator for BucketRangeMultimapIterator<V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if let Some(values) = self.back_values.as_mut() {
                if let Some(value) = values.pop_back() {
                    return Some(Ok(value));
                }
                self.back_values = None;
            }

            if self.front_bucket > self.back_bucket {
                self.finished = true;
                return None;
            }

            let bucket = self.back_bucket as u64;
            self.back_bucket -= 1;

            match self.table.get(&BucketedKey::new(self.base_key, bucket)) {
                Ok(values) => {
                    let mut collected = VecDeque::new();
                    for value_result in values {
                        match value_result {
                            Ok(value_guard) => {
                                collected.push_back(V::from(value_guard.value()));
                            }
                            Err(err) => {
                                self.finished = true;
                                return Some(Err(BucketError::IterationError(format!(
                                    "Database error during point lookup: {}",
                                    err
                                ))));
                            }
                        }
                    }
                    if collected.is_empty() {
                        continue;
                    }
                    self.back_values = Some(collected);
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(BucketError::IterationError(format!(
                        "Database error during point lookup: {}",
                        err
                    ))));
                }
            }
        }
    }
}

/// Extension trait for bucket iteration on read-only tables.
///
/// Bucket iteration uses per-bucket point lookups for the requested
/// sequence range, skipping buckets that have no stored value.
///
/// This consumes the table handle so the iterator can own it.
pub trait BucketIterExt<V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<BucketRangeIterator<V>, BucketError>;
}

impl<V> BucketIterExt<V> for ReadOnlyTable<BucketedKey<u64>, V>
where
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<BucketRangeIterator<V>, BucketError> {
        BucketRangeIterator::new(self, key_builder, base_key, start_sequence, end_sequence)
    }
}

/// Extension trait for bucket iteration on read-only multimap tables.
///
/// Returns a flattened iterator over values for the base key within the
/// requested bucket range, using per-bucket point lookups.
///
/// This consumes the table handle so the iterator can own it.
pub trait BucketMultimapIterExt<V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<BucketRangeMultimapIterator<V>, BucketError>;
}

impl<V> BucketMultimapIterExt<V> for ReadOnlyMultimapTable<BucketedKey<u64>, V>
where
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    fn bucket_range(
        self,
        key_builder: &KeyBuilder,
        base_key: u64,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<BucketRangeMultimapIterator<V>, BucketError> {
        BucketRangeMultimapIterator::new(self, key_builder, base_key, start_sequence, end_sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{Database, MultimapTableDefinition, ReadableDatabase, TableDefinition};
    use tempfile::NamedTempFile;

    const TEST_TABLE: TableDefinition<'static, BucketedKey<u64>, String> =
        TableDefinition::new("test_table");
    const TEST_MULTIMAP: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
        MultimapTableDefinition::new("test_multimap");

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
            let iter = BucketRangeIterator::new(
                read_txn.open_table(TEST_TABLE)?,
                &key_builder,
                123u64,
                0,
                199,
            )?;
            assert_eq!(iter.bucket_range(), (0, 1));

            // Test invalid range
            let invalid_iter = BucketRangeIterator::new(
                read_txn.open_table(TEST_TABLE)?,
                &key_builder,
                123u64,
                200,
                100,
            );
            assert!(invalid_iter.is_err());
        }

        // Test value iteration and base key filtering
        {
            let read_txn = db.begin_read()?;
            let iter = BucketRangeIterator::new(
                read_txn.open_table(TEST_TABLE)?,
                &key_builder,
                123u64,
                0,
                299,
            )?;
            let values: Vec<String> = iter.collect::<Result<_, _>>()?;
            assert_eq!(
                values,
                vec![
                    "value_50".to_string(),
                    "value_150".to_string(),
                    "value_250".to_string()
                ]
            );

            let iter = BucketRangeIterator::new(
                read_txn.open_table(TEST_TABLE)?,
                &key_builder,
                123u64,
                0,
                299,
            )?;
            let values: Vec<String> = iter.rev().collect::<Result<_, _>>()?;
            assert_eq!(
                values,
                vec![
                    "value_250".to_string(),
                    "value_150".to_string(),
                    "value_50".to_string()
                ]
            );

            let iter =
                read_txn
                    .open_table(TEST_TABLE)?
                    .bucket_range(&key_builder, 456u64, 0, 299)?;
            let values: Vec<String> = iter.collect::<Result<_, _>>()?;
            assert_eq!(
                values,
                vec!["other_50".to_string(), "other_150".to_string()]
            );
        }

        Ok(())
    }

    #[test]
    fn test_multimap_functionality() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let key_builder = KeyBuilder::new(100)?;

        {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_multimap_table(TEST_MULTIMAP)?;

                table.insert(key_builder.bucketed_key(123u64, 50), 10u64)?;
                table.insert(key_builder.bucketed_key(123u64, 50), 20u64)?;
                table.insert(key_builder.bucketed_key(123u64, 150), 30u64)?;
                table.insert(key_builder.bucketed_key(123u64, 150), 40u64)?;

                table.insert(key_builder.bucketed_key(456u64, 50), 99u64)?;
                table.insert(key_builder.bucketed_key(456u64, 50), 100u64)?;
            }
            write_txn.commit()?;
        }

        {
            let read_txn = db.begin_read()?;
            let iter = BucketRangeMultimapIterator::new(
                read_txn.open_multimap_table(TEST_MULTIMAP)?,
                &key_builder,
                123u64,
                0,
                199,
            )?;
            assert_eq!(iter.bucket_range(), (0, 1));

            let values: Vec<u64> = iter.collect::<Result<_, _>>()?;
            assert_eq!(values, vec![10u64, 20u64, 30u64, 40u64]);

            let iter = BucketRangeMultimapIterator::new(
                read_txn.open_multimap_table(TEST_MULTIMAP)?,
                &key_builder,
                123u64,
                0,
                199,
            )?;
            let values: Vec<u64> = iter.rev().collect::<Result<_, _>>()?;
            assert_eq!(values, vec![40u64, 30u64, 20u64, 10u64]);

            let iter = read_txn.open_multimap_table(TEST_MULTIMAP)?.bucket_range(
                &key_builder,
                456u64,
                0,
                99,
            )?;
            let values: Vec<u64> = iter.collect::<Result<_, _>>()?;
            assert_eq!(values, vec![99u64, 100u64]);
        }

        Ok(())
    }
}
