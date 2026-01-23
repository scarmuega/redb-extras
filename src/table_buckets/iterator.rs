//! Table bucket range iterator implementation.
//!
//! Provides efficient iteration over bucket ranges for specific base keys
//! by opening bucket-specific tables on demand.

use crate::key_buckets::BucketError;
use crate::table_buckets::TableBucketBuilder;
use redb::{ReadOnlyMultimapTable, ReadOnlyTable, ReadTransaction, TableError};
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::marker::PhantomData;

/// Iterator over a range of buckets for a specific base key.
///
/// Each bucket is stored in its own table. The iterator opens each bucket
/// table and performs a point lookup for the base key.
///
/// Implements `DoubleEndedIterator` for reverse iteration.
pub struct TableBucketRangeIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    txn: &'a ReadTransaction,
    builder: &'a TableBucketBuilder,
    base_key: K,
    start_bucket: u64,
    end_bucket: u64,
    front_bucket: i64,
    back_bucket: i64,
    finished: bool,
    _phantom: PhantomData<V>,
}

impl<'a, K, V> TableBucketRangeIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
    V: redb::Value + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    /// Create a new table bucket range iterator.
    pub fn new(
        txn: &'a ReadTransaction,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Self, BucketError> {
        if start_sequence > end_sequence {
            return Err(BucketError::InvalidRange {
                start: start_sequence,
                end: end_sequence,
            });
        }

        let bucket_size = builder.bucket_size();
        let start_bucket = start_sequence / bucket_size;
        let end_bucket = end_sequence / bucket_size;

        Ok(Self {
            txn,
            builder,
            base_key,
            start_bucket,
            end_bucket,
            front_bucket: start_bucket as i64,
            back_bucket: end_bucket as i64,
            finished: false,
            _phantom: PhantomData,
        })
    }

    /// Get the bucket range.
    pub fn bucket_range(&self) -> (u64, u64) {
        (self.start_bucket, self.end_bucket)
    }

    fn open_table(&self, bucket: u64) -> Result<Option<ReadOnlyTable<K, V>>, BucketError> {
        let definition = self.builder.table_definition::<K, V>(bucket);
        match self.txn.open_table(definition) {
            Ok(table) => Ok(Some(table)),
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(err) => Err(BucketError::IterationError(format!(
                "Failed to open bucket table {}: {}",
                bucket, err
            ))),
        }
    }
}

impl<'a, K, V> Iterator for TableBucketRangeIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
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

            let table = match self.open_table(bucket) {
                Ok(Some(table)) => table,
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };

            match table.get(self.base_key.clone()) {
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

impl<'a, K, V> DoubleEndedIterator for TableBucketRangeIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
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

            let table = match self.open_table(bucket) {
                Ok(Some(table)) => table,
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };

            match table.get(self.base_key.clone()) {
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
pub struct TableBucketRangeMultimapIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    txn: &'a ReadTransaction,
    builder: &'a TableBucketBuilder,
    base_key: K,
    start_bucket: u64,
    end_bucket: u64,
    front_bucket: i64,
    back_bucket: i64,
    finished: bool,
    front_values: Option<VecDeque<V>>,
    back_values: Option<VecDeque<V>>,
}

impl<'a, K, V> TableBucketRangeMultimapIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
    V: redb::Key + 'static,
    for<'b> V: From<V::SelfType<'b>>,
{
    /// Create a new table bucket range iterator for a multimap table.
    pub fn new(
        txn: &'a ReadTransaction,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Self, BucketError> {
        if start_sequence > end_sequence {
            return Err(BucketError::InvalidRange {
                start: start_sequence,
                end: end_sequence,
            });
        }

        let bucket_size = builder.bucket_size();
        let start_bucket = start_sequence / bucket_size;
        let end_bucket = end_sequence / bucket_size;

        Ok(Self {
            txn,
            builder,
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

    fn open_table(&self, bucket: u64) -> Result<Option<ReadOnlyMultimapTable<K, V>>, BucketError> {
        let definition = self.builder.multimap_table_definition::<K, V>(bucket);
        match self.txn.open_multimap_table(definition) {
            Ok(table) => Ok(Some(table)),
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(err) => Err(BucketError::IterationError(format!(
                "Failed to open bucket table {}: {}",
                bucket, err
            ))),
        }
    }
}

impl<'a, K, V> Iterator for TableBucketRangeMultimapIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
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

            let table = match self.open_table(bucket) {
                Ok(Some(table)) => table,
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };

            match table.get(self.base_key.clone()) {
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

impl<'a, K, V> DoubleEndedIterator for TableBucketRangeMultimapIterator<'a, K, V>
where
    K: redb::Key + Clone + 'static,
    for<'b> K: Borrow<K::SelfType<'b>>,
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

            let table = match self.open_table(bucket) {
                Ok(Some(table)) => table,
                Ok(None) => continue,
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            };

            match table.get(self.base_key.clone()) {
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

/// Extension trait for table bucket iteration on read transactions.
pub trait TableBucketIterExt {
    fn table_bucket_range<'a, K, V>(
        &'a self,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<TableBucketRangeIterator<'a, K, V>, BucketError>
    where
        K: redb::Key + Clone + 'static,
        for<'b> K: Borrow<K::SelfType<'b>>,
        V: redb::Value + 'static,
        for<'b> V: From<V::SelfType<'b>>;
}

impl TableBucketIterExt for ReadTransaction {
    fn table_bucket_range<'a, K, V>(
        &'a self,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<TableBucketRangeIterator<'a, K, V>, BucketError>
    where
        K: redb::Key + Clone + 'static,
        for<'b> K: Borrow<K::SelfType<'b>>,
        V: redb::Value + 'static,
        for<'b> V: From<V::SelfType<'b>>,
    {
        TableBucketRangeIterator::<K, V>::new(self, builder, base_key, start_sequence, end_sequence)
    }
}

/// Extension trait for table bucket iteration on read transactions for multimap tables.
pub trait TableBucketMultimapIterExt {
    fn table_bucket_multimap_range<'a, K, V>(
        &'a self,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<TableBucketRangeMultimapIterator<'a, K, V>, BucketError>
    where
        K: redb::Key + Clone + 'static,
        for<'b> K: Borrow<K::SelfType<'b>>,
        V: redb::Key + 'static,
        for<'b> V: From<V::SelfType<'b>>;
}

impl TableBucketMultimapIterExt for ReadTransaction {
    fn table_bucket_multimap_range<'a, K, V>(
        &'a self,
        builder: &'a TableBucketBuilder,
        base_key: K,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<TableBucketRangeMultimapIterator<'a, K, V>, BucketError>
    where
        K: redb::Key + Clone + 'static,
        for<'b> K: Borrow<K::SelfType<'b>>,
        V: redb::Key + 'static,
        for<'b> V: From<V::SelfType<'b>>,
    {
        TableBucketRangeMultimapIterator::<K, V>::new(
            self,
            builder,
            base_key,
            start_sequence,
            end_sequence,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_buckets::TableBucketBuilder;
    use redb::{Database, ReadableDatabase};
    use tempfile::NamedTempFile;

    #[test]
    fn test_table_bucket_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let builder = TableBucketBuilder::new(100, "table_bucket")?;

        {
            let write_txn = db.begin_write()?;
            {
                {
                    let mut table =
                        write_txn.open_table(builder.table_definition::<u64, String>(0))?;
                    table.insert(123u64, "value_50".to_string())?;
                    table.insert(456u64, "other_50".to_string())?;
                }

                {
                    let mut table =
                        write_txn.open_table(builder.table_definition::<u64, String>(1))?;
                    table.insert(123u64, "value_150".to_string())?;
                }

                {
                    let mut table =
                        write_txn.open_table(builder.table_definition::<u64, String>(2))?;
                    table.insert(123u64, "value_250".to_string())?;
                }
            }
            write_txn.commit()?;
        }

        let read_txn = db.begin_read()?;
        let iter = TableBucketRangeIterator::new(&read_txn, &builder, 123u64, 0, 299)?;
        assert_eq!(iter.bucket_range(), (0, 2));

        let values: Vec<String> = iter.collect::<Result<_, _>>()?;
        assert_eq!(
            values,
            vec![
                "value_50".to_string(),
                "value_150".to_string(),
                "value_250".to_string()
            ]
        );

        let iter = TableBucketRangeIterator::new(&read_txn, &builder, 123u64, 0, 299)?;
        let values: Vec<String> = iter.rev().collect::<Result<_, _>>()?;
        assert_eq!(
            values,
            vec![
                "value_250".to_string(),
                "value_150".to_string(),
                "value_50".to_string()
            ]
        );

        let iter = read_txn.table_bucket_range(&builder, 456u64, 0, 299)?;
        let values: Vec<String> = iter.collect::<Result<_, _>>()?;
        assert_eq!(values, vec!["other_50".to_string()]);

        let invalid_iter =
            TableBucketRangeIterator::<u64, String>::new(&read_txn, &builder, 123u64, 200, 100);
        assert!(invalid_iter.is_err());

        Ok(())
    }

    #[test]
    fn test_table_bucket_multimap_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_file = NamedTempFile::new()?;
        let db = Database::create(temp_file.path())?;
        let builder = TableBucketBuilder::new(100, "table_bucket_multimap")?;

        {
            let write_txn = db.begin_write()?;
            {
                {
                    let mut table = write_txn
                        .open_multimap_table(builder.multimap_table_definition::<u64, u64>(0))?;
                    table.insert(123u64, 10u64)?;
                    table.insert(123u64, 20u64)?;
                    table.insert(456u64, 99u64)?;
                    table.insert(456u64, 100u64)?;
                }

                {
                    let mut table = write_txn
                        .open_multimap_table(builder.multimap_table_definition::<u64, u64>(1))?;
                    table.insert(123u64, 30u64)?;
                    table.insert(123u64, 40u64)?;
                }
            }
            write_txn.commit()?;
        }

        let read_txn = db.begin_read()?;
        let iter = TableBucketRangeMultimapIterator::new(&read_txn, &builder, 123u64, 0, 199)?;
        assert_eq!(iter.bucket_range(), (0, 1));

        let values: Vec<u64> = iter.collect::<Result<_, _>>()?;
        assert_eq!(values, vec![10u64, 20u64, 30u64, 40u64]);

        let iter = TableBucketRangeMultimapIterator::new(&read_txn, &builder, 123u64, 0, 199)?;
        let values: Vec<u64> = iter.rev().collect::<Result<_, _>>()?;
        assert_eq!(values, vec![40u64, 30u64, 20u64, 10u64]);

        let iter = read_txn.table_bucket_multimap_range(&builder, 456u64, 0, 99)?;
        let values: Vec<u64> = iter.collect::<Result<_, _>>()?;
        assert_eq!(values, vec![99u64, 100u64]);

        Ok(())
    }
}
