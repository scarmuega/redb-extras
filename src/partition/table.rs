//! Generic partitioned table implementation.
//!
//! Provides the core storage infrastructure for sharded and segmented data
//! that can work with any value type.

use crate::partition::config::PartitionConfig;
use crate::partition::scan::{enumerate_segments, find_head_segment, SegmentInfo};
use crate::partition::shard::select_shard;
use crate::partition::PartitionError;
use crate::Result;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::HashMap;

/// Encodes a segment key with the format: \\[key_len\\]\\[key\\]\\[shard\\]\\[segment\\]
pub fn encode_segment_key(key: &[u8], shard: u16, segment: u16) -> Result<Vec<u8>> {
    let mut encoded_key = Vec::with_capacity(4 + key.len() + 4);

    // Add key length (4 bytes big-endian)
    encoded_key.extend_from_slice(&(key.len() as u32).to_be_bytes());

    // Add base key
    encoded_key.extend_from_slice(key);

    // Add shard (2 bytes big-endian)
    encoded_key.extend_from_slice(&shard.to_be_bytes());

    // Add segment (2 bytes big-endian)
    encoded_key.extend_from_slice(&segment.to_be_bytes());

    Ok(encoded_key)
}

// Type aliases for complex return types
type SegmentDataMap = HashMap<u16, Vec<(SegmentInfo, Option<Vec<u8>>)>>;
type SegmentSimpleMap = HashMap<u16, Vec<(u16, Vec<u8>)>>;
type SegmentResult = Option<(SegmentInfo, Vec<u8>)>;

/// Table definition for segment data storage
pub const SEGMENT_TABLE: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("redb_extras_segments");

/// Table definition for meta data storage (head segment tracking)
pub const META_TABLE: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("redb_extras_meta");

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

    /// Ensures required tables exist in the database.
    ///
    /// This method creates the segment table and optionally the meta table
    /// if they don't already exist.
    ///
    /// # Arguments
    /// * `db` - The database instance
    ///
    /// # Returns
    /// Ok on success, error on failure
    pub fn ensure_table_exists(&self, db: &Database) -> Result<()> {
        let txn = db
            .begin_write()
            .map_err(|e| PartitionError::DatabaseError(format!("Failed to begin write: {}", e)))?;

        {
            let _segment_table = txn.open_table(SEGMENT_TABLE).map_err(|e| {
                PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
            })?;

            if self.config.use_meta {
                let _meta_table = txn.open_table(META_TABLE).map_err(|e| {
                    PartitionError::DatabaseError(format!("Failed to open meta table: {}", e))
                })?;
            }
        }

        txn.commit().map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to commit table creation: {}", e))
        })?;

        Ok(())
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
    pub fn select_shard(&self, key: &[u8], element_id: u64) -> Result<u16> {
        Ok(select_shard(key, element_id, self.config.shard_count)?)
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

    /// Collects all segments across all shards for a given base key.
    ///
    /// This method iterates through all shards and collects all segments
    /// that belong to the specified base key.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    ///
    /// # Returns
    /// HashMap where key is shard ID and value is vector of (segment_info, segment_data) tuples
    pub fn collect_all_segments(&self, key: &[u8]) -> Result<SegmentDataMap> {
        let mut result = HashMap::new();

        // Open the segment table
        let table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        // Iterate through all shards
        for shard in 0..self.table.config.shard_count {
            let mut shard_segments = Vec::new();

            // Enumerate segments for this shard
            let mut segment_iter = enumerate_segments(&table, key, shard)?;

            while let Some(segment_result) = segment_iter.next() {
                let segment_info = segment_result?;
                shard_segments.push((segment_info.clone(), segment_info.segment_data.clone()));
            }

            if !shard_segments.is_empty() {
                result.insert(shard, shard_segments);
            }
        }

        Ok(result)
    }

    /// Enumerates all segments for a given base key across all shards.
    ///
    /// This method returns segment data in a simplified format
    /// for easier consumption by callers.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    ///
    /// # Returns
    /// HashMap where key is shard ID and value is vector of (segment_id, segment_data) tuples
    pub fn enumerate_all_segments(&self, key: &[u8]) -> Result<SegmentSimpleMap> {
        let mut result = HashMap::new();

        // Open the segment table
        let table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        // Iterate through all shards
        for shard in 0..self.table.config.shard_count {
            let mut shard_segments = Vec::new();

            // Enumerate segments for this shard
            let mut segment_iter = enumerate_segments(&table, key, shard)?;

            while let Some(segment_result) = segment_iter.next() {
                let segment_info = segment_result?;
                if let Some(data) = segment_info.segment_data {
                    shard_segments.push((segment_info.segment_id, data));
                }
            }

            if !shard_segments.is_empty() {
                result.insert(shard, shard_segments);
            }
        }

        Ok(result)
    }

    /// Reads data for a specific segment.
    ///
    /// If segment_info already contains data, it's returned directly.
    /// Otherwise, the data is read from the database.
    ///
    /// # Arguments
    /// * `segment_info` - Information about the segment to read
    ///
    /// # Returns
    /// Option containing (segment_info, segment_data) or None if segment doesn't exist
    pub fn read_segment_data(&self, segment_info: &SegmentInfo) -> Result<SegmentResult> {
        // If segment_info already has data, return it
        if let Some(ref data) = segment_info.segment_data {
            return Ok(Some((segment_info.clone(), data.clone())));
        }

        // Otherwise, read from the database
        let table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        match table.get(&*segment_info.segment_key) {
            Ok(Some(value_guard)) => {
                let data = value_guard.value().to_vec();
                let mut info_with_data = segment_info.clone();
                info_with_data.segment_data = Some(data.clone());
                Ok(Some((info_with_data, data)))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                Err(PartitionError::DatabaseError(format!("Failed to read segment: {}", e)).into())
            }
        }
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

    /// Reads segment data for the given segment info.
    ///
    /// If segment_info already contains data, it's returned directly.
    /// Otherwise, the data is read from the database.
    ///
    /// # Arguments
    /// * `segment_info` - Information about the segment to read
    ///
    /// # Returns
    /// Option containing (segment_info, segment_data) or None if segment doesn't exist
    pub fn read_segment_data(&self, segment_info: &SegmentInfo) -> Result<SegmentResult> {
        // If segment_info already has data, return it
        if let Some(ref data) = segment_info.segment_data {
            return Ok(Some((segment_info.clone(), data.clone())));
        }

        // Otherwise, read from the database
        let table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        let result = match table.get(&*segment_info.segment_key) {
            Ok(Some(value_guard)) => {
                let data = value_guard.value().to_vec();
                let mut info_with_data = segment_info.clone();
                info_with_data.segment_data = Some(data.clone());
                Ok(Some((info_with_data, data)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PartitionError::DatabaseError(format!(
                "Failed to read segment: {}",
                e
            ))),
        };

        // Drop table before returning result
        drop(table);
        Ok(result?)
    }

    /// Gets the table reference.
    pub fn table(&self) -> &PartitionedTable<V> {
        self.table
    }

    /// Finds the head segment using scan method (when meta table is disabled).
    ///
    /// This method scans all segments for the given (key, shard) pair
    /// and returns the one with the highest segment ID.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    /// * `shard` - The shard ID
    ///
    /// # Returns
    /// The head segment ID, or None if no segments exist
    pub fn find_head_segment_scan(&self, key: &[u8], shard: u16) -> Result<Option<u16>> {
        let table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        Ok(find_head_segment(&table, key, shard)?)
    }

    /// Writes data to a specific segment.
    ///
    /// This method overwrites any existing data at the segment key.
    ///
    /// # Arguments
    /// * `segment_key` - The encoded segment key
    /// * `data` - The data to write
    ///
    /// # Returns
    /// Ok on success, error on failure
    pub fn write_segment_data(&self, segment_key: &[u8], data: &[u8]) -> Result<()> {
        let mut table = self.txn.open_table(SEGMENT_TABLE).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to open segment table: {}", e))
        })?;

        table.insert(segment_key, data).map_err(|e| {
            PartitionError::DatabaseError(format!("Failed to write segment: {}", e))
        })?;

        Ok(())
    }

    /// Creates a new segment with the given data.
    ///
    /// The segment_id should be the next available ID for this shard.
    ///
    /// # Arguments
    /// * `key` - The base key
    /// * `shard` - The shard ID
    /// * `segment_id` - The segment ID
    /// * `data` - The segment data
    ///
    /// # Returns
    /// Ok on success, error on failure
    pub fn create_new_segment(
        &self,
        key: &[u8],
        shard: u16,
        segment_id: u16,
        data: &[u8],
    ) -> Result<()> {
        let segment_key = encode_segment_key(key, shard, segment_id)?;
        self.write_segment_data(&segment_key, data)
    }

    /// Updates the head segment with new data, rolling if necessary.
    ///
    /// This method checks if the new data fits in the current head segment.
    /// If it doesn't fit, a new segment is created.
    ///
    /// # Arguments
    /// * `key` - The base key
    /// * `shard` - The shard ID
    /// * `data` - The new segment data
    ///
    /// # Returns
    /// Tuple of (was_rolled, new_segment_id) where:
    /// - was_rolled: true if a new segment was created
    /// - new_segment_id: ID of the segment that now contains the data
    pub fn update_head_segment(&self, key: &[u8], shard: u16, data: &[u8]) -> Result<(bool, u16)> {
        // Find current head segment
        let head_segment = self.find_head_segment_scan(key, shard)?;

        match head_segment {
            Some(segment_id) => {
                // Check if data fits in current segment
                if data.len() <= self.table.config.segment_max_bytes {
                    // Update existing segment
                    let segment_key = encode_segment_key(key, shard, segment_id)?;
                    self.write_segment_data(&segment_key, data)?;
                    Ok((false, segment_id))
                } else {
                    // Roll to new segment
                    let new_segment_id = segment_id + 1;
                    let new_segment_key = encode_segment_key(key, shard, new_segment_id)?;
                    self.write_segment_data(&new_segment_key, data)?;
                    Ok((true, new_segment_id))
                }
            }
            None => {
                // No segments exist, create first one
                let segment_key = encode_segment_key(key, shard, 0)?;
                self.write_segment_data(&segment_key, data)?;
                Ok((true, 0))
            }
        }
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

        let key = b"test_key";
        let element_id = 12345;

        let shard = table.select_shard(key, element_id).unwrap();
        assert!(shard < 8);

        // Should be deterministic
        let shard2 = table.select_shard(key, element_id).unwrap();
        assert_eq!(shard, shard2);
    }
}
