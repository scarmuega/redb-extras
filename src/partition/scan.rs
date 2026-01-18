//! Segment enumeration via prefix scanning.
//!
//! This module provides functionality for discovering and enumerating segments
//! when meta table is disabled. It uses redb's range scanning capabilities
//! to efficiently find segments for a given base key and shard.

use crate::partition::PartitionError;
use crate::Result;
use redb::ReadableTable;
use std::marker::PhantomData;

/// Builds a segment prefix key for scanning all segments of a given (base_key, shard) pair.
/// Segment keys have the format: [key_len][base_key][shard][segment]
fn build_segment_prefix(base_key: &[u8], shard: u16) -> Result<Vec<u8>> {
    let mut prefix = Vec::with_capacity(4 + base_key.len() + 2);

    // Add key length (4 bytes big-endian)
    prefix.extend_from_slice(&(base_key.len() as u32).to_be_bytes());

    // Add base key
    prefix.extend_from_slice(base_key);

    // Add shard (2 bytes big-endian)
    prefix.extend_from_slice(&shard.to_be_bytes());

    Ok(prefix)
}

/// Information about a discovered segment.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    /// The segment identifier (sequential number)
    pub segment_id: u16,
    /// The full encoded segment key
    pub segment_key: Vec<u8>,
    /// The segment value data (if available)
    pub segment_data: Option<Vec<u8>>,
}

impl SegmentInfo {
    /// Creates a new segment info.
    pub fn new(segment_id: u16, segment_key: Vec<u8>) -> Self {
        Self {
            segment_id,
            segment_key,
            segment_data: None,
        }
    }

    /// Creates a new segment info with data.
    pub fn with_data(segment_id: u16, segment_key: Vec<u8>, segment_data: Vec<u8>) -> Self {
        Self {
            segment_id,
            segment_key,
            segment_data: Some(segment_data),
        }
    }
}

/// Enumerates all segments for a given base key and shard.
///
/// This function uses prefix scanning to find all segments that belong to
/// a specific (base_key, shard) pair. It returns segments in ascending
/// order of segment ID.
///
/// # Arguments
/// * `table` - The redb table to scan
/// * `base_key` - The base key to search for
/// * `shard` - The shard identifier
///
/// # Returns
/// Iterator over segment information
pub fn enumerate_segments<'a, T>(
    table: &'a T,
    base_key: &[u8],
    shard: u16,
) -> Result<SegmentIterator<'a>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let (start_key, end_key) = build_segment_scan_range(base_key, shard)?;
    let range = table
        .range(start_key.as_slice()..end_key.as_slice())
        .map_err(|e| {
            crate::error::Error::Partition(PartitionError::SegmentScanFailed(format!(
                "Failed to create range iterator: {}",
                e
            )))
        })?;

    Ok(SegmentIterator {
        range,
        base_key: base_key.to_vec(),
        shard,
        _phantom: PhantomData,
    })
}

/// Finds the head (highest-numbered) segment for a base key and shard.
///
/// This function scans all segments for the given (base_key, shard) pair
/// and returns the one with the highest segment ID. This is used during
/// writes to determine which segment to append to.
///
/// # Arguments
/// * `table` - The redb table to scan
/// * `base_key` - The base key to search for
/// * `shard` - The shard identifier
///
/// # Returns
/// The head segment ID, or None if no segments exist
pub fn find_head_segment<T>(table: &T, base_key: &[u8], shard: u16) -> Result<Option<u16>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut iter = enumerate_segments(table, base_key, shard)?;
    let mut head_segment = None;

    while let Some(segment_result) = iter.next() {
        let segment_info = segment_result?;
        head_segment = Some(segment_info.segment_id);
    }

    Ok(head_segment)
}

/// Builds the range bounds for scanning segments of a given base key and shard.
///
/// The range includes all keys that start with the segment prefix for the
/// given (base_key, shard) pair, ensuring we only scan relevant segments.
///
/// # Arguments
/// * `base_key` - The base key
/// * `shard` - The shard identifier
///
/// # Returns
/// Tuple of (start_key, end_key) for range scanning
fn build_segment_scan_range(base_key: &[u8], shard: u16) -> Result<(Vec<u8>, Vec<u8>)> {
    let start_key = build_segment_prefix(base_key, shard)?;

    // For the end key, increment the last byte of the prefix to create an
    // exclusive upper bound that includes all keys with this prefix
    let mut end_key = start_key.clone();
    if let Some(last_byte) = end_key.last_mut() {
        *last_byte = last_byte.saturating_add(1);
    } else {
        return Err(crate::error::Error::Partition(
            PartitionError::SegmentScanFailed(
                "Prefix key is empty, cannot create range".to_string(),
            ),
        ));
    }

    Ok((start_key, end_key))
}

/// Extracts the segment ID from an encoded segment key.
///
/// Segment keys have the format: [key_len][base_key][shard][segment]
/// The segment ID is the last 2 bytes of the key.
///
/// # Arguments
/// * `encoded_key` - The full encoded segment key
///
/// # Returns
/// The extracted segment ID
fn extract_segment_id(encoded_key: &[u8]) -> Result<u16> {
    if encoded_key.len() < 6 {
        // Minimum: 4-byte length + 1-byte base_key + 2-byte shard
        return Err(crate::error::Error::Partition(
            PartitionError::SegmentScanFailed(
                "Encoded key too short to contain segment ID".to_string(),
            ),
        ));
    }

    let segment_bytes = &encoded_key[encoded_key.len() - 2..];
    Ok(u16::from_be_bytes([segment_bytes[0], segment_bytes[1]]))
}

/// Validates that an encoded key matches the expected base key and shard.
///
/// This ensures that keys found during prefix scanning actually belong to the
/// expected (base_key, shard) pair, protecting against false positives
/// from the range scan.
///
/// # Arguments
/// * `encoded_key` - The encoded key to validate
/// * `expected_base_key` - The expected base key
/// * `expected_shard` - The expected shard
///
/// # Returns
/// true if the key matches, false otherwise
fn validate_key_match(encoded_key: &[u8], expected_base_key: &[u8], expected_shard: u16) -> bool {
    if encoded_key.len() < 4 {
        return false;
    }

    // Extract and validate base key
    let key_len = u32::from_be_bytes([
        encoded_key[0],
        encoded_key[1],
        encoded_key[2],
        encoded_key[3],
    ]) as usize;

    if encoded_key.len() < 4 + key_len + 4 {
        // Not enough bytes for base_key + shard + segment
        return false;
    }

    let base_key_slice = &encoded_key[4..4 + key_len];
    if base_key_slice != expected_base_key {
        return false;
    }

    // Extract and validate shard
    let shard_start = 4 + key_len;
    let shard_bytes = &encoded_key[shard_start..shard_start + 2];
    let shard = u16::from_be_bytes([shard_bytes[0], shard_bytes[1]]);

    shard == expected_shard
}

/// Iterator over segments found during prefix scanning.
///
/// This iterator wraps a redb range iterator and filters/validates the
/// results to ensure they match the expected base key and shard.
pub struct SegmentIterator<'a> {
    range: redb::Range<'a, &'static [u8], &'static [u8]>,
    base_key: Vec<u8>,
    shard: u16,
    _phantom: PhantomData<()>,
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Result<SegmentInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.range.next() {
                Some(Ok((key_guard, value_guard))) => {
                    let key = key_guard.value();
                    let value = value_guard.value();

                    // Validate that this key matches our expected base_key and shard
                    if !validate_key_match(key, &self.base_key, self.shard) {
                        continue; // Skip keys that don't match (shouldn't happen with proper range)
                    }

                    // Extract segment ID
                    match extract_segment_id(key) {
                        Ok(segment_id) => {
                            let segment_info =
                                SegmentInfo::with_data(segment_id, key.to_vec(), value.to_vec());
                            return Some(Ok(segment_info));
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Err(e)) => {
                    return Some(Err(PartitionError::SegmentScanFailed(format!(
                        "Database error during iteration: {}",
                        e
                    ))
                    .into()));
                }
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use redb::{Database, TableDefinition};

    const TEST_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("test_scan");

    #[test]
    fn test_build_segment_scan_range() {
        let base_key = b"test_key";
        let shard = 42;

        let (start, end) = build_segment_scan_range(base_key, shard).unwrap();

        // Start should be the segment prefix
        let expected_prefix = build_segment_prefix(base_key, shard).unwrap();
        assert_eq!(start, expected_prefix);

        // End should be start + 1 on the last byte
        assert_eq!(end.len(), start.len());
        assert_eq!(end[..end.len() - 1], start[..start.len() - 1]);
        assert_eq!(end[end.len() - 1], start[start.len() - 1] + 1);
    }

    #[test]
    fn test_extract_segment_id() {
        // Create a mock encoded key: [len=4][key][shard=42][segment=123]
        let base_key = b"test";
        let shard = 42u16;
        let segment = 123u16;

        let mut key = Vec::new();
        key.extend_from_slice(&4u32.to_be_bytes());
        key.extend_from_slice(base_key);
        key.extend_from_slice(&shard.to_be_bytes());
        key.extend_from_slice(&segment.to_be_bytes());

        let extracted = extract_segment_id(&key).unwrap();
        assert_eq!(extracted, 123);
    }

    #[test]
    fn test_extract_segment_id_invalid() {
        let short_key = b"short";
        let result = extract_segment_id(short_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_key_match() {
        let base_key = b"test_key";
        let shard = 42u16;
        let segment = 123u16;

        // Create a valid key
        let mut key = Vec::new();
        key.extend_from_slice(&(base_key.len() as u32).to_be_bytes());
        key.extend_from_slice(base_key);
        key.extend_from_slice(&shard.to_be_bytes());
        key.extend_from_slice(&segment.to_be_bytes());

        assert!(validate_key_match(&key, base_key, shard));

        // Test wrong base key
        assert!(!validate_key_match(&key, b"wrong_key", shard));

        // Test wrong shard
        assert!(!validate_key_match(&key, base_key, 99));
    }

    #[test]
    fn test_enumerate_segments() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        let base_key = b"test_key";
        let shard = 42u16;

        {
            let mut table = write_txn.open_table(TEST_TABLE).unwrap();

            // Insert some test segments
            for segment in 0..3u16 {
                let segment_key =
                    crate::partition::table::encode_segment_key(base_key, shard, segment).unwrap();
                let segment_data = format!("segment_{}", segment).into_bytes();
                table.insert(&*segment_key, &*segment_data).unwrap();
            }
        }

        write_txn.commit().unwrap();

        // Test enumeration
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TEST_TABLE).unwrap();

        let mut iter = enumerate_segments(&table, base_key, shard).unwrap();
        let mut segments = Vec::new();

        while let Some(segment_result) = iter.next() {
            segments.push(segment_result.unwrap());
        }

        assert_eq!(segments.len(), 3);

        // Should be in ascending order
        for (i, segment) in segments.iter().enumerate() {
            assert_eq!(segment.segment_id, i as u16);
        }
    }

    #[test]
    fn test_find_head_segment() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        let base_key = b"test_key";
        let shard = 42u16;

        {
            let mut table = write_txn.open_table(TEST_TABLE).unwrap();

            // Insert segments 0, 2, and 5 (non-sequential)
            for segment in [0u16, 2u16, 5u16] {
                let segment_key =
                    crate::partition::table::encode_segment_key(base_key, shard, segment).unwrap();
                let segment_data = format!("segment_{}", segment).into_bytes();
                table.insert(&*segment_key, &*segment_data).unwrap();
            }
        }

        write_txn.commit().unwrap();

        // Test finding head segment
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TEST_TABLE).unwrap();

        let head_segment = find_head_segment(&table, base_key, shard).unwrap();
        assert_eq!(head_segment, Some(5));
    }

    #[test]
    fn test_find_head_segment_empty() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();

        // Create an empty table first
        let write_txn = db.begin_write().unwrap();
        {
            let _table = write_txn.open_table(TEST_TABLE).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TEST_TABLE).unwrap();

        let head_segment = find_head_segment(&table, b"nonexistent", 0).unwrap();
        assert_eq!(head_segment, None);
    }

    #[test]
    fn test_segment_info() {
        let segment_info = SegmentInfo::new(42, b"test_key".to_vec());
        assert_eq!(segment_info.segment_id, 42);
        assert_eq!(segment_info.segment_key, b"test_key");
        assert!(segment_info.segment_data.is_none());

        let segment_info = SegmentInfo::with_data(42, b"test_key".to_vec(), b"data".to_vec());
        assert_eq!(segment_info.segment_data, Some(b"data".to_vec()));
    }
}
