//! Bucketed key implementations.
//!
//! Provides KeyBuilder for configuration and BucketedKey for storage.

use crate::buckets::BucketError;
use redb::{Key, Value};
use std::cmp::Ordering;
use std::fmt::Debug;

/// Builder for creating bucketed keys with consistent configuration.
///
/// KeyBuilder holds the bucket configuration and can be reused to create
/// bucketed keys for any base key type and sequence.
#[derive(Debug, Clone)]
pub struct KeyBuilder {
    bucket_size: u64,
}

impl KeyBuilder {
    /// Create a new KeyBuilder with the specified bucket size.
    ///
    /// # Arguments
    /// * `bucket_size` - Size of each bucket for integer division (must be > 0)
    ///
    /// # Returns
    /// Configured KeyBuilder or error if bucket_size is invalid
    pub fn new(bucket_size: u64) -> Result<Self, BucketError> {
        if bucket_size == 0 {
            return Err(BucketError::InvalidBucketSize(bucket_size));
        }
        Ok(Self { bucket_size })
    }

    /// Create a bucketed key from the given base key and sequence.
    ///
    /// The bucket is calculated as `sequence / bucket_size` using integer division.
    ///
    /// # Arguments
    /// * `base_key` - The base key (any type implementing redb::Key)
    /// * `sequence` - The sequence value to bucket
    ///
    /// # Returns
    /// BucketedKey with bucket as prefix and base_key as secondary component
    pub fn bucketed_key<K: Key>(&self, base_key: K, sequence: u64) -> BucketedKey<K> {
        let bucket = sequence / self.bucket_size;
        BucketedKey { base_key, bucket }
    }

    /// Get the configured bucket size.
    pub fn bucket_size(&self) -> u64 {
        self.bucket_size
    }
}

/// A bucketed key that implements redb::Key for storage.
///
/// BucketedKey stores a base key along with its computed bucket.
/// The bucket serves as the primary sort key (prefix) while the base key
/// provides secondary sorting within each bucket.
#[derive(Debug, Clone)]
pub struct BucketedKey<K: Key> {
    pub base_key: K,
    pub bucket: u64,
}

impl<K: Key> BucketedKey<K> {
    /// Create a new BucketedKey directly.
    ///
    /// Note: Typically you should use KeyBuilder::bucketed_key() instead
    /// to ensure consistent bucket calculation.
    pub fn new(base_key: K, bucket: u64) -> Self {
        Self { base_key, bucket }
    }

    /// Get reference to the base key.
    pub fn base_key(&self) -> &K {
        &self.base_key
    }

    /// Get the bucket number.
    pub fn bucket(&self) -> u64 {
        self.bucket
    }
}

// For now, we'll implement a simple version that works with u64 base keys
impl Value for BucketedKey<u64> {
    type SelfType<'a>
        = BucketedKey<u64>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(16) // 8 bytes bucket + 8 bytes u64 base key
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.len() < 16 {
            panic!(
                "BucketedKey data too short: expected at least 16 bytes, got {}",
                data.len()
            );
        }

        // Read bucket (first 8 bytes, little-endian)
        let bucket = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        // Read base key (next 8 bytes, little-endian)
        let base_key = u64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        BucketedKey { base_key, bucket }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        // Serialize bucket as 8-byte little-endian
        let bucket_bytes = value.bucket.to_le_bytes();

        // Serialize base key as 8-byte little-endian
        let base_key_bytes = value.base_key.to_le_bytes();

        // Concatenate bucket + base key
        let mut result = Vec::with_capacity(16);
        result.extend_from_slice(&bucket_bytes);
        result.extend_from_slice(&base_key_bytes);

        result
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("redb_extras::buckets::BucketedKey<u64>")
    }
}

impl Key for BucketedKey<u64> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // Extract bucket from both keys (first 8 bytes)
        if data1.len() < 16 || data2.len() < 16 {
            panic!("BucketedKey data too short for comparison");
        }

        let bucket1 = u64::from_le_bytes([
            data1[0], data1[1], data1[2], data1[3], data1[4], data1[5], data1[6], data1[7],
        ]);
        let bucket2 = u64::from_le_bytes([
            data2[0], data2[1], data2[2], data2[3], data2[4], data2[5], data2[6], data2[7],
        ]);

        // First compare bucket
        match bucket1.cmp(&bucket2) {
            Ordering::Equal => {
                // If buckets equal, compare base keys
                let base1 = u64::from_le_bytes([
                    data1[8], data1[9], data1[10], data1[11], data1[12], data1[13], data1[14],
                    data1[15],
                ]);
                let base2 = u64::from_le_bytes([
                    data2[8], data2[9], data2[10], data2[11], data2[12], data2[13], data2[14],
                    data2[15],
                ]);
                base1.cmp(&base2)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_builder_creation() {
        // Valid bucket size
        let builder = KeyBuilder::new(1000);
        assert!(builder.is_ok());
        assert_eq!(builder.unwrap().bucket_size(), 1000);

        // Invalid bucket size
        let builder = KeyBuilder::new(0);
        assert!(builder.is_err());
    }

    #[test]
    fn test_bucketed_key_creation() {
        let builder = KeyBuilder::new(1000).unwrap();

        // Test bucket calculation
        let key1 = builder.bucketed_key(123u64, 500);
        assert_eq!(key1.bucket(), 0);
        assert_eq!(key1.base_key(), &123u64);

        let key2 = builder.bucketed_key(123u64, 1500);
        assert_eq!(key2.bucket(), 1);
        assert_eq!(key2.base_key(), &123u64);

        let key3 = builder.bucketed_key(123u64, 2500);
        assert_eq!(key3.bucket(), 2);
    }

    #[test]
    fn test_bucketed_key_serialization() {
        let builder = KeyBuilder::new(1000).unwrap();
        let key = builder.bucketed_key(123u64, 1500); // bucket 1

        // Serialize to bytes
        let bytes: Vec<u8> = BucketedKey::as_bytes(&key);
        assert_eq!(bytes.len(), 16);

        // Deserialize back
        let deserialized: BucketedKey<u64> = BucketedKey::from_bytes(&bytes);
        assert_eq!(deserialized.bucket(), 1);
        assert_eq!(deserialized.base_key(), &123u64);
    }

    #[test]
    fn test_bucketed_key_ordering() {
        let builder = KeyBuilder::new(1000).unwrap();

        // Create keys with different buckets
        let key1 = builder.bucketed_key(123u64, 500); // bucket 0
        let key2 = builder.bucketed_key(123u64, 1500); // bucket 1
        let key3 = builder.bucketed_key(456u64, 500); // bucket 0, different base

        // Serialize for comparison
        let bytes1: Vec<u8> = BucketedKey::as_bytes(&key1);
        let bytes2: Vec<u8> = BucketedKey::as_bytes(&key2);
        let bytes3: Vec<u8> = BucketedKey::as_bytes(&key3);

        // Bucket should be primary sort key
        assert_eq!(
            BucketedKey::<u64>::compare(&bytes1, &bytes2),
            Ordering::Less
        );
        assert_eq!(
            BucketedKey::<u64>::compare(&bytes2, &bytes1),
            Ordering::Greater
        );

        // Same bucket, base key should determine order
        assert_eq!(
            BucketedKey::<u64>::compare(&bytes1, &bytes3),
            Ordering::Less
        );
        assert_eq!(
            BucketedKey::<u64>::compare(&bytes3, &bytes1),
            Ordering::Greater
        );
    }
}
