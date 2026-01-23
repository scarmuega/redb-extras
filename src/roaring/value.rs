//! Roaring bitmap value type for partitioned tables.
//!
//! Provides encoding, decoding, and size information for RoaringTreemap values
//! stored in partitioned segments.

use super::RoaringError;
use crate::{MergeableValue, Result};
use redb::Value as RedbValue;
use roaring::RoaringTreemap;

/// Value type for RoaringTreemap in partitioned tables.
///
/// This struct provides the bridge between the generic partitioned storage
/// and roaring-specific value operations. It handles:
/// - Serialization/deserialization of RoaringTreemap
/// - Size queries for segment rolling decisions
/// - Version management for future migrations
#[derive(Debug, Clone, PartialEq)]
pub struct RoaringValue {
    bitmap: RoaringTreemap,
}

impl RoaringValue {
    /// Creates a new RoaringValue from an existing bitmap.
    pub fn new(bitmap: RoaringTreemap) -> Self {
        Self { bitmap }
    }

    /// Creates an empty RoaringValue.
    pub fn empty() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    /// Returns a reference to the underlying bitmap.
    pub fn bitmap(&self) -> &RoaringTreemap {
        &self.bitmap
    }

    /// Returns a mutable reference to the underlying bitmap.
    pub fn bitmap_mut(&mut self) -> &mut RoaringTreemap {
        &mut self.bitmap
    }

    /// Consumes the value and returns the underlying bitmap.
    pub fn into_bitmap(self) -> RoaringTreemap {
        self.bitmap
    }

    /// Encodes a RoaringTreemap into storage format.
    ///
    /// # Arguments
    /// * `bitmap` - The roaring bitmap to encode
    ///
    /// # Returns
    /// Encoded bytes ready for storage
    pub fn encode(&self) -> Result<Vec<u8>> {
        Self::encode_bitmap(&self.bitmap)
    }

    /// Encodes a RoaringTreemap into storage format.
    ///
    /// # Arguments
    /// * `bitmap` - The roaring bitmap to encode
    ///
    /// # Returns
    /// Encoded bytes ready for storage
    pub fn encode_bitmap(bitmap: &RoaringTreemap) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        bitmap
            .serialize_into(&mut buf)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?;

        // Add version prefix (current version = 1)
        let mut result = Vec::with_capacity(1 + buf.len());
        result.push(1u8); // Version byte
        result.extend_from_slice(&buf);

        Ok(result)
    }

    /// Decodes storage bytes into a RoaringValue.
    ///
    /// # Arguments
    /// * `data` - The encoded value bytes
    ///
    /// # Returns
    /// Decoded RoaringValue
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(RoaringError::InvalidBitmap("Empty data".to_string()).into());
        }

        let version = data[0];
        let bitmap_bytes = &data[1..];

        if version != 1 {
            return Err(
                RoaringError::InvalidBitmap(format!("Unsupported version: {}", version)).into(),
            );
        }

        let bitmap = RoaringTreemap::deserialize_from(bitmap_bytes)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?;
        Ok(Self { bitmap })
    }

    /// Gets the serialized size of a RoaringTreemap.
    ///
    /// This size is used by the partition layer to determine when to roll
    /// segments based on the configured maximum segment size.
    ///
    /// # Arguments
    /// * `bitmap` - The roaring bitmap to measure
    ///
    /// # Returns
    /// Serialized size in bytes (including version prefix)
    pub fn get_serialized_size(&self) -> Result<usize> {
        Self::get_serialized_size_for(&self.bitmap)
    }

    /// Gets the serialized size of a RoaringTreemap.
    ///
    /// This size is used by the partition layer to determine when to roll
    /// segments based on the configured maximum segment size.
    ///
    /// # Arguments
    /// * `bitmap` - The roaring bitmap to measure
    ///
    /// # Returns
    /// Serialized size in bytes (including version prefix)
    pub fn get_serialized_size_for(bitmap: &RoaringTreemap) -> Result<usize> {
        let mut buf = Vec::new();
        bitmap
            .serialize_into(&mut buf)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?;

        // Include 1 byte for version prefix
        Ok(1 + buf.len())
    }

    /// Creates a RoaringValue from a single value.
    pub fn from_single(value: u64) -> Self {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(value);
        Self { bitmap }
    }

    /// Creates a RoaringValue from an iterator of values.
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        let values: Vec<u64> = iter.into_iter().collect();
        let bitmap =
            RoaringTreemap::from_sorted_iter(values.iter().cloned()).unwrap_or_else(|_| {
                let mut bitmap = RoaringTreemap::new();
                for value in &values {
                    bitmap.insert(*value);
                }
                bitmap
            });
        Self { bitmap }
    }

    /// Returns the number of members in the bitmap.
    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    /// Returns true if the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

impl From<RoaringTreemap> for RoaringValue {
    fn from(value: RoaringTreemap) -> Self {
        Self { bitmap: value }
    }
}

impl Default for RoaringValue {
    fn default() -> Self {
        Self::empty()
    }
}

impl MergeableValue for RoaringValue {
    fn merge(existing: Option<Self>, incoming: Self) -> Self {
        match existing {
            Some(mut existing) => {
                existing.bitmap.extend(incoming.bitmap.into_iter());
                existing
            }
            None => incoming,
        }
    }
}

impl RedbValue for RoaringValue {
    type SelfType<'a>
        = RoaringValue
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None // Variable width serialization
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        RoaringValue::decode(data).unwrap_or_else(|_| RoaringValue::empty())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.encode().unwrap_or_else(|_| Vec::new())
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("RoaringTreemap")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        bitmap.insert(1000);
        let value = RoaringValue::from(bitmap);

        let encoded = value.encode().unwrap();
        let decoded = RoaringValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_empty_bitmap() {
        let value = RoaringValue::empty();

        let encoded = value.encode().unwrap();
        let decoded = RoaringValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_serialized_size() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(2);

        let value = RoaringValue::from(bitmap);

        let size = value.get_serialized_size().unwrap();
        assert!(size > 1); // At least version byte
        assert!(size < 1000); // Should be reasonably small

        let encoded = value.encode().unwrap();
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_single_value() {
        let value = RoaringValue::from_single(42);

        assert_eq!(value.len(), 1);
        assert!(value.bitmap().contains(42));

        let encoded = value.encode().unwrap();
        let decoded = RoaringValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_from_iter() {
        let values = vec![1, 5, 10, 100];
        let value = RoaringValue::from_iter(values.clone());

        assert_eq!(value.len(), values.len() as u64);
        for member in &values {
            assert!(value.bitmap().contains(*member));
        }

        let encoded = value.encode().unwrap();
        let decoded = RoaringValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_invalid_version() {
        let mut invalid_data = vec![99]; // Invalid version
        invalid_data.extend_from_slice(b"fake_data");

        let result = RoaringValue::decode(&invalid_data);
        assert!(result.is_err());
    }
}
