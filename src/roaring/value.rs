//! Roaring bitmap value type for partitioned tables.
//!
//! Provides encoding, decoding, and size information for RoaringTreemap values
//! stored in partitioned segments.

use crate::error::{RoaringError};
use crate::error::Result;
use crate::encoding::key::{encode_roaring_value, decode_roaring_value, VALUE_VERSION};
use roaring::RoaringTreemap;

/// Value handler for RoaringTreemap in partitioned tables.
/// 
/// This struct provides the bridge between the generic partitioned storage
/// and roaring-specific value operations. It handles:
/// - Serialization/deserialization of RoaringTreemap
/// - Size queries for segment rolling decisions
/// - Version management for future migrations
#[derive(Debug, Clone)]
pub struct RoaringValue;

impl RoaringValue {
    /// Creates a new RoaringValue handler.
    pub fn new() -> Self {
        Self
    }
    
    /// Encodes a RoaringTreemap into storage format.
    /// 
    /// # Arguments
    /// * `bitmap` - The roaring bitmap to encode
    /// 
    /// # Returns
    /// Encoded bytes ready for storage
    pub fn encode(&self, bitmap: &RoaringTreemap) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?;
        
        Ok(encode_roaring_value(&buf))
    }
    
    /// Decodes storage bytes into a RoaringTreemap.
    /// 
    /// # Arguments
    /// * `data` - The encoded value bytes
    /// 
    /// # Returns
    /// Decoded RoaringTreemap
    pub fn decode(&self, data: &[u8]) -> Result<RoaringTreemap> {
        let (version, bitmap_bytes) = decode_roaring_value(data)
            .map_err(|e| RoaringError::InvalidBitmap(e.to_string()))?;
        
        if version != VALUE_VERSION {
            return Err(RoaringError::InvalidBitmap(
                format!("Unsupported version: {}", version)
            ).into());
        }
        
        Ok(RoaringTreemap::deserialize_from(bitmap_bytes)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?)
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
    pub fn get_serialized_size(&self, bitmap: &RoaringTreemap) -> Result<usize> {
        let mut buf = Vec::new();
        bitmap.serialize_into(&mut buf)
            .map_err(|e| RoaringError::SerializationFailed(e.to_string()))?;
        
        // Include 1 byte for version prefix
        Ok(1 + buf.len())
    }
    
    /// Creates an empty RoaringTreemap.
    pub fn empty(&self) -> RoaringTreemap {
        RoaringTreemap::new()
    }
    
    /// Creates a RoaringTreemap from a single value.
    pub fn from_single(&self, value: u64) -> RoaringTreemap {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(value);
        bitmap
    }
    
    /// Creates a RoaringTreemap from an iterator of values.
    pub fn from_iter<I>(&self, iter: I) -> RoaringTreemap
    where
        I: IntoIterator<Item = u64>,
    {
        let values: Vec<u64> = iter.into_iter().collect();
        RoaringTreemap::from_sorted_iter(values.iter().cloned())
            .unwrap_or_else(|_| {
                let mut bitmap = RoaringTreemap::new();
                for value in &values {
                    bitmap.insert(*value);
                }
                bitmap
            })
    }
}

impl Default for RoaringValue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_decode_roundtrip() {
        let handler = RoaringValue::new();
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        bitmap.insert(1000);
        
        let encoded = handler.encode(&bitmap).unwrap();
        let decoded = handler.decode(&encoded).unwrap();
        
        assert_eq!(bitmap, decoded);
    }
    
    #[test]
    fn test_empty_bitmap() {
        let handler = RoaringValue::new();
        let bitmap = RoaringTreemap::new();
        
        let encoded = handler.encode(&bitmap).unwrap();
        let decoded = handler.decode(&encoded).unwrap();
        
        assert_eq!(bitmap, decoded);
        assert_eq!(decoded.len(), 0);
    }
    
    #[test]
    fn test_serialized_size() {
        let handler = RoaringValue::new();
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        
        let size = handler.get_serialized_size(&bitmap).unwrap();
        assert!(size > 1); // At least version byte
        assert!(size < 1000); // Should be reasonably small
        
        let encoded = handler.encode(&bitmap).unwrap();
        assert_eq!(size, encoded.len());
    }
    
    #[test]
    fn test_single_value() {
        let handler = RoaringValue::new();
        let bitmap = handler.from_single(42);
        
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(42));
        
        let encoded = handler.encode(&bitmap).unwrap();
        let decoded = handler.decode(&encoded).unwrap();
        
        assert_eq!(bitmap, decoded);
    }
    
    #[test]
    fn test_from_iter() {
        let handler = RoaringValue::new();
        let values = vec![1, 5, 10, 100];
        let bitmap = handler.from_iter(values.clone());
        
        assert_eq!(bitmap.len(), values.len());
        for value in &values {
            assert!(bitmap.contains(*value));
        }
        
        let encoded = handler.encode(&bitmap).unwrap();
        let decoded = handler.decode(&encoded).unwrap();
        
        assert_eq!(bitmap, decoded);
    }
    
    #[test]
    fn test_invalid_version() {
        let handler = RoaringValue::new();
        let mut invalid_data = vec![99]; // Invalid version
        invalid_data.extend_from_slice(b"fake_data");
        
        let result = handler.decode(&invalid_data);
        assert!(result.is_err());
    }
}