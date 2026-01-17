//! Key encoding and decoding for partitioned storage.
//!
//! Keys are encoded with a length prefix to handle arbitrary base keys safely:
//!
//! Segment key: [key_len][base_key][shard][segment]
//! Meta key:    [key_len][base_key][shard]

use crate::error::EncodingError;
pub type Result<T> = std::result::Result<T, EncodingError>;
use std::convert::TryInto;

/// Current encoding version for segment values
pub const VALUE_VERSION: u8 = 1;

/// Encodes a segment key with the format: [key_len][base_key][shard][segment]
///
/// # Arguments
/// * `base_key` - The opaque user-provided key
/// * `shard` - The shard identifier (0-65535)
/// * `segment` - The segment identifier (0-65535)
///
/// # Returns
/// Encoded key bytes
pub fn encode_segment_key(base_key: &[u8], shard: u16, segment: u16) -> Result<Vec<u8>> {
    let key_len: u32 = base_key
        .len()
        .try_into()
        .map_err(|_| EncodingError::InvalidKeyEncoding("Key too long".to_string()))?;

    let mut buf = Vec::with_capacity(4 + base_key.len() + 4);

    // Length prefix
    buf.extend_from_slice(&key_len.to_be_bytes());

    // Base key
    buf.extend_from_slice(base_key);

    // Shard and segment
    buf.extend_from_slice(&shard.to_be_bytes());
    buf.extend_from_slice(&segment.to_be_bytes());

    Ok(buf)
}

/// Encodes a meta key with the format: [key_len][base_key][shard]
///
/// # Arguments
/// * `base_key` - The opaque user-provided key
/// * `shard` - The shard identifier (0-65535)
///
/// # Returns
/// Encoded meta key bytes
pub fn encode_meta_key(base_key: &[u8], shard: u16) -> Result<Vec<u8>> {
    let key_len: u32 = base_key
        .len()
        .try_into()
        .map_err(|_| EncodingError::InvalidKeyEncoding("Key too long".to_string()))?;

    let mut buf = Vec::with_capacity(4 + base_key.len() + 2);

    // Length prefix
    buf.extend_from_slice(&key_len.to_be_bytes());

    // Base key
    buf.extend_from_slice(base_key);

    // Shard only
    buf.extend_from_slice(&shard.to_be_bytes());

    Ok(buf)
}

/// Builds a prefix for iterating over all segments of a given base key and shard
///
/// # Arguments
/// * `base_key` - The opaque user-provided key
/// * `shard` - The shard identifier (0-65535)
///
/// # Returns
/// Prefix bytes for range scanning
pub fn build_segment_prefix(base_key: &[u8], shard: u16) -> Result<Vec<u8>> {
    let key_len: u32 = base_key
        .len()
        .try_into()
        .map_err(|_| EncodingError::InvalidKeyEncoding("Key too long".to_string()))?;

    let mut buf = Vec::with_capacity(4 + base_key.len() + 2);

    // Length prefix
    buf.extend_from_slice(&key_len.to_be_bytes());

    // Base key
    buf.extend_from_slice(base_key);

    // Shard (segment starts after this)
    buf.extend_from_slice(&shard.to_be_bytes());

    Ok(buf)
}

/// Builds a prefix for iterating over all shards of a given base key
///
/// # Arguments
/// * `base_key` - The opaque user-provided key
///
/// # Returns
/// Prefix bytes for range scanning
pub fn build_base_key_prefix(base_key: &[u8]) -> Result<Vec<u8>> {
    let key_len: u32 = base_key
        .len()
        .try_into()
        .map_err(|_| EncodingError::InvalidKeyEncoding("Key too long".to_string()))?;

    let mut buf = Vec::with_capacity(4 + base_key.len());

    // Length prefix
    buf.extend_from_slice(&key_len.to_be_bytes());

    // Base key only (shards start after this)
    buf.extend_from_slice(base_key);

    Ok(buf)
}

/// Encodes a roaring value with version prefix: [version][roaring_bytes]
///
/// # Arguments
/// * `roaring_bytes` - Serialized RoaringTreemap bytes
///
/// # Returns
/// Encoded value bytes
pub fn encode_roaring_value(roaring_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + roaring_bytes.len());
    buf.push(VALUE_VERSION);
    buf.extend_from_slice(roaring_bytes);
    buf
}

/// Decodes a roaring value, extracting version and data
///
/// # Arguments
/// * `encoded_value` - The encoded value bytes
///
/// # Returns
/// Tuple of (version, roaring_bytes)
pub fn decode_roaring_value(encoded_value: &[u8]) -> Result<(u8, &[u8])> {
    if encoded_value.is_empty() {
        return Err(EncodingError::InvalidValueEncoding("Empty value".to_string()).into());
    }

    let version = encoded_value[0];
    if version != VALUE_VERSION {
        return Err(EncodingError::UnsupportedVersion(version).into());
    }

    Ok((version, &encoded_value[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_segment_key() {
        let base_key = b"test_key";
        let shard = 42;
        let segment = 123;

        let encoded = encode_segment_key(base_key, shard, segment).unwrap();
        assert_eq!(encoded.len(), 4 + base_key.len() + 4);
    }

    #[test]
    fn test_encode_decode_meta_key() {
        let base_key = b"test_key";
        let shard = 42;

        let encoded = encode_meta_key(base_key, shard).unwrap();
        assert!(encoded.len() == 4 + base_key.len() + 2);
    }

    #[test]
    fn test_build_prefixes() {
        let base_key = b"test_key";
        let shard = 42;

        let segment_prefix = build_segment_prefix(base_key, shard).unwrap();
        let base_prefix = build_base_key_prefix(base_key).unwrap();

        assert!(segment_prefix.len() > base_prefix.len());
        assert!(segment_prefix.starts_with(&base_prefix));
    }

    #[test]
    fn test_encode_decode_roaring_value() {
        let roaring_data = b"simulated_roaring_bytes";
        let encoded = encode_roaring_value(roaring_data);

        assert_eq!(encoded[0], VALUE_VERSION);
        assert_eq!(&encoded[1..], roaring_data);

        let (version, data) = decode_roaring_value(&encoded).unwrap();
        assert_eq!(version, VALUE_VERSION);
        assert_eq!(data, roaring_data);
    }
}
