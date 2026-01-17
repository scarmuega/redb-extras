//! Crate-scoped error handling for redb-extras.
//!
//! This module provides a unified error type for public APIs while maintaining
//! precise error information for internal utilities.

use std::fmt;

/// Result type for the public API
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type exposed to users of the crate.
/// 
/// This provides a simple interface for facade users while wrapping more specific
/// internal error types for debugging and advanced usage.
#[derive(Debug)]
pub enum Error {
    /// Errors from the partition layer (generic storage mechanics)
    Partition(PartitionError),
    
    /// Errors from the roaring layer (bitmap-specific operations)
    Roaring(RoaringError),
    
    /// Errors from key/value encoding/decoding
    Encoding(EncodingError),
    
    /// Invalid input parameters
    InvalidInput(String),
    
    /// Transaction-related errors
    TransactionFailed(String),
}

impl From<PartitionError> for Error {
    fn from(err: PartitionError) -> Self {
        Error::Partition(err)
    }
}

impl From<RoaringError> for Error {
    fn from(err: RoaringError) -> Self {
        Error::Roaring(err)
    }
}

impl From<EncodingError> for Error {
    fn from(err: EncodingError) -> Self {
        Error::Encoding(err)
    }
}

/// Errors specific to the partition layer.
/// These are concerned with generic storage mechanics and are independent of value types.
#[derive(Debug)]
pub enum PartitionError {
    /// Invalid shard count configuration
    InvalidShardCount(u16),
    
    /// Invalid segment size configuration
    InvalidSegmentSize(usize),
    
    /// Meta table operations failed
    MetaOperationFailed(String),
    
    /// Segment scan failed
    SegmentScanFailed(String),
    
    /// Database operation failed
    DatabaseError(String),
}

/// Errors specific to the roaring layer.
/// These are concerned with bitmap operations and value-specific semantics.
#[derive(Debug)]
pub enum RoaringError {
    /// Failed to serialize/deserialize RoaringTreemap
    SerializationFailed(String),
    
    /// Compaction operation failed
    CompactionFailed(String),
    
    /// Invalid roaring bitmap data
    InvalidBitmap(String),
    
    /// Size query failed
    SizeQueryFailed(String),
}

/// Errors specific to key/value encoding.
/// These handle the binary format used for storage.
#[derive(Debug)]
pub enum EncodingError {
    /// Invalid key encoding
    InvalidKeyEncoding(String),
    
    /// Invalid value encoding
    InvalidValueEncoding(String),
    
    /// Buffer too small for encoding
    BufferTooSmall { need: usize, have: usize },
    
    /// Unsupported encoding version
    UnsupportedVersion(u8),
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionError::InvalidShardCount(count) => {
                write!(f, "Invalid shard count {}: must be between 1 and 65535", count)
            }
            PartitionError::InvalidSegmentSize(size) => {
                write!(f, "Invalid segment size {}: must be greater than 0", size)
            }
            PartitionError::MetaOperationFailed(msg) => {
                write!(f, "Meta table operation failed: {}", msg)
            }
            PartitionError::SegmentScanFailed(msg) => {
                write!(f, "Segment scan failed: {}", msg)
            }
            PartitionError::DatabaseError(msg) => {
                write!(f, "Database error: {}", msg)
            }
        }
    }
}

impl fmt::Display for RoaringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoaringError::SerializationFailed(msg) => {
                write!(f, "Roaring serialization failed: {}", msg)
            }
            RoaringError::CompactionFailed(msg) => {
                write!(f, "Compaction failed: {}", msg)
            }
            RoaringError::InvalidBitmap(msg) => {
                write!(f, "Invalid roaring bitmap: {}", msg)
            }
            RoaringError::SizeQueryFailed(msg) => {
                write!(f, "Size query failed: {}", msg)
            }
        }
    }
}

impl fmt::Display for EncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodingError::InvalidKeyEncoding(msg) => {
                write!(f, "Invalid key encoding: {}", msg)
            }
            EncodingError::InvalidValueEncoding(msg) => {
                write!(f, "Invalid value encoding: {}", msg)
            }
            EncodingError::BufferTooSmall { need, have } => {
                write!(f, "Buffer too small: need {} bytes, have {}", need, have)
            }
            EncodingError::UnsupportedVersion(version) => {
                write!(f, "Unsupported encoding version: {}", version)
            }
        }
    }
}