//! Generic partitioned storage module.
//!
//! This module provides reusable infrastructure for sharded and segmented storage
//! that is independent of value types. It can be used with any value type that
//! implements the necessary traits.

use std::fmt;

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

    /// Encoding operation failed
    EncodingError(String),
}

impl std::error::Error for PartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionError::InvalidShardCount(count) => {
                write!(
                    f,
                    "Invalid shard count {}: must be between 1 and 65535",
                    count
                )
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
            PartitionError::EncodingError(ref err) => {
                write!(f, "Encoding error: {}", err)
            }
        }
    }
}

pub mod config;
pub mod scan;
pub mod shard;
pub mod table;
pub mod traits;

// Re-export main types for public API
pub use config::PartitionConfig;
pub use scan::{enumerate_segments, find_head_segment, SegmentInfo, SegmentIterator};
pub use table::{PartitionedRead, PartitionedTable, PartitionedWrite};
