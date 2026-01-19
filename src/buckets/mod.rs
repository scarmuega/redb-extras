//! Bucketed storage utility module.
//!
//! This module provides bucket-based key grouping for sequence data.
//! It enables efficient range queries by organizing sequences into
//! deterministic buckets using configurable bucket sizes.

use std::fmt;

/// Errors specific to the bucket layer.
#[derive(Debug)]
pub enum BucketError {
    /// Invalid bucket size configuration
    InvalidBucketSize(u64),

    /// Invalid bucket range for iteration
    InvalidRange { start: u64, end: u64 },

    /// Serialization operation failed
    SerializationError(String),

    /// Iteration over bucket range failed
    IterationError(String),
}

impl fmt::Display for BucketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketError::InvalidBucketSize(size) => {
                write!(f, "Invalid bucket size {}: must be greater than 0", size)
            }
            BucketError::InvalidRange { start, end } => {
                write!(
                    f,
                    "Invalid bucket range: start {} must be <= end {}",
                    start, end
                )
            }
            BucketError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
            BucketError::IterationError(msg) => {
                write!(f, "Bucket iteration error: {}", msg)
            }
        }
    }
}

impl std::error::Error for BucketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub mod iterator;
pub mod key;

// Re-export main types for public API
pub use iterator::{
    BucketIterExt, BucketMultimapIterExt, BucketRangeIterator, BucketRangeMultimapIterator,
};
pub use key::{BucketedKey, KeyBuilder};
