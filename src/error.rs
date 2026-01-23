//! Crate-scoped error handling for redb-extras.
//!
//! This module provides a unified error type for public APIs while maintaining
//! precise error information for internal utilities.

use std::fmt;

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type exposed to users of the crate.
///
/// This provides a simple interface for facade users while wrapping more specific
/// internal error types for debugging and advanced usage.
#[derive(Debug)]
pub enum Error {
    /// Errors from the partition layer (generic storage mechanics)
    Partition(crate::partition::PartitionError),

    /// Errors from the roaring layer (bitmap-specific operations)
    Roaring(crate::roaring::RoaringError),

    /// Errors from the bucket layer (bucket-specific operations)
    Bucket(crate::key_buckets::BucketError),

    /// Errors from the database copy utilities
    DbCopy(crate::dbcopy::DbCopyError),

    /// Invalid input parameters
    InvalidInput(String),

    /// Transaction-related errors
    TransactionFailed(String),
}

impl From<crate::partition::PartitionError> for Error {
    fn from(err: crate::partition::PartitionError) -> Self {
        Error::Partition(err)
    }
}

impl From<crate::roaring::RoaringError> for Error {
    fn from(err: crate::roaring::RoaringError) -> Self {
        Error::Roaring(err)
    }
}

impl From<crate::key_buckets::BucketError> for Error {
    fn from(err: crate::key_buckets::BucketError) -> Self {
        Error::Bucket(err)
    }
}

impl From<crate::dbcopy::DbCopyError> for Error {
    fn from(err: crate::dbcopy::DbCopyError) -> Self {
        Error::DbCopy(err)
    }
}

impl From<redb::StorageError> for Error {
    fn from(err: redb::StorageError) -> Self {
        Error::TransactionFailed(format!("Storage error: {}", err))
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Partition(err) => err.source(),
            Error::Roaring(err) => err.source(),
            Error::Bucket(err) => err.source(),
            Error::DbCopy(err) => err.source(),
            Error::InvalidInput(_) => None,
            Error::TransactionFailed(_) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Partition(err) => write!(f, "Partition error: {}", err),
            Error::Roaring(err) => write!(f, "Roaring error: {}", err),
            Error::Bucket(err) => write!(f, "Bucket error: {}", err),
            Error::DbCopy(err) => write!(f, "Database copy error: {}", err),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Error::TransactionFailed(msg) => write!(f, "Transaction failed: {}", msg),
        }
    }
}
