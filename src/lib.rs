pub mod dbcopy;
pub mod error;
pub mod key_buckets;
pub mod partition;
pub mod roaring;
pub mod table_buckets;

// Re-export common types for convenience
pub use error::{Error, Result};

/// Trait for merging values when consolidating bucket tables.
pub trait MergeableValue: Sized {
    /// Merge an incoming value with an existing value (if any).
    fn merge(existing: Option<Self>, incoming: Self) -> Self;
}
