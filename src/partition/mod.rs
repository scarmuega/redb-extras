//! Generic partitioned storage module.
//! 
//! This module provides reusable infrastructure for sharded and segmented storage
//! that is independent of value types. It can be used with any value type that
//! implements the necessary traits.

pub mod config;
pub mod shard;
pub mod table;

// Re-export main types for public API
pub use config::PartitionConfig;
pub use table::{PartitionedTable, PartitionedRead, PartitionedWrite};