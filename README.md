# redb-extras

Use-case agnostic utilities for [redb](https://github.com/ciberred/redb), featuring sharded roaring bitmap tables.

## Overview

`redb-extras` provides focused storage primitives that solve common low-level problems while maintaining explicit, synchronous behavior and integrating naturally with redb's transaction model.

### Main Feature: Partitioned Roaring Bitmap Tables

A key-value store where:
- **Keys** are opaque byte slices (you control the structure)
- **Values** are Roaring bitmaps (`RoaringTreemap<u64>`)
- **Automatic sharding** spreads writes for hot keys
- **Segment-based storage** controls write amplification
- **KV-like mental model** - sharding is invisible to callers

## Quick Start

```rust
use redb::Database;
use redb_extras::PartitionedRoaringTable;

// Create database and table
let db = Database::create("example.db")?;
let table = PartitionedRoaringTable::new("sessions", Default::default());

// Insert data
let mut write_txn = db.begin_write()?;
{
    let mut writer = table.write(&mut write_txn);
    writer.insert_member(b"user_123", 1001)?;
    writer.insert_member(b"user_123", 1002)?;
}
write_txn.commit()?;

// Read data
let read_txn = db.begin_read()?;
let reader = table.read(&read_txn);
let sessions = reader.get(b"user_123")?;
println!("User has {} sessions", sessions.len());
```

## Features

- **Bounded write cost** - avoid rewriting ever-growing bitmap blobs
- **Deterministic sharding** - consistent element placement across shards  
- **Size-based segmentation** - segments roll when serialized size exceeds threshold
- **Opaque keys** - you control key structure and semantics
- **Explicit transactions** - no background threads, fully synchronous
- **No domain semantics** - pure storage primitive for higher-level systems

## Configuration

```rust
use redb_extras::{RoaringConfig, PartitionConfig};

let config = RoaringConfig::new(
    PartitionConfig::new(
        16,              // 16 shards for write distribution
        64 * 1024,       // 64KB segments for balance
        true,            // Use meta table for O(1) head discovery
    )?
);
```

## Use Cases

- Session tracking systems
- Real-time analytics indices
- Event sourcing append-only logs
- Large-scale set operations
- Any scenario needing efficient bitmap storage with controlled write amplification

## Architecture

The library uses a layered design:

- **Facade Layer** (`PartitionedRoaringTable`): High-level opinionated API
- **Partition Layer**: Generic sharded + segmented byte storage
- **Value Layer**: Roaring-specific value handling and optimization

This separation allows advanced users to use lower-level primitives directly when the facade is too restrictive.

## Dependencies

- `redb` - Embedded B-tree database with ACID transactions
- `roaring` - Fast compressed bitmap implementation
- `xxhash-rust` - High-speed hashing for shard selection

## License

Apache License 2.0