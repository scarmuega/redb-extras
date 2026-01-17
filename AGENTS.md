# redb-extras
*A design document*

---

## Overview

`redb-extras` is a collection of **use-case–agnostic utilities built on top of redb**.

The crate is intended as an **extension toolbox** for redb, hosting small, focused primitives that:

- solve common low-level storage problems,
- remain explicit and synchronous,
- integrate naturally with redb’s transaction model,
- avoid imposing domain semantics.

The first utility provided by this crate is **Sharded Roaring Bitmap Tables**, documented below.

---

## 1. Scope & Goals (Sharded Roaring Bitmap Tables)

### 1.1 Scope

This library provides a **key–value–like abstraction** on top of **redb** where:

- Keys are **opaque byte slices** supplied by the consumer
- Values are **Roaring bitmaps** (concretely `roaring::RoaringTreemap`)
- Large values are **sharded and segmented** transparently to avoid write amplification
- The consumer **does not deal with shards or segments**
- No domain semantics (eg. “tags”, “buckets”, “time”) are imposed by the library

The library is **not**:
- A time-series index
- A tagging system
- A query engine
- A general-purpose bitmap abstraction (only roaring is supported)

It is a **storage primitive** that can be embedded into higher-level systems.

---

### 1.2 Goals

- **Bounded write cost**
  - Avoid rewriting ever-growing bitmap blobs
  - Writes should scale with *segment size*, not total cardinality

- **KV-like mental model**
  - `key -> bitmap`
  - Sharding and segmentation are invisible to the caller

- **Opaque keys**
  - Caller fully controls key structure and semantics
  - Keys may already embed time, bucket, or composite data

- **Good read performance**
  - Efficient union across shards and segments
  - Support full bitmap materialization and iteration

- **Redb-native**
  - Explicit transaction handling
  - No background threads
  - Deterministic, synchronous behavior

---

## 2. Architectural Decisions

### 2.1 Loosely coupled utility modules

The design explicitly separates concerns into loosely coupled utility modules:

- **Partitioning mechanics** (`partition/`)
  - Sharding
  - Segmentation
  - Storage key encoding
  - Optional metadata (head segment)
  - Generic infrastructure reusable across value types

from:

- **Value semantics** (`roaring/`)
  - How values are encoded/decoded
  - How values are updated
  - How multiple segments are merged
  - Value-specific optimizations

This separation allows utilities to be developed, tested, and used independently while maintaining clear boundaries.

### 2.2 Composition facades provide opinionated APIs

Public APIs are implemented as composition facades that combine lower-level utilities:

- **Facade layer** (`lib.rs`): `PartitionedRoaringTable` combines partition + roaring concerns
- **Opinionated design**: Facades provide high-level convenience methods that handle transactions and complex operations internally
- **Flexibility via primitives**: Advanced users can use lower-level utilities directly when facades are too restrictive
- **Separate configurations**: Each utility maintains its own configuration structure, facades combine them

This pattern balances convenience for common use cases with flexibility for advanced scenarios.

### 2.3 Crate-scoped error handling with layered internals

Error handling follows a layered approach:

- **Single public error**: `crate::Error` wraps all internal errors for simple facade usage
- **Utility-specific errors**: `PartitionError`, `RoaringError`, `EncodingError` provide internal precision
- **Automatic conversion**: `From` traits provide seamless error propagation between layers
- **Future-proof**: Any new facades can reuse the same `crate::Error` without breaking changes

This approach provides simple error handling for facade users while maintaining precise error information for utility developers.

### 2.4 Trait-based abstraction for value-specific operations

Value-specific operations are abstracted through traits:

- **Roaring-specific trait**: `RoaringTableTrait` defines operations that need roaring knowledge
- **Size queries**: Traits provide mechanisms for value-specific size information
- **Future extensibility**: New value types can implement the same generic interfaces
- **Clean boundaries**: Generic storage layer remains unaware of value-specific semantics

This allows the generic `PartitionedTable<V>` to work with any value type while preserving value-specific optimizations.

### 2.5 Explicit module boundaries and scoped exports

Module boundaries are explicitly defined and exports are carefully scoped:

- **Root exports**: Only facades and essential public types
- **Utility exports**: Each utility module exports its own types for advanced users
- **Internal details**: Implementation details remain private within modules
- **Clear dependencies**: Dependencies between modules are explicit and minimal

This provides a clean public API while enabling advanced usage patterns.

---

### 2.6 Concrete value type with trait abstractions for operations

- Values are **concretely** `roaring::RoaringTreemap`
- Value-specific operations are abstracted via traits to maintain clean layer boundaries
- Traits focus on operational concerns (size queries, compaction) not value representation
- This provides simplicity for values while enabling proper separation of concerns

If other value types are needed in the future, they can implement the same operational interfaces.

---

### 2.3 Sharding is deterministic and content-agnostic

- Shards are chosen via a deterministic hash over:
  - `base_key`
  - element id (`u64`)
- Purpose:
  - Spread writes for hot keys
  - Avoid a single hot segment per key

No semantic meaning is attached to shards.

---

### 2.4 Segmentation is size-based

- Segments are rolled when their **serialized size** exceeds a threshold
- This matches roaring’s non-linear compression behavior
- Cardinality-based rolling is intentionally avoided

---

### 2.5 Meta table is optional but supported

Two modes are supported:

- **With meta table**
  - O(1) discovery of writable segment
  - Stable write cost regardless of segment count

- **Without meta table**
  - Head segment discovered by scanning
  - Simpler but less predictable write cost

This is a runtime configuration choice.

---

## 3. Storage Model

### 3.1 Logical view

From the user’s perspective:

```
base_key -> RoaringTreemap<u64>
```

---

### 3.2 Physical storage layout

Internally, the bitmap is split into:

```
(base_key, shard, segment) -> roaring_bytes
```

Where:
- `shard ∈ [0..shard_count)`
- `segment ∈ [0..head_seg]`

---

### 3.3 Key encoding

Keys are **length-prefixed** to keep `base_key` fully opaque.

#### Segment key
```
[u32_be key_len]
[base_key bytes]
[u16_be shard]
[u16_be segment]
```

#### Meta key
```
[u32_be key_len]
[base_key bytes]
[u16_be shard]
```

This allows:
- Exact lookups
- Efficient iteration by `(base_key, shard)`
- Safe handling of arbitrary key bytes

---

### 3.4 Value encoding

Segment values contain:

```
[u8 version]
[roaring serialized bytes]
```

- `version = 1`
- Allows future migrations

---

## 4. Public API

### 4.1 Main facade type

```rust
pub struct PartitionedRoaringTable {
    inner: PartitionedTable<RoaringValue>,
}
```

### 4.2 Configuration

```rust
pub struct RoaringConfig {
    pub partition: PartitionConfig,
}

pub struct PartitionConfig {
    pub shard_count: u16,
    pub segment_max_bytes: usize,
    pub use_meta: bool,
}
```

---

### 4.3 Write operations

```rust
insert_member(base_key: &[u8], id: u64)
insert_many(base_key: &[u8], ids: impl Iterator<Item = u64>)
compact_key(base_key: &[u8])
```

All writes occur inside a `redb::WriteTransaction`.

---

### 4.4 Read operations

```rust
get(base_key: &[u8]) -> RoaringTreemap
iter_ids(base_key: &[u8]) -> impl Iterator<Item = u64>
```

All reads occur inside a `redb::ReadTransaction`.

---

## 5. Internal Modules

```
src/
  lib.rs                     # PartitionedRoaringTable facade + public exports

  error.rs                   # Crate-level Error + utility-specific errors

  encoding/
    mod.rs
    key.rs                   # Key encoding/decoding utilities

  partition/
    mod.rs
    config.rs                # PartitionConfig
    shard.rs                 # Shard selection logic (internal implementation)
    meta.rs                  # Meta-table operations
    table.rs                 # Generic PartitionedTable<V>
    scan.rs                  # Segment enumeration

  roaring/
    mod.rs
    value.rs                 # RoaringValue struct + encoding/decoding + size query
    traits.rs                # RoaringTableTrait (roaring-specific table operations)
    compact.rs               # Roaring-aware compaction utilities
```

---

## 6. Architectural Patterns

### 6.1 Facade Pattern

The design uses a facade pattern to provide an opinionated API:

- **Facade Layer (`lib.rs`)**: `PartitionedRoaringTable` combines partition + roaring concerns
- **Partition Layer (`partition/`)**: Generic sharded + segmented byte storage
- **Value Layer (`roaring/`)**: Roaring-specific value handling

The facade is opinionated - if users need flexibility, they should use lower-level primitives directly.

### 6.2 Trait-Based Abstraction

Roaring-specific table operations are abstracted via `RoaringTableTrait`:

- Size query for values
- Compaction operations
- Scaffold methods for union/intersection

This allows the generic `PartitionedTable<V>` to work with any value type while preserving roaring-specific optimizations.

### 6.3 Error Handling Strategy

Crate-scoped error handling:

- **Single Public Error**: `crate::Error` wraps all internal errors
- **Utility-Specific Errors**: `PartitionError`, `RoaringError`, `EncodingError` for internal precision
- **Automatic Conversions**: `From` traits provide seamless error propagation
- **Future Facades**: Any new facades can reuse the same `crate::Error`

## 7. Module Responsibilities

### 7.1 `encoding/`

**Purpose:** Stable on-disk encoding.

- `key.rs`
  - Encode/decode segment keys
  - Encode/decode meta keys
  - Build base-key prefixes

---

### 7.2 `partition/`

**Purpose:** Generic sharded + segmented byte storage.

- `config.rs`
  - `PartitionConfig`

- `shard.rs`
  - Deterministic shard selection using hashing

- `meta.rs`
  - Meta-table adapter
  - `get_head_seg`
  - `set_head_seg`
  - Optional repair via scan

- `scan.rs`
  - Segment enumeration via prefix scanning
  - Used when meta is disabled

- `table.rs`
  - `PartitionedTable<V>` (generic over value types)
  - `PartitionedRead<V>`
  - `PartitionedWrite<V>`
  - Does **not** know roaring semantics

---

### 7.3 `roaring/`

**Purpose:** Roaring-specific value handling and table operations.

- `value.rs`
  - Encode/decode `RoaringTreemap`
  - Version handling
  - Size query for value serialization
  - `RoaringValue` struct implementation

- `traits.rs`
  - `RoaringTableTrait` for roaring-specific table operations
  - Size query interface
  - Scaffold methods for union/intersection (future implementation)

- `compact.rs`
  - Roaring-aware compaction
  - Segment rewriting and balancing
  - Reusable infrastructure for any value type

---

### 7.4 Facade Layer (`lib.rs`)

**Purpose:** Opinionated public API combining partition + roaring.

- `PartitionedRoaringTable`
  - Facade wrapping `PartitionedTable<RoaringValue>`
  - Opinionated API with transaction handling
  - Crate-scoped error handling
  - High-level methods like `insert_member`, `get`, `compact_key`

- Configuration:
  - `RoaringConfig` combining partition settings
  - Export of essential types for public use

---

## 8. Write Path Algorithm

### `insert_member(base_key, id)`

1. Compute shard:
   ```
   shard = hash(base_key || id) % shard_count
   ```

2. Discover head segment:
   - via meta table, or
   - via scan

3. Load segment bitmap (if any)

4. Deserialize roaring bitmap

5. Insert id

6. Serialize bitmap

7. If serialized size ≤ `segment_max_bytes`:
   - overwrite segment

8. Else:
   - roll segment
   - create new segment with single id
   - update meta (if enabled)

All operations occur in one write transaction.

---

## 9. Read Path Algorithm

### `get(base_key)`

1. For each shard:
   - enumerate segments
   - deserialize roaring bitmaps
   - union into accumulator

2. Return accumulated bitmap

---

## 10. Compaction

### Purpose
- Reduce read fanout
- Control segment counts

### Algorithm
For each `(base_key, shard)`:
1. Load all segments
2. Union into one bitmap
3. Re-split into balanced segments
4. Rewrite segments
5. Update meta

Compaction is explicit and caller-driven.

---

## 11. Libraries Used

### Storage
- `redb`
  - Embedded B-tree KV store
  - ACID transactions

### Bitmap
- `roaring`
  - `RoaringTreemap` (u64)
  - Fast union/intersection
  - Stable serialization

### Hashing
- `xxhash-rust`
  - Deterministic
  - Fast shard selection

### Errors
- `thiserror`

### Optional
- `bytes` (buffer reuse)
- `smallvec` (stack-allocated scratch buffers)

---

## 12. Non-Goals / Explicit Exclusions

- Background compaction threads
- Query planners or boolean query DSLs
- Schema or key validation
- Automatic bucket/time handling
- Multi-value table integration

---

## 13. Summary

This library provides:

- A **predictable-write-cost** roaring bitmap store
- A **clean KV-like API**
- **Opaque keys** with consumer-defined semantics
- A **clear layering** between storage mechanics and roaring logic
- A design that fits naturally into **redb’s transaction model**

It is intentionally small, explicit, and composable.

