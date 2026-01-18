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
- it is a collection of **storage primitive** that can be embedded into higher-level systems.

---

## Architectural Decisions

### Loosely coupled utility modules

The design explicitly separates concerns into loosely coupled utility modules:

- **Partitioning mechanics** (`partition/`)
  - Sharding
  - Segmentation
  - Storage key encoding
  - Optional metadata (head segment)
  - Generic infrastructure reusable across value types

- **Roaring Bitmap value** (`roaring/`)
  - How values are encoded/decoded
  - How values are updated
  - How multiple segments are merged
  - Value-specific optimizations

This separation allows utilities to be developed, tested, and used independently while maintaining clear boundaries.

### Explicit module boundaries and scoped exports

Module boundaries are explicitly defined and exports are carefully scoped:

- **Root exports**: Only facades and essential public types
- **Utility exports**: Each utility module exports its own types for advanced users
- **Internal details**: Implementation details remain private within modules
- **Clear dependencies**: Dependencies between modules are explicit and minimal

This provides a clean public API while enabling advanced usage patterns.

---
