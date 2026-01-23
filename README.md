# redb-extras

Use-case agnostic utilities built on top of [redb](https://github.com/ciberred/redb).
Each utility is standalone and can be adopted independently.

## Database copy (dbcopy)

Copy selected tables between databases using an explicit plan. The destination
must not already contain the tables being copied.

```rust
use redb::{Database, TableDefinition};
use redb_extras::dbcopy::{copy_database, CopyPlan};

const USERS: TableDefinition<&str, u64> = TableDefinition::new("users");

let source = Database::create("source.redb")?;
let destination = Database::create("destination.redb")?;

let plan = CopyPlan::new().table(USERS);
copy_database(&source, &destination, &plan)?;
```

## Partitioned storage (partition)

Generic sharded + segmented storage that manages segment tables and metadata.
You can write raw segments yourself or layer a value handler on top.

```rust
use redb::Database;
use redb_extras::partition::{PartitionConfig, PartitionedTable, PartitionedWrite};

let db = Database::create("example.redb")?;
let config = PartitionConfig::new(16, 64 * 1024, true)?;
let table: PartitionedTable<()> = PartitionedTable::new("events", config);
table.ensure_table_exists(&db)?;

let mut write_txn = db.begin_write()?;
let writer = PartitionedWrite::new(&table, &mut write_txn);
let shard = table.select_shard(b"user_123", 42)?;
writer.update_head_segment(b"user_123", shard, b"payload")?;
write_txn.commit()?;
```

## Roaring bitmap values (roaring)

Roaring bitmap value helpers plus extension traits to read/write bitmap values
directly from redb tables.

```rust
use redb::{Database, TableDefinition};
use redb_extras::roaring::{RoaringValue, RoaringValueReadOnlyTable, RoaringValueTable};

const SESSIONS: TableDefinition<&str, RoaringValue> = TableDefinition::new("sessions");

let db = Database::create("example.redb")?;
let mut write_txn = db.begin_write()?;
{
    let mut table = write_txn.open_table(SESSIONS)?;
    table.insert_member("user_123", 1001)?;
    table.insert_member("user_123", 1002)?;
}
write_txn.commit()?;

let read_txn = db.begin_read()?;
let table = read_txn.open_table(SESSIONS)?;
let bitmap = table.get_bitmap("user_123")?;
println!("{}", bitmap.len());
```

## Bucketed keys (key_buckets)

Bucketed keys attach a bucket prefix to a base key for efficient range scans.
Current helpers are specialized for `u64` base keys.

```rust
use redb::{Database, TableDefinition};
use redb_extras::key_buckets::{BucketIterExt, BucketedKey, KeyBuilder};

const EVENTS: TableDefinition<BucketedKey<u64>, String> = TableDefinition::new("events");

let db = Database::create("example.redb")?;
let key_builder = KeyBuilder::new(100)?;

let mut write_txn = db.begin_write()?;
{
    let mut table = write_txn.open_table(EVENTS)?;
    table.insert(key_builder.bucketed_key(42u64, 10), "a".to_string())?;
    table.insert(key_builder.bucketed_key(42u64, 110), "b".to_string())?;
}
write_txn.commit()?;

let read_txn = db.begin_read()?;
let table = read_txn.open_table(EVENTS)?;
let values: Vec<String> = table
    .bucket_range(&key_builder, 42u64, 0, 199)?
    .collect::<Result<_, _>>()?;
```

## Table buckets (table_buckets)

Bucket-per-table storage for sequences where you want table-level separation
instead of key prefixes. The builder leaks table name strings to satisfy redb's
`'static` table name requirement.

```rust
use redb::Database;
use redb_extras::table_buckets::{TableBucketBuilder, TableBucketIterExt};

let db = Database::create("example.redb")?;
let builder = TableBucketBuilder::new(100, "events")?;

let mut write_txn = db.begin_write()?;
{
    let mut table = write_txn.open_table(builder.table_definition::<u64, String>(0))?;
    table.insert(42u64, "a".to_string())?;
}
write_txn.commit()?;

let read_txn = db.begin_read()?;
let values: Vec<String> = read_txn
    .table_bucket_range(&builder, 42u64, 0, 99)?
    .collect::<Result<_, _>>()?;
```

## Dependencies

- `redb` - Embedded B-tree database with ACID transactions
- `roaring` - Compressed bitmap implementation
- `xxhash-rust` - Hashing for shard selection

## License

Apache License 2.0
