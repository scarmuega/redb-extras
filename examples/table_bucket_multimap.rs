//! Minimal example for table-bucketed multimap iteration.

use redb::{Database, ReadableDatabase};
use redb_extras::table_buckets::{TableBucketBuilder, TableBucketMultimapIterExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("table_bucket_multimap.redb")?;
    let builder = TableBucketBuilder::new(100, "table_bucketed")?;
    let write_txn = db.begin_write()?;

    {
        let mut table =
            write_txn.open_multimap_table(builder.multimap_table_definition::<u64, u64>(0))?;
        table.insert(42u64, 1u64)?;
        table.insert(42u64, 2u64)?;
    }

    {
        let mut table =
            write_txn.open_multimap_table(builder.multimap_table_definition::<u64, u64>(1))?;
        table.insert(42u64, 3u64)?;
    }

    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let values: Vec<u64> = read_txn
        .table_bucket_multimap_range(&builder, 42u64, 0, 199)?
        .collect::<Result<_, _>>()?;

    println!("table bucket values: {:?}", values);
    Ok(())
}
