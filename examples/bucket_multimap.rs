//! Minimal example for bucketed multimap iteration.

use redb::{Database, MultimapTableDefinition, ReadableDatabase};
use redb_extras::key_buckets::{BucketMultimapIterExt, BucketedKey, KeyBuilder};

const MULTIMAP: MultimapTableDefinition<'static, BucketedKey<u64>, u64> =
    MultimapTableDefinition::new("bucketed_multimap");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("bucket_multimap.redb")?;
    let key_builder = KeyBuilder::new(100)?;
    let write_txn = db.begin_write()?;

    {
        let mut table = write_txn.open_multimap_table(MULTIMAP)?;
        table.insert(key_builder.bucketed_key(42u64, 10), 1u64)?;
        table.insert(key_builder.bucketed_key(42u64, 10), 2u64)?;
        table.insert(key_builder.bucketed_key(42u64, 120), 3u64)?;
    }

    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_multimap_table(MULTIMAP)?;
    let values: Vec<u64> = table
        .bucket_range(&key_builder, 42u64, 0, 199)?
        .collect::<Result<_, _>>()?;

    println!("bucket values: {:?}", values);
    Ok(())
}
