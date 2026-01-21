use super::{copy_database, CopyPlan, DbCopyError};
use crate::Error;
use redb::{Database, MultimapTableDefinition, ReadableDatabase, TableDefinition};
use tempfile::NamedTempFile;

const USERS: TableDefinition<&str, u64> = TableDefinition::new("users");
const BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("blobs");
const TAGS: MultimapTableDefinition<&str, u64> = MultimapTableDefinition::new("tags");

#[test]
fn copies_tables_and_multimaps() {
    let source_file = NamedTempFile::new().unwrap();
    let dest_file = NamedTempFile::new().unwrap();
    let source = Database::create(source_file.path()).unwrap();
    let dest = Database::create(dest_file.path()).unwrap();

    let write_txn = source.begin_write().unwrap();
    {
        let mut users = write_txn.open_table(USERS).unwrap();
        users.insert("alice", 1).unwrap();
        users.insert("bob", 2).unwrap();

        let mut blobs = write_txn.open_table(BLOBS).unwrap();
        blobs.insert("one", b"first".as_slice()).unwrap();
        blobs.insert("two", b"second".as_slice()).unwrap();

        let mut tags = write_txn.open_multimap_table(TAGS).unwrap();
        tags.insert("alice", 10).unwrap();
        tags.insert("alice", 20).unwrap();
        tags.insert("bob", 30).unwrap();
    }
    write_txn.commit().unwrap();

    let plan = CopyPlan::new().table(USERS).table(BLOBS).multimap(TAGS);

    copy_database(&source, &dest, &plan).unwrap();

    let read_txn = dest.begin_read().unwrap();
    let users = read_txn.open_table(USERS).unwrap();
    let blobs = read_txn.open_table(BLOBS).unwrap();
    let tags = read_txn.open_multimap_table(TAGS).unwrap();

    assert_eq!(users.get("alice").unwrap().unwrap().value(), 1);
    assert_eq!(users.get("bob").unwrap().unwrap().value(), 2);
    assert_eq!(
        blobs.get("one").unwrap().unwrap().value(),
        b"first".as_slice()
    );
    assert_eq!(
        blobs.get("two").unwrap().unwrap().value(),
        b"second".as_slice()
    );

    let mut alice_tags: Vec<u64> = tags
        .get("alice")
        .unwrap()
        .map(|value| value.unwrap().value())
        .collect();
    alice_tags.sort_unstable();
    assert_eq!(alice_tags, vec![10, 20]);

    let bob_tags: Vec<u64> = tags
        .get("bob")
        .unwrap()
        .map(|value| value.unwrap().value())
        .collect();
    assert_eq!(bob_tags, vec![30]);
}

#[test]
fn destination_conflicts_detected_before_copy() {
    let source_file = NamedTempFile::new().unwrap();
    let dest_file = NamedTempFile::new().unwrap();
    let source = Database::create(source_file.path()).unwrap();
    let dest = Database::create(dest_file.path()).unwrap();

    let source_txn = source.begin_write().unwrap();
    {
        let mut users = source_txn.open_table(USERS).unwrap();
        users.insert("alice", 1).unwrap();
    }
    source_txn.commit().unwrap();

    let dest_txn = dest.begin_write().unwrap();
    {
        let mut users = dest_txn.open_table(USERS).unwrap();
        users.insert("existing", 99).unwrap();
    }
    dest_txn.commit().unwrap();

    let plan = CopyPlan::new().table(USERS);
    let result = copy_database(&source, &dest, &plan);

    match result {
        Err(Error::DbCopy(DbCopyError::DestinationTablesExist(conflicts))) => {
            assert_eq!(conflicts, vec!["table users"]);
        }
        other => panic!("unexpected result: {other:?}"),
    }
}
