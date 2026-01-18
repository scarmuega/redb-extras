//! Integration tests for roaring table operations with different key types.

#[cfg(test)]
mod tests {
    use redb::{Database, TableDefinition};
    use redb_extras::roaring::{
        RoaringValue, RoaringValueReadOnlyTable as _, RoaringValueTable as _,
    };
    use tempfile::NamedTempFile;

    // Define tables with different key types
    const BYTE_TABLE: TableDefinition<&[u8], RoaringValue> = TableDefinition::new("test_byte");
    const STRING_TABLE: TableDefinition<&str, RoaringValue> = TableDefinition::new("test_string");
    const U64_TABLE: TableDefinition<u64, RoaringValue> = TableDefinition::new("test_u64");

    #[test]
    fn test_roaring_table_operations_byte_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        {
            let mut table = write_txn.open_table(BYTE_TABLE).unwrap();

            // Test insert operations
            table.insert_member(b"user1", 42).unwrap();
            table.insert_member(b"user1", 100).unwrap();
            table.insert_member(b"user2", 200).unwrap();

            // Test contains operation
            assert!(table.contains_member(b"user1", 42).unwrap());
            assert!(table.contains_member(b"user1", 100).unwrap());
            assert!(!table.contains_member(b"user1", 999).unwrap());
            assert!(table.contains_member(b"user2", 200).unwrap());

            // Test member count
            assert_eq!(table.get_member_count(b"user1").unwrap(), 2);
            assert_eq!(table.get_member_count(b"user2").unwrap(), 1);
            assert_eq!(table.get_member_count(b"nonexistent").unwrap(), 0);

            // Test remove operation
            table.remove_member(b"user1", 42).unwrap();
            assert!(!table.contains_member(b"user1", 42).unwrap());
            assert!(table.contains_member(b"user1", 100).unwrap());
            assert_eq!(table.get_member_count(b"user1").unwrap(), 1);

            // Test batch insert
            let batch_members = vec![300, 400, 500];
            table
                .insert_members(b"user3", batch_members.clone())
                .unwrap();
            assert_eq!(table.get_member_count(b"user3").unwrap(), 3);
            for member in &batch_members {
                assert!(table.contains_member(b"user3", *member).unwrap());
            }

            // Test batch remove
            let remove_members = vec![300, 400];
            table
                .remove_members(b"user3", remove_members.clone())
                .unwrap();
            assert_eq!(table.get_member_count(b"user3").unwrap(), 1);
            assert!(!table.contains_member(b"user3", 300).unwrap());
            assert!(!table.contains_member(b"user3", 400).unwrap());
            assert!(table.contains_member(b"user3", 500).unwrap());

            // Test clear bitmap
            table.clear_bitmap(b"user1").unwrap();
            assert_eq!(table.get_member_count(b"user1").unwrap(), 0);
            assert!(!table.contains_member(b"user1", 100).unwrap());
        }

        write_txn.commit().unwrap();
    }

    #[test]
    fn test_roaring_table_operations_string_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        {
            let mut table = write_txn.open_table(STRING_TABLE).unwrap();

            // Test insert operations
            table.insert_member("category1", 10).unwrap();
            table.insert_member("category1", 20).unwrap();
            table.insert_member("category2", 30).unwrap();

            // Test contains operation
            assert!(table.contains_member("category1", 10).unwrap());
            assert!(table.contains_member("category1", 20).unwrap());
            assert!(!table.contains_member("category1", 999).unwrap());

            // Test member count
            assert_eq!(table.get_member_count("category1").unwrap(), 2);
            assert_eq!(table.get_member_count("category2").unwrap(), 1);

            // Test remove operation
            table.remove_member("category1", 10).unwrap();
            assert!(!table.contains_member("category1", 10).unwrap());
            assert!(table.contains_member("category1", 20).unwrap());

            // Test clear bitmap
            table.clear_bitmap("category1").unwrap();
            assert_eq!(table.get_member_count("category1").unwrap(), 0);
        }

        write_txn.commit().unwrap();
    }

    #[test]
    fn test_roaring_table_operations_u64_key() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        {
            let mut table = write_txn.open_table(U64_TABLE).unwrap();

            // Test insert operations
            table.insert_member(100, 1000).unwrap();
            table.insert_member(100, 2000).unwrap();
            table.insert_member(200, 3000).unwrap();

            // Test contains operation
            assert!(table.contains_member(100, 1000).unwrap());
            assert!(table.contains_member(100, 2000).unwrap());
            assert!(!table.contains_member(100, 9999).unwrap());

            // Test member count
            assert_eq!(table.get_member_count(100).unwrap(), 2);
            assert_eq!(table.get_member_count(200).unwrap(), 1);

            // Test remove operation
            table.remove_member(100, 1000).unwrap();
            assert!(!table.contains_member(100, 1000).unwrap());
            assert!(table.contains_member(100, 2000).unwrap());

            // Test clear bitmap
            table.clear_bitmap(100).unwrap();
            assert_eq!(table.get_member_count(100).unwrap(), 0);
        }

        write_txn.commit().unwrap();
    }

    #[test]
    fn test_readonly_table_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        // Insert some test data
        {
            let mut table = write_txn.open_table(BYTE_TABLE).unwrap();
            table.insert_member(b"test", 42).unwrap();
            table.insert_member(b"test", 100).unwrap();
        }
        write_txn.commit().unwrap();

        // Test readonly operations
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(BYTE_TABLE).unwrap();

        assert!(table.contains_member(b"test", 42).unwrap());
        assert!(table.contains_member(b"test", 100).unwrap());
        assert!(!table.contains_member(b"test", 999).unwrap());
        assert_eq!(table.get_member_count(b"test").unwrap(), 2);

        // Test iteration
        let members: Vec<u64> = table.iter_members(b"test").unwrap().collect();
        assert_eq!(members.len(), 2);
        assert!(members.contains(&42));
        assert!(members.contains(&100));
    }

    #[test]
    fn test_empty_key_handling() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        {
            let mut table = write_txn.open_table(BYTE_TABLE).unwrap();

            // Operations on non-existent keys should work gracefully
            assert!(!table.contains_member(b"nonexistent", 42).unwrap());
            assert_eq!(table.get_member_count(b"nonexistent").unwrap(), 0);

            // Remove from non-existent key should not error
            table.remove_member(b"nonexistent", 42).unwrap();

            // Clear non-existent key should not error
            table.clear_bitmap(b"nonexistent").unwrap();

            // Batch operations on non-existent key should work
            table.insert_members(b"newkey", vec![1, 2, 3]).unwrap();
            assert_eq!(table.get_member_count(b"newkey").unwrap(), 3);
        }

        write_txn.commit().unwrap();
    }

    #[test]
    fn test_large_batch_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::create(temp_file.path()).unwrap();
        let write_txn = db.begin_write().unwrap();

        {
            let mut table = write_txn.open_table(BYTE_TABLE).unwrap();

            // Test large batch insert (1000 elements)
            let large_batch: Vec<u64> = (0..1000).collect();
            table
                .insert_members(b"large_key", large_batch.clone())
                .unwrap();
            assert_eq!(table.get_member_count(b"large_key").unwrap(), 1000);

            // Test removing half of them
            let remove_batch: Vec<u64> = (0..500).collect();
            table.remove_members(b"large_key", remove_batch).unwrap();
            assert_eq!(table.get_member_count(b"large_key").unwrap(), 500);

            // Verify remaining elements
            for i in 500..1000 {
                assert!(table.contains_member(b"large_key", i).unwrap());
            }
        }

        write_txn.commit().unwrap();
    }
}
