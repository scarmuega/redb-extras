//! Example usage of the roaring bitmap utility.
//!
//! This example demonstrates basic CRUD operations on roaring bitmaps
//! stored in redb tables.

use redb::{Database, TableDefinition};
use redb_extras::roaring::{RoaringValue, RoaringValueReadOnlyTable as _, RoaringValueTable as _};

// Define tables with different key types to demonstrate generic support
const BYTE_TABLE: TableDefinition<&[u8], RoaringValue> = TableDefinition::new("byte_bitmaps");
const STRING_TABLE: TableDefinition<&str, RoaringValue> = TableDefinition::new("string_bitmaps");
const U64_TABLE: TableDefinition<u64, RoaringValue> = TableDefinition::new("u64_bitmaps");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory database for this example
    let db = Database::create("example.redb")?;

    // Define keys of different types
    let user_key = b"users";
    let product_key = "products";
    let category_key = 42u64;

    // Start a write transaction
    let write_txn = db.begin_write()?;
    {
        // Open our bitmap tables
        let mut byte_table = write_txn.open_table(BYTE_TABLE)?;
        let mut string_table = write_txn.open_table(STRING_TABLE)?;
        let mut u64_table = write_txn.open_table(U64_TABLE)?;

        // Insert some members into different bitmaps with different key types
        println!("Inserting members into bitmaps with different key types...");

        // Table 1: Byte slice keys
        byte_table.insert_member(user_key, 1)?;
        byte_table.insert_member(user_key, 42)?;
        byte_table.insert_member(user_key, 100)?;
        byte_table.insert_member(user_key, 999)?;

        // Table 2: String keys
        string_table.insert_member(product_key, 10)?;
        string_table.insert_member(product_key, 20)?;
        string_table.insert_member(product_key, 30)?;

        // Table 3: u64 keys
        u64_table.insert_member(category_key, 1001)?;
        u64_table.insert_member(category_key, 1002)?;
        u64_table.insert_member(category_key, 1003)?;

        println!("Successfully inserted members into all table types!");
    }

    // Commit the transaction
    write_txn.commit()?;

    // Start a read transaction to verify the data
    let read_txn = db.begin_read()?;
    {
        let byte_table = read_txn.open_table(BYTE_TABLE)?;
        let string_table = read_txn.open_table(STRING_TABLE)?;
        let u64_table = read_txn.open_table(U64_TABLE)?;

        println!("\nReading bitmaps from all table types:");

        // Read and display byte key table
        let user_bitmap = byte_table.get_bitmap(user_key)?;
        let users: Vec<u64> = user_bitmap.iter().collect();
        println!("Users (byte key): {:?}", users);
        println!("User count: {}", user_bitmap.len());

        // Check if specific users exist
        println!("Contains user 42: {}", user_bitmap.contains(42));
        println!("Contains user 999: {}", user_bitmap.contains(999));

        // Read and display string key table
        let product_bitmap = string_table.get_bitmap(product_key)?;
        let products: Vec<u64> = product_bitmap.iter().collect();
        println!("Products (string key): {:?}", products);
        println!("Product count: {}", product_bitmap.len());

        // Check if specific products exist
        println!("Contains product 20: {}", product_bitmap.contains(20));
        println!("Contains product 99: {}", product_bitmap.contains(99));

        // Read and display u64 key table
        let category_bitmap = u64_table.get_bitmap(category_key)?;
        let categories: Vec<u64> = category_bitmap.iter().collect();
        println!("Categories (u64 key): {:?}", categories);
        println!("Category count: {}", category_bitmap.len());

        // Check if specific categories exist
        println!("Contains category 1001: {}", category_bitmap.contains(1001));
        println!("Contains category 9999: {}", category_bitmap.contains(9999));

        // Try to read a non-existent key in each table type
        let nonexistent_byte_key = b"nonexistent";
        let nonexistent_string_key = "nonexistent";
        let nonexistent_u64_key = 999u64;

        let byte_bitmap = byte_table.get_bitmap(nonexistent_byte_key)?;
        let string_bitmap = string_table.get_bitmap(&nonexistent_string_key)?;
        let u64_bitmap = u64_table.get_bitmap(nonexistent_u64_key)?;

        if byte_bitmap.is_empty() && string_bitmap.is_empty() && u64_bitmap.is_empty() {
            println!("All non-existent keys correctly returned empty bitmaps");
        } else {
            println!("Some non-existent keys had unexpected data");
        }
    }

    println!("\nExample completed successfully!");
    Ok(())
}
