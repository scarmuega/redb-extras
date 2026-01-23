use super::{RoaringValue, RoaringValueReadOnlyTable, RoaringValueTable};
use crate::Result;
use redb::ReadableTable;
use roaring::RoaringTreemap;

// Implementation for byte slice keys
impl RoaringValueReadOnlyTable<'_, &[u8]> for redb::ReadOnlyTable<&'static [u8], RoaringValue> {
    fn get_bitmap(&self, key: &[u8]) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueReadOnlyTable<'txn, &[u8]>
    for redb::Table<'txn, &'static [u8], RoaringValue>
{
    fn get_bitmap(&self, key: &[u8]) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueTable<'txn, &[u8]> for redb::Table<'txn, &'static [u8], RoaringValue> {
    fn insert_member(&mut self, key: &[u8], member: u64) -> Result<()> {
        // Read existing value or create empty bitmap
        let existing_bitmap = self.get_bitmap(key)?;
        let mut bitmap = existing_bitmap;

        // Insert the new member
        bitmap.insert(member);

        // Store the updated bitmap
        let value = RoaringValue::from(bitmap);
        Self::insert(self, key, &value)?;

        Ok(())
    }

    fn remove_member(&mut self, key: &[u8], member: u64) -> Result<()> {
        // Read existing value
        let mut bitmap = self.get_bitmap(key)?;

        // Remove the member
        bitmap.remove(member);

        // Store the updated bitmap or remove if empty
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }

        Ok(())
    }

    fn replace_bitmap(&mut self, key: &[u8], bitmap: RoaringTreemap) -> Result<()> {
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }
        Ok(())
    }

    fn remove_key(&mut self, key: &[u8]) -> Result<()> {
        Self::remove(self, key)?;

        Ok(())
    }
}

// Implementation for string keys
impl RoaringValueReadOnlyTable<'_, &str> for redb::ReadOnlyTable<&'static str, RoaringValue> {
    fn get_bitmap(&self, key: &str) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueReadOnlyTable<'txn, &str> for redb::Table<'txn, &'static str, RoaringValue> {
    fn get_bitmap(&self, key: &str) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueTable<'txn, &str> for redb::Table<'txn, &'static str, RoaringValue> {
    fn insert_member(&mut self, key: &str, member: u64) -> Result<()> {
        // Read existing value or create empty bitmap
        let existing_bitmap = self.get_bitmap(key)?;
        let mut bitmap = existing_bitmap;

        // Insert the new member
        bitmap.insert(member);

        // Store the updated bitmap
        let value = RoaringValue::from(bitmap);
        Self::insert(self, key, &value)?;

        Ok(())
    }

    fn remove_member(&mut self, key: &str, member: u64) -> Result<()> {
        // Read existing value
        let mut bitmap = self.get_bitmap(key)?;

        // Remove the member
        bitmap.remove(member);

        // Store the updated bitmap or remove if empty
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }

        Ok(())
    }

    fn replace_bitmap(&mut self, key: &str, bitmap: RoaringTreemap) -> Result<()> {
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }
        Ok(())
    }

    fn remove_key(&mut self, key: &str) -> Result<()> {
        Self::remove(self, key)?;
        Ok(())
    }
}

// Implementation for u64 keys
impl RoaringValueReadOnlyTable<'_, u64> for redb::ReadOnlyTable<u64, RoaringValue> {
    fn get_bitmap(&self, key: u64) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueReadOnlyTable<'txn, u64> for redb::Table<'txn, u64, RoaringValue> {
    fn get_bitmap(&self, key: u64) -> Result<RoaringTreemap> {
        if let Some(guard) = self.get(key)? {
            Ok(guard.value().bitmap().to_owned())
        } else {
            Ok(RoaringTreemap::new())
        }
    }
}

impl<'txn> RoaringValueTable<'txn, u64> for redb::Table<'txn, u64, RoaringValue> {
    fn insert_member(&mut self, key: u64, member: u64) -> Result<()> {
        // Read existing value or create empty bitmap
        let existing_bitmap = self.get_bitmap(key)?;
        let mut bitmap = existing_bitmap;

        // Insert the new member
        bitmap.insert(member);

        // Store the updated bitmap
        let value = RoaringValue::from(bitmap);
        Self::insert(self, key, &value)?;

        Ok(())
    }

    fn remove_member(&mut self, key: u64, member: u64) -> Result<()> {
        // Read existing value
        let mut bitmap = self.get_bitmap(key)?;

        // Remove the member
        bitmap.remove(member);

        // Store the updated bitmap or remove if empty
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }

        Ok(())
    }

    fn replace_bitmap(&mut self, key: u64, bitmap: RoaringTreemap) -> Result<()> {
        if bitmap.is_empty() {
            Self::remove(self, key)?;
        } else {
            let value = RoaringValue::from(bitmap);
            Self::insert(self, key, &value)?;
        }
        Ok(())
    }

    fn remove_key(&mut self, key: u64) -> Result<()> {
        Self::remove(self, key)?;
        Ok(())
    }
}
