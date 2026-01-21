//! Database copy utilities for redb.
//!
//! This module provides helpers to copy data between databases using
//! explicit table definitions supplied by callers.

use crate::Result;
use redb::{
    Database, MultimapTableDefinition, MultimapTableHandle, ReadTransaction, ReadableDatabase,
    ReadableMultimapTable, ReadableTable, TableDefinition, TableError, TableHandle,
    WriteTransaction,
};
use std::fmt;
use std::marker::PhantomData;

#[cfg(test)]
mod tests;

/// Errors returned by database copy operations.
#[derive(Debug)]
pub enum DbCopyError {
    /// One or more destination tables already exist.
    DestinationTablesExist(Vec<String>),

    /// Failed to check destination tables.
    DestinationCheckFailed(String),

    /// Failed to open a source table.
    SourceTableOpenFailed(String),

    /// Failed to open a destination table.
    DestinationTableOpenFailed(String),

    /// Failed while copying table contents.
    TableCopyFailed(String),

    /// Transaction failures during copy.
    TransactionFailed(String),

    /// Failed to commit the destination transaction.
    CommitFailed(String),
}

impl std::error::Error for DbCopyError {}

impl fmt::Display for DbCopyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbCopyError::DestinationTablesExist(names) => {
                write!(f, "Destination already contains: {}", names.join(", "))
            }
            DbCopyError::DestinationCheckFailed(msg) => {
                write!(f, "Destination check failed: {}", msg)
            }
            DbCopyError::SourceTableOpenFailed(msg) => {
                write!(f, "Source table open failed: {}", msg)
            }
            DbCopyError::DestinationTableOpenFailed(msg) => {
                write!(f, "Destination table open failed: {}", msg)
            }
            DbCopyError::TableCopyFailed(msg) => write!(f, "Table copy failed: {}", msg),
            DbCopyError::TransactionFailed(msg) => write!(f, "Transaction failed: {}", msg),
            DbCopyError::CommitFailed(msg) => write!(f, "Commit failed: {}", msg),
        }
    }
}

enum CopyKind {
    Table,
    Multimap,
}

impl fmt::Display for CopyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyKind::Table => write!(f, "table"),
            CopyKind::Multimap => write!(f, "multimap table"),
        }
    }
}

trait CopyStep {
    fn name(&self) -> &str;
    fn kind(&self) -> CopyKind;
    fn preflight(&self, destination: &ReadTransaction) -> std::result::Result<bool, TableError>;
    fn copy(
        &self,
        source: &ReadTransaction,
        destination: &mut WriteTransaction,
    ) -> std::result::Result<(), DbCopyError>;

    fn display_name(&self) -> String {
        format!("{} {}", self.kind(), self.name())
    }
}

/// Builder for a database copy plan.
#[derive(Default)]
pub struct CopyPlan {
    steps: Vec<Box<dyn CopyStep>>,
}

impl CopyPlan {
    /// Create a new empty copy plan.
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Add a normal table to the copy plan.
    pub fn table<K: redb::Key + 'static, V: redb::Value + 'static>(
        mut self,
        table: TableDefinition<'_, K, V>,
    ) -> Self {
        self.steps.push(Box::new(TablePlan::new(table)));
        self
    }

    /// Add a multimap table to the copy plan.
    pub fn multimap<K: redb::Key + 'static, V: redb::Key + 'static>(
        mut self,
        table: MultimapTableDefinition<'_, K, V>,
    ) -> Self {
        self.steps.push(Box::new(MultimapPlan::new(table)));
        self
    }
}

/// Copy all tables described by `plan` from `source` to `destination`.
pub fn copy_database(source: &Database, destination: &Database, plan: &CopyPlan) -> Result<()> {
    let source_read = source
        .begin_read()
        .map_err(|err| DbCopyError::TransactionFailed(format!("source read: {}", err)))?;
    let destination_read = destination
        .begin_read()
        .map_err(|err| DbCopyError::TransactionFailed(format!("destination read: {}", err)))?;

    let mut conflicts = Vec::new();
    for step in &plan.steps {
        match step.preflight(&destination_read) {
            Ok(true) => conflicts.push(step.display_name()),
            Ok(false) => {}
            Err(err) => {
                return Err(DbCopyError::DestinationCheckFailed(format!(
                    "{}: {}",
                    step.display_name(),
                    err
                ))
                .into())
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(DbCopyError::DestinationTablesExist(conflicts).into());
    }

    drop(destination_read);

    let mut destination_write = destination
        .begin_write()
        .map_err(|err| DbCopyError::TransactionFailed(format!("destination write: {}", err)))?;

    for step in &plan.steps {
        step.copy(&source_read, &mut destination_write)?;
    }

    destination_write
        .commit()
        .map_err(|err| DbCopyError::CommitFailed(err.to_string()))?;

    Ok(())
}

struct TablePlan<K: redb::Key + 'static, V: redb::Value + 'static> {
    name: String,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K: redb::Key + 'static, V: redb::Value + 'static> TablePlan<K, V> {
    fn new(table: TableDefinition<'_, K, V>) -> Self {
        Self {
            name: table.name().to_string(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn definition(&self) -> TableDefinition<'_, K, V> {
        TableDefinition::new(self.name.as_str())
    }
}

impl<K: redb::Key + 'static, V: redb::Value + 'static> CopyStep for TablePlan<K, V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> CopyKind {
        CopyKind::Table
    }

    fn preflight(&self, destination: &ReadTransaction) -> std::result::Result<bool, TableError> {
        match destination.open_table(self.definition()) {
            Ok(_) => Ok(true),
            Err(TableError::TableDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn copy(
        &self,
        source: &ReadTransaction,
        destination: &mut WriteTransaction,
    ) -> std::result::Result<(), DbCopyError> {
        let source_table = source.open_table(self.definition()).map_err(|err| {
            DbCopyError::SourceTableOpenFailed(format!("{}: {}", self.display_name(), err))
        })?;
        let mut destination_table = destination.open_table(self.definition()).map_err(|err| {
            DbCopyError::DestinationTableOpenFailed(format!("{}: {}", self.display_name(), err))
        })?;
        let iter = source_table.iter().map_err(|err| {
            DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
        })?;

        for entry in iter {
            let (key, value) = entry.map_err(|err| {
                DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
            })?;
            destination_table
                .insert(key.value(), value.value())
                .map_err(|err| {
                    DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
                })?;
        }

        Ok(())
    }
}

struct MultimapPlan<K: redb::Key + 'static, V: redb::Key + 'static> {
    name: String,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K: redb::Key + 'static, V: redb::Key + 'static> MultimapPlan<K, V> {
    fn new(table: MultimapTableDefinition<'_, K, V>) -> Self {
        Self {
            name: table.name().to_string(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn definition(&self) -> MultimapTableDefinition<'_, K, V> {
        MultimapTableDefinition::new(self.name.as_str())
    }
}

impl<K: redb::Key + 'static, V: redb::Key + 'static> CopyStep for MultimapPlan<K, V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> CopyKind {
        CopyKind::Multimap
    }

    fn preflight(&self, destination: &ReadTransaction) -> std::result::Result<bool, TableError> {
        match destination.open_multimap_table(self.definition()) {
            Ok(_) => Ok(true),
            Err(TableError::TableDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn copy(
        &self,
        source: &ReadTransaction,
        destination: &mut WriteTransaction,
    ) -> std::result::Result<(), DbCopyError> {
        let source_table = source
            .open_multimap_table(self.definition())
            .map_err(|err| {
                DbCopyError::SourceTableOpenFailed(format!("{}: {}", self.display_name(), err))
            })?;
        let mut destination_table =
            destination
                .open_multimap_table(self.definition())
                .map_err(|err| {
                    DbCopyError::DestinationTableOpenFailed(format!(
                        "{}: {}",
                        self.display_name(),
                        err
                    ))
                })?;
        let iter = source_table.iter().map_err(|err| {
            DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
        })?;

        for entry in iter {
            let (key, values) = entry.map_err(|err| {
                DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
            })?;
            for value in values {
                let value = value.map_err(|err| {
                    DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
                })?;
                destination_table
                    .insert(key.value(), value.value())
                    .map_err(|err| {
                        DbCopyError::TableCopyFailed(format!("{}: {}", self.display_name(), err))
                    })?;
            }
        }

        Ok(())
    }
}
