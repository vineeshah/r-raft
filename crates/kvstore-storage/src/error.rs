//! Storage error type shared by the RocksDB engine and state machine.

use thiserror::Error;

/// Errors produced by the durable storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// A RocksDB operation failed.
    #[error("rocksdb: {0}")]
    Rocks(#[from] rocksdb::Error),

    /// (De)serialization of a stored value failed.
    #[error("encoding: {0}")]
    Encoding(#[from] bincode::Error),

    /// A required column family was missing from the database handle.
    #[error("missing column family: {0}")]
    MissingColumnFamily(&'static str),

    /// A command payload could not be interpreted by the state machine.
    #[error("invalid command: {0}")]
    InvalidCommand(String),
}
