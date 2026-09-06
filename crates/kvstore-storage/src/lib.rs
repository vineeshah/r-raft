//! Durable storage layer for the KV store.
//!
//! Provides the persistence implementations the Raft core depends on, backed by
//! a single RocksDB database per node:
//! - [`RocksRaftStorage`] implements [`kvstore_raft::RaftStorage`] — the
//!   replicated log, hard state, and snapshots.
//! - [`KvStateMachine`] implements [`kvstore_raft::StateMachine`] — committed
//!   [`Command`]s applied to the user-visible key space.
//!
//! Open the database with [`RocksEngine::open`], then take the two views.

#![forbid(unsafe_code)]

pub mod command;
pub mod engine;
pub mod error;

pub use command::{Command, CommandResult};
pub use engine::{KvStateMachine, RocksEngine, RocksRaftStorage};
pub use error::StorageError;
