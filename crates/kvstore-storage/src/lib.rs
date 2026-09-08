//! Durable storage layer for the KV store.
//!
//! Provides the persistence implementations the Raft core depends on:
//! - `RaftStorage`: durable log, hard state, and snapshots.
//! - `StateMachine`: applies committed commands to the user-visible key space.
//!
//! Both are backed by RocksDB (added in Phase 1) using separate column families
//! for the Raft log, metadata, and application data.

#![forbid(unsafe_code)]

/// Placeholder marker until the RocksDB-backed engine lands in Phase 1.
pub const ENGINE: &str = "rocksdb";

#[cfg(test)]
mod tests {
    #[test]
    fn engine_is_rocksdb() {
        assert_eq!(super::ENGINE, "rocksdb");
    }
}
