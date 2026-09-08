//! Property test: the log is a faithful, ordered store.
//!
//! For any contiguous run of appended entries, reading `[1, last+1)` back must
//! return exactly what was written, in order — the invariant the Raft log
//! matching property is built on.

use kvstore_raft::{LogEntry, RaftStorage};
use kvstore_storage::RocksEngine;
use proptest::prelude::*;
use tempfile::TempDir;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn append_then_read_all_roundtrips(
        terms in prop::collection::vec(1u64..5, 1..40),
        payload in prop::collection::vec(any::<u8>(), 0..8),
    ) {
        let dir = TempDir::new().unwrap();
        let engine = RocksEngine::open(dir.path()).unwrap();
        let mut store = engine.raft_storage();

        let mut entries = Vec::new();
        for (i, term) in terms.iter().enumerate() {
            entries.push(LogEntry {
                term: *term,
                index: (i as u64) + 1,
                command: payload.clone(),
            });
        }
        store.append(&entries).unwrap();

        prop_assert_eq!(store.last_index().unwrap(), entries.len() as u64);
        let read = store.entries(1, entries.len() as u64 + 1).unwrap();
        prop_assert_eq!(read, entries);
    }
}
