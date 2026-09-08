//! Integration tests for the RocksDB storage engine.
//!
//! These focus on the two properties that matter most for a consensus store:
//! entries and metadata are read back exactly as written, and they survive a
//! process restart (modeled here by dropping and reopening the engine).

use kvstore_raft::{HardState, LogEntry, RaftStorage, Snapshot, StateMachine};
use kvstore_storage::{Command, CommandResult, RocksEngine};
use tempfile::TempDir;

fn entry(term: u64, index: u64, cmd: &[u8]) -> LogEntry {
    LogEntry {
        term,
        index,
        command: cmd.to_vec(),
    }
}

#[test]
fn appends_and_reads_back_entries() {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut store = engine.raft_storage();

    assert_eq!(store.last_index().unwrap(), 0);
    assert_eq!(store.first_index().unwrap(), 1);

    store
        .append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")])
        .unwrap();

    assert_eq!(store.last_index().unwrap(), 3);
    assert_eq!(store.entry(2).unwrap().unwrap().command, b"b");
    assert_eq!(store.term(3).unwrap(), Some(2));
    let range = store.entries(1, 3).unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].index, 1);
    assert_eq!(range[1].index, 2);
}

#[test]
fn hard_state_survives_restart() {
    let dir = TempDir::new().unwrap();
    let hs = HardState {
        current_term: 7,
        voted_for: Some(3),
        commit_index: 5,
    };
    {
        let engine = RocksEngine::open(dir.path()).unwrap();
        let mut store = engine.raft_storage();
        store.save_hard_state(&hs).unwrap();
    }
    // Reopen — simulates a crash + restart.
    let engine = RocksEngine::open(dir.path()).unwrap();
    let store = engine.raft_storage();
    assert_eq!(store.hard_state().unwrap(), hs);
}

#[test]
fn log_survives_restart() {
    let dir = TempDir::new().unwrap();
    {
        let engine = RocksEngine::open(dir.path()).unwrap();
        let mut store = engine.raft_storage();
        store
            .append(&[entry(1, 1, b"x"), entry(1, 2, b"y")])
            .unwrap();
    }
    let engine = RocksEngine::open(dir.path()).unwrap();
    let store = engine.raft_storage();
    assert_eq!(store.last_index().unwrap(), 2);
    assert_eq!(store.entry(1).unwrap().unwrap().command, b"x");
    assert_eq!(store.entry(2).unwrap().unwrap().command, b"y");
}

#[test]
fn truncate_suffix_removes_conflicting_tail() {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut store = engine.raft_storage();
    store
        .append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(1, 3, b"c")])
        .unwrap();

    store.truncate_suffix(2).unwrap();

    assert_eq!(store.last_index().unwrap(), 1);
    assert!(store.entry(2).unwrap().is_none());
    assert!(store.entry(3).unwrap().is_none());
}

#[test]
fn snapshot_compacts_prefix_and_reports_indices() {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut store = engine.raft_storage();
    store
        .append(&[entry(1, 1, b"a"), entry(1, 2, b"b"), entry(2, 3, b"c")])
        .unwrap();

    let snap = Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: b"state".to_vec(),
    };
    store.save_snapshot(&snap).unwrap();
    store.truncate_prefix(2).unwrap();

    assert_eq!(store.first_index().unwrap(), 3);
    assert_eq!(store.last_index().unwrap(), 3);
    // Term at the snapshot boundary comes from the snapshot, not the log.
    assert_eq!(store.term(2).unwrap(), Some(1));
    assert_eq!(store.snapshot().unwrap().unwrap(), snap);
}

#[test]
fn state_machine_applies_put_delete_and_cas() {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut sm = engine.state_machine();

    let put = Command::Put {
        key: b"k".to_vec(),
        value: b"v1".to_vec(),
    };
    assert_eq!(
        CommandResult::decode(&sm.apply(&put.encode().unwrap()).unwrap()).unwrap(),
        CommandResult::Ok
    );
    assert_eq!(sm.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));

    // CAS with wrong expectation fails and leaves the value untouched.
    let bad_cas = Command::CompareAndSwap {
        key: b"k".to_vec(),
        expected: Some(b"wrong".to_vec()),
        new_value: b"v2".to_vec(),
    };
    assert_eq!(
        CommandResult::decode(&sm.apply(&bad_cas.encode().unwrap()).unwrap()).unwrap(),
        CommandResult::Swapped(false)
    );
    assert_eq!(sm.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));

    // CAS with the right expectation succeeds.
    let good_cas = Command::CompareAndSwap {
        key: b"k".to_vec(),
        expected: Some(b"v1".to_vec()),
        new_value: b"v2".to_vec(),
    };
    assert_eq!(
        CommandResult::decode(&sm.apply(&good_cas.encode().unwrap()).unwrap()).unwrap(),
        CommandResult::Swapped(true)
    );
    assert_eq!(sm.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));

    // Delete removes the key.
    let del = Command::Delete { key: b"k".to_vec() };
    sm.apply(&del.encode().unwrap()).unwrap();
    assert_eq!(sm.get(b"k").unwrap(), None);
}

#[test]
fn state_machine_snapshot_roundtrips() {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut sm = engine.state_machine();

    for (k, v) in [(&b"a"[..], &b"1"[..]), (b"b", b"2"), (b"c", b"3")] {
        let put = Command::Put {
            key: k.to_vec(),
            value: v.to_vec(),
        };
        sm.apply(&put.encode().unwrap()).unwrap();
    }
    let snap = sm.snapshot().unwrap();

    // Mutate away from the snapshot, then restore and confirm we're back.
    sm.apply(&Command::Delete { key: b"a".to_vec() }.encode().unwrap())
        .unwrap();
    assert_eq!(sm.get(b"a").unwrap(), None);

    sm.restore(&snap).unwrap();
    assert_eq!(sm.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(sm.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(sm.get(b"c").unwrap().as_deref(), Some(&b"3"[..]));
}
