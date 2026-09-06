//! RocksDB-backed durable storage.
//!
//! A single RocksDB database per node holds three column families:
//! - `raft_log` — the replicated log, keyed by big-endian log index;
//! - `meta` — Raft's [`HardState`] and the latest [`Snapshot`];
//! - `data` — the key-value state machine's user data.
//!
//! One database (and one write-ahead log) backs both the Raft log and the state
//! machine, so a shared [`Arc<Db>`] is handed to both [`RocksRaftStorage`] and
//! [`KvStateMachine`].

use std::path::Path;
use std::sync::Arc;

use kvstore_raft::{HardState, LogEntry, LogIndex, RaftStorage, Snapshot, StateMachine, Term};
use rocksdb::{DBWithThreadMode, MultiThreaded, Options, WriteBatch, WriteOptions};

use crate::command::{Command, CommandResult};
use crate::error::StorageError;

/// Multi-threaded RocksDB handle: `cf_handle` returns an `Arc<BoundColumnFamily>`
/// that can be held across the shared `Arc<Db>` without borrowing the database.
type Db = DBWithThreadMode<MultiThreaded>;

const CF_LOG: &str = "raft_log";
const CF_META: &str = "meta";
const CF_DATA: &str = "data";

const KEY_HARD_STATE: &[u8] = b"hard_state";
const KEY_SNAPSHOT: &[u8] = b"snapshot";

fn encode_index(index: LogIndex) -> [u8; 8] {
    index.to_be_bytes()
}

fn decode_index(bytes: &[u8]) -> Result<LogIndex, StorageError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::InvalidCommand("malformed log key".into()))?;
    Ok(LogIndex::from_be_bytes(arr))
}

/// Opens the shared RocksDB database and vends the Raft and state-machine views.
pub struct RocksEngine {
    db: Arc<Db>,
}

impl RocksEngine {
    /// Open (creating if needed) the database rooted at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = Db::open_cf(&opts, path, [CF_LOG, CF_META, CF_DATA])?;
        Ok(Self { db: Arc::new(db) })
    }

    /// A [`RaftStorage`] view over the log and Raft metadata.
    pub fn raft_storage(&self) -> RocksRaftStorage {
        RocksRaftStorage {
            db: Arc::clone(&self.db),
        }
    }

    /// A [`StateMachine`] view over the application data.
    pub fn state_machine(&self) -> KvStateMachine {
        KvStateMachine {
            db: Arc::clone(&self.db),
        }
    }
}

/// RocksDB-backed Raft log and consensus metadata.
pub struct RocksRaftStorage {
    db: Arc<Db>,
}

impl RocksRaftStorage {
    fn cf(&self, name: &'static str) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(name)
            .ok_or(StorageError::MissingColumnFamily(name))
    }

    /// Sync writes for safety-critical metadata (hard state, snapshots).
    fn sync_opts() -> WriteOptions {
        let mut w = WriteOptions::default();
        w.set_sync(true);
        w
    }
}

impl RaftStorage for RocksRaftStorage {
    type Error = StorageError;

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), Self::Error> {
        let cf = self.cf(CF_META)?;
        let bytes = bincode::serialize(state)?;
        self.db
            .put_cf_opt(&cf, KEY_HARD_STATE, bytes, &Self::sync_opts())?;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        let cf = self.cf(CF_META)?;
        match self.db.get_cf(&cf, KEY_HARD_STATE)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(HardState::default()),
        }
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Self::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let cf = self.cf(CF_LOG)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            batch.put_cf(&cf, encode_index(entry.index), bincode::serialize(entry)?);
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn entries(&self, low: LogIndex, high: LogIndex) -> Result<Vec<LogEntry>, Self::Error> {
        let cf = self.cf(CF_LOG)?;
        let mut out = Vec::new();
        for index in low..high {
            if let Some(bytes) = self.db.get_cf(&cf, encode_index(index))? {
                out.push(bincode::deserialize(&bytes)?);
            }
        }
        Ok(out)
    }

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry>, Self::Error> {
        let cf = self.cf(CF_LOG)?;
        match self.db.get_cf(&cf, encode_index(index))? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        if index == 0 {
            return Ok(Some(0));
        }
        if let Some(snapshot) = self.snapshot()? {
            if index == snapshot.last_included_index {
                return Ok(Some(snapshot.last_included_term));
            }
        }
        Ok(self.entry(index)?.map(|e| e.term))
    }

    fn first_index(&self) -> Result<LogIndex, Self::Error> {
        if let Some(snapshot) = self.snapshot()? {
            return Ok(snapshot.last_included_index + 1);
        }
        let cf = self.cf(CF_LOG)?;
        let mut it = self.db.raw_iterator_cf(&cf);
        it.seek_to_first();
        match it.key() {
            Some(key) => decode_index(key),
            None => Ok(1),
        }
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        let cf = self.cf(CF_LOG)?;
        let mut it = self.db.raw_iterator_cf(&cf);
        it.seek_to_last();
        match it.key() {
            Some(key) => decode_index(key),
            None => Ok(self.snapshot()?.map(|s| s.last_included_index).unwrap_or(0)),
        }
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        let cf = self.cf(CF_LOG)?;
        let last = self.last_index()?;
        let mut batch = WriteBatch::default();
        for index in from..=last {
            batch.delete_cf(&cf, encode_index(index));
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn truncate_prefix(&mut self, upto: LogIndex) -> Result<(), Self::Error> {
        let cf = self.cf(CF_LOG)?;
        let first = self.first_index()?;
        let mut batch = WriteBatch::default();
        for index in first..=upto {
            batch.delete_cf(&cf, encode_index(index));
        }
        self.db.write(batch)?;
        Ok(())
    }

    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        let cf = self.cf(CF_META)?;
        let bytes = bincode::serialize(snapshot)?;
        self.db
            .put_cf_opt(&cf, KEY_SNAPSHOT, bytes, &Self::sync_opts())?;
        Ok(())
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        let cf = self.cf(CF_META)?;
        match self.db.get_cf(&cf, KEY_SNAPSHOT)? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }
}

/// RocksDB-backed key-value state machine.
pub struct KvStateMachine {
    db: Arc<Db>,
}

impl KvStateMachine {
    fn cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(CF_DATA)
            .ok_or(StorageError::MissingColumnFamily(CF_DATA))
    }

    /// Read the current value for `key` (a local, non-linearizable read).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let cf = self.cf()?;
        Ok(self.db.get_cf(&cf, key)?)
    }
}

impl StateMachine for KvStateMachine {
    type Error = StorageError;

    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let cf = self.cf()?;
        let result = match Command::decode(command)? {
            Command::Put { key, value } => {
                self.db.put_cf(&cf, &key, &value)?;
                CommandResult::Ok
            }
            Command::Delete { key } => {
                self.db.delete_cf(&cf, &key)?;
                CommandResult::Ok
            }
            Command::CompareAndSwap {
                key,
                expected,
                new_value,
            } => {
                let current = self.db.get_cf(&cf, &key)?;
                let matches = current.as_deref() == expected.as_deref();
                if matches {
                    self.db.put_cf(&cf, &key, &new_value)?;
                }
                CommandResult::Swapped(matches)
            }
        };
        result.encode()
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        let cf = self.cf()?;
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
            let (k, v) = item?;
            pairs.push((k.to_vec(), v.to_vec()));
        }
        Ok(bincode::serialize(&pairs)?)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        let cf = self.cf()?;
        // Clear existing data, then load the snapshot's pairs.
        let mut batch = WriteBatch::default();
        for item in self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
            let (k, _) = item?;
            batch.delete_cf(&cf, k);
        }
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(snapshot)?;
        for (k, v) in pairs {
            batch.put_cf(&cf, k, v);
        }
        self.db.write(batch)?;
        Ok(())
    }
}
