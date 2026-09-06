//! In-memory [`RaftStorage`] for tests and deterministic simulation.
//!
//! Behaviorally identical to the RocksDB engine but with no durability, so the
//! Raft core can be exercised without touching disk. Kept in the library (not
//! behind `#[cfg(test)]`) so the simulation crate can reuse it.

use std::convert::Infallible;

use crate::storage::{HardState, LogEntry, RaftStorage, Snapshot};
use crate::{LogIndex, Term};

/// A volatile, contiguous-log storage backend.
#[derive(Debug, Default)]
pub struct MemStorage {
    hard: HardState,
    /// Entries with indices `base+1 ..= base+log.len()`.
    log: Vec<LogEntry>,
    /// Index of the entry just before `log[0]` (advances on prefix truncation).
    base: LogIndex,
    snapshot: Option<Snapshot>,
}

impl MemStorage {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.base {
            return None;
        }
        self.log.get((index - self.base - 1) as usize)
    }
}

impl RaftStorage for MemStorage {
    type Error = Infallible;

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), Self::Error> {
        self.hard = *state;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        Ok(self.hard)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Self::Error> {
        self.log.extend_from_slice(entries);
        Ok(())
    }

    fn entries(&self, low: LogIndex, high: LogIndex) -> Result<Vec<LogEntry>, Self::Error> {
        Ok((low..high).filter_map(|i| self.get(i).cloned()).collect())
    }

    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry>, Self::Error> {
        Ok(self.get(index).cloned())
    }

    fn term(&self, index: LogIndex) -> Result<Option<Term>, Self::Error> {
        if index == 0 {
            return Ok(Some(0));
        }
        if let Some(s) = &self.snapshot {
            if index == s.last_included_index {
                return Ok(Some(s.last_included_term));
            }
        }
        Ok(self.get(index).map(|e| e.term))
    }

    fn first_index(&self) -> Result<LogIndex, Self::Error> {
        if let Some(s) = &self.snapshot {
            return Ok(s.last_included_index + 1);
        }
        Ok(self.log.first().map(|e| e.index).unwrap_or(1))
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.base + self.log.len() as LogIndex)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        if from > self.base {
            self.log.truncate((from - self.base - 1) as usize);
        }
        Ok(())
    }

    fn truncate_prefix(&mut self, upto: LogIndex) -> Result<(), Self::Error> {
        if upto > self.base {
            let drop = (upto - self.base) as usize;
            let drop = drop.min(self.log.len());
            self.log.drain(0..drop);
            self.base = upto;
        }
        Ok(())
    }

    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(self.snapshot.clone())
    }
}
