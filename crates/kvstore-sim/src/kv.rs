//! The key-value operations the simulated clients issue, and an in-memory state
//! machine that applies them. Every operation carries a unique id so the
//! simulator can match a committed log entry back to the client call that
//! produced it, even across leader changes.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A client operation, uniquely tagged with `id`.
///
/// Reads go through the log too (as a [`KvOp::Get`] command), which gives every
/// operation a single, unambiguous linearization point when it is applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvCommand {
    /// Unique operation id (also the client-call correlation key).
    pub id: u64,
    /// The operation to perform.
    pub op: KvOp,
}

/// The operations the register supports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOp {
    /// Read the value at `key`.
    Get { key: u64 },
    /// Set `key` to `value`.
    Put { key: u64, value: u64 },
    /// Set `key` to `new` iff its current value equals `expected`
    /// (`None` = must be absent).
    Cas {
        key: u64,
        expected: Option<u64>,
        new: u64,
    },
}

impl KvOp {
    /// The key this operation touches (linearizability is checked per key).
    pub fn key(&self) -> u64 {
        match self {
            KvOp::Get { key } | KvOp::Put { key, .. } | KvOp::Cas { key, .. } => *key,
        }
    }
}

/// The observable result of applying an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvOutput {
    /// A read returned this value (`None` if the key was absent).
    Read(Option<u64>),
    /// A put completed.
    Written,
    /// A compare-and-swap completed; `true` if the swap took effect.
    Cas(bool),
}

/// A deterministic in-memory key-value state machine.
#[derive(Debug, Default, Clone)]
pub struct MemKv {
    map: HashMap<u64, u64>,
}

impl MemKv {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a command and return its observable output.
    pub fn apply(&mut self, cmd: &KvCommand) -> KvOutput {
        match &cmd.op {
            KvOp::Get { key } => KvOutput::Read(self.map.get(key).copied()),
            KvOp::Put { key, value } => {
                self.map.insert(*key, *value);
                KvOutput::Written
            }
            KvOp::Cas { key, expected, new } => {
                let current = self.map.get(key).copied();
                if current == *expected {
                    self.map.insert(*key, *new);
                    KvOutput::Cas(true)
                } else {
                    KvOutput::Cas(false)
                }
            }
        }
    }
}

/// Encode a command into log-entry bytes.
pub fn encode(cmd: &KvCommand) -> Vec<u8> {
    bincode::serialize(cmd).expect("command serializes")
}

/// Decode a command from log-entry bytes.
pub fn decode(bytes: &[u8]) -> KvCommand {
    bincode::deserialize(bytes).expect("command deserializes")
}
