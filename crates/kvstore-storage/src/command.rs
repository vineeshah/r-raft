//! The key-value command set replicated through the Raft log.
//!
//! A leader serializes a [`Command`] into a log entry's opaque `command` bytes;
//! every replica deserializes and applies it deterministically once the entry
//! commits, so all replicas converge to identical state.

use serde::{Deserialize, Serialize};

use crate::error::StorageError;

/// A mutation applied to the key-value state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Unconditionally set `key` to `value`.
    Put {
        /// Key to write.
        key: Vec<u8>,
        /// Value to store.
        value: Vec<u8>,
    },
    /// Remove `key` if present.
    Delete {
        /// Key to remove.
        key: Vec<u8>,
    },
    /// Set `key` to `new_value` only if its current value equals `expected`
    /// (`None` meaning "the key must not exist").
    CompareAndSwap {
        /// Key to conditionally write.
        key: Vec<u8>,
        /// Required current value, or `None` to require absence.
        expected: Option<Vec<u8>>,
        /// Value to set on a successful compare.
        new_value: Vec<u8>,
    },
}

impl Command {
    /// Encode the command into log-entry bytes.
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        Ok(bincode::serialize(self)?)
    }

    /// Decode a command from log-entry bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

/// Outcome returned to the client after a command is applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    /// A `Put` or `Delete` completed.
    Ok,
    /// A `CompareAndSwap` completed; `true` if the swap took effect.
    Swapped(bool),
}

impl CommandResult {
    /// Encode the result into the opaque bytes returned by `StateMachine::apply`.
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        Ok(bincode::serialize(self)?)
    }

    /// Decode a result from `StateMachine::apply` output bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        Ok(bincode::deserialize(bytes)?)
    }
}
