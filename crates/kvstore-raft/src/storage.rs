//! Persistence contracts the Raft core depends on.
//!
//! The core itself performs no I/O; it drives these traits, whose concrete
//! implementations (RocksDB-backed) live in the `kvstore-storage` crate. Two
//! concerns are separated:
//!
//! - [`RaftStorage`] — the *replicated log* and Raft's own durable metadata
//!   ([`HardState`], snapshots). This is machinery internal to consensus.
//! - [`StateMachine`] — the *application* that committed log entries are applied
//!   to (here, the key-value store). Raft treats each command as opaque bytes.

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

/// A single entry in the replicated log.
///
/// `command` is opaque to Raft: it is handed to the [`StateMachine`] once the
/// entry is committed. `index` is 1-based and contiguous within a log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term in which the leader created this entry.
    pub term: Term,
    /// Position of this entry in the log (1-based).
    pub index: LogIndex,
    /// Application command, opaque to the consensus layer.
    pub command: Vec<u8>,
}

/// The subset of Raft state that MUST survive a crash to preserve safety.
///
/// Losing this risks double-voting in a term or forgetting a granted vote,
/// either of which can violate election safety. It must be flushed to stable
/// storage before responding to the RPC that changed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    /// Latest term the server has seen.
    pub current_term: Term,
    /// Candidate that received this server's vote in the current term, if any.
    pub voted_for: Option<NodeId>,
    /// Highest log index known to be committed.
    pub commit_index: LogIndex,
}

/// A point-in-time snapshot of the state machine plus the log position it covers.
///
/// Snapshots let a node discard log entries up to `last_included_index` and let
/// a lagging follower catch up without replaying the entire history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The log index this snapshot replaces (inclusive).
    pub last_included_index: LogIndex,
    /// The term of `last_included_index`.
    pub last_included_term: Term,
    /// Serialized state-machine contents.
    pub data: Vec<u8>,
}

/// Durable storage for the Raft log and consensus metadata.
///
/// Implementations own crash-consistency: after any method returns `Ok`, the
/// effect must survive a process restart. Indexing convention:
/// - the first real entry is at index 1;
/// - index 0 is the sentinel "before the log begins";
/// - after compaction, [`first_index`](RaftStorage::first_index) advances past
///   the snapshot and lower entries are no longer retrievable.
pub trait RaftStorage {
    /// Error type surfaced by storage operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist the crash-critical [`HardState`].
    fn save_hard_state(&mut self, state: &HardState) -> Result<(), Self::Error>;

    /// Load the persisted [`HardState`], or the default on a fresh store.
    fn hard_state(&self) -> Result<HardState, Self::Error>;

    /// Append entries to the end of the log.
    ///
    /// Callers guarantee `entries` are contiguous and begin at
    /// `last_index() + 1`; conflicting suffixes are removed via
    /// [`truncate_suffix`](RaftStorage::truncate_suffix) first.
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Self::Error>;

    /// Fetch entries in the half-open range `[low, high)`.
    fn entries(&self, low: LogIndex, high: LogIndex) -> Result<Vec<LogEntry>, Self::Error>;

    /// Fetch a single entry by index, if present.
    fn entry(&self, index: LogIndex) -> Result<Option<LogEntry>, Self::Error>;

    /// The term of the entry at `index`, if known (covers the snapshot boundary).
    fn term(&self, index: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Index of the first retrievable entry (1 on a fresh log; higher after
    /// compaction).
    fn first_index(&self) -> Result<LogIndex, Self::Error>;

    /// Index of the last entry (0 if the log is empty).
    fn last_index(&self) -> Result<LogIndex, Self::Error>;

    /// Remove all entries with index `>= from` (used to resolve log conflicts).
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error>;

    /// Remove all entries with index `<= upto` (compaction after a snapshot).
    fn truncate_prefix(&mut self, upto: LogIndex) -> Result<(), Self::Error>;

    /// Persist a snapshot.
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error>;

    /// Load the most recent snapshot, if any.
    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error>;
}

/// The application whose state is replicated via the Raft log.
///
/// Raft calls [`apply`](StateMachine::apply) exactly once per committed entry,
/// in log order. Implementations must be deterministic: the same command
/// sequence must yield the same state on every replica.
pub trait StateMachine {
    /// Error type surfaced by state-machine operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Apply one committed command and return an opaque result for the client.
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Serialize the full current state for snapshotting.
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error>;

    /// Replace current state with a previously produced snapshot.
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error>;
}
