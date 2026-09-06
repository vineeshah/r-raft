//! The message vocabulary the Raft core consumes and emits.
//!
//! These are transport-agnostic Rust types. The `kvstore-server` crate converts
//! between them and the generated protobuf types; the simulation harness passes
//! them directly. Every message carries its sender and term so the receiver can
//! apply the "update term / step down" rule uniformly.

use crate::{LogEntry, LogIndex, NodeId, Term};

/// A message delivered to [`RaftNode::step`](crate::RaftNode::step).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Candidate soliciting a vote.
    RequestVote(RequestVote),
    /// Response to a vote request.
    RequestVoteResp(RequestVoteResp),
    /// Leader replicating entries (empty `entries` acts as a heartbeat).
    AppendEntries(AppendEntries),
    /// Response to an append.
    AppendEntriesResp(AppendEntriesResp),
}

impl Message {
    /// The sender of this message.
    pub fn from(&self) -> NodeId {
        match self {
            Message::RequestVote(m) => m.from,
            Message::RequestVoteResp(m) => m.from,
            Message::AppendEntries(m) => m.from,
            Message::AppendEntriesResp(m) => m.from,
        }
    }

    /// The term this message was sent in.
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote(m) => m.term,
            Message::RequestVoteResp(m) => m.term,
            Message::AppendEntries(m) => m.term,
            Message::AppendEntriesResp(m) => m.term,
        }
    }
}

/// `RequestVote` RPC arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    /// Candidate requesting the vote.
    pub from: NodeId,
    /// Candidate's term.
    pub term: Term,
    /// Index of the candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
}

/// `RequestVote` RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResp {
    /// Responding node.
    pub from: NodeId,
    /// Responder's current term (for the candidate to update itself).
    pub term: Term,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// `AppendEntries` RPC arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// The leader.
    pub from: NodeId,
    /// Leader's term.
    pub term: Term,
    /// Index of the log entry immediately preceding `entries`.
    pub prev_log_index: LogIndex,
    /// Term of the entry at `prev_log_index`.
    pub prev_log_term: Term,
    /// New entries to store (empty for a heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

/// `AppendEntries` RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResp {
    /// Responding follower.
    pub from: NodeId,
    /// Responder's current term.
    pub term: Term,
    /// Whether the append succeeded (log matched at `prev_log_index`).
    pub success: bool,
    /// On success, the follower's highest matching index (lets the leader
    /// advance `match_index` without recomputing).
    pub match_index: LogIndex,
    /// On failure, the first index of the conflicting term — a hint that lets
    /// the leader skip an entire term when backing up, rather than one entry
    /// per round trip.
    pub conflict_index: LogIndex,
}

/// A side effect the host runtime must carry out after stepping the core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Deliver `message` to node `to`.
    Send {
        /// Destination node.
        to: NodeId,
        /// Message to deliver.
        message: Message,
    },
    /// Apply these newly committed entries to the state machine, in order.
    Apply {
        /// Committed entries not yet applied.
        entries: Vec<LogEntry>,
    },
}
