//! From-scratch Raft consensus core.
//!
//! # Design: pure and deterministic
//!
//! This crate contains **no I/O, no networking, and no wall-clock time**. The
//! Raft node is a state machine driven entirely by:
//! - inbound [`Message`]s (RPC requests/responses, client proposals),
//! - logical [`tick`](RaftNode::tick)s that advance an internal logical clock.
//!
//! In response it emits [`Action`]s the host runtime must carry out (send a
//! message, persist state, apply a committed entry). Keeping the core pure is
//! what makes deterministic simulation testing possible: the entire cluster can
//! be single-stepped inside one process with an injected network and clock.
//!
//! Persistence is abstracted behind [`RaftStorage`]; the host provides an
//! implementation (see the `kvstore-storage` crate for the RocksDB-backed one).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// A node identifier within a Raft group.
pub type NodeId = u64;

/// A monotonically increasing Raft term.
pub type Term = u64;

/// A position in the replicated log (1-based; 0 means "before the first entry").
pub type LogIndex = u64;

/// The role a Raft node currently occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive; redirects clients and resets its election timer on heartbeats.
    Follower,
    /// Campaigning for leadership after an election timeout.
    Candidate,
    /// Serving client writes and replicating them to followers.
    Leader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_distinct() {
        assert_ne!(Role::Follower, Role::Leader);
        assert_ne!(Role::Candidate, Role::Leader);
    }
}
