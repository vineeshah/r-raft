//! Deterministic simulation testing and linearizability checking for the Raft
//! core.
//!
//! Because [`kvstore_raft`] is pure — a node is driven only by messages and
//! logical ticks — an entire cluster can run inside a single deterministic
//! process. This crate provides that: a virtual [`net`]work with fault injection
//! (partitions, drops, delays, crashes), a [`sim`]ulator that drives the nodes
//! and records history + safety invariants, and a from-scratch
//! [`linearizability`] checker to prove the recorded history is consistent.
//!
//! The payoff is reproducibility: a failure found at seed *N* replays exactly at
//! seed *N*, so bugs that surface only under a specific interleaving of a
//! partition and an election are debuggable instead of flaky.

#![forbid(unsafe_code)]

pub mod kv;
pub mod linearizability;
pub mod net;
pub mod rng;
pub mod sim;

pub use kv::{KvOp, KvOutput};
pub use linearizability::{is_linearizable, Op};
pub use sim::{SimConfig, Simulator};
