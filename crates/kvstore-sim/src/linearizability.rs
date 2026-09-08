//! A from-scratch linearizability checker.
//!
//! Given a concurrent history of completed operations (each with a real-time
//! `[call, ret]` interval and its observed output), decide whether some
//! sequential ordering exists that (a) respects real-time precedence — if op A
//! returned before op B was called, A comes before B — and (b) matches every
//! operation's output against a sequential model of the register.
//!
//! The search is the Wing–Gong / Lowe algorithm: repeatedly try to linearize an
//! operation that could go next (its call precedes the earliest remaining
//! return), backtracking on dead ends, with a memo of failed `(remaining, state)`
//! configurations to prune the exponential search. Linearizability is
//! compositional, so we split the history per key and check each independently —
//! which keeps each search small.

use std::collections::{HashMap, HashSet};

use crate::kv::{KvOp, KvOutput};

/// One completed operation in the recorded history.
#[derive(Debug, Clone)]
pub struct Op {
    /// Logical time the client invoked the operation.
    pub call: u64,
    /// Logical time the client observed the response.
    pub ret: u64,
    /// What the client asked for.
    pub input: KvOp,
    /// What the client observed.
    pub output: KvOutput,
}

/// Check whether the full history is linearizable.
pub fn is_linearizable(history: &[Op]) -> bool {
    let mut by_key: HashMap<u64, Vec<Op>> = HashMap::new();
    for op in history {
        by_key.entry(op.input.key()).or_default().push(op.clone());
    }
    by_key.values().all(|ops| check_register(ops))
}

/// Apply `input` to a register holding `state`, returning the resulting state if
/// (and only if) `output` is what the sequential model would have produced.
fn model_apply(state: Option<u64>, input: &KvOp, output: &KvOutput) -> Option<Option<u64>> {
    match (input, output) {
        (KvOp::Get { .. }, KvOutput::Read(v)) if *v == state => Some(state),
        (KvOp::Put { value, .. }, KvOutput::Written) => Some(Some(*value)),
        (KvOp::Cas { expected, new, .. }, KvOutput::Cas(observed)) => {
            let success = state == *expected;
            if success == *observed {
                Some(if success { Some(*new) } else { state })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn check_register(ops: &[Op]) -> bool {
    let n = ops.len();
    // The bitmask memo assumes <= 63 ops per key; simulation workloads stay well
    // under this. Guard so a larger history fails loudly rather than silently.
    assert!(
        n <= 63,
        "per-key history too large for the bitmask checker: {n}"
    );
    if n == 0 {
        return true;
    }
    let full: u64 = (1u64 << n) - 1;
    let mut memo: HashSet<(u64, Option<u64>)> = HashSet::new();
    recurse(ops, full, None, &mut memo)
}

fn recurse(
    ops: &[Op],
    remaining: u64,
    state: Option<u64>,
    memo: &mut HashSet<(u64, Option<u64>)>,
) -> bool {
    if remaining == 0 {
        return true;
    }
    if !memo.insert((remaining, state)) {
        return false; // already proven a dead end
    }

    // Only ops whose call precedes the earliest remaining return may go next;
    // anything else is forced to come after some still-unlinearized op.
    let min_ret = (0..ops.len())
        .filter(|i| remaining & (1 << i) != 0)
        .map(|i| ops[i].ret)
        .min()
        .unwrap();

    for i in 0..ops.len() {
        if remaining & (1 << i) == 0 || ops[i].call > min_ret {
            continue;
        }
        if let Some(next_state) = model_apply(state, &ops[i].input, &ops[i].output) {
            if recurse(ops, remaining & !(1 << i), next_state, memo) {
                return true;
            }
        }
    }
    false
}
