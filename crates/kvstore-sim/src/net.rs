//! A virtual, deterministic network with fault injection.
//!
//! Messages are delivered after a randomized delay (reorders them), may be
//! dropped, and are blocked between nodes in different partition groups or
//! to/from a crashed node. All randomness comes from the shared seeded [`Rng`],
//! so a scenario replays identically every run.

use std::collections::{HashMap, HashSet};

use kvstore_raft::{Message, NodeId};

use crate::rng::Rng;

struct Packet {
    deliver_at: u64,
    from: NodeId,
    to: NodeId,
    msg: Message,
}

/// The simulated network fabric.
pub struct Network {
    in_flight: Vec<Packet>,
    /// Partition group per node; nodes in different groups cannot communicate.
    group: HashMap<NodeId, u8>,
    /// Crashed (paused) nodes: unreachable in both directions.
    down: HashSet<NodeId>,
    max_delay: u64,
    drop_prob: f64,
}

impl Network {
    /// Create a healthy network: all nodes in group 0, none down.
    pub fn new(ids: &[NodeId], max_delay: u64, drop_prob: f64) -> Self {
        Self {
            in_flight: Vec::new(),
            group: ids.iter().map(|&id| (id, 0u8)).collect(),
            down: HashSet::new(),
            max_delay: max_delay.max(1),
            drop_prob,
        }
    }

    fn reachable(&self, a: NodeId, b: NodeId) -> bool {
        !self.down.contains(&a)
            && !self.down.contains(&b)
            && self.group.get(&a) == self.group.get(&b)
    }

    /// Whether a node is currently crashed.
    pub fn is_down(&self, id: NodeId) -> bool {
        self.down.contains(&id)
    }

    /// Enqueue a message, subject to drop and reachability.
    pub fn send(&mut self, from: NodeId, to: NodeId, msg: Message, now: u64, rng: &mut Rng) {
        if !self.reachable(from, to) || rng.chance(self.drop_prob) {
            return;
        }
        let delay = 1 + rng.below(self.max_delay);
        self.in_flight.push(Packet {
            deliver_at: now + delay,
            from,
            to,
            msg,
        });
    }

    /// Remove and return all messages due by `now` that are still deliverable
    /// (a partition that formed after sending drops the in-flight message).
    pub fn take_ready(&mut self, now: u64) -> Vec<(NodeId, Message)> {
        let mut ready = Vec::new();
        let packets = std::mem::take(&mut self.in_flight);
        let mut still = Vec::with_capacity(packets.len());
        for pkt in packets {
            if pkt.deliver_at > now {
                still.push(pkt);
            } else if self.reachable(pkt.from, pkt.to) {
                ready.push((pkt.to, pkt.msg));
            }
            // otherwise: dropped in transit by a partition/crash
        }
        self.in_flight = still;
        ready
    }

    // --- Fault injection ---

    /// Fully isolate a node into its own partition group.
    pub fn isolate(&mut self, id: NodeId) {
        self.group.insert(id, id as u8);
    }

    /// Split the cluster into two groups that cannot talk to each other.
    pub fn partition(&mut self, side_a: &[NodeId], side_b: &[NodeId]) {
        for &id in side_a {
            self.group.insert(id, 1);
        }
        for &id in side_b {
            self.group.insert(id, 2);
        }
    }

    /// Reunite every node into one partition group.
    pub fn heal_all(&mut self) {
        for g in self.group.values_mut() {
            *g = 0;
        }
    }

    /// Crash (pause) a node; its in-flight messages are dropped and it neither
    /// ticks nor receives until restarted. Its durable state is preserved.
    pub fn crash(&mut self, id: NodeId) {
        self.down.insert(id);
    }

    /// Restart a previously crashed node.
    pub fn restart(&mut self, id: NodeId) {
        self.down.remove(&id);
    }
}
