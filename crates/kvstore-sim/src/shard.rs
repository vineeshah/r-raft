//! Multi-Raft sharding: partition the keyspace into ranges, each owned by an
//! independent Raft group.
//!
//! This is how systems like TiKV and CockroachDB scale writes horizontally: no
//! single Raft group is a bottleneck because different key ranges commit in
//! parallel on separate groups (each with its own leader). Here each shard is a
//! full [`Simulator`] — its own group of nodes, log, and leader — and a
//! [`ShardMap`] routes a key to its shard by contiguous range.
//!
//! Because different shards own disjoint keys, a fault in one shard's group
//! (partition, crashed node) cannot affect another shard's availability, and the
//! merged client history is linearizable per key by composition.

use crate::kv::KvOp;
use crate::linearizability::Op;
use crate::sim::{SimConfig, Simulator};

/// Maps keys to shards by contiguous range.
#[derive(Debug, Clone)]
pub struct ShardMap {
    shards: u64,
    width: u64,
}

impl ShardMap {
    /// A keyspace of `shards * width` split into `shards` equal contiguous ranges.
    pub fn new(shards: u64, width: u64) -> Self {
        assert!(shards > 0 && width > 0);
        Self { shards, width }
    }

    /// Number of shards.
    pub fn shards(&self) -> u64 {
        self.shards
    }

    /// The shard owning `key` (keys at/above the last range clamp to it).
    pub fn shard_of(&self, key: u64) -> u64 {
        (key / self.width).min(self.shards - 1)
    }
}

/// A sharded cluster: one independent Raft group per shard.
pub struct ShardedCluster {
    map: ShardMap,
    shards: Vec<Simulator>,
}

impl ShardedCluster {
    /// Build `num_shards` groups of `nodes_per_shard` nodes each, seeded
    /// deterministically from `seed`.
    pub fn new(num_shards: u64, nodes_per_shard: u64, width: u64, seed: u64) -> Self {
        let shards = (0..num_shards)
            .map(|s| Simulator::new(SimConfig::new(nodes_per_shard, seed ^ (s + 1))))
            .collect();
        Self {
            map: ShardMap::new(num_shards, width),
            shards,
        }
    }

    /// The routing table.
    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// Route an operation to the shard that owns its key. Returns `false` if that
    /// shard has no leader to accept the write right now.
    pub fn submit(&mut self, op: KvOp) -> bool {
        let shard = self.map.shard_of(op.key()) as usize;
        self.shards[shard].submit(op)
    }

    /// Advance every shard by one tick.
    pub fn step(&mut self) {
        for shard in &mut self.shards {
            shard.step();
        }
    }

    /// Advance every shard by `n` ticks.
    pub fn run(&mut self, n: u64) {
        for _ in 0..n {
            self.step();
        }
    }

    /// Heal and drain every shard until all in-flight operations resolve.
    pub fn drain(&mut self, max_ticks: u64) {
        for shard in &mut self.shards {
            shard.drain(max_ticks);
        }
    }

    /// Mutable access to one shard's group (for targeted fault injection).
    pub fn shard(&mut self, shard: u64) -> &mut Simulator {
        &mut self.shards[shard as usize]
    }

    /// The leader of each shard's group (index = shard id).
    pub fn leaders(&self) -> Vec<Option<u64>> {
        self.shards.iter().map(|s| s.leader()).collect()
    }

    /// The combined client history across all shards. Keys are disjoint per
    /// shard, so linearizability of the whole is the conjunction of the parts.
    pub fn history(&self) -> Vec<Op> {
        self.shards
            .iter()
            .flat_map(|s| s.history().iter().cloned())
            .collect()
    }

    /// The first safety-invariant violation seen in any shard, if any.
    pub fn violation(&self) -> Option<String> {
        self.shards
            .iter()
            .find_map(|s| s.violation().map(|v| v.to_string()))
    }
}
