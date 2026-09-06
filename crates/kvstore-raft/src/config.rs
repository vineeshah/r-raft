//! Static configuration for a Raft node.

use crate::NodeId;

/// Timing and membership parameters for a [`RaftNode`](crate::RaftNode).
///
/// All timeouts are counted in logical [`tick`](crate::RaftNode::tick)s, not
/// wall-clock time. The host runtime decides how long a tick is (e.g. 10ms);
/// the core stays time-free so it can be single-stepped in simulation.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node's id.
    pub id: NodeId,
    /// All voting members of the group, including this node.
    pub voters: Vec<NodeId>,
    /// Minimum election timeout, in ticks.
    pub election_timeout_min: u64,
    /// Maximum election timeout, in ticks (randomized within `[min, max]` to
    /// avoid split votes).
    pub election_timeout_max: u64,
    /// Ticks between leader heartbeats. Must be well below the election timeout.
    pub heartbeat_interval: u64,
    /// Seed for the per-node election-timeout randomizer (keeps sims reproducible).
    pub rng_seed: u64,
}

impl Config {
    /// A conventional configuration: election timeout 10–20 ticks, heartbeat
    /// every 2 ticks. Seed defaults to the node id.
    pub fn new(id: NodeId, voters: Vec<NodeId>) -> Self {
        Self {
            id,
            voters,
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 2,
            rng_seed: id,
        }
    }

    /// Number of votes required to form a majority quorum.
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Peers other than this node.
    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        let me = self.id;
        self.voters.iter().copied().filter(move |v| *v != me)
    }
}
