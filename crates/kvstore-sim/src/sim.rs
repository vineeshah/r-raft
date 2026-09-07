//! The deterministic simulator.
//!
//! Owns every node's [`RaftNode`] (over volatile [`MemStorage`]) and an
//! in-memory state machine, plus a virtual [`Network`]. Time advances in
//! discrete ticks: each tick every live node is ticked, its emitted actions are
//! routed, and due messages are delivered. Scenarios interleave client
//! submissions and fault injection between ticks.
//!
//! Two things are watched continuously and recorded for after-the-fact checks:
//! - **invariants** — at most one leader per term, and no two different entries
//!   committed at the same log index (state-machine safety);
//! - **history** — each completed client operation with its `[call, ret]`
//!   interval and observed output, for linearizability checking.

use std::collections::{BTreeMap, HashMap};

use kvstore_raft::message::Action;
use kvstore_raft::testing::MemStorage;
use kvstore_raft::{Config, LogEntry, LogIndex, NodeId, RaftNode, Term};

use crate::kv::{decode, encode, KvCommand, KvOp, MemKv};
use crate::linearizability::Op;
use crate::net::Network;
use crate::rng::Rng;

/// Parameters for a simulation.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of nodes (ids `1..=nodes`).
    pub nodes: u64,
    /// Maximum message delay in ticks.
    pub max_delay: u64,
    /// Per-message drop probability in `[0, 1]`.
    pub drop_prob: f64,
    /// Master seed; drives the network and per-node election jitter.
    pub seed: u64,
    /// Election timeout bounds (ticks).
    pub election_min: u64,
    /// Election timeout upper bound (ticks).
    pub election_max: u64,
    /// Heartbeat interval (ticks).
    pub heartbeat: u64,
}

impl SimConfig {
    /// A reasonable default for `nodes` nodes and a given seed.
    pub fn new(nodes: u64, seed: u64) -> Self {
        Self {
            nodes,
            max_delay: 2,
            drop_prob: 0.0,
            seed,
            election_min: 10,
            election_max: 20,
            heartbeat: 2,
        }
    }
}

struct NodeState {
    raft: RaftNode<MemStorage>,
    kv: MemKv,
}

struct Pending {
    op_id: u64,
    input: KvOp,
    call: u64,
}

/// A running simulation.
pub struct Simulator {
    ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, NodeState>,
    net: Network,
    rng: Rng,
    tick: u64,
    next_op: u64,
    pending: HashMap<(NodeId, LogIndex), Pending>,
    history: Vec<Op>,
    applied: HashMap<LogIndex, (Term, u64)>,
    leaders_seen: HashMap<Term, NodeId>,
    violation: Option<String>,
}

impl Simulator {
    /// Build a simulation from its configuration.
    pub fn new(cfg: SimConfig) -> Self {
        let ids: Vec<NodeId> = (1..=cfg.nodes).collect();
        let mut nodes = BTreeMap::new();
        for &id in &ids {
            let mut raft_cfg = Config::new(id, ids.clone());
            raft_cfg.election_timeout_min = cfg.election_min;
            raft_cfg.election_timeout_max = cfg.election_max;
            raft_cfg.heartbeat_interval = cfg.heartbeat;
            raft_cfg.rng_seed = cfg.seed ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let raft = RaftNode::new(raft_cfg, MemStorage::new()).unwrap();
            nodes.insert(
                id,
                NodeState {
                    raft,
                    kv: MemKv::new(),
                },
            );
        }
        let net = Network::new(&ids, cfg.max_delay, cfg.drop_prob);
        Self {
            ids,
            nodes,
            net,
            rng: Rng::new(cfg.seed),
            tick: 0,
            next_op: 0,
            pending: HashMap::new(),
            history: Vec::new(),
            applied: HashMap::new(),
            leaders_seen: HashMap::new(),
            violation: None,
        }
    }

    /// Advance the simulation by one tick.
    pub fn step(&mut self) {
        self.tick += 1;

        let live: Vec<NodeId> = self
            .ids
            .iter()
            .copied()
            .filter(|id| !self.net.is_down(*id))
            .collect();
        for id in live {
            let actions = self.nodes.get_mut(&id).unwrap().raft.tick().unwrap();
            self.handle_actions(id, actions);
        }

        for (to, msg) in self.net.take_ready(self.tick) {
            if self.net.is_down(to) {
                continue;
            }
            let actions = self.nodes.get_mut(&to).unwrap().raft.step(msg).unwrap();
            self.handle_actions(to, actions);
        }

        self.check_leader_safety();
    }

    /// Advance by `n` ticks.
    pub fn run(&mut self, n: u64) {
        for _ in 0..n {
            self.step();
        }
    }

    /// Submit a client operation to the current leader. Returns `false` if there
    /// is no unique leader to accept it right now.
    pub fn submit(&mut self, op: KvOp) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let id = self.next_op;
        self.next_op += 1;
        let cmd = KvCommand { id, op: op.clone() };
        let proposed = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .raft
            .propose(encode(&cmd))
            .unwrap();
        match proposed {
            Some((index, actions)) => {
                self.pending.insert(
                    (leader, index),
                    Pending {
                        op_id: id,
                        input: op,
                        call: self.tick,
                    },
                );
                self.handle_actions(leader, actions);
                true
            }
            None => false,
        }
    }

    /// Heal the network, restart all nodes, and run until every in-flight client
    /// operation has been resolved (committed and applied, or aborted).
    pub fn drain(&mut self, max_ticks: u64) {
        self.net.heal_all();
        let ids = self.ids.clone();
        for id in ids {
            self.net.restart(id);
        }
        let mut i = 0;
        while !self.pending.is_empty() && i < max_ticks {
            self.step();
            i += 1;
        }
        // A few extra ticks for the last applies to propagate.
        self.run(20);
    }

    // --- Fault injection (delegated to the network) ---

    /// Isolate a node from all others.
    pub fn isolate(&mut self, id: NodeId) {
        self.net.isolate(id);
    }
    /// Partition the cluster into two disjoint sides.
    pub fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        self.net.partition(a, b);
    }
    /// Heal all partitions.
    pub fn heal_all(&mut self) {
        self.net.heal_all();
    }
    /// Crash (pause) a node, preserving its durable state.
    pub fn crash(&mut self, id: NodeId) {
        self.net.crash(id);
    }
    /// Restart a crashed node.
    pub fn restart(&mut self, id: NodeId) {
        self.net.restart(id);
    }

    // --- Observation ---

    /// The unique leader among live nodes, if exactly one exists.
    pub fn leader(&self) -> Option<NodeId> {
        let mut found = None;
        for (&id, node) in &self.nodes {
            if !self.net.is_down(id) && node.raft.is_leader() {
                if found.is_some() {
                    return None; // more than one apparent leader
                }
                found = Some(id);
            }
        }
        found
    }

    /// The recorded history of completed operations.
    pub fn history(&self) -> &[Op] {
        &self.history
    }

    /// The invariant violation, if any was detected.
    pub fn violation(&self) -> Option<&str> {
        self.violation.as_deref()
    }

    /// The current simulation tick.
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// A shared RNG draw, for scenarios that want deterministic choices.
    pub fn rng(&mut self) -> &mut Rng {
        &mut self.rng
    }

    // --- Internals ---

    fn handle_actions(&mut self, id: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    self.net.send(id, to, message, self.tick, &mut self.rng);
                }
                Action::Apply { entries } => {
                    for entry in entries {
                        self.apply_entry(id, entry);
                    }
                }
            }
        }
    }

    fn apply_entry(&mut self, id: NodeId, entry: LogEntry) {
        let cmd = decode(&entry.command);
        let output = self.nodes.get_mut(&id).unwrap().kv.apply(&cmd);

        // State-machine safety: the same index must never apply two different
        // entries across the cluster.
        match self.applied.get(&entry.index) {
            Some(&(term, op_id)) if term != entry.term || op_id != cmd.id => {
                self.violation.get_or_insert_with(|| {
                    format!(
                        "divergence at log index {}: two different entries",
                        entry.index
                    )
                });
            }
            None => {
                self.applied.insert(entry.index, (entry.term, cmd.id));
            }
            _ => {}
        }

        if let Some(pending) = self.pending.remove(&(id, entry.index)) {
            if pending.op_id == cmd.id {
                self.history.push(Op {
                    call: pending.call,
                    ret: self.tick,
                    input: pending.input,
                    output,
                });
            }
            // Otherwise this slot was taken by a different entry: the pending
            // op's proposal never committed, so it took no effect — drop it.
        }
    }

    fn check_leader_safety(&mut self) {
        for (&id, node) in &self.nodes {
            if self.net.is_down(id) || !node.raft.is_leader() {
                continue;
            }
            let term = node.raft.current_term();
            match self.leaders_seen.get(&term) {
                Some(&prev) if prev != id => {
                    self.violation.get_or_insert_with(|| {
                        format!("two leaders in term {term}: {prev} and {id}")
                    });
                }
                _ => {
                    self.leaders_seen.insert(term, id);
                }
            }
        }
    }
}
