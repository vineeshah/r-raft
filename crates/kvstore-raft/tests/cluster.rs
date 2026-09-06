//! Deterministic, synchronous multi-node tests for the Raft core.
//!
//! A [`Cluster`] owns several [`RaftNode`]s and a simple in-process network.
//! Because the core is pure, driving it is just: tick every node, route the
//! actions they emit, deliver messages, repeat. No threads, no clocks, no
//! flakiness — the same seed yields the same run every time. This harness is the
//! seed of the Phase 4 fault-injection simulator.

use std::collections::{BTreeMap, HashSet};

use kvstore_raft::message::Action;
use kvstore_raft::testing::MemStorage;
use kvstore_raft::{Config, LogEntry, Message, NodeId, RaftNode, Role};

struct Cluster {
    nodes: BTreeMap<NodeId, RaftNode<MemStorage>>,
    inbox: Vec<(NodeId, Message)>,
    applied: BTreeMap<NodeId, Vec<LogEntry>>,
    partitioned: HashSet<NodeId>,
}

impl Cluster {
    fn new(ids: &[NodeId]) -> Self {
        let mut nodes = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for &id in ids {
            let mut cfg = Config::new(id, ids.to_vec());
            // Distinct seeds so nodes don't all time out on the same tick.
            cfg.rng_seed = id.wrapping_mul(2_654_435_761).wrapping_add(1);
            nodes.insert(id, RaftNode::new(cfg, MemStorage::new()).unwrap());
            applied.insert(id, Vec::new());
        }
        Self {
            nodes,
            inbox: Vec::new(),
            applied,
            partitioned: HashSet::new(),
        }
    }

    fn route(&mut self, from: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => {
                    if !self.partitioned.contains(&from) && !self.partitioned.contains(&to) {
                        self.inbox.push((to, message));
                    }
                }
                Action::Apply { entries } => {
                    self.applied.get_mut(&from).unwrap().extend(entries);
                }
            }
        }
    }

    fn deliver_until_idle(&mut self) {
        let mut guard = 0;
        while let Some((to, msg)) = self.inbox.pop() {
            guard += 1;
            assert!(guard < 100_000, "message delivery did not converge");
            if self.partitioned.contains(&to) {
                continue;
            }
            let actions = self.nodes.get_mut(&to).unwrap().step(msg).unwrap();
            self.route(to, actions);
        }
    }

    fn tick_all(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            let actions = self.nodes.get_mut(&id).unwrap().tick().unwrap();
            self.route(id, actions);
            self.deliver_until_idle();
        }
    }

    fn run(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.tick_all();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.role() == Role::Leader && !self.partitioned.contains(&n.id()))
            .map(|n| n.id())
            .collect();
        // At most one leader among connected nodes at a stable point.
        match leaders.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    fn propose(&mut self, command: &[u8]) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let result = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .propose(command.to_vec())
            .unwrap();
        match result {
            Some((_index, actions)) => {
                self.route(leader, actions);
                self.deliver_until_idle();
                true
            }
            None => false,
        }
    }

    fn isolate(&mut self, id: NodeId) {
        self.partitioned.insert(id);
    }

    fn heal(&mut self, id: NodeId) {
        self.partitioned.remove(&id);
    }

    fn applied_commands(&self, id: NodeId) -> Vec<Vec<u8>> {
        self.applied[&id]
            .iter()
            .map(|e| e.command.clone())
            .collect()
    }
}

#[test]
fn elects_exactly_one_leader() {
    let mut c = Cluster::new(&[1, 2, 3]);
    c.run(40);
    let leader = c.leader().expect("a leader should emerge");
    let term = c.nodes[&leader].current_term();
    assert!(term >= 1);
    // Followers recognize the leader for the current term.
    for n in c.nodes.values() {
        if n.id() != leader {
            assert_eq!(n.leader_id(), Some(leader));
            assert_eq!(n.current_term(), term);
        }
    }
}

#[test]
fn leader_replicates_and_all_apply() {
    let mut c = Cluster::new(&[1, 2, 3]);
    c.run(40);
    assert!(c.leader().is_some());

    assert!(c.propose(b"set x=1"));
    c.run(10); // let the commit index propagate to followers

    for id in [1, 2, 3] {
        assert_eq!(
            c.applied_commands(id),
            vec![b"set x=1".to_vec()],
            "node {id} should have applied the command"
        );
    }
}

#[test]
fn survives_leader_failure() {
    let mut c = Cluster::new(&[1, 2, 3]);
    c.run(40);
    let old_leader = c.leader().expect("initial leader");

    // A first write commits under the original leader.
    assert!(c.propose(b"a"));
    c.run(10);

    // Kill the leader; the remaining two must elect a new one.
    c.isolate(old_leader);
    c.run(60);
    let new_leader = c.leader().expect("a new leader after failover");
    assert_ne!(new_leader, old_leader);

    // Writes continue on the new leader and reach the surviving followers.
    assert!(c.propose(b"b"));
    c.run(10);
    for id in [1, 2, 3] {
        if id == old_leader {
            continue;
        }
        assert_eq!(c.applied_commands(id), vec![b"a".to_vec(), b"b".to_vec()]);
    }
}

#[test]
fn old_leader_rejoins_and_converges() {
    let mut c = Cluster::new(&[1, 2, 3]);
    c.run(40);
    let old_leader = c.leader().expect("initial leader");
    assert!(c.propose(b"a"));
    c.run(10);

    // Partition the leader, elect a new one, commit more writes.
    c.isolate(old_leader);
    c.run(60);
    assert!(c.propose(b"b"));
    assert!(c.propose(b"c"));
    c.run(10);

    // Heal the partition; the old leader must step down and catch up.
    c.heal(old_leader);
    c.run(60);

    let expected = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    assert_eq!(c.applied_commands(old_leader), expected);
    assert!(!c.nodes[&old_leader].is_leader());
}
