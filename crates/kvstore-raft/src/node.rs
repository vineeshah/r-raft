//! The Raft node: a pure, deterministic consensus state machine.
//!
//! [`RaftNode`] owns no networking and reads no clock. It is advanced only by:
//! - [`tick`](RaftNode::tick) — one step of the logical clock, which may fire an
//!   election timeout or a leader heartbeat;
//! - [`step`](RaftNode::step) — an inbound [`Message`];
//! - [`propose`](RaftNode::propose) — a client command (leader only).
//!
//! Each returns the [`Action`]s the host must perform (send messages, apply
//! committed entries). Persistence happens through the injected [`RaftStorage`],
//! so the only impurity — the disk — is itself swappable, which is what lets the
//! whole cluster run inside a deterministic simulation.
//!
//! Scope: this implements leader election, log replication with fast conflict
//! backtracking, commit advancement, and client proposals. Snapshot transfer
//! over the wire and dynamic membership changes are layered on in later phases.

use std::collections::{HashMap, HashSet};

use crate::config::Config;
use crate::message::{
    Action, AppendEntries, AppendEntriesResp, Message, RequestVote, RequestVoteResp,
};
use crate::storage::HardState;
use crate::{LogEntry, LogIndex, NodeId, RaftStorage, Role, Term};

/// A single Raft replica.
pub struct RaftNode<S: RaftStorage> {
    config: Config,
    storage: S,

    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,

    commit_index: LogIndex,
    last_applied: LogIndex,

    // Election bookkeeping.
    election_elapsed: u64,
    heartbeat_elapsed: u64,
    randomized_timeout: u64,
    rng_state: u64,
    votes_granted: HashSet<NodeId>,

    // Leader bookkeeping (per follower).
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
}

impl<S: RaftStorage> RaftNode<S> {
    /// Construct a node, recovering persisted term/vote/commit state.
    pub fn new(config: Config, storage: S) -> Result<Self, S::Error> {
        let hs = storage.hard_state()?;
        let seed = if config.rng_seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            config.rng_seed
        };
        let mut node = Self {
            config,
            storage,
            role: Role::Follower,
            current_term: hs.current_term,
            voted_for: hs.voted_for,
            leader_id: None,
            commit_index: hs.commit_index,
            // The state machine is persisted alongside the log, so on recovery
            // everything up to commit_index is already applied.
            last_applied: hs.commit_index,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_timeout: 0,
            rng_state: seed,
            votes_granted: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        };
        node.reset_election_timer();
        Ok(node)
    }

    // --- Accessors (primarily for the host runtime and tests) ---

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.config.id
    }
    /// Current role.
    pub fn role(&self) -> Role {
        self.role
    }
    /// Current term.
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    /// The leader this node currently recognizes, if any.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    /// Highest committed log index.
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    /// Whether this node currently believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// Borrow the underlying storage (e.g. for local reads).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    // --- Driving the state machine ---

    /// Advance the logical clock by one tick.
    pub fn tick(&mut self) -> Result<Vec<Action>, S::Error> {
        match self.role {
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_timeout {
                    return self.start_election();
                }
                Ok(Vec::new())
            }
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                    self.heartbeat_elapsed = 0;
                    return self.broadcast_append();
                }
                Ok(Vec::new())
            }
        }
    }

    /// Process an inbound message.
    pub fn step(&mut self, msg: Message) -> Result<Vec<Action>, S::Error> {
        // Universal rule: a higher term means we are stale — step down first.
        if msg.term() > self.current_term {
            self.step_down(msg.term())?;
        }
        match msg {
            Message::RequestVote(m) => self.handle_request_vote(m),
            Message::RequestVoteResp(m) => self.handle_request_vote_resp(m),
            Message::AppendEntries(m) => self.handle_append_entries(m),
            Message::AppendEntriesResp(m) => self.handle_append_entries_resp(m),
        }
    }

    /// Append a client command (leader only).
    ///
    /// Returns the assigned log index and the replication actions, or `None` if
    /// this node is not the leader.
    pub fn propose(
        &mut self,
        command: Vec<u8>,
    ) -> Result<Option<(LogIndex, Vec<Action>)>, S::Error> {
        if self.role != Role::Leader {
            return Ok(None);
        }
        let index = self.storage.last_index()? + 1;
        self.storage.append(&[LogEntry {
            term: self.current_term,
            index,
            command,
        }])?;
        self.match_index.insert(self.id(), index);

        let mut actions = self.broadcast_append()?;
        // Single-node clusters commit immediately (self is a majority).
        actions.extend(self.maybe_advance_commit()?);
        Ok(Some((index, actions)))
    }

    // --- Role transitions ---

    fn step_down(&mut self, term: Term) -> Result<(), S::Error> {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes_granted.clear();
        self.persist_hard_state()
    }

    fn start_election(&mut self) -> Result<Vec<Action>, S::Error> {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id());
        self.leader_id = None;
        self.votes_granted.clear();
        self.votes_granted.insert(self.id());
        self.reset_election_timer();
        self.persist_hard_state()?;

        // Won already? (single-node cluster)
        if self.votes_granted.len() >= self.config.quorum() {
            return self.become_leader();
        }

        let (last_log_index, last_log_term) = self.last_log_info()?;
        let mut actions = Vec::new();
        for peer in self.config.peers() {
            actions.push(Action::Send {
                to: peer,
                message: Message::RequestVote(RequestVote {
                    from: self.id(),
                    term: self.current_term,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
        Ok(actions)
    }

    fn become_leader(&mut self) -> Result<Vec<Action>, S::Error> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id());
        self.heartbeat_elapsed = 0;

        let last = self.storage.last_index()?;
        self.next_index.clear();
        self.match_index.clear();
        self.match_index.insert(self.id(), last);
        for peer in self.config.peers() {
            self.next_index.insert(peer, last + 1);
            self.match_index.insert(peer, 0);
        }
        self.broadcast_append()
    }

    // --- RequestVote ---

    fn handle_request_vote(&mut self, m: RequestVote) -> Result<Vec<Action>, S::Error> {
        let mut granted = false;
        if m.term >= self.current_term {
            let (last_index, last_term) = self.last_log_info()?;
            let log_ok = m.last_log_term > last_term
                || (m.last_log_term == last_term && m.last_log_index >= last_index);
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(m.from);
            if can_vote && log_ok {
                granted = true;
                self.voted_for = Some(m.from);
                self.reset_election_timer();
                self.persist_hard_state()?;
            }
        }
        Ok(vec![Action::Send {
            to: m.from,
            message: Message::RequestVoteResp(RequestVoteResp {
                from: self.id(),
                term: self.current_term,
                vote_granted: granted,
            }),
        }])
    }

    fn handle_request_vote_resp(&mut self, m: RequestVoteResp) -> Result<Vec<Action>, S::Error> {
        if self.role != Role::Candidate || m.term != self.current_term {
            return Ok(Vec::new());
        }
        if m.vote_granted {
            self.votes_granted.insert(m.from);
            if self.votes_granted.len() >= self.config.quorum() {
                return self.become_leader();
            }
        }
        Ok(Vec::new())
    }

    // --- AppendEntries ---

    fn handle_append_entries(&mut self, m: AppendEntries) -> Result<Vec<Action>, S::Error> {
        // Reject stale leaders.
        if m.term < self.current_term {
            return Ok(vec![self.append_reply(m.from, false, 0, 0)]);
        }

        // Valid leader for this term: (re)establish follower state.
        self.role = Role::Follower;
        self.leader_id = Some(m.from);
        self.reset_election_timer();

        // Log-matching check at prev_log_index.
        let last = self.storage.last_index()?;
        if m.prev_log_index > last {
            // We're missing entries; tell the leader where our log ends.
            return Ok(vec![self.append_reply(m.from, false, 0, last + 1)]);
        }
        if m.prev_log_index > 0 {
            let existing = self.storage.term(m.prev_log_index)?;
            if existing != Some(m.prev_log_term) {
                let conflict_index = self.first_index_of_term(m.prev_log_index, existing)?;
                return Ok(vec![self.append_reply(m.from, false, 0, conflict_index)]);
            }
        }

        // Logs match: splice in the new entries, resolving any conflict.
        self.append_entries_from(&m.entries)?;

        // Advance commit up to the last entry this message vouches for.
        let last_new = m.prev_log_index + m.entries.len() as u64;
        let mut actions = Vec::new();
        if m.leader_commit > self.commit_index {
            let new_commit = m.leader_commit.min(last_new);
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                self.persist_hard_state()?;
                actions.extend(self.take_committed()?);
            }
        }

        actions.push(self.append_reply(m.from, true, last_new, 0));
        Ok(actions)
    }

    fn handle_append_entries_resp(
        &mut self,
        m: AppendEntriesResp,
    ) -> Result<Vec<Action>, S::Error> {
        if self.role != Role::Leader || m.term != self.current_term {
            return Ok(Vec::new());
        }
        if m.success {
            self.match_index.insert(m.from, m.match_index);
            self.next_index.insert(m.from, m.match_index + 1);
            self.maybe_advance_commit()
        } else {
            // Fast backup: jump straight to the follower's conflict hint.
            let next = m.conflict_index.max(1);
            self.next_index.insert(m.from, next);
            match self.replicate_to(m.from)? {
                Some(action) => Ok(vec![action]),
                None => Ok(Vec::new()),
            }
        }
    }

    // --- Replication helpers ---

    fn broadcast_append(&mut self) -> Result<Vec<Action>, S::Error> {
        let peers: Vec<NodeId> = self.config.peers().collect();
        let mut actions = Vec::new();
        for peer in peers {
            if let Some(action) = self.replicate_to(peer)? {
                actions.push(action);
            }
        }
        Ok(actions)
    }

    fn replicate_to(&mut self, peer: NodeId) -> Result<Option<Action>, S::Error> {
        let next = *self.next_index.get(&peer).unwrap_or(&1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.storage.term(prev_log_index)?.unwrap_or(0);
        let last = self.storage.last_index()?;
        let entries = self.storage.entries(next, last + 1)?;
        Ok(Some(Action::Send {
            to: peer,
            message: Message::AppendEntries(AppendEntries {
                from: self.id(),
                term: self.current_term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }),
        }))
    }

    /// Find the highest index committable by a majority in the current term,
    /// advance `commit_index` to it, and emit the entries to apply.
    fn maybe_advance_commit(&mut self) -> Result<Vec<Action>, S::Error> {
        let last = self.storage.last_index()?;
        let quorum = self.config.quorum();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1..=last).rev() {
            let mut count = 1; // this leader holds entry n
            for peer in self.config.peers() {
                if *self.match_index.get(&peer).unwrap_or(&0) >= n {
                    count += 1;
                }
            }
            // Safety: only commit an entry from the current term directly.
            if count >= quorum && self.storage.term(n)? == Some(self.current_term) {
                new_commit = n;
                break;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.persist_hard_state()?;
            return self.take_committed();
        }
        Ok(Vec::new())
    }

    /// Splice `entries` into the log, truncating on the first conflict.
    fn append_entries_from(&mut self, entries: &[LogEntry]) -> Result<(), S::Error> {
        for (offset, entry) in entries.iter().enumerate() {
            let last = self.storage.last_index()?;
            if entry.index > last {
                self.storage.append(&entries[offset..])?;
                return Ok(());
            }
            if self.storage.term(entry.index)? != Some(entry.term) {
                self.storage.truncate_suffix(entry.index)?;
                self.storage.append(&entries[offset..])?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Collect committed-but-unapplied entries and mark them applied.
    fn take_committed(&mut self) -> Result<Vec<Action>, S::Error> {
        if self.commit_index <= self.last_applied {
            return Ok(Vec::new());
        }
        let entries = self
            .storage
            .entries(self.last_applied + 1, self.commit_index + 1)?;
        self.last_applied = self.commit_index;
        if entries.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![Action::Apply { entries }])
        }
    }

    // --- Small helpers ---

    fn append_reply(
        &self,
        to: NodeId,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
    ) -> Action {
        Action::Send {
            to,
            message: Message::AppendEntriesResp(AppendEntriesResp {
                from: self.id(),
                term: self.current_term,
                success,
                match_index,
                conflict_index,
            }),
        }
    }

    /// First index sharing `term` with the entry at `upto`, scanning backward —
    /// lets the leader skip a whole conflicting term in one round trip.
    fn first_index_of_term(
        &self,
        upto: LogIndex,
        term: Option<Term>,
    ) -> Result<LogIndex, S::Error> {
        let first = self.storage.first_index()?;
        let mut idx = upto;
        while idx > first {
            if self.storage.term(idx - 1)? == term {
                idx -= 1;
            } else {
                break;
            }
        }
        Ok(idx)
    }

    fn last_log_info(&self) -> Result<(LogIndex, Term), S::Error> {
        let last_index = self.storage.last_index()?;
        let last_term = self.storage.term(last_index)?.unwrap_or(0);
        Ok((last_index, last_term))
    }

    fn persist_hard_state(&mut self) -> Result<(), S::Error> {
        self.storage.save_hard_state(&HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index,
        })
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let span = self.config.election_timeout_max - self.config.election_timeout_min + 1;
        self.randomized_timeout = self.config.election_timeout_min + self.next_rand() % span;
    }

    /// xorshift64 — deterministic per-node PRNG for election-timeout jitter.
    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }
}
