//! The Raft driver: a single task that exclusively owns the [`RaftNode`] and the
//! state machine, turning the pure core into a live networked node.
//!
//! Everything funnels through one mpsc channel of [`Event`]s, so there are no
//! locks around consensus state and no data races: ticks, inbound RPCs, RPC
//! responses, and client requests are all serialized through the loop. The
//! actions the core emits are executed here — outbound RPCs are spawned as tasks
//! whose responses feed back in as [`Event::Response`], and committed entries are
//! applied to the state machine, waking any client waiting on that log index.

use std::collections::HashMap;

use kvstore_proto::raft::raft_client::RaftClient;
use kvstore_raft::message::{Action, AppendEntries, Message, RequestVote};
use kvstore_raft::{LogIndex, NodeId, RaftNode, StateMachine};
use kvstore_storage::{KvStateMachine, RocksRaftStorage};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::convert;

/// Result of a client proposal.
#[derive(Debug)]
pub enum ProposeReply {
    /// Command committed and applied; carries the state machine's result bytes.
    Applied(Vec<u8>),
    /// This node is not the leader; hint the caller toward the known leader.
    NotLeader(Option<NodeId>),
}

/// Result of a client read.
#[derive(Debug)]
pub enum ReadReply {
    /// The current value for the key (or `None` if absent).
    Value(Option<Vec<u8>>),
    /// This node cannot serve a linearizable read; try the leader.
    NotLeader(Option<NodeId>),
}

/// Events processed by the driver loop.
pub enum Event {
    /// A logical clock tick.
    Tick,
    /// An inbound Raft request needing a synchronous reply.
    Inbound {
        msg: Message,
        reply: oneshot::Sender<Message>,
    },
    /// A Raft response fed back from an outbound RPC we issued.
    Response(Message),
    /// A client write to be proposed to the log.
    Propose {
        command: Vec<u8>,
        reply: oneshot::Sender<ProposeReply>,
    },
    /// A client read.
    Read {
        key: Vec<u8>,
        linearizable: bool,
        reply: oneshot::Sender<ReadReply>,
    },
}

/// Cheap, clonable handle the gRPC services use to talk to the driver.
#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Event>,
}

impl Handle {
    /// Feed a tick.
    pub async fn tick(&self) {
        let _ = self.tx.send(Event::Tick).await;
    }

    async fn inbound(&self, msg: Message) -> anyhow::Result<Message> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Event::Inbound { msg, reply: tx }).await.ok();
        Ok(rx.await?)
    }

    /// Handle an inbound `RequestVote`, returning the node's response.
    pub async fn request_vote(&self, m: RequestVote) -> anyhow::Result<Message> {
        self.inbound(Message::RequestVote(m)).await
    }

    /// Handle an inbound `AppendEntries`, returning the node's response.
    pub async fn append_entries(&self, m: AppendEntries) -> anyhow::Result<Message> {
        self.inbound(Message::AppendEntries(m)).await
    }

    /// Propose a client command and await its outcome.
    pub async fn propose(&self, command: Vec<u8>) -> anyhow::Result<ProposeReply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::Propose { command, reply: tx })
            .await
            .ok();
        Ok(rx.await?)
    }

    /// Read a key.
    pub async fn read(&self, key: Vec<u8>, linearizable: bool) -> anyhow::Result<ReadReply> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::Read {
                key,
                linearizable,
                reply: tx,
            })
            .await
            .ok();
        Ok(rx.await?)
    }
}

/// Owns consensus state; runs in its own task.
pub struct Driver {
    node: RaftNode<RocksRaftStorage>,
    sm: KvStateMachine,
    peers: HashMap<NodeId, String>,
    waiters: HashMap<LogIndex, oneshot::Sender<ProposeReply>>,
    tx: mpsc::Sender<Event>,
    was_leader: bool,
}

impl Driver {
    /// Create the driver and its handle. Spawn [`Driver::run`] on the returned
    /// receiver to start processing.
    pub fn new(
        node: RaftNode<RocksRaftStorage>,
        sm: KvStateMachine,
        peers: HashMap<NodeId, String>,
    ) -> (Self, Handle, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel(1024);
        let handle = Handle { tx: tx.clone() };
        let driver = Self {
            node,
            sm,
            peers,
            waiters: HashMap::new(),
            tx,
            was_leader: false,
        };
        (driver, handle, rx)
    }

    /// The event loop. Runs until all handles are dropped.
    pub async fn run(mut self, mut rx: mpsc::Receiver<Event>) {
        while let Some(event) = rx.recv().await {
            if let Err(err) = self.handle(event).await {
                warn!(%err, "driver failed to process event");
            }
        }
    }

    async fn handle(&mut self, event: Event) -> anyhow::Result<()> {
        match event {
            Event::Tick => {
                let actions = self.node.tick()?;
                self.route(actions);
            }
            Event::Response(msg) => {
                let actions = self.node.step(msg)?;
                self.route(actions);
            }
            Event::Inbound { msg, reply } => {
                let from = msg.from();
                let actions = self.node.step(msg)?;
                self.route_with_reply(actions, from, reply);
            }
            Event::Propose { command, reply } => {
                match self.node.propose(command)? {
                    Some((index, actions)) => {
                        metrics::counter!("kv_proposals_total").increment(1);
                        // Register before routing: a single-node cluster applies
                        // synchronously inside route().
                        self.waiters.insert(index, reply);
                        self.route(actions);
                    }
                    None => {
                        let _ = reply.send(ProposeReply::NotLeader(self.node.leader_id()));
                    }
                }
            }
            Event::Read {
                key,
                linearizable,
                reply,
            } => {
                if linearizable && !self.node.is_leader() {
                    let _ = reply.send(ReadReply::NotLeader(self.node.leader_id()));
                } else {
                    let value = self.sm.get(&key)?;
                    let _ = reply.send(ReadReply::Value(value));
                }
            }
        }
        self.record_metrics();
        Ok(())
    }

    /// Publish current consensus state to the metrics registry. These are cheap
    /// no-ops if no Prometheus recorder is installed (e.g. in tests).
    fn record_metrics(&mut self) {
        let is_leader = self.node.is_leader();
        metrics::gauge!("kv_raft_term").set(self.node.current_term() as f64);
        metrics::gauge!("kv_raft_commit_index").set(self.node.commit_index() as f64);
        metrics::gauge!("kv_is_leader").set(if is_leader { 1.0 } else { 0.0 });
        if is_leader && !self.was_leader {
            metrics::counter!("kv_leader_changes_total").increment(1);
        }
        self.was_leader = is_leader;
    }

    /// Execute actions, sending the reply to `from` back through `reply` instead
    /// of over the network.
    fn route_with_reply(
        &mut self,
        actions: Vec<Action>,
        from: NodeId,
        reply: oneshot::Sender<Message>,
    ) {
        let mut reply = Some(reply);
        for action in actions {
            match action {
                Action::Send { to, message } if to == from && is_response(&message) => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(message);
                    }
                }
                Action::Send { to, message } => self.send_outbound(to, message),
                Action::Apply { entries } => self.apply(entries),
            }
        }
    }

    fn route(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, message } => self.send_outbound(to, message),
                Action::Apply { entries } => self.apply(entries),
            }
        }
    }

    fn apply(&mut self, entries: Vec<kvstore_raft::LogEntry>) {
        for entry in entries {
            match self.sm.apply(&entry.command) {
                Ok(result) => {
                    metrics::counter!("kv_raft_applied_total").increment(1);
                    if let Some(waiter) = self.waiters.remove(&entry.index) {
                        let _ = waiter.send(ProposeReply::Applied(result));
                    }
                }
                Err(err) => warn!(%err, index = entry.index, "state machine apply failed"),
            }
        }
    }

    /// Fire off an outbound RPC; its response re-enters the loop as
    /// [`Event::Response`].
    fn send_outbound(&self, to: NodeId, message: Message) {
        let Some(addr) = self.peers.get(&to).cloned() else {
            debug!(to, "no address for peer; dropping message");
            return;
        };
        let tx = self.tx.clone();
        tokio::spawn(async move {
            if let Some(response) = send_rpc(&addr, message, to).await {
                let _ = tx.send(Event::Response(response)).await;
            }
        });
    }
}

fn is_response(message: &Message) -> bool {
    matches!(
        message,
        Message::RequestVoteResp(_) | Message::AppendEntriesResp(_)
    )
}

/// Issue a single Raft RPC to `addr`, returning the response as a core message.
/// Failures (peer down, partitioned) return `None`; the leader retries on its
/// next heartbeat, so dropping is safe.
async fn send_rpc(addr: &str, message: Message, responder: NodeId) -> Option<Message> {
    let endpoint = format!("http://{addr}");
    match message {
        Message::RequestVote(rv) => {
            let mut client = RaftClient::connect(endpoint).await.ok()?;
            let resp = client
                .request_vote(convert::request_vote_to_pb(&rv))
                .await
                .ok()?;
            Some(Message::RequestVoteResp(
                convert::request_vote_resp_from_pb(resp.into_inner(), responder),
            ))
        }
        Message::AppendEntries(ae) => {
            let mut client = RaftClient::connect(endpoint).await.ok()?;
            let resp = client
                .append_entries(convert::append_entries_to_pb(&ae))
                .await
                .ok()?;
            Some(Message::AppendEntriesResp(
                convert::append_entries_resp_from_pb(resp.into_inner(), responder),
            ))
        }
        // Responses are never sent outbound as fresh RPCs.
        Message::RequestVoteResp(_) | Message::AppendEntriesResp(_) => None,
    }
}
