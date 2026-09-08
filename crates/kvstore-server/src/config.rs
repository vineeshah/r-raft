//! Node configuration assembled from CLI arguments.

use std::collections::HashMap;

use anyhow::{bail, Context};
use kvstore_raft::NodeId;

/// Resolved configuration for a running node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's id.
    pub id: NodeId,
    /// Address this node's gRPC server binds to.
    pub listen: String,
    /// Directory for RocksDB data.
    pub data_dir: String,
    /// Map of peer id -> `host:port` for every *other* node.
    pub peers: HashMap<NodeId, String>,
    /// Milliseconds per logical Raft tick.
    pub tick_ms: u64,
}

impl NodeConfig {
    /// Parse peers of the form `id=host:port` and assemble the config.
    pub fn build(
        id: NodeId,
        listen: String,
        data_dir: String,
        peer_specs: &[String],
        tick_ms: u64,
    ) -> anyhow::Result<Self> {
        let mut peers = HashMap::new();
        for spec in peer_specs {
            let (id_str, addr) = spec
                .split_once('=')
                .with_context(|| format!("peer must be id=host:port, got `{spec}`"))?;
            let peer_id: NodeId = id_str
                .parse()
                .with_context(|| format!("invalid peer id in `{spec}`"))?;
            if peer_id == id {
                bail!("peer id {peer_id} collides with this node's id");
            }
            peers.insert(peer_id, addr.to_string());
        }
        Ok(Self {
            id,
            listen,
            data_dir,
            peers,
            tick_ms,
        })
    }

    /// All voting members, including this node, sorted for determinism.
    pub fn voters(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.peers.keys().copied().collect();
        ids.push(self.id);
        ids.sort_unstable();
        ids
    }
}
