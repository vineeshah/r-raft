//! `kvstored` — a single node of the distributed KV store.
//!
//! Phase 0: parses configuration and initializes tracing. The Raft runtime and
//! gRPC services are wired in Phase 3.

use clap::Parser;

/// Command-line configuration for a KV store node.
#[derive(Debug, Parser)]
#[command(name = "kvstored", version, about)]
struct Args {
    /// This node's unique identifier within its Raft group.
    #[arg(long, env = "KV_NODE_ID")]
    node_id: u64,

    /// Address to bind the gRPC server to.
    #[arg(long, env = "KV_LISTEN", default_value = "127.0.0.1:5400")]
    listen: String,

    /// Directory for durable RocksDB data.
    #[arg(long, env = "KV_DATA_DIR", default_value = "./data")]
    data_dir: String,

    /// Peer addresses in the form `id=host:port`, repeatable.
    #[arg(long = "peer")]
    peers: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        node_id = args.node_id,
        listen = %args.listen,
        data_dir = %args.data_dir,
        peers = ?args.peers,
        "kvstored starting (scaffold — runtime lands in Phase 3)"
    );

    Ok(())
}
