//! `kvstored` — a single node of the distributed KV store.

use clap::Parser;
use kvstore_server::{run, NodeConfig};

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

    /// Milliseconds per logical Raft tick.
    #[arg(long, env = "KV_TICK_MS", default_value_t = 50)]
    tick_ms: u64,

    /// Address to expose Prometheus metrics on (`/metrics`).
    #[arg(long, env = "KV_METRICS_LISTEN", default_value = "127.0.0.1:9400")]
    metrics_listen: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    // Install the Prometheus exporter once per process; it serves /metrics on
    // its own HTTP listener.
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(args.metrics_listen)
        .install()?;
    tracing::info!(metrics = %args.metrics_listen, "metrics endpoint listening");

    let cfg = NodeConfig::build(
        args.node_id,
        args.listen,
        args.data_dir,
        &args.peers,
        args.tick_ms,
    )?;
    run(cfg).await
}
