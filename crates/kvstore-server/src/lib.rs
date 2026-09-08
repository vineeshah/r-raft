//! A single node of the distributed KV store: it opens durable storage, builds
//! the Raft core, spawns the [`driver`] that owns it, drives logical ticks from
//! a wall-clock timer, and serves the `Raft` and `Kv` gRPC services.

#![forbid(unsafe_code)]
// tonic service methods return `Result<_, tonic::Status>`; `Status` is large by
// clippy's threshold, but this is the framework's idiomatic signature.
#![allow(clippy::result_large_err)]

pub mod config;
pub mod convert;
pub mod driver;
pub mod service;

use std::time::Duration;

use kvstore_proto::kv::kv_server::KvServer;
use kvstore_proto::raft::raft_server::RaftServer;
use kvstore_raft::{Config, RaftNode};
use kvstore_storage::RocksEngine;
use tonic::transport::Server;
use tracing::info;

pub use config::NodeConfig;

use crate::driver::Driver;
use crate::service::{KvSvc, RaftSvc};

/// Run a node to completion (serves until the process is stopped).
pub async fn run(cfg: NodeConfig) -> anyhow::Result<()> {
    let engine = RocksEngine::open(&cfg.data_dir)?;
    let raft_cfg = Config::new(cfg.id, cfg.voters());
    let node = RaftNode::new(raft_cfg, engine.raft_storage())?;

    let (driver, handle, rx) = Driver::new(node, engine.state_machine(), cfg.peers.clone());
    tokio::spawn(driver.run(rx));

    // Wall-clock timer -> logical ticks.
    {
        let handle = handle.clone();
        let period = Duration::from_millis(cfg.tick_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                handle.tick().await;
            }
        });
    }

    let addr = cfg.listen.parse()?;
    info!(node_id = cfg.id, %addr, peers = ?cfg.peers, "node serving");
    Server::builder()
        .add_service(RaftServer::new(RaftSvc::new(handle.clone())))
        .add_service(KvServer::new(KvSvc::new(handle)))
        .serve(addr)
        .await?;
    Ok(())
}
