//! End-to-end test over real gRPC: boot a 3-node cluster on loopback, then
//! exercise the client API. This proves the full stack — election, replication,
//! apply, and the client/inter-node transports — works together, not just the
//! pure core (which the `kvstore-raft` cluster tests already cover).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use kvstore_proto::kv::kv_client::KvClient;
use kvstore_proto::kv::{GetRequest, PutRequest};
use kvstore_server::{run, NodeConfig};
use tempfile::TempDir;
use tonic::transport::Channel;

fn node_config(id: u64, ports: &HashMap<u64, u16>, data_dir: &str) -> NodeConfig {
    let peers: Vec<String> = ports
        .iter()
        .filter(|(peer, _)| **peer != id)
        .map(|(peer, port)| format!("{peer}=127.0.0.1:{port}"))
        .collect();
    NodeConfig::build(
        id,
        format!("127.0.0.1:{}", ports[&id]),
        data_dir.to_string(),
        &peers,
        30, // fast ticks for a snappy test
    )
    .unwrap()
}

/// Try to connect to `endpoint`, retrying until the deadline (servers may still
/// be binding).
async fn connect(endpoint: String, deadline: Instant) -> Option<KvClient<Channel>> {
    while Instant::now() < deadline {
        if let Ok(client) = KvClient::connect(endpoint.clone()).await {
            return Some(client);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test]
async fn cluster_serves_reads_and_writes_over_grpc() {
    let ids = [1u64, 2, 3];
    let ports: HashMap<u64, u16> = [(1u64, 15401u16), (2, 15402), (3, 15403)]
        .into_iter()
        .collect();

    // Each node gets its own data directory; keep the guards alive for the test.
    let dirs: Vec<TempDir> = ids.iter().map(|_| TempDir::new().unwrap()).collect();
    for (i, &id) in ids.iter().enumerate() {
        let cfg = node_config(id, &ports, dirs[i].path().to_str().unwrap());
        tokio::spawn(async move {
            let _ = run(cfg).await;
        });
    }

    let deadline = Instant::now() + Duration::from_secs(15);
    let endpoints: Vec<String> = ids
        .iter()
        .map(|id| format!("http://127.0.0.1:{}", ports[id]))
        .collect();

    // Find the leader by trying to write to each node until one accepts.
    let mut leader_endpoint = None;
    'outer: while Instant::now() < deadline {
        for ep in &endpoints {
            let Some(mut client) = connect(ep.clone(), deadline).await else {
                continue;
            };
            let put = client
                .put(PutRequest {
                    key: b"hello".to_vec(),
                    value: b"world".to_vec(),
                })
                .await;
            if put.is_ok() {
                leader_endpoint = Some(ep.clone());
                break 'outer;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let leader_endpoint =
        leader_endpoint.expect("cluster should elect a leader and accept a write");

    // A linearizable read on the leader returns what we wrote.
    let mut client = KvClient::connect(leader_endpoint).await.unwrap();
    let resp = client
        .get(GetRequest {
            key: b"hello".to_vec(),
            linearizable: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    assert_eq!(resp.value, b"world");
}
