# cameron — a distributed key-value store in Rust

A replicated, sharded key-value store built to demonstrate the core techniques
behind systems like **etcd**, **TiKV**, and **CockroachDB**: **Raft consensus
implemented from scratch**, durable log-structured storage, a gRPC wire
protocol, and — the part most projects skip — **verified correctness under
network faults**.

> Status: **Phase 0 (scaffolding)**. See the roadmap below.

## Why this project

Distributed storage is where the hard, interesting problems live: consensus,
replication, failure handling, and consistency. This project implements those
directly rather than gluing libraries together — the Raft algorithm is written
from first principles and tested with **deterministic simulation** (inject
partitions, clock skew, and crashes; reproduce any failure from a seed) plus a
**linearizability checker** to prove the store stays consistent when the network
misbehaves.

## Architecture

```
        clients ──gRPC──▶ router / coordinator (multi-Raft, Phase 5)
                                │
              ┌─────────────────┼─────────────────┐
         Raft group        Raft group        Raft group     ← inter-node gRPC
         (node A/B/C)       (node …)          (node …)
              │                                    │
           RocksDB   (Raft log + state machine, per node)
```

### Crates

| Crate | Responsibility |
|-------|----------------|
| `kvstore-proto` | Protobuf definitions + generated gRPC (client API + Raft RPC) |
| `kvstore-raft` | Raft core — pure, deterministic, I/O-free consensus logic |
| `kvstore-storage` | `RaftStorage` + `StateMachine` over RocksDB |
| `kvstore-server` | `kvstored` node: wires Raft to tonic/tokio |
| `kvstore-client` | `kvctl` CLI + client library |
| `xtask` | Developer task runner (cluster up/down, bench) |

The Raft core is deliberately **side-effect-free**: it consumes messages and
logical ticks, and emits actions for the host to perform. That single decision
makes the whole cluster runnable inside one deterministic simulation.

## Roadmap

- [x] **Phase 0** — workspace, tooling, CI/CD, protobuf pipeline
- [ ] **Phase 1** — RocksDB storage engine (log, hard state, snapshots, recovery)
- [ ] **Phase 2** — Raft from scratch (election, replication, snapshots, membership)
- [ ] **Phase 3** — gRPC networking, `kvctl`, docker-compose cluster
- [ ] **Phase 4** — deterministic simulation, linearizability checking, observability, benchmarks
- [ ] **Phase 5** — multi-Raft sharding (range partitioning, routing, rebalancing)

## Tech stack

Rust · tokio · tonic/prost (gRPC) · RocksDB · tracing + Prometheus + Grafana ·
madsim (deterministic simulation) · proptest · criterion · GitHub Actions ·
Docker Compose

## Development

```sh
cargo build --workspace     # build everything
cargo test  --workspace     # run tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all             # format
cargo xtask cluster-up      # (Phase 3) local cluster
```

Requires the Rust stable toolchain and `protoc` (protobuf compiler).

## License

MIT
