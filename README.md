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
| `kvstore-sim` | Deterministic simulator, fault injection, linearizability checker |
| `xtask` | Developer task runner (cluster up/down, bench) |

The Raft core is deliberately **side-effect-free**: it consumes messages and
logical ticks, and emits actions for the host to perform. That single decision
makes the whole cluster runnable inside one deterministic simulation.

**Sharding (multi-Raft).** To scale writes past a single group, the keyspace is
range-partitioned across many independent Raft groups (as in TiKV/CockroachDB), so
disjoint key ranges commit in parallel and a fault in one shard's group can't take
down another. This is implemented and verified in `kvstore-sim` (routing,
per-shard consensus, cross-shard linearizability, fault isolation).

## Roadmap

- [x] **Phase 0** — workspace, tooling, CI/CD, protobuf pipeline
- [x] **Phase 1** — RocksDB storage engine (log, hard state, snapshots, recovery)
- [x] **Phase 2** — Raft from scratch (election, replication, commit, recovery)
- [x] **Phase 3** — gRPC networking, `kvctl`, docker-compose cluster
- [x] **Phase 4** — deterministic simulation, linearizability checking, observability, benchmarks
- [x] **Phase 5** — multi-Raft sharding: range partitioning + routing + per-shard
  fault isolation, modeled and verified in the simulator (networked-server
  integration with a live placement driver is the next step)

## Correctness & performance

Consensus is only worth anything if it's correct under failure, so correctness
is tested directly rather than hoped for:

- **Deterministic simulation (`kvstore-sim`).** The pure core runs inside a
  single-process simulator with a virtual, seeded network: randomized message
  delay/reorder, drops, partitions, and node crash/restart. Every run is
  reproducible from its seed, so a bug found once reproduces exactly.
- **Continuous safety invariants.** The simulator asserts *at most one leader
  per term* and *no two different entries committed at the same index* on every
  tick.
- **Linearizability checking.** A from-scratch Wing–Gong/Lowe checker verifies
  the recorded client history is linearizable — under faults, not just on the
  happy path. Its own unit tests confirm it *rejects* non-linearizable histories
  (so passing means something).
- **Observability.** Prometheus metrics (`kv_raft_term`, `kv_raft_commit_index`,
  `kv_is_leader`, applied/proposal counters, leader changes) via a `/metrics`
  endpoint, with a provisioned Grafana dashboard in the compose stack.
- **Benchmarks (criterion).** e.g. the 3-node consensus + apply path commits
  **~326K writes/sec** in the in-memory simulator (pure algorithm, no disk or
  network), and RocksDB state-machine put/get throughput. Run `cargo xtask
  bench`.

## Tech stack

Rust · tokio · tonic/prost (gRPC) · RocksDB · tracing + Prometheus + Grafana ·
custom deterministic simulator + linearizability checker · proptest · criterion ·
GitHub Actions · Docker Compose

## Development

```sh
cargo build --workspace     # build everything
cargo test  --workspace     # run tests (incl. 3-node gRPC e2e)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all             # format

cargo xtask cluster-up      # build + start the 3-node docker cluster
kvctl --endpoint http://127.0.0.1:5401 put foo bar
kvctl --endpoint http://127.0.0.1:5401 get foo --linearizable
docker compose stop node1   # kill a node; the cluster keeps serving
cargo xtask cluster-down    # tear it down
```

Requires the Rust stable toolchain and `protoc` (protobuf compiler).

## License

MIT
