//! Throughput benchmarks for the RocksDB-backed state machine.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kvstore_raft::StateMachine;
use kvstore_storage::{Command, RocksEngine};
use tempfile::TempDir;

fn bench_put(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut sm = engine.state_machine();
    let mut i: u64 = 0;

    let mut group = c.benchmark_group("state_machine");
    group.throughput(Throughput::Elements(1));
    group.bench_function("put", |b| {
        b.iter(|| {
            let cmd = Command::Put {
                key: i.to_be_bytes().to_vec(),
                value: b"value".to_vec(),
            };
            i += 1;
            sm.apply(&cmd.encode().unwrap()).unwrap();
        });
    });
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let engine = RocksEngine::open(dir.path()).unwrap();
    let mut sm = engine.state_machine();

    // Pre-populate 10k keys.
    for i in 0..10_000u64 {
        let cmd = Command::Put {
            key: i.to_be_bytes().to_vec(),
            value: b"value".to_vec(),
        };
        sm.apply(&cmd.encode().unwrap()).unwrap();
    }

    let mut i: u64 = 0;
    let mut group = c.benchmark_group("state_machine");
    group.throughput(Throughput::Elements(1));
    group.bench_function("get", |b| {
        b.iter(|| {
            let key = (i % 10_000).to_be_bytes();
            i += 1;
            sm.get(&key).unwrap()
        });
    });
    group.finish();
}

criterion_group!(benches, bench_put, bench_get);
criterion_main!(benches);
