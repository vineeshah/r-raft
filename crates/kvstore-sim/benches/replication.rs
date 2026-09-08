//! Throughput benchmark: how quickly a healthy 3-node cluster commits and
//! applies a batch of writes in the deterministic simulator. Measures the pure
//! consensus + apply path with no real I/O or networking overhead.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kvstore_sim::kv::KvOp;
use kvstore_sim::{SimConfig, Simulator};

fn commit_batch(ops: u64) {
    let mut sim = Simulator::new(SimConfig::new(3, 1));
    sim.run(30); // elect a leader

    let mut submitted = 0u64;
    while submitted < ops {
        if sim.submit(KvOp::Put {
            key: submitted % 16,
            value: submitted,
        }) {
            submitted += 1;
        }
        sim.step();
    }
    sim.drain(10_000);
    assert_eq!(sim.history().len() as u64, ops);
}

fn bench_replication(c: &mut Criterion) {
    let ops = 500u64;
    let mut group = c.benchmark_group("replication");
    group.throughput(Throughput::Elements(ops));
    group.sample_size(20);
    group.bench_function("commit_500_writes_3_nodes", |b| {
        b.iter(|| commit_batch(ops));
    });
    group.finish();
}

criterion_group!(benches, bench_replication);
criterion_main!(benches);
