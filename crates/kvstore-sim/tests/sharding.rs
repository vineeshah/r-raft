//! Tests for multi-Raft sharding: correct routing, per-shard linearizability
//! under a cross-shard workload, and fault isolation between shards.

use kvstore_sim::kv::KvOp;
use kvstore_sim::rng::Rng;
use kvstore_sim::{is_linearizable, ShardMap, ShardedCluster};

#[test]
fn keys_route_to_contiguous_ranges() {
    let m = ShardMap::new(4, 100);
    assert_eq!(m.shard_of(0), 0);
    assert_eq!(m.shard_of(99), 0);
    assert_eq!(m.shard_of(100), 1);
    assert_eq!(m.shard_of(250), 2);
    assert_eq!(m.shard_of(399), 3);
    // Keys beyond the keyspace clamp to the last shard.
    assert_eq!(m.shard_of(10_000), 3);
}

#[test]
fn cross_shard_workload_is_linearizable() {
    for seed in [1u64, 7, 42] {
        let mut c = ShardedCluster::new(4, 3, 100, seed);
        c.run(40); // elect a leader in every group

        let mut rng = Rng::new(seed ^ 0xABCD);
        for _ in 0..200 {
            let key = rng.below(400); // spans all four shards
            let op = match rng.below(3) {
                0 => KvOp::Get { key },
                1 => KvOp::Put {
                    key,
                    value: rng.below(1000),
                },
                _ => KvOp::Cas {
                    key,
                    expected: if rng.chance(0.5) {
                        Some(rng.below(1000))
                    } else {
                        None
                    },
                    new: rng.below(1000),
                },
            };
            c.submit(op);
            c.step();
        }
        c.drain(3000);

        assert!(c.violation().is_none(), "seed {seed}: {:?}", c.violation());
        assert!(
            is_linearizable(&c.history()),
            "seed {seed}: merged cross-shard history not linearizable"
        );
        assert!(!c.history().is_empty());
        assert!(
            c.leaders().iter().all(|l| l.is_some()),
            "every shard should have elected a leader"
        );
    }
}

#[test]
fn a_shard_outage_does_not_stop_other_shards() {
    // 3 shards, 3 nodes each. Keys: shard0 [0,99], shard1 [100,199], shard2 [200,299].
    let mut c = ShardedCluster::new(3, 3, 100, 5);
    c.run(40);

    // Take down a majority of shard 0's group: it loses quorum and stalls.
    c.shard(0).crash(2);
    c.shard(0).crash(3);
    let shard0_committed = c.shard(0).history().len();

    // Drive a workload on shards 1 and 2 (and attempt shard 0, which won't commit).
    for i in 0..80u64 {
        c.submit(KvOp::Put {
            key: 10, // shard 0 — should not commit while it lacks quorum
            value: i,
        });
        c.submit(KvOp::Put {
            key: 100 + (i % 40), // shard 1, spread across keys
            value: i,
        });
        c.submit(KvOp::Put {
            key: 200 + (i % 40), // shard 2, spread across keys
            value: i,
        });
        c.step();
    }

    assert_eq!(
        c.shard(0).history().len(),
        shard0_committed,
        "shard 0 must not commit anything without a quorum"
    );
    assert!(
        !c.shard(1).history().is_empty(),
        "shard 1 should keep serving during shard 0's outage"
    );
    assert!(
        !c.shard(2).history().is_empty(),
        "shard 2 should keep serving during shard 0's outage"
    );

    // Recover shard 0 and confirm the whole cluster converges and stays correct.
    c.shard(0).restart(2);
    c.shard(0).restart(3);
    c.drain(3000);

    assert!(c.violation().is_none(), "{:?}", c.violation());
    assert!(is_linearizable(&c.history()));
}
