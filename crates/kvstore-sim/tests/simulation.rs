//! Deterministic simulation tests: run a randomized client workload against a
//! simulated 3-node cluster while injecting faults, then assert that (a) no
//! safety invariant was ever violated and (b) the recorded history is
//! linearizable. Every run is seeded, so any failure reproduces exactly.

use kvstore_sim::kv::KvOp;
use kvstore_sim::{is_linearizable, SimConfig, Simulator};

const KEYS: [u64; 3] = [0, 1, 2];

/// Run one full scenario at `seed`. When `faults` is set, inject a partition,
/// heal, crash, and restart at fixed points, plus a small message-drop rate.
fn run_scenario(seed: u64, faults: bool) {
    let mut cfg = SimConfig::new(3, seed);
    cfg.max_delay = 2;
    cfg.drop_prob = if faults { 0.05 } else { 0.0 };

    let mut sim = Simulator::new(cfg);
    sim.run(30); // settle an initial leader

    for round in 0..150u64 {
        if faults {
            match round {
                40 => sim.partition(&[1], &[2, 3]),
                75 => sim.heal_all(),
                100 => sim.crash(2),
                125 => sim.restart(2),
                _ => {}
            }
        }

        // Decide whether to submit an operation this round (deterministic).
        if sim.rng().below(10) < 4 {
            let key = KEYS[sim.rng().below(KEYS.len() as u64) as usize];
            let op = match sim.rng().below(3) {
                0 => KvOp::Get { key },
                1 => KvOp::Put {
                    key,
                    value: sim.rng().below(100),
                },
                _ => {
                    let expected = if sim.rng().chance(0.5) {
                        Some(sim.rng().below(100))
                    } else {
                        None
                    };
                    KvOp::Cas {
                        key,
                        expected,
                        new: sim.rng().below(100),
                    }
                }
            };
            sim.submit(op);
        }
        sim.step();
    }

    // Heal, restart, and drain so every committed op is applied on its origin.
    sim.drain(3000);

    assert!(
        sim.violation().is_none(),
        "seed {seed} (faults={faults}) violated a safety invariant: {:?}",
        sim.violation()
    );
    assert!(
        is_linearizable(sim.history()),
        "seed {seed} (faults={faults}) produced a non-linearizable history of {} ops",
        sim.history().len()
    );
    assert!(
        !sim.history().is_empty(),
        "seed {seed}: expected some completed operations"
    );
}

#[test]
fn healthy_cluster_is_linearizable() {
    for seed in [1u64, 2, 3, 7, 42] {
        run_scenario(seed, false);
    }
}

#[test]
fn cluster_under_faults_is_linearizable() {
    for seed in [1u64, 7, 42, 123, 9999] {
        run_scenario(seed, true);
    }
}

#[test]
fn a_leader_emerges_and_serves() {
    let mut sim = Simulator::new(SimConfig::new(5, 55));
    sim.run(60);
    assert!(
        sim.leader().is_some(),
        "a 5-node cluster should elect a leader"
    );
    assert!(sim.submit(KvOp::Put { key: 1, value: 1 }));
    sim.drain(500);
    assert!(sim.violation().is_none());
    assert!(is_linearizable(sim.history()));
}
