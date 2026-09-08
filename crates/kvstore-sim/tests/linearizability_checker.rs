//! Unit tests for the linearizability checker itself — it must accept valid
//! histories and, just as importantly, reject invalid ones.

use kvstore_sim::kv::{KvOp, KvOutput};
use kvstore_sim::{is_linearizable, Op};

fn op(call: u64, ret: u64, input: KvOp, output: KvOutput) -> Op {
    Op {
        call,
        ret,
        input,
        output,
    }
}

#[test]
fn sequential_write_then_read_is_linearizable() {
    let h = vec![
        op(0, 1, KvOp::Put { key: 1, value: 10 }, KvOutput::Written),
        op(2, 3, KvOp::Get { key: 1 }, KvOutput::Read(Some(10))),
    ];
    assert!(is_linearizable(&h));
}

#[test]
fn read_of_unwritten_value_is_not_linearizable() {
    let h = vec![
        op(0, 1, KvOp::Put { key: 1, value: 10 }, KvOutput::Written),
        op(2, 3, KvOp::Get { key: 1 }, KvOutput::Read(Some(99))),
    ];
    assert!(!is_linearizable(&h));
}

#[test]
fn stale_read_after_write_is_not_linearizable() {
    // v=1 fully precedes v=2 fully precedes the read; the read must see 2.
    let h = vec![
        op(0, 1, KvOp::Put { key: 1, value: 1 }, KvOutput::Written),
        op(2, 3, KvOp::Put { key: 1, value: 2 }, KvOutput::Written),
        op(4, 5, KvOp::Get { key: 1 }, KvOutput::Read(Some(1))),
    ];
    assert!(!is_linearizable(&h));
}

#[test]
fn concurrent_writes_allow_either_order() {
    // Both writes overlap; a following read may see either — here it sees 2,
    // which is linearizable by ordering put(1) before put(2).
    let h = vec![
        op(0, 3, KvOp::Put { key: 1, value: 1 }, KvOutput::Written),
        op(1, 4, KvOp::Put { key: 1, value: 2 }, KvOutput::Written),
        op(5, 6, KvOp::Get { key: 1 }, KvOutput::Read(Some(2))),
    ];
    assert!(is_linearizable(&h));
}

#[test]
fn compare_and_swap_semantics_are_enforced() {
    // CAS(expected=None) succeeds on an absent key, then CAS(expected=1->2).
    let good = vec![
        op(
            0,
            1,
            KvOp::Cas {
                key: 1,
                expected: None,
                new: 1,
            },
            KvOutput::Cas(true),
        ),
        op(
            2,
            3,
            KvOp::Cas {
                key: 1,
                expected: Some(1),
                new: 2,
            },
            KvOutput::Cas(true),
        ),
        op(4, 5, KvOp::Get { key: 1 }, KvOutput::Read(Some(2))),
    ];
    assert!(is_linearizable(&good));

    // A CAS that claims success against the wrong expected value is impossible.
    let bad = vec![
        op(0, 1, KvOp::Put { key: 1, value: 5 }, KvOutput::Written),
        op(
            2,
            3,
            KvOp::Cas {
                key: 1,
                expected: Some(9),
                new: 7,
            },
            KvOutput::Cas(true),
        ),
    ];
    assert!(!is_linearizable(&bad));
}

#[test]
fn per_key_independence() {
    // Two keys, interleaved; each key is independently consistent.
    let h = vec![
        op(0, 1, KvOp::Put { key: 1, value: 10 }, KvOutput::Written),
        op(0, 1, KvOp::Put { key: 2, value: 20 }, KvOutput::Written),
        op(2, 3, KvOp::Get { key: 2 }, KvOutput::Read(Some(20))),
        op(2, 3, KvOp::Get { key: 1 }, KvOutput::Read(Some(10))),
    ];
    assert!(is_linearizable(&h));
}
