//! Generated protobuf types and gRPC service stubs for the distributed KV store.
//!
//! Two protocol surfaces live here:
//! - [`kv`]: the client-facing key-value API.
//! - [`raft`]: the inter-node Raft consensus RPCs.

// tonic generates `Result<_, tonic::Status>` return types; `Status` is large by
// clippy's threshold. This is generated code we don't control, so allow it here.
#![allow(clippy::result_large_err)]

/// Client-facing key-value service (`kvstore.v1`).
pub mod kv {
    tonic::include_proto!("kvstore.v1");
}

/// Inter-node Raft consensus service (`kvstore.raft.v1`).
pub mod raft {
    tonic::include_proto!("kvstore.raft.v1");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_messages_roundtrip() {
        // Sanity check that generated types are usable.
        let req = kv::PutRequest {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        assert_eq!(req.key, b"k");
    }

    #[test]
    fn raft_log_entry_constructs() {
        let e = raft::LogEntry {
            term: 1,
            index: 1,
            command: vec![],
        };
        assert_eq!(e.term, 1);
    }
}
