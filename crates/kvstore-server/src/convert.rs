//! Conversions between the pure core's message types and the generated protobuf
//! types. The core stays transport-agnostic; all wire concerns live here.

use kvstore_proto::raft as pb;
use kvstore_raft::message::{AppendEntries, AppendEntriesResp, RequestVote, RequestVoteResp};
use kvstore_raft::LogEntry;

pub fn entry_to_pb(e: &LogEntry) -> pb::LogEntry {
    pb::LogEntry {
        term: e.term,
        index: e.index,
        command: e.command.clone(),
    }
}

pub fn entry_from_pb(e: pb::LogEntry) -> LogEntry {
    LogEntry {
        term: e.term,
        index: e.index,
        command: e.command,
    }
}

pub fn request_vote_to_pb(m: &RequestVote) -> pb::RequestVoteRequest {
    pb::RequestVoteRequest {
        term: m.term,
        candidate_id: m.from,
        last_log_index: m.last_log_index,
        last_log_term: m.last_log_term,
    }
}

pub fn request_vote_from_pb(m: pb::RequestVoteRequest) -> RequestVote {
    RequestVote {
        from: m.candidate_id,
        term: m.term,
        last_log_index: m.last_log_index,
        last_log_term: m.last_log_term,
    }
}

pub fn request_vote_resp_to_pb(m: &RequestVoteResp) -> pb::RequestVoteResponse {
    pb::RequestVoteResponse {
        term: m.term,
        vote_granted: m.vote_granted,
    }
}

/// `responder` is the node that produced the response (not carried on the wire,
/// but the core needs to know which peer replied).
pub fn request_vote_resp_from_pb(m: pb::RequestVoteResponse, responder: u64) -> RequestVoteResp {
    RequestVoteResp {
        from: responder,
        term: m.term,
        vote_granted: m.vote_granted,
    }
}

pub fn append_entries_to_pb(m: &AppendEntries) -> pb::AppendEntriesRequest {
    pb::AppendEntriesRequest {
        term: m.term,
        leader_id: m.from,
        prev_log_index: m.prev_log_index,
        prev_log_term: m.prev_log_term,
        entries: m.entries.iter().map(entry_to_pb).collect(),
        leader_commit: m.leader_commit,
    }
}

pub fn append_entries_from_pb(m: pb::AppendEntriesRequest) -> AppendEntries {
    AppendEntries {
        from: m.leader_id,
        term: m.term,
        prev_log_index: m.prev_log_index,
        prev_log_term: m.prev_log_term,
        entries: m.entries.into_iter().map(entry_from_pb).collect(),
        leader_commit: m.leader_commit,
    }
}

pub fn append_entries_resp_to_pb(m: &AppendEntriesResp) -> pb::AppendEntriesResponse {
    pb::AppendEntriesResponse {
        term: m.term,
        success: m.success,
        match_index: m.match_index,
        conflict_index: m.conflict_index,
    }
}

pub fn append_entries_resp_from_pb(
    m: pb::AppendEntriesResponse,
    responder: u64,
) -> AppendEntriesResp {
    AppendEntriesResp {
        from: responder,
        term: m.term,
        success: m.success,
        match_index: m.match_index,
        conflict_index: m.conflict_index,
    }
}
