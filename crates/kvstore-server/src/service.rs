//! gRPC service implementations: the inter-node `Raft` service and the
//! client-facing `Kv` service. Both are thin adapters that translate protobuf
//! requests into driver events and translate the results back.

use kvstore_proto::kv::kv_server::Kv;
use kvstore_proto::kv::{
    CasRequest, CasResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest,
    PutResponse,
};
use kvstore_proto::raft::raft_server::Raft;
use kvstore_proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use kvstore_raft::message::Message;
use kvstore_storage::{Command, CommandResult};
use tonic::{Request, Response, Status};

use crate::convert;
use crate::driver::{Handle, ProposeReply, ReadReply};

/// Inter-node consensus RPCs.
pub struct RaftSvc {
    handle: Handle,
}

impl RaftSvc {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl Raft for RaftSvc {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let rv = convert::request_vote_from_pb(request.into_inner());
        match self.handle.request_vote(rv).await {
            Ok(Message::RequestVoteResp(resp)) => {
                Ok(Response::new(convert::request_vote_resp_to_pb(&resp)))
            }
            Ok(_) => Err(Status::internal("unexpected reply to RequestVote")),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let ae = convert::append_entries_from_pb(request.into_inner());
        match self.handle.append_entries(ae).await {
            Ok(Message::AppendEntriesResp(resp)) => {
                Ok(Response::new(convert::append_entries_resp_to_pb(&resp)))
            }
            Ok(_) => Err(Status::internal("unexpected reply to AppendEntries")),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn install_snapshot(
        &self,
        _request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "snapshot transfer lands in a later phase",
        ))
    }
}

/// Client-facing key-value RPCs.
pub struct KvSvc {
    handle: Handle,
}

impl KvSvc {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    /// Propose a write command and map the outcome to a gRPC result.
    async fn write(&self, command: Command) -> Result<CommandResult, Status> {
        let bytes = command
            .encode()
            .map_err(|e| Status::internal(e.to_string()))?;
        match self.handle.propose(bytes).await {
            Ok(ProposeReply::Applied(result)) => {
                CommandResult::decode(&result).map_err(|e| Status::internal(e.to_string()))
            }
            Ok(ProposeReply::NotLeader(leader)) => Err(not_leader(leader)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[tonic::async_trait]
impl Kv for KvSvc {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        match self.handle.read(req.key, req.linearizable).await {
            Ok(ReadReply::Value(Some(value))) => {
                Ok(Response::new(GetResponse { found: true, value }))
            }
            Ok(ReadReply::Value(None)) => Ok(Response::new(GetResponse {
                found: false,
                value: Vec::new(),
            })),
            Ok(ReadReply::NotLeader(leader)) => Err(not_leader(leader)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        self.write(Command::Put {
            key: req.key,
            value: req.value,
        })
        .await?;
        Ok(Response::new(PutResponse {}))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        self.write(Command::Delete { key: req.key }).await?;
        Ok(Response::new(DeleteResponse {}))
    }

    async fn compare_and_swap(
        &self,
        request: Request<CasRequest>,
    ) -> Result<Response<CasResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .write(Command::CompareAndSwap {
                key: req.key,
                expected: req.expected,
                new_value: req.new_value,
            })
            .await?;
        let swapped = matches!(result, CommandResult::Swapped(true));
        Ok(Response::new(CasResponse { swapped }))
    }
}

fn not_leader(leader: Option<u64>) -> Status {
    match leader {
        Some(id) => Status::unavailable(format!("not leader; try node {id}")),
        None => Status::unavailable("not leader; leader unknown"),
    }
}
