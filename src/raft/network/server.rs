use std::sync::Arc;

use openraft::{raft, Raft};
use tokio::sync::RwLock;

use crate::grpc::{self, node::raft_service_server::RaftService};

use super::utils;

struct Server {
    raft: Arc<RwLock<Raft<crate::raft::types::TypeConfig>>>,
}

impl Server {
    fn new(raft_instance: Arc<RwLock<Raft<crate::raft::types::TypeConfig>>>) -> Self {
        Self {
            raft: raft_instance.clone(),
        }
    }
}

#[tonic::async_trait]
impl RaftService for Server {
    async fn append_entries(
        &self,
        request: tonic::Request<grpc::node::AppendEntriesRequest>,
    ) -> Result<tonic::Response<grpc::node::AppendEntriesResponse>, tonic::Status> {
        let raft_instance = self.raft.write().await;

        let request = request.into_inner().into();
        let res = raft_instance.append_entries(request).await;

        match res {
            Ok(res) => Ok(tonic::Response::new(res.into())),
            Err(err) => Err(utils::raft_error_to_tonic_status(err)),
        }
    }

    async fn vote(
        &self,
        request: tonic::Request<grpc::node::VoteRequest>,
    ) -> Result<tonic::Response<grpc::node::VoteResponse>, tonic::Status> {
        let raft_instance = self.raft.write().await;

        let request = request.into_inner().into();
        let res = raft_instance.vote(request).await;

        match res {
            Ok(res) => Ok(tonic::Response::new(res.into())),
            Err(err) => Err(utils::raft_error_to_tonic_status(err)),
        }
    }
    async fn install_snapshot(
        &self,
        request: tonic::Request<grpc::node::SnapshotRequest>,
    ) -> Result<tonic::Response<grpc::node::SnapshotResponse>, tonic::Status> {
        let raft_instance = self.raft.write().await;

        let request = request.into_inner();
        let vote = request.vote.into();
        let snapshot = request.snapshot.into();

        let res = raft_instance.install_full_snapshot(vote, snapshot).await;

        match res {
            Ok(res) => Ok(tonic::Response::new(res.into())),
            Err(err) => Err(utils::fatal_to_tonic_status(err)),
        }
    }
}
