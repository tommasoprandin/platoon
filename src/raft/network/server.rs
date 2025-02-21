use openraft::Raft;
use tokio::task::JoinHandle;
use tonic::transport::Error;
use tracing::instrument;

use crate::grpc::node::raft_service_server::RaftServiceServer;
use crate::grpc::{self, node::raft_service_server::RaftService};

use crate::grpc::utils;

#[derive(Clone)]
pub struct RaftServer {
    raft: Raft<crate::raft::types::TypeConfig>,
    port: u16,
}

impl RaftServer {
    pub fn new(raft_instance: Raft<crate::raft::types::TypeConfig>, port: u16) -> Self {
        Self {
            raft: raft_instance,
            port,
        }
    }

    pub fn start(self) -> JoinHandle<Result<(), Error>> {
        tokio::task::spawn(async move {
            let address = format!("0.0.0.0:{}", self.port);
            tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(self))
                .serve(address.parse().expect("Invalid address"))
                .await
        })
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    #[instrument(level = "debug", skip(self))]
    async fn append_entries(
        &self,
        request: tonic::Request<grpc::node::AppendEntriesRequest>,
    ) -> Result<tonic::Response<grpc::node::AppendEntriesResponse>, tonic::Status> {
        let request = request.into_inner().into();
        let res = self.raft.append_entries(request).await;

        match res {
            Ok(res) => {
                tracing::debug!("append_entries success");
                Ok(tonic::Response::new(res.into()))
            }
            Err(err) => {
                tracing::error!(
                    message = "Failure in append_entries",
                    error = err.to_string()
                );
                Err(utils::raft_error_to_tonic_status(err))
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn vote(
        &self,
        request: tonic::Request<grpc::node::VoteRequest>,
    ) -> Result<tonic::Response<grpc::node::VoteResponse>, tonic::Status> {
        let request = request.into_inner().into();
        let res = self.raft.vote(request).await;

        match res {
            Ok(res) => {
                tracing::debug!("vote success");
                Ok(tonic::Response::new(res.into()))
            }
            Err(err) => {
                tracing::error!(message = "Failure in vote", error = err.to_string());
                Err(utils::raft_error_to_tonic_status(err))
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<grpc::node::SnapshotRequest>,
    ) -> Result<tonic::Response<grpc::node::SnapshotResponse>, tonic::Status> {
        let request = request.into_inner();
        let vote = request.vote.into();
        let snapshot = request.snapshot.into();

        let res = self.raft.install_full_snapshot(vote, snapshot).await;

        match res {
            Ok(res) => {
                tracing::debug!("install_snapshot succeeded");
                Ok(tonic::Response::new(res.into()))
            }
            Err(err) => {
                tracing::error!(
                    message = "Failure in install_snapshot",
                    error = err.to_string()
                );
                Err(utils::fatal_to_tonic_status(err))
            }
        }
    }
}
