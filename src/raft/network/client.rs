use std::future::Future;

use openraft::{
    error::{
        Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Timeout,
        Unreachable,
    },
    network::RPCOption,
    raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse},
    AnyError, OptionalSend, RPCTypes, RaftNetwork, RaftNetworkFactory,
};
use tracing::instrument;

use crate::{
    grpc::node::{raft_service_client::RaftServiceClient, SnapshotRequest},
    raft::{self, types::TypeConfig},
};

use crate::grpc::utils::tonic_status_to_rpc_error;

#[derive(Debug)]
pub struct Client(String);

pub struct ClientFactory();

impl RaftNetworkFactory<TypeConfig> for ClientFactory {
    type Network = Client;

    async fn new_client(
        &mut self,
        _target: raft::types::NodeId,
        node: &raft::types::Node,
    ) -> Self::Network {
        Client(node.addr.to_owned())
    }
}

impl RaftNetwork<TypeConfig> for Client {
    #[instrument(level = "debug")]
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<raft::types::NodeId>,
        RPCError<raft::types::NodeId, raft::types::Node, RaftError<raft::types::NodeId>>,
    > {
        let mut client = RaftServiceClient::connect(format!("http://{}", &self.0))
            .await
            .map_err(|err| {
                tracing::error!(
                    message = format!("Error connecting to {}", &self.0),
                    error = err.to_string()
                );
                RPCError::Unreachable(Unreachable::new(&err))
            })?;

        let mut request = tonic::Request::new(rpc.into());
        request.set_timeout(option.soft_ttl());

        let response = client.append_entries(request).await;

        match response {
            Ok(res) => {
                tracing::debug!("append_entries RPC success");
                Ok(res.into_inner().into())
            }
            Err(status) => {
                tracing::error!(message = "Error sending append_entries RPC", ?status);
                Err(tonic_status_to_rpc_error(status))
            }
        }
    }

    #[instrument(level = "debug")]
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<raft::types::NodeId>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<raft::types::NodeId>,
        RPCError<raft::types::NodeId, raft::types::Node, RaftError<raft::types::NodeId>>,
    > {
        let mut client = RaftServiceClient::connect(format!("http://{}", &self.0))
            .await
            .map_err(|err| {
                tracing::error!(
                    message = format!("Error connecting to {}", &self.0),
                    error = err.to_string()
                );
                RPCError::Unreachable(Unreachable::new(&err))
            })?;

        let mut request = tonic::Request::new(rpc.into());
        request.set_timeout(option.soft_ttl());

        let response = client.vote(request).await;

        match response {
            Ok(res) => {
                tracing::debug!("vote RPC success");
                Ok(res.into_inner().into())
            }
            Err(status) => {
                tracing::error!(message = "Error sending vote RPC", ?status);
                Err(tonic_status_to_rpc_error(status))
            }
        }
    }

    #[instrument(level = "debug", skip(_cancel))]
    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<raft::types::NodeId>,
        snapshot: openraft::Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<
        SnapshotResponse<raft::types::NodeId>,
        StreamingError<TypeConfig, Fatal<raft::types::NodeId>>,
    > {
        let mut client = RaftServiceClient::connect(format!("http://{}", &self.0))
            .await
            .map_err(|err| {
                tracing::error!(
                    message = format!("Error connecting to {}", &self.0),
                    error = err.to_string()
                );
                StreamingError::Unreachable(Unreachable::new(&err))
            })?;

        let rpc = SnapshotRequest {
            vote: vote.into(),
            snapshot: snapshot.into(),
        };

        let mut request = tonic::Request::new(rpc.into());
        request.set_timeout(option.soft_ttl());

        let response = client.install_snapshot(request).await;

        match response {
            Ok(res) => {
                tracing::debug!("full_snapshot RPC success");
                Ok(res.into_inner().into())
            }
            Err(status) => {
                tracing::error!(message = "Error sending full_snapshot RPC", ?status);
                Err(StreamingError::Network(NetworkError::new(
                    &AnyError::error(status.to_string()),
                )))
            }
        }
    }
}
