use std::future::Future;

use openraft::{
    error::{Fatal, RPCError, RaftError, ReplicationClosed, StreamingError},
    network::RPCOption,
    raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse},
    OptionalSend, RaftNetwork, RaftNetworkFactory,
};
use tracing::{instrument, Level};

use crate::{
    grpc::node::{raft_service_client::RaftServiceClient, SnapshotRequest},
    raft::{self, types::TypeConfig},
};

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
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<raft::types::NodeId>,
        RPCError<raft::types::NodeId, raft::types::Node, RaftError<raft::types::NodeId>>,
    > {
        let mut client = RaftServiceClient::connect(format!("https://{}", &self.0))
            .await
            .unwrap();

        let request = tonic::Request::new(rpc.into());

        let response = client.append_entries(request).await.unwrap();
        Ok(response.into_inner().into())
    }
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<raft::types::NodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<raft::types::NodeId>,
        RPCError<raft::types::NodeId, raft::types::Node, RaftError<raft::types::NodeId>>,
    > {
        let addr = format!("http://{}", &self.0);
        tracing::event!(Level::INFO, address = addr);
        let mut client = RaftServiceClient::connect(addr).await.unwrap();

        let request = tonic::Request::new(rpc.into());

        let response = client.vote(request).await.unwrap();
        Ok(response.into_inner().into())
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<raft::types::NodeId>,
        snapshot: openraft::Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<raft::types::NodeId>,
        StreamingError<TypeConfig, Fatal<raft::types::NodeId>>,
    > {
        let mut client = RaftServiceClient::connect(format!("http://{}", &self.0))
            .await
            .unwrap();

        let rpc = SnapshotRequest {
            vote: vote.into(),
            snapshot: snapshot.into(),
        };

        let request = tonic::Request::new(rpc.into());

        let response = client.install_snapshot(request).await.unwrap();
        Ok(response.into_inner().into())
    }
}
