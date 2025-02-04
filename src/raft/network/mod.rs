use std::sync::Arc;

use openraft::{
    error::{InstallSnapshotError, RPCError, RaftError},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    OptionalSend, OptionalSync, RaftNetwork, RaftNetworkFactory,
};
use tokio::sync::RwLock;

use super::{
    storage::{engine::StorageEngine, state_machine::StateMachine},
    types,
};

pub struct Network {}

pub struct NetworkFactory<'a, SE: StorageEngine + OptionalSend + OptionalSync> {
    state_machine: &'a StateMachine<SE>,
}

impl<SE> RaftNetworkFactory<types::TypeConfig> for NetworkFactory<'static, SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync,
{
    type Network = Network;

    #[doc = " Create a new network instance sending RPCs to the target node."]
    #[doc = ""]
    #[doc = " This function should **not** create a connection but rather a client that will connect when"]
    #[doc = " required. Therefore there is chance it will build a client that is unable to send out"]
    #[doc = " anything, e.g., in case the Node network address is configured incorrectly. But this method"]
    #[doc = " does not return an error because openraft can only ignore it."]
    #[doc = ""]
    #[doc = " The method is intentionally async to give the implementation a chance to use asynchronous"]
    #[doc = " sync primitives to serialize access to the common internal object, if needed."]
    async fn new_client(&mut self, target: types::NodeId, node: &types::Node) -> Self::Network {
        todo!()
    }
}

impl RaftNetwork<types::TypeConfig> for Network {
    #[doc = " Send an AppendEntries RPC to the target."]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<types::TypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<types::NodeId>,
        RPCError<types::NodeId, types::Node, RaftError<types::NodeId>>,
    > {
        todo!()
    }

    #[doc = " Send an InstallSnapshot RPC to the target."]
    #[cfg(not(feature = "generic-snapshot-data"))]
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<types::TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<types::NodeId>,
        RPCError<types::NodeId, types::Node, RaftError<types::NodeId, InstallSnapshotError>>,
    > {
        todo!()
    }

    #[doc = " Send a RequestVote RPC to the target."]
    async fn vote(
        &mut self,
        rpc: VoteRequest<types::NodeId>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<types::NodeId>,
        RPCError<types::NodeId, types::Node, RaftError<types::NodeId>>,
    > {
        todo!()
    }
}
