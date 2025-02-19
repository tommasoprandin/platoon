use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    io::Cursor,
};

use openraft::{
    error::{
        Fatal, Infallible, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError,
        Timeout, Unreachable,
    },
    raft::{self, SnapshotResponse, VoteRequest, VoteResponse},
    AnyError, CommittedLeaderId, EntryPayload, Snapshot, StoredMembership,
};
use tonic::Code::{Aborted, Cancelled, DeadlineExceeded, Unavailable};

use crate::{
    grpc::{self, node::append_entries_response::Status, types::NodeIdSet},
    raft::types::{Request, TypeConfig},
};

use super::app::UpdateVehicleResponse;

impl From<grpc::types::Vote> for crate::raft::types::Vote {
    fn from(value: grpc::types::Vote) -> Self {
        crate::raft::types::Vote {
            leader_id: openraft::LeaderId {
                term: value.term,
                voted_for: value.voted_for,
            },
            committed: value.committed,
        }
    }
}

impl From<crate::raft::types::Vote> for grpc::types::Vote {
    fn from(value: crate::raft::types::Vote) -> Self {
        grpc::types::Vote {
            term: value.leader_id.term,
            voted_for: value.leader_id.voted_for,
            committed: value.committed,
        }
    }
}

impl From<grpc::types::LogId> for crate::raft::types::LogId {
    fn from(value: grpc::types::LogId) -> Self {
        crate::raft::types::LogId {
            leader_id: CommittedLeaderId::new(value.term, 0),
            index: value.index,
        }
    }
}

impl From<crate::raft::types::LogId> for grpc::types::LogId {
    fn from(value: crate::raft::types::LogId) -> Self {
        grpc::types::LogId {
            term: value.leader_id.term,
            index: value.index,
        }
    }
}

impl From<grpc::types::Vehicle> for crate::types::Vehicle {
    fn from(value: grpc::types::Vehicle) -> Self {
        crate::types::Vehicle {
            id: value.id,
            position: crate::types::VehiclePosition {
                x: value.x,
                y: value.y,
            },
            speed: crate::types::VehicleSpeed {
                speed: value.speed,
                heading: value.heading,
            },
        }
    }
}

impl From<crate::types::Vehicle> for grpc::types::Vehicle {
    fn from(value: crate::types::Vehicle) -> Self {
        grpc::types::Vehicle {
            id: value.id,
            x: value.position.x,
            y: value.position.y,
            speed: value.speed.speed,
            heading: value.speed.heading,
        }
    }
}

impl From<grpc::types::Node> for crate::raft::types::Node {
    fn from(value: grpc::types::Node) -> Self {
        crate::raft::types::Node {
            addr: value.address,
        }
    }
}

impl From<crate::raft::types::Node> for grpc::types::Node {
    fn from(value: crate::raft::types::Node) -> Self {
        grpc::types::Node {
            address: value.addr,
        }
    }
}

impl From<grpc::types::Membership>
    for openraft::Membership<crate::raft::types::NodeId, crate::raft::types::Node>
{
    fn from(value: grpc::types::Membership) -> Self {
        let configs = value
            .configs
            .into_iter()
            .map(|node_id_set| node_id_set.node_ids.into_iter().collect::<BTreeSet<u64>>())
            .collect();

        let mut nodes = BTreeMap::new();
        for (k, v) in value.nodes {
            nodes.insert(k, v.into());
        }
        openraft::Membership::new(configs, nodes)
    }
}

impl From<openraft::Membership<crate::raft::types::NodeId, crate::raft::types::Node>>
    for grpc::types::Membership
{
    fn from(
        value: openraft::Membership<crate::raft::types::NodeId, crate::raft::types::Node>,
    ) -> Self {
        let configs = value
            .get_joint_config()
            .iter()
            .map(|set| {
                let mut vec_set = NodeIdSet { node_ids: vec![] };

                for entry in set.iter() {
                    vec_set.node_ids.push(entry.to_owned());
                }

                vec_set
            })
            .collect();

        let mut nodes = HashMap::new();

        for (k, v) in value.nodes() {
            nodes.insert(k.to_owned(), v.to_owned().into());
        }

        grpc::types::Membership { configs, nodes }
    }
}

impl From<grpc::types::Entry> for crate::raft::types::LogEntry {
    fn from(value: grpc::types::Entry) -> Self {
        let log_id = value.log_id.into();

        let payload = match value.payload {
            None => EntryPayload::Blank,
            Some(value) => match value {
                grpc::types::entry::Payload::Set(payload) => {
                    EntryPayload::Normal(Request::Set(payload.vehicle.into()))
                }
                grpc::types::entry::Payload::Delete(payload) => {
                    EntryPayload::Normal(Request::Delete(payload.vehicle_id))
                }
                grpc::types::entry::Payload::Membership(payload) => {
                    EntryPayload::Membership(payload.membership.into())
                }
            },
        };
        crate::raft::types::LogEntry { log_id, payload }
    }
}

impl From<crate::raft::types::LogEntry> for grpc::types::Entry {
    fn from(value: crate::raft::types::LogEntry) -> Self {
        let log_id = value.log_id.into();
        let payload = match value.payload {
            EntryPayload::Blank => None,
            EntryPayload::Normal(payload) => Some(match payload {
                Request::Set(vehicle) => {
                    grpc::types::entry::Payload::Set(grpc::types::SetPayload {
                        vehicle: vehicle.into(),
                    })
                }
                Request::Delete(vehicle_id) => {
                    grpc::types::entry::Payload::Delete(grpc::types::DeletePayload { vehicle_id })
                }
            }),
            EntryPayload::Membership(membership) => Some(grpc::types::entry::Payload::Membership(
                grpc::types::MembershipPayload {
                    membership: membership.into(),
                },
            )),
        };
        grpc::types::Entry { log_id, payload }
    }
}

impl From<grpc::node::AppendEntriesRequest> for raft::AppendEntriesRequest<TypeConfig> {
    fn from(value: grpc::node::AppendEntriesRequest) -> Self {
        let vote = value.vote.into();
        let prev_log_id = value.prev_log_id.map(|log_id| log_id.into());
        let leader_commit = value.leader_commit.map(|log_id| log_id.into());
        let entries = value
            .entries
            .into_iter()
            .map(|entry| entry.into())
            .collect();
        raft::AppendEntriesRequest {
            vote,
            prev_log_id,
            leader_commit,
            entries,
        }
    }
}

impl From<raft::AppendEntriesRequest<TypeConfig>> for grpc::node::AppendEntriesRequest {
    fn from(value: raft::AppendEntriesRequest<TypeConfig>) -> Self {
        let vote = value.vote.into();
        let prev_log_id = value.prev_log_id.map(|log_id| log_id.into());
        let entries = value
            .entries
            .into_iter()
            .map(|entry| entry.into())
            .collect();
        let leader_commit = value.leader_commit.map(|log_id| log_id.into());
        grpc::node::AppendEntriesRequest {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        }
    }
}

impl From<raft::AppendEntriesResponse<crate::raft::types::NodeId>>
    for grpc::node::AppendEntriesResponse
{
    fn from(value: raft::AppendEntriesResponse<crate::raft::types::NodeId>) -> Self {
        match value {
            raft::AppendEntriesResponse::Success => grpc::node::AppendEntriesResponse {
                status: Some(Status::Success(grpc::node::Success {})),
            },
            raft::AppendEntriesResponse::PartialSuccess(log_id) => {
                grpc::node::AppendEntriesResponse {
                    status: Some(Status::PartialSuccess(grpc::node::PartialSuccess {
                        upto: log_id.map(|log_id| log_id.into()),
                    })),
                }
            }
            raft::AppendEntriesResponse::Conflict => grpc::node::AppendEntriesResponse {
                status: Some(Status::Conflict(grpc::node::Conflict {})),
            },
            raft::AppendEntriesResponse::HigherVote(vote) => grpc::node::AppendEntriesResponse {
                status: Some(Status::HigherVote(grpc::node::HigherVote {
                    mine: vote.into(),
                })),
            },
        }
    }
}

impl From<grpc::node::AppendEntriesResponse>
    for raft::AppendEntriesResponse<crate::raft::types::NodeId>
{
    fn from(value: grpc::node::AppendEntriesResponse) -> Self {
        match value.status {
            None => unreachable!(),
            Some(value) => match value {
                Status::Success(_) => raft::AppendEntriesResponse::Success,
                Status::PartialSuccess(partial_success) => {
                    raft::AppendEntriesResponse::PartialSuccess(
                        partial_success.upto.map(|log_id| log_id.into()),
                    )
                }
                Status::Conflict(_) => raft::AppendEntriesResponse::Conflict,
                Status::HigherVote(higher_vote) => {
                    raft::AppendEntriesResponse::HigherVote(higher_vote.mine.into())
                }
            },
        }
    }
}

pub fn raft_error_to_tonic_status(
    value: RaftError<crate::raft::types::NodeId, Infallible>,
) -> tonic::Status {
    match value {
        openraft::error::RaftError::APIError(_) => {
            tonic::Status::internal("Raft instance returned unknown API error")
        }
        openraft::error::RaftError::Fatal(fatal) => fatal_to_tonic_status(fatal),
    }
}

pub fn tonic_status_to_rpc_error(
    value: tonic::Status,
) -> RPCError<
    crate::raft::types::NodeId,
    crate::raft::types::Node,
    RaftError<crate::raft::types::NodeId>,
> {
    let err = AnyError::error(value.to_string());
    RPCError::Network(NetworkError::new(&err))
}

pub fn fatal_to_tonic_status(value: Fatal<crate::raft::types::NodeId>) -> tonic::Status {
    match value {
        openraft::error::Fatal::StorageError(storage_error) => {
            tonic::Status::internal(format!("Error in internal storage: {storage_error:?}"))
        }
        openraft::error::Fatal::Panicked => {
            tonic::Status::internal("Internal Raft instance thread panicked")
        }
        openraft::error::Fatal::Stopped => tonic::Status::internal("Raft instance is stopped"),
    }
}

impl From<grpc::node::VoteRequest> for VoteRequest<crate::raft::types::NodeId> {
    fn from(value: grpc::node::VoteRequest) -> Self {
        let vote = value.vote.into();
        let last_log_id = value.last_log_id.map(|log_id| log_id.into());
        VoteRequest { vote, last_log_id }
    }
}

impl From<VoteRequest<crate::raft::types::NodeId>> for grpc::node::VoteRequest {
    fn from(value: VoteRequest<crate::raft::types::NodeId>) -> Self {
        let vote = value.vote.into();
        let last_log_id = value.last_log_id.map(|log_id| log_id.into());
        grpc::node::VoteRequest { vote, last_log_id }
    }
}

impl From<VoteResponse<crate::raft::types::NodeId>> for grpc::node::VoteResponse {
    fn from(value: VoteResponse<crate::raft::types::NodeId>) -> Self {
        let vote = value.vote.into();
        let vote_granted = value.vote_granted;
        let last_log_id = value.last_log_id.map(|log_id| log_id.into());
        grpc::node::VoteResponse {
            vote,
            vote_granted,
            last_log_id,
        }
    }
}

impl From<grpc::node::VoteResponse> for VoteResponse<crate::raft::types::NodeId> {
    fn from(value: grpc::node::VoteResponse) -> Self {
        let vote = value.vote.into();
        let vote_granted = value.vote_granted;
        let last_log_id = value.last_log_id.map(|log_id| log_id.into());
        VoteResponse {
            vote,
            vote_granted,
            last_log_id,
        }
    }
}

impl From<grpc::types::StoredMembership>
    for StoredMembership<crate::raft::types::NodeId, crate::raft::types::Node>
{
    fn from(value: grpc::types::StoredMembership) -> Self {
        StoredMembership::new(
            value.log_id.map(|log_id| log_id.into()),
            value.membership.into(),
        )
    }
}

impl From<StoredMembership<crate::raft::types::NodeId, crate::raft::types::Node>>
    for grpc::types::StoredMembership
{
    fn from(value: StoredMembership<crate::raft::types::NodeId, crate::raft::types::Node>) -> Self {
        let log_id = value.log_id().clone().map(|log_id| log_id.into());
        let membership = value.membership().clone().into();
        grpc::types::StoredMembership { log_id, membership }
    }
}

impl From<grpc::types::Snapshot> for Snapshot<TypeConfig> {
    fn from(value: grpc::types::Snapshot) -> Self {
        Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: value.last_log_id.map(|log_id| log_id.into()),
                last_membership: value.last_membership.into(),
                snapshot_id: value.id,
            },
            snapshot: Box::new(Cursor::new(value.data)),
        }
    }
}

impl From<Snapshot<TypeConfig>> for grpc::types::Snapshot {
    fn from(value: Snapshot<TypeConfig>) -> Self {
        let last_log_id = value.meta.last_log_id.map(|log_id| log_id.into());
        let last_membership = value.meta.last_membership.into();
        let id = value.meta.snapshot_id;
        let data = value.snapshot.into_inner();
        grpc::types::Snapshot {
            last_log_id,
            last_membership,
            id,
            data,
        }
    }
}

impl From<SnapshotResponse<crate::raft::types::NodeId>> for grpc::node::SnapshotResponse {
    fn from(value: SnapshotResponse<crate::raft::types::NodeId>) -> Self {
        grpc::node::SnapshotResponse {
            vote: value.vote.into(),
        }
    }
}

impl From<grpc::node::SnapshotResponse> for SnapshotResponse<crate::raft::types::NodeId> {
    fn from(value: grpc::node::SnapshotResponse) -> Self {
        SnapshotResponse {
            vote: value.vote.into(),
        }
    }
}
