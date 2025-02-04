use std::{fmt::Display, io::Cursor};

use openraft::{impls::OneshotResponder, BasicNode, RaftTypeConfig, SnapshotMeta, TokioRuntime};
use serde::{Deserialize, Serialize};

use crate::types::Vehicle;
use crate::types::VehicleId;

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Set(Vehicle),
    Get(VehicleId),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Blank,
    Set(Result<(), Error>),
    Get(Result<Option<Vehicle>, Error>),
    Membership(Result<(), Error>),
}
pub type AsyncRuntime = TokioRuntime;
pub type Node = BasicNode;
pub type LogId = openraft::LogId<NodeId>;
pub type LogEntry = openraft::Entry<TypeConfig>;
pub type Responder = OneshotResponder<TypeConfig>;
pub type NodeId = u64;
pub type LogIndex = u64;
pub type VoteId = u64;
pub type Vote = openraft::Vote<NodeId>;
pub type SnapshotId = String;
pub type SnapshotData = Cursor<Vec<u8>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,
    pub data: Vec<u8>,
}

#[derive(
    Debug, Clone, Copy, Hash, Default, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize,
)]
pub struct TypeConfig {}
impl RaftTypeConfig for TypeConfig {
    type D = Request;
    type R = Response;
    type Node = Node;
    type Entry = LogEntry;
    type Responder = Responder;
    type AsyncRuntime = AsyncRuntime;
    type NodeId = NodeId;
    type SnapshotData = SnapshotData;
}

impl Display for TypeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("").as_str())
    }
}
