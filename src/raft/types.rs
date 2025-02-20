use std::{fmt::Display, io::Cursor};

use openraft::StorageIOError;
use openraft::{impls::OneshotResponder, BasicNode, RaftTypeConfig, SnapshotMeta, TokioRuntime};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::Vehicle;
use crate::types::VehicleId;

#[derive(Debug, Serialize, Deserialize, Error)]
pub enum Error {
    #[error("I/O storage error")]
    Storage(#[from] StorageIOError<NodeId>),
    #[error("Generic error: {0}")]
    Generic(String),
}

type RaftResult<T> = Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Set(Vehicle),
    Delete(VehicleId),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Blank,
    Set(RaftResult<()>),
    Delete(RaftResult<()>),
    Membership(RaftResult<()>),
}
pub type AsyncRuntime = TokioRuntime;
pub type Node = BasicNode;
pub type LogId = openraft::LogId<NodeId>;
pub type LogEntry = openraft::Entry<TypeConfig>;
pub type Responder = OneshotResponder<TypeConfig>;
pub type NodeId = u64;
pub type LogIndex = u64;
pub type Vote = openraft::Vote<NodeId>;
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
