use std::{collections::BTreeMap, io::Cursor, sync::Arc};
use tokio::sync::RwLock;

use openraft::{
    storage::RaftStateMachine, EntryPayload, LogId, OptionalSend, OptionalSync,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use uuid::Uuid;

use crate::{
    raft::{
        self,
        types::{Request, Response, StoredSnapshot},
    },
    types,
};

use super::engine::StorageEngine;

#[derive(Debug)]
struct StateMachineData {
    state: BTreeMap<types::VehicleId, types::Vehicle>,
    membership: StoredMembership<raft::types::NodeId, raft::types::Node>,
    last_applied: Option<LogId<raft::types::NodeId>>,
}

#[derive(Debug, Clone)]
pub struct StateMachine<SE: StorageEngine + OptionalSend + OptionalSync> {
    data: Arc<RwLock<StateMachineData>>,
    storage: Arc<RwLock<SE>>,
}

impl<SE: StorageEngine + OptionalSend + OptionalSync> StateMachine<SE> {
    pub fn new(storage: Arc<RwLock<SE>>) -> Self {
        Self {
            data: Arc::new(RwLock::new(StateMachineData {
                state: BTreeMap::new(),
                membership: Default::default(),
                last_applied: None,
            })),
            storage,
        }
    }

    pub async fn get_state(&self) -> Vec<types::Vehicle> {
        let inner = self.data.read().await;

        inner
            .state
            .iter()
            .map(|(_, vehicle)| vehicle.clone())
            .collect()
    }
}

impl<SE> RaftSnapshotBuilder<raft::types::TypeConfig> for StateMachine<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + 'static,
{
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<raft::types::TypeConfig>, StorageError<raft::types::NodeId>> {
        let mut storage = self.storage.write().await;
        let data = self.data.read().await;
        let meta = SnapshotMeta {
            last_log_id: data.last_applied.clone(),
            last_membership: data.membership.clone(),
            snapshot_id: Uuid::now_v7().to_string(),
        };
        let ser = bincode::serialize(&data.state).map_err(|err| StorageError::IO {
            source: StorageIOError::write_snapshot(None, &err),
        })?;

        storage
            .save_snapshot(raft::types::StoredSnapshot {
                meta: meta.clone(),
                data: ser.clone(),
            })
            .await?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(ser)),
        })
    }
}

impl<SE> RaftStateMachine<raft::types::TypeConfig> for StateMachine<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + 'static,
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<raft::types::NodeId>>,
            StoredMembership<raft::types::NodeId, raft::types::Node>,
        ),
        StorageError<raft::types::NodeId>,
    > {
        let data = self.data.read().await;
        Ok((data.last_applied.clone(), data.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<raft::types::Response>, StorageError<raft::types::NodeId>>
    where
        I: IntoIterator<Item = raft::types::LogEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut data = self.data.write().await;
        let mut responses = vec![];

        for entry in entries {
            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(Response::Blank);
                    data.last_applied = Some(entry.log_id);
                }
                EntryPayload::Normal(payload) => match payload {
                    Request::Set(vehicle) => {
                        data.state.insert(vehicle.id.clone(), vehicle);
                        responses.push(Response::Set(Ok(())));
                        data.last_applied = Some(entry.log_id);
                    }
                    Request::Delete(vehicle_id) => {
                        let _ = data.state.remove(&vehicle_id);
                        responses.push(Response::Delete(Ok(())));
                        data.last_applied = Some(entry.log_id);
                    }
                },
                EntryPayload::Membership(membership) => {
                    data.membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(Response::Membership(Ok(())));
                    data.last_applied = Some(entry.log_id);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            data: self.data.clone(),
            storage: self.storage.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<raft::types::SnapshotData>, StorageError<raft::types::NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<raft::types::NodeId, raft::types::Node>,
        snapshot: Box<raft::types::SnapshotData>,
    ) -> Result<(), StorageError<raft::types::NodeId>> {
        let mut storage = self.storage.write().await;
        let mut data = self.data.write().await;

        let ser = snapshot.into_inner();
        let state = bincode::deserialize(&ser).map_err(|err| StorageError::IO {
            source: StorageIOError::read_snapshot(None, &err),
        })?;

        // save in db
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: ser,
        };
        storage.save_snapshot(stored).await?;

        // update data
        data.last_applied = meta.last_log_id;
        data.membership = meta.last_membership.clone();
        data.state = state;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<raft::types::TypeConfig>>, StorageError<raft::types::NodeId>> {
        let storage = self.storage.read().await;

        let snapshot = storage.read_snapshot().await?;

        if let Some(snapshot) = snapshot {
            Ok(Some(Snapshot {
                meta: snapshot.meta,
                snapshot: Box::new(Cursor::new(snapshot.data)),
            }))
        } else {
            Ok(None)
        }
    }
}
