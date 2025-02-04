use std::fmt::Debug;
use std::io::ErrorKind::Other;
use std::{ops::RangeBounds, sync::Arc};
use tokio::sync::RwLock;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, OptionalSend, OptionalSync, Vote};
use openraft::{RaftLogReader, StorageError};

use super::super::types;
use super::engine::StorageEngine;

pub struct LogStorage<SE: StorageEngine + OptionalSend + OptionalSync> {
    engine: Arc<RwLock<SE>>,
}

impl<SE: StorageEngine + OptionalSend + OptionalSync> LogStorage<SE> {
    pub fn new(engine: Arc<RwLock<SE>>) -> Self {
        Self { engine }
    }
}

impl<SE> RaftLogReader<types::TypeConfig> for LogStorage<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + 'static,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<types::LogEntry>, StorageError<types::NodeId>> {
        self.engine.read().await.range_log(range).await
    }
}

impl<SE> RaftLogStorage<types::TypeConfig> for LogStorage<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + 'static,
{
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<types::TypeConfig>, StorageError<types::NodeId>> {
        let last_purged_log_id = self.engine.read().await.last_purged_entry().await?;
        let last_log_id = self
            .engine
            .read()
            .await
            .last_entry()
            .await?
            .or_else(|| last_purged_log_id.clone());
        Ok(LogState {
            last_log_id,
            last_purged_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            engine: self.engine.clone(),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<types::NodeId>,
    ) -> Result<(), StorageError<types::NodeId>> {
        self.engine.write().await.save_vote(vote.clone()).await
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<types::NodeId>>, StorageError<types::NodeId>> {
        self.engine.read().await.read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<types::TypeConfig>,
    ) -> Result<(), StorageError<types::NodeId>>
    where
        I: IntoIterator<Item = types::LogEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let res = self.engine.write().await.insert_entries(entries).await;
        callback.log_io_completed(res.clone().map_err(|err| std::io::Error::new(Other, err)));
        res
    }

    async fn truncate(
        &mut self,
        log_id: LogId<types::NodeId>,
    ) -> Result<(), StorageError<types::NodeId>> {
        self.engine.write().await.truncate_log(log_id).await
    }

    async fn purge(
        &mut self,
        log_id: LogId<types::NodeId>,
    ) -> Result<(), StorageError<types::NodeId>> {
        self.engine.write().await.purge_log(log_id).await
    }
}
