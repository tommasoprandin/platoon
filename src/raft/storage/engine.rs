//! This module contains the traits and implementation for a openraft storage engine, that allows persistent
//! storage and retrieval of logs, votes (terms) and state machine snapshots.

use std::ops::RangeBounds;

use openraft::{OptionalSend, StorageError};

use super::super::types::{self, LogIndex};

/// This trait defines all the operations a [`StorageEngine`] supports. All the methods are `async`
/// to natively support the `tokio` runtime. Ofter implementations will wrap `sync` APIs.
pub trait StorageEngine {
    /// Inserts a single log entry into the persistent storage.
    ///
    /// This operation ensures the provided entry is durably stored and can be retrieved later,
    /// even after system restarts. The entry is appended to the log in sequential order.
    ///
    /// Returns a Result indicating success or a StorageError if the operation fails.
    async fn insert_entry(
        &mut self,
        entry: types::LogEntry,
    ) -> Result<(), StorageError<types::NodeId>>;

    /// Inserts a multiple log entries into the persistent storage.
    ///
    /// This operation ensures the provided entry is durably stored and can be retrieved later,
    /// even after system restarts. The entry is appended to the log in sequential order.
    ///
    /// Returns a Result indicating success or a StorageError if the operation fails.
    fn insert_entries<I>(
        &mut self,
        entries: I,
    ) -> impl std::future::Future<Output = Result<(), StorageError<types::NodeId>>> + Send
    where
        I: IntoIterator<Item = types::LogEntry> + OptionalSend;

    /// Returns the latest entry in the persistent storage. Latest means the one with highest index.
    ///
    /// Returns a Result with an Option containing the Entry if present, or a StorageError if the operation fails.
    fn last_entry(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<types::LogId>, StorageError<types::NodeId>>>
           + OptionalSend;

    /// Returns the entry with highest index that was purged, if any exists.
    ///
    /// When logs are purged up to a certain index, this method returns the details of the last purged log entry.
    /// This is useful for maintaining consistency and tracking what historical data has been removed from storage.
    ///
    /// Returns a Result with an Option containing the purged Entry if available, or None if no entries have been purged.
    /// Returns a StorageError if the operation fails.
    fn last_purged_entry(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<types::LogId>, StorageError<types::NodeId>>>
           + OptionalSend;

    /// Retrieves a range of log entries from the persistent storage based on the provided range bounds.
    ///
    /// This operation allows fetching a subset of entries from the log based on their indices.
    /// The range can be specified using any type that implements RangeBounds<LogIndex>.
    ///
    /// - `range`: A range specification defining which entries to retrieve (e.g., 1..5, 2.., ..3)
    ///
    /// Returns a Result containing a Vec of entries within the specified range, or a StorageError if the operation fails.
    fn range_log<RB>(
        &self,
        range: RB,
    ) -> impl std::future::Future<Output = Result<Vec<types::LogEntry>, StorageError<types::NodeId>>>
           + Send
    where
        RB: RangeBounds<LogIndex> + Send;

    /// Deletes all entries in the log from the specified entry (inclusive) to the end.
    ///
    /// This operation permanently removes all log entries with index greater than or equal
    /// to the index of the provided entry. This is typically used to handle log conflicts
    /// or to clean up after snapshot creation.
    ///
    /// - `from`: The entry from which to start truncation (inclusive)
    ///
    /// Returns a Result containing an Option with the index of the last deleted entry if any entries
    /// were deleted, or None if no entries were deleted. Returns a StorageError if the operation fails.
    fn truncate_log(
        &mut self,
        from: openraft::LogId<types::NodeId>,
    ) -> impl std::future::Future<Output = Result<(), StorageError<types::NodeId>>> + Send;

    /// Deletes all entries in the log from the beginning up to the specified entry (inclusive).
    ///
    /// This operation permanently removes all log entries with index less than or equal
    /// to the index of the provided entry. This is typically used after creating a snapshot
    /// to free up storage space.
    ///
    /// - `to`: The entry up to which to delete (inclusive)
    ///
    /// Returns a Result indicating success or a StorageError if the operation fails.
    fn purge_log(
        &mut self,
        to: openraft::LogId<types::NodeId>,
    ) -> impl std::future::Future<Output = Result<(), StorageError<types::NodeId>>> + Send;

    /// Returns the current vote stored in the storage.
    ///
    /// At most one vote can be stored at a time. This operation retrieves the vote if one exists,
    /// or returns None if no vote is stored.
    ///
    /// Returns a Result containing an Option with the vote if present, or a StorageError if the operation fails.
    fn read_vote(
        &self,
    ) -> impl std::future::Future<
        Output = Result<Option<openraft::Vote<types::NodeId>>, StorageError<types::NodeId>>,
    > + Send;

    /// Inserts a new vote into the storage, replacing any existing vote if present.
    ///
    /// This operation ensures the provided vote is durably stored and can be retrieved later,
    /// even after system restarts. If there was a previous vote stored, it will be overwritten
    /// by the new vote.
    ///
    /// - `vote`: The vote to store
    ///
    /// Returns a Result indicating success or a StorageError if the operation fails.
    fn save_vote(
        &mut self,
        vote: openraft::Vote<types::NodeId>,
    ) -> impl std::future::Future<Output = Result<(), StorageError<types::NodeId>>> + Send;

    /// Reads the latest snapshot of the state machine from the storage.
    ///
    /// Only one snapshot can be stored at a time. This operation retrieves the snapshot if one exists,
    /// or returns None if no snapshot is stored.
    ///
    /// Returns a Result containing an Option with the snapshot data if present, or a StorageError if
    /// the operation fails.
    fn read_snapshot(
        &self,
    ) -> impl std::future::Future<
        Output = Result<Option<types::StoredSnapshot>, StorageError<types::NodeId>>,
    > + Send;

    /// Saves a snapshot taken from the state machine to persistent storage.
    ///
    /// This operation stores the provided snapshot data and overwrites any previously stored snapshot.
    /// It is the responsibility of the implementer to handle serialization/deserialization of the snapshot data.
    /// Only one snapshot can be stored at a time.
    ///
    /// - `data`: The snapshot data to store
    ///
    /// Returns a Result indicating success or a StorageError if the operation fails.
    fn save_snapshot(
        &mut self,
        snapshot: types::StoredSnapshot,
    ) -> impl std::future::Future<Output = Result<(), StorageError<types::NodeId>>> + Send;
}

pub mod redb {
    use openraft::{StorageError, StorageIOError};
    use redb::{Key, ReadableTable, TableError, TypeName, Value};
    use serde::{Deserialize, Serialize};

    use crate::raft::types;
    use std::{fmt::Debug, ops::RangeBounds, path::Path, sync::Arc};

    use super::StorageEngine;

    #[derive(Debug)]
    struct Bincode<T: Debug + Serialize + for<'a> Deserialize<'a>>(pub(crate) T);

    impl<T: Debug + Serialize + for<'a> Deserialize<'a>> Value for Bincode<T> {
        type SelfType<'a>
            = T
        where
            Self: 'a;

        type AsBytes<'a>
            = Vec<u8>
        where
            Self: 'a;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a,
        {
            bincode::deserialize(data)
                .expect(format!("Couldn't deserialize from {:#?}", data).as_str())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
        where
            Self: 'b,
        {
            bincode::serialize(value)
                .expect(format!("Couldn't serialize from {:#?}", value).as_str())
        }

        fn type_name() -> redb::TypeName {
            TypeName::new(format!("Bincode<{}>", std::any::type_name::<T>()).as_str())
        }
    }

    impl<T: std::fmt::Debug + Serialize + for<'a> Deserialize<'a> + Ord> Key for Bincode<T> {
        fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
            let val1 = Self::from_bytes(data1);
            let val2 = Self::from_bytes(data2);
            val1.cmp(&val2)
        }
    }
    const LOG_TABLE: redb::TableDefinition<types::LogIndex, Bincode<types::LogEntry>> =
        redb::TableDefinition::new("log");
    const VOTE_TABLE: redb::TableDefinition<u64, Bincode<types::Vote>> =
        redb::TableDefinition::new("vote");
    const LOGID_TABLE: redb::TableDefinition<u64, Bincode<types::LogId>> =
        redb::TableDefinition::new("purged");
    const SNAPSHOT_TABLE: redb::TableDefinition<u64, Bincode<types::StoredSnapshot>> =
        redb::TableDefinition::new("snapshot");

    enum LogIdKeys {
        LastId = 0,
        LastPurged = 1,
    }

    pub struct RedbStorageEngine {
        db: Arc<redb::Database>,
    }

    impl RedbStorageEngine {
        pub async fn new(path: &Path) -> Result<Self, StorageError<types::NodeId>> {
            let db = redb::Database::create(path).map_err(|err| StorageError::IO {
                source: StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    &err,
                ),
            })?;

            Ok(Self { db: Arc::new(db) })
        }
    }

    impl StorageEngine for RedbStorageEngine {
        async fn insert_entry(
            &mut self,
            entry: types::LogEntry,
        ) -> Result<(), StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let log_id = entry.log_id.clone();
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_log_entry(log_id, &err),
                })?;

                {
                    let mut log_table =
                        write_txn
                            .open_table(LOG_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_log_entry(log_id, &err),
                            })?;
                    log_table
                        .insert(entry.log_id.index, &entry)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_log_entry(log_id, &err),
                        })?;

                    let mut logid_table =
                        write_txn
                            .open_table(LOGID_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_log_entry(log_id, &err),
                            })?;
                    logid_table
                        .insert(LogIdKeys::LastId as u64, entry.log_id.clone())
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_log_entry(log_id, &err),
                        })?;
                }

                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_log_entry(log_id, &err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn insert_entries<I>(&mut self, entries: I) -> Result<(), StorageError<types::NodeId>>
        where
            I: IntoIterator<Item = types::LogEntry>,
        {
            let db = self.db.clone();
            let entries: Vec<_> = entries.into_iter().collect();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                {
                    let mut log_table =
                        write_txn
                            .open_table(LOG_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;
                    let mut logid_table =
                        write_txn
                            .open_table(LOGID_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;
                    for entry in entries {
                        let log_id = entry.log_id.clone();
                        log_table
                            .insert(log_id.index, &entry)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_log_entry(log_id, &err),
                            })?;

                        logid_table
                            .insert(LogIdKeys::LastId as u64, entry.log_id.clone())
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_log_entry(log_id, &err),
                            })?;
                    }
                }

                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn last_entry(&self) -> Result<Option<types::LogId>, StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(
                move || -> Result<Option<types::LogId>, StorageError<types::NodeId>> {
                    let read_txn = db.begin_read().map_err(|err| StorageError::IO {
                        source: StorageIOError::read_logs(&err),
                    })?;

                    let log_table = match read_txn.open_table(LOGID_TABLE) {
                        Ok(table) => table,
                        Err(e) => match e {
                            TableError::TableDoesNotExist(_) => return Ok(None),
                            e => {
                                return Err(StorageError::IO {
                                    source: StorageIOError::read_logs(&e),
                                })
                            }
                        },
                    };

                    let last = log_table
                        .get(LogIdKeys::LastId as u64)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_logs(&err),
                        })?
                        .map(|v| v.value());

                    Ok(last)
                },
            )
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn last_purged_entry(
            &self,
        ) -> Result<Option<types::LogId>, StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(
                move || -> Result<Option<types::LogId>, StorageError<types::NodeId>> {
                    let read_txn = db.begin_read().map_err(|err| StorageError::IO {
                        source: StorageIOError::read_logs(&err),
                    })?;

                    let purged_table = match read_txn.open_table(LOGID_TABLE) {
                        Ok(table) => table,
                        Err(e) => match e {
                            TableError::TableDoesNotExist(_) => return Ok(None),
                            e => {
                                return Err(StorageError::IO {
                                    source: StorageIOError::read_logs(&e),
                                })
                            }
                        },
                    };

                    let last_purged_id = purged_table
                        .get(LogIdKeys::LastPurged as u64)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_logs(&err),
                        })?
                        .map(|v| v.value());

                    Ok(last_purged_id)
                },
            )
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn range_log<RB>(
            &self,
            range: RB,
        ) -> Result<Vec<types::LogEntry>, StorageError<types::NodeId>>
        where
            RB: RangeBounds<types::LogIndex> + Send,
        {
            let db = self.db.clone();
            let range = (range.start_bound().cloned(), range.end_bound().cloned());
            tokio::task::spawn_blocking(
                move || -> Result<Vec<types::LogEntry>, StorageError<types::NodeId>> {
                    let read_txn = db.begin_read().map_err(|err| StorageError::IO {
                        source: StorageIOError::read_logs(&err),
                    })?;

                    let log_table = match read_txn.open_table(LOG_TABLE) {
                        Ok(table) => table,
                        Err(e) => match e {
                            TableError::TableDoesNotExist(_) => return Ok(vec![]),
                            e => {
                                return Err(StorageError::IO {
                                    source: StorageIOError::read_logs(&e),
                                })
                            }
                        },
                    };

                    let entries: Vec<_> = log_table
                        .range(range)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_logs(&err),
                        })?
                        .flat_map(|res| res.map(|(_, v)| v.value()))
                        .collect();

                    Ok(entries)
                },
            )
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn truncate_log(
            &mut self,
            from: openraft::LogId<types::NodeId>,
        ) -> Result<(), StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                {
                    let mut log_table =
                        write_txn
                            .open_table(LOG_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;

                    log_table
                        .retain_in(from.index.., |_, _| false)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_logs(&err),
                        })?;

                    let last = log_table
                        .last()
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_logs(&err),
                        })?
                        .map(|(_, v)| v.value());

                    let mut logid_table =
                        write_txn
                            .open_table(LOGID_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;

                    if let Some(entry) = last {
                        logid_table
                            .insert(LogIdKeys::LastId as u64, &entry.log_id)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;
                    } else {
                        logid_table
                            .remove(LogIdKeys::LastId as u64)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;
                    }
                }

                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn purge_log(
            &mut self,
            to: openraft::LogId<types::NodeId>,
        ) -> Result<(), StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                {
                    let mut log_table =
                        write_txn
                            .open_table(LOG_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;

                    log_table
                        .retain_in(..=to.index, |_, _| false)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_logs(&err),
                        })?;

                    let last = log_table
                        .last()
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_logs(&err),
                        })?
                        .map(|(k, v)| (k.value(), v.value()));

                    let last_id = if let Some(last_saved) = last {
                        if last_saved.0 < to.index {
                            to
                        } else {
                            last_saved.1.log_id
                        }
                    } else {
                        to
                    };

                    let mut logid_table =
                        write_txn
                            .open_table(LOGID_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_logs(&err),
                            })?;

                    logid_table
                        .insert(LogIdKeys::LastPurged as u64, &to)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_logs(&err),
                        })?;

                    logid_table
                        .insert(LogIdKeys::LastId as u64, &last_id)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_logs(&err),
                        })?;
                }
                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_logs(&err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn read_vote(
            &self,
        ) -> Result<Option<openraft::Vote<types::NodeId>>, StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || ->
            Result<
                Option<openraft::Vote<types::NodeId>>,
                        StorageError<types::NodeId>> {
                let read_txn = db.begin_read().map_err(|err| StorageError::IO {
                    source: StorageIOError::read_vote(&err),
                })?;

                let vote_table = match read_txn.open_table(VOTE_TABLE) {
                    Ok(table) => table,
                    Err(e) => {
                        match e {
                            TableError::TableDoesNotExist(_) => return Ok(None),
                            e => return Err(StorageError::IO { source: StorageIOError::read_vote(&e) })
                        }
                    }
                };

                let vote = vote_table.first()
                    .map_err(|err| StorageError::IO {
                        source: StorageIOError::read_vote(&err),
                    })?.map(|(_, v)| v.value());

                Ok(vote)

            }).await.expect("Catastrophic runtime error, task panicked")
        }

        async fn save_vote(
            &mut self,
            vote: openraft::Vote<types::NodeId>,
        ) -> Result<(), StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_vote(&err),
                })?;

                {
                    let mut vote_table =
                        write_txn
                            .open_table(VOTE_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_vote(&err),
                            })?;
                    vote_table.insert(0, vote).map_err(|err| StorageError::IO {
                        source: StorageIOError::write_vote(&err),
                    })?;
                }

                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_vote(&err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn read_snapshot(
            &self,
        ) -> Result<Option<types::StoredSnapshot>, StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(
                move || -> Result<Option<types::StoredSnapshot>, StorageError<types::NodeId>> {
                    let read_txn = db.begin_read().map_err(|err| StorageError::IO {
                        source: StorageIOError::read_snapshot(None, &err),
                    })?;

                    let snapshot_table = match read_txn.open_table(SNAPSHOT_TABLE) {
                        Ok(table) => table,
                        Err(e) => match e {
                            TableError::TableDoesNotExist(_) => return Ok(None),
                            e => {
                                return Err(StorageError::IO {
                                    source: StorageIOError::read_snapshot(None, &e),
                                })
                            }
                        },
                    };

                    let snapshot = snapshot_table
                        .first()
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::read_snapshot(None, &err),
                        })?
                        .map(|(_, v)| v.value());

                    Ok(snapshot)
                },
            )
            .await
            .expect("Catastrophic runtime error, task panicked")
        }

        async fn save_snapshot(
            &mut self,
            snapshot: types::StoredSnapshot,
        ) -> Result<(), StorageError<types::NodeId>> {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || -> Result<(), StorageError<types::NodeId>> {
                let write_txn = db.begin_write().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_snapshot(None, &err),
                })?;

                {
                    let mut snapshot_table =
                        write_txn
                            .open_table(SNAPSHOT_TABLE)
                            .map_err(|err| StorageError::IO {
                                source: StorageIOError::write_snapshot(None, &err),
                            })?;
                    snapshot_table
                        .insert(0, snapshot)
                        .map_err(|err| StorageError::IO {
                            source: StorageIOError::write_snapshot(None, &err),
                        })?;
                }

                write_txn.commit().map_err(|err| StorageError::IO {
                    source: StorageIOError::write_snapshot(None, &err),
                })?;

                Ok(())
            })
            .await
            .expect("Catastrophic runtime error, task panicked")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::raft::types::{LogId, TypeConfig, Vote};
        use async_tempfile::TempFile;
        use openraft::{CommittedLeaderId, Entry, EntryPayload};
        use tokio;

        /// Helper function to create a test log entry
        fn create_test_entry(index: u64, term: u64) -> Entry<TypeConfig> {
            let log_id = LogId {
                index,
                leader_id: CommittedLeaderId::new(term, 0),
            };
            Entry {
                log_id: log_id.clone(),
                payload: EntryPayload::Blank,
            }
        }

        #[tokio::test]
        async fn test_insert_single_entry() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let test_entry = create_test_entry(1, 1);

            // Insert a single entry
            let result = storage.insert_entry(test_entry.clone()).await;
            assert!(result.is_ok());

            // Verify the entry was stored by retrieving latest
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), test_entry.log_id);
        }

        #[tokio::test]
        async fn test_insert_multiple_entries() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // Insert multiple entries
            let result = storage.insert_entries(entries.clone()).await;
            assert!(result.is_ok());

            // Verify the last entry is retrievable
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), entries.last().unwrap().log_id);
        }

        #[tokio::test]
        async fn test_overwrite_entry() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let initial_entry = create_test_entry(1, 1);
            let updated_entry = create_test_entry(1, 2); // Same index, different term

            // Insert initial entry
            storage.insert_entry(initial_entry).await.unwrap();

            // Overwrite with updated entry
            storage.insert_entry(updated_entry.clone()).await.unwrap();

            // Verify the updated entry is retrieved
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), updated_entry.log_id);
        }

        #[tokio::test]
        async fn test_sequential_insert_entries() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

            // Insert entries one by one
            for i in 1..=3 {
                let entry = create_test_entry(i, 1);
                let result = storage.insert_entry(entry).await;
                assert!(result.is_ok());
            }

            // Verify the last entry
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), create_test_entry(3, 1).log_id);
        }

        #[tokio::test]
        async fn test_range_log() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
                create_test_entry(4, 1),
                create_test_entry(5, 1),
            ];

            // Insert all entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Test full range
            let all_entries = storage.range_log(1..=5).await.unwrap();
            assert_eq!(all_entries.len(), 5);
            assert_eq!(all_entries[0].log_id, entries[0].log_id);
            assert_eq!(all_entries[4].log_id, entries[4].log_id);

            // Test partial range
            let mid_entries = storage.range_log(2..4).await.unwrap();
            assert_eq!(mid_entries.len(), 2);
            assert_eq!(mid_entries[0].log_id, entries[1].log_id);
            assert_eq!(mid_entries[1].log_id, entries[2].log_id);

            // Test range from start
            let start_entries = storage.range_log(..3).await.unwrap();
            assert_eq!(start_entries.len(), 2);
            assert_eq!(start_entries[0].log_id, entries[0].log_id);
            assert_eq!(start_entries[1].log_id, entries[1].log_id);

            // Test range to end
            let end_entries = storage.range_log(4..).await.unwrap();
            assert_eq!(end_entries.len(), 2);
            assert_eq!(end_entries[0].log_id, entries[3].log_id);
            assert_eq!(end_entries[1].log_id, entries[4].log_id);
        }

        #[tokio::test]
        async fn test_truncate_log() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
                create_test_entry(4, 1),
                create_test_entry(5, 1),
            ];

            // Insert all entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Truncate from index 3
            let truncate_from = entries[2].log_id.clone();
            let result = storage.truncate_log(truncate_from).await;
            assert!(result.is_ok());

            // Verify remaining entries
            let remaining = storage.range_log(1..).await.unwrap();
            assert_eq!(remaining.len(), 2); // Should only have entries 1 and 2 left
            assert_eq!(remaining[0].log_id, entries[0].log_id);
            assert_eq!(remaining[1].log_id, entries[1].log_id);

            // Verify latest entry
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), entries[1].log_id);
        }

        #[tokio::test]
        async fn test_purge_log() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
                create_test_entry(4, 1),
                create_test_entry(5, 1),
            ];

            // Insert all entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Purge up to index 3 (inclusive)
            let purge_to = entries[2].log_id.clone();
            let result = storage.purge_log(purge_to).await;
            assert!(result.is_ok());

            // Verify remaining entries
            let remaining = storage.range_log(1..).await.unwrap();
            assert_eq!(remaining.len(), 2); // Should only have entries 4 and 5 left
            assert_eq!(remaining[0].log_id, entries[3].log_id);
            assert_eq!(remaining[1].log_id, entries[4].log_id);

            // Verify latest entry remains unchanged
            let latest = storage.last_entry().await.unwrap();
            assert!(latest.is_some());
            assert_eq!(latest.unwrap(), entries[4].log_id);
        }

        #[tokio::test]
        async fn test_truncate_and_purge_log_empty_ranges() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // Insert entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Truncate beyond last entry
            let truncate_from = create_test_entry(4, 1).log_id;
            let result = storage.truncate_log(truncate_from).await;
            assert!(result.is_ok());

            // Verify all entries still exist
            let remaining = storage.range_log(1..).await.unwrap();
            assert_eq!(remaining.len(), 3);

            // Purge before first entry
            let purge_to = create_test_entry(0, 1).log_id;
            let result = storage.purge_log(purge_to).await;
            assert!(result.is_ok());

            // Verify all entries still exist
            let remaining = storage.range_log(1..).await.unwrap();
            assert_eq!(remaining.len(), 3);
        }

        #[tokio::test]
        async fn test_truncate_and_purge_log_sequential() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
                create_test_entry(4, 1),
                create_test_entry(5, 1),
            ];

            // Insert all entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // First truncate from index 4
            let truncate_from = entries[3].log_id.clone();
            storage.truncate_log(truncate_from).await.unwrap();

            // Then purge up to index 2
            let purge_to = entries[1].log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Verify remaining entries
            let remaining = storage.range_log(1..).await.unwrap();
            assert_eq!(remaining.len(), 1); // Should only have entry 3 left
            assert_eq!(remaining[0].log_id, entries[2].log_id);
        }

        #[tokio::test]
        async fn test_write_and_read_vote() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let test_vote = Vote::new(1, 0);

            // Write vote
            let write_result = storage.save_vote(test_vote.clone()).await;
            assert!(write_result.is_ok());

            // Read and verify vote
            let read_result = storage.read_vote().await.unwrap();
            assert!(read_result.is_some());
            assert_eq!(read_result.unwrap(), test_vote);
        }

        #[tokio::test]
        async fn test_overwrite_vote() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let initial_vote = Vote::new(1, 0);
            let updated_vote = Vote::new(2, 1);

            // Write initial vote
            storage.save_vote(initial_vote).await.unwrap();

            // Overwrite with new vote
            storage.save_vote(updated_vote.clone()).await.unwrap();

            // Read and verify updated vote
            let read_result = storage.read_vote().await.unwrap();
            assert!(read_result.is_some());
            assert_eq!(read_result.unwrap(), updated_vote);
        }

        #[tokio::test]
        async fn test_read_vote_empty_storage() {
            let tempfile = TempFile::new().await.unwrap();
            let storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

            // Read vote from empty storage
            let read_result = storage.read_vote().await.unwrap();
            assert!(read_result.is_none());
        }

        #[tokio::test]
        async fn test_multiple_vote_operations() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

            // Initial state should be empty
            assert!(storage.read_vote().await.unwrap().is_none());

            // Write first vote
            let vote1 = Vote::new(1, 0);
            storage.save_vote(vote1.clone()).await.unwrap();
            assert_eq!(storage.read_vote().await.unwrap().unwrap(), vote1);

            // Write second vote
            let vote2 = Vote::new(2, 0);
            storage.save_vote(vote2.clone()).await.unwrap();
            assert_eq!(storage.read_vote().await.unwrap().unwrap(), vote2);

            // Write third vote
            let vote3 = Vote::new(3, 1);
            storage.save_vote(vote3.clone()).await.unwrap();
            assert_eq!(storage.read_vote().await.unwrap().unwrap(), vote3);
        }

        #[tokio::test]
        async fn test_vote_persistence() {
            let tempfile = TempFile::new().await.unwrap();

            // Create storage and write vote
            {
                let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
                let test_vote = Vote::new(1, 0);
                storage.save_vote(test_vote).await.unwrap();
            } // Storage is dropped here

            // Create new storage instance and verify vote persists
            {
                let storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
                let read_vote = storage.read_vote().await.unwrap();
                assert!(read_vote.is_some());
                assert_eq!(read_vote.unwrap(), Vote::new(1, 0));
            }
        }

        #[tokio::test]
        async fn test_write_and_read_snapshot() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let test_data = vec![1, 2, 3, 4, 5];
            let test_snapshot = types::StoredSnapshot {
                meta: Default::default(), // Add appropriate snapshot metadata
                data: test_data.clone(),
            };

            // Write snapshot
            let write_result = storage.save_snapshot(test_snapshot).await;
            assert!(write_result.is_ok());

            // Read and verify snapshot
            let read_result = storage.read_snapshot().await.unwrap();
            assert!(read_result.is_some());
            assert_eq!(read_result.unwrap().data, test_data);
        }

        #[tokio::test]
        async fn test_overwrite_snapshot() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let initial_data = vec![1, 2, 3];
            let updated_data = vec![4, 5, 6];

            let initial_snapshot = types::StoredSnapshot {
                meta: Default::default(),
                data: initial_data,
            };
            let updated_snapshot = types::StoredSnapshot {
                meta: Default::default(),
                data: updated_data.clone(),
            };

            // Write initial snapshot
            storage.save_snapshot(initial_snapshot).await.unwrap();

            // Overwrite with new snapshot
            storage.save_snapshot(updated_snapshot).await.unwrap();

            // Read and verify updated snapshot
            let read_result = storage.read_snapshot().await.unwrap();
            assert!(read_result.is_some());
            assert_eq!(read_result.unwrap().data, updated_data);
        }

        #[tokio::test]
        async fn test_multiple_snapshot_operations() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

            // Initial state should be empty
            assert!(storage.read_snapshot().await.unwrap().is_none());

            // Write first snapshot
            let snapshot1 = types::StoredSnapshot {
                meta: Default::default(),
                data: vec![1, 2],
            };
            storage.save_snapshot(snapshot1).await.unwrap();
            assert_eq!(
                storage.read_snapshot().await.unwrap().unwrap().data,
                vec![1, 2]
            );

            // Write second snapshot
            let snapshot2 = types::StoredSnapshot {
                meta: Default::default(),
                data: vec![3, 4],
            };
            storage.save_snapshot(snapshot2).await.unwrap();
            assert_eq!(
                storage.read_snapshot().await.unwrap().unwrap().data,
                vec![3, 4]
            );

            // Write third snapshot
            let snapshot3 = types::StoredSnapshot {
                meta: Default::default(),
                data: vec![5, 6],
            };
            storage.save_snapshot(snapshot3).await.unwrap();
            assert_eq!(
                storage.read_snapshot().await.unwrap().unwrap().data,
                vec![5, 6]
            );
        }

        #[tokio::test]
        async fn test_snapshot_persistence() {
            let tempfile = TempFile::new().await.unwrap();
            let test_data = vec![1, 2, 3];

            // Create storage and write snapshot
            {
                let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
                let test_snapshot = types::StoredSnapshot {
                    meta: Default::default(),
                    data: test_data.clone(),
                };
                storage.save_snapshot(test_snapshot).await.unwrap();
            } // Storage is dropped here

            // Create new storage instance and verify snapshot persists
            {
                let storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
                let read_snapshot = storage.read_snapshot().await.unwrap();
                assert!(read_snapshot.is_some());
                assert_eq!(read_snapshot.unwrap().data, test_data);
            }

            tokio::fs::remove_file(tempfile.file_path()).await.unwrap();
        }

        #[tokio::test]
        async fn test_snapshot_with_empty_data() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let empty_snapshot = types::StoredSnapshot {
                meta: Default::default(),
                data: vec![],
            };

            // Write empty snapshot
            storage.save_snapshot(empty_snapshot).await.unwrap();

            // Read and verify empty snapshot
            let read_result = storage.read_snapshot().await.unwrap();
            assert!(read_result.is_some());
            assert!(read_result.unwrap().data.is_empty());
        }

        #[tokio::test]
        async fn test_last_purged_empty_storage() {
            let tempfile = TempFile::new().await.unwrap();
            let storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

            // Check last purged entry in empty storage
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn test_last_purged_after_purge() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // Insert entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Initially no purged entries
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_none());

            // Purge up to index 2
            let purge_to = entries[1].log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Check last purged entry
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), entries[1].log_id);
        }

        #[tokio::test]
        async fn test_last_purged_multiple_purges() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
                create_test_entry(4, 1),
                create_test_entry(5, 1),
            ];

            // Insert entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // First purge up to index 2
            let purge_to = entries[1].log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Check first purge result
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), entries[1].log_id);

            // Second purge up to index 4
            let purge_to = entries[3].log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Check second purge result
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), entries[3].log_id);
        }

        #[tokio::test]
        async fn test_last_purged_persistence() {
            let tempfile = TempFile::new().await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // First storage instance
            {
                let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

                // Insert entries and purge
                storage.insert_entries(entries.clone()).await.unwrap();
                let purge_to = entries[1].log_id.clone();
                storage.purge_log(purge_to).await.unwrap();
            } // Storage is dropped here

            // Second storage instance
            {
                let storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();

                // Check if purged entry persists
                let result = storage.last_purged_entry().await.unwrap();
                assert!(result.is_some());
                assert_eq!(result.unwrap(), entries[1].log_id);
            }
        }

        #[tokio::test]
        async fn test_last_purged_after_truncate() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // Insert entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Truncate some entries
            let truncate_from = entries[1].log_id.clone();
            storage.truncate_log(truncate_from).await.unwrap();

            // Verify truncation doesn't affect last purged entry
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_none());

            // Now purge some entries
            let purge_to = entries[0].log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Verify last purged entry after purge
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), entries[0].log_id);
        }

        #[tokio::test]
        async fn test_last_purged_purge_all() {
            let tempfile = TempFile::new().await.unwrap();
            let mut storage = RedbStorageEngine::new(tempfile.file_path()).await.unwrap();
            let entries = vec![
                create_test_entry(1, 1),
                create_test_entry(2, 1),
                create_test_entry(3, 1),
            ];

            // Insert entries
            storage.insert_entries(entries.clone()).await.unwrap();

            // Purge all entries
            let purge_to = entries.last().unwrap().log_id.clone();
            storage.purge_log(purge_to).await.unwrap();

            // Verify last purged entry is the last entry
            let result = storage.last_purged_entry().await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), entries[2].log_id);
        }
    }
}
