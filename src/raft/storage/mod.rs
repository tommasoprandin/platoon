mod engine;
mod log;
mod state_machine;

mod redb {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
        sync::Arc,
    };

    use openraft::{testing::StoreBuilder, StorageError};
    use tokio::sync::RwLock;

    use crate::raft::{self, types::TypeConfig};

    use super::{engine::redb::RedbStorageEngine, log::LogStorage, state_machine::StateMachine};

    pub struct RedbStorageBuilder {}

    impl RedbStorageBuilder {
        pub async fn new() -> Self {
            Self {}
        }

        #[cfg(test)]
        pub async fn build_test(
            &self,
            tempfile_path: &Path,
        ) -> Result<
            (
                (),
                LogStorage<RedbStorageEngine>,
                StateMachine<RedbStorageEngine>,
            ),
            StorageError<raft::types::NodeId>,
        > {
            if cfg!(test) {
                let engine = Arc::new(RwLock::new(RedbStorageEngine::new(tempfile_path).await?));
                Ok((
                    (),
                    LogStorage::new(engine.clone()),
                    StateMachine::new(engine.clone()),
                ))
            } else {
                panic!("Cannot use build_test in a non test environment!");
            }
        }
    }
    impl
        StoreBuilder<TypeConfig, LogStorage<RedbStorageEngine>, StateMachine<RedbStorageEngine>, ()>
        for RedbStorageBuilder
    {
        async fn build(
            &self,
        ) -> Result<
            (
                (),
                LogStorage<RedbStorageEngine>,
                StateMachine<RedbStorageEngine>,
            ),
            StorageError<raft::types::NodeId>,
        > {
            let engine = Arc::new(RwLock::new(
                RedbStorageEngine::new(Path::new("db.redb")).await?,
            ));
            Ok((
                (),
                LogStorage::new(engine.clone()),
                StateMachine::new(engine.clone()),
            ))
        }
    }
}

// Generated test functions
// Generated test functions
#[cfg(test)]
mod tests {
    use async_tempfile::TempFile;
    use openraft::testing::StoreBuilder;

    use crate::raft::{
        storage::{
            engine::{self},
            log::LogStorage,
            redb::RedbStorageBuilder,
            state_machine::StateMachine,
        },
        types::TypeConfig,
    };

    #[tokio::test]
    async fn test_redb_store_last_membership_in_log_initial() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::last_membership_in_log_initial(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_last_membership_in_log() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::last_membership_in_log(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_last_membership_in_log_multi_step() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::last_membership_in_log_multi_step(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_initial() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_initial(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_from_log_and_empty_sm() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_from_log_and_empty_sm(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_from_empty_log_and_sm() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_from_empty_log_and_sm(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_from_log_le_sm_last_applied() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_from_log_le_sm_last_applied(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_from_log_gt_sm_last_applied_1() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_from_log_gt_sm_last_applied_1(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_membership_from_log_gt_sm_last_applied_2() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_membership_from_log_gt_sm_last_applied_2(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_without_init() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_without_init(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_membership_from_log_and_sm() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_membership_from_log_and_sm(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_with_state() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_with_state(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_last_log_gt_sm() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_last_log_gt_sm(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_last_log_lt_sm() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_last_log_lt_sm(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_log_ids() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_log_ids(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_initial_state_re_apply_committed() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_initial_state_re_apply_committed(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_save_vote() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::save_vote(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_log_entries() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_log_entries(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_limited_get_log_entries() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::limited_get_log_entries(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_try_get_log_entry() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::try_get_log_entry(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_initial_logs() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::initial_logs(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_log_state() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_log_state(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_get_log_id() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::get_log_id(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_last_id_in_log() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::last_id_in_log(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_last_applied_state() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::last_applied_state(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_purge_logs_upto_0() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::purge_logs_upto_0(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_purge_logs_upto_5() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::purge_logs_upto_5(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_purge_logs_upto_20() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::purge_logs_upto_20(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_delete_logs_since_11() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::delete_logs_since_11(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_delete_logs_since_0() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::delete_logs_since_0(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_append_to_log() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::append_to_log(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_snapshot_meta() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::snapshot_meta(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_apply_single() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::apply_single(ls, sm)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_redb_store_apply_multiple() {
        let tempfile = TempFile::new().await.unwrap();
        let (_, ls, sm) = RedbStorageBuilder::new()
            .await
            .build_test(tempfile.file_path())
            .await
            .unwrap();
        openraft::testing::Suite::<
            TypeConfig,
            LogStorage<engine::redb::RedbStorageEngine>,
            StateMachine<engine::redb::RedbStorageEngine>,
            RedbStorageBuilder,
            _,
        >::apply_multiple(ls, sm)
        .await
        .unwrap();
    }
}
