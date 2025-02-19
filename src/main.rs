use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use grpc::{
    app::platoon_service_server::PlatoonServiceServer,
    node::raft_service_server::RaftServiceServer, types::APP_FILE_DESCRIPTOR,
};
use openraft::{BasicNode, Config, Raft};
use raft::{
    network::{client::ClientFactory, server::RaftServer},
    storage::{engine::redb::RedbStorageEngine, log::LogStorage, state_machine::StateMachine},
};
use server::PlatoonServer;
use tokio::{select, sync::RwLock};
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::instrument;

pub mod grpc;
mod raft;
mod server;
mod types;

#[tokio::main]
#[instrument]
async fn main() {
    // Tracing
    console_subscriber::ConsoleLayer::builder()
        .server_addr(([0, 0, 0, 0], 6669))
        .init();

    // Create raft instance
    // Create storage
    let storage_engine = Arc::new(RwLock::new(
        RedbStorageEngine::new(PathBuf::from("db.redb").as_path())
            .await
            .expect("Failed to instantiate redb storage engine!"),
    ));
    // Node id
    let id: u64 = std::env::var("NODE_ID")
        .expect("Variable NODE_ID is not set")
        .parse()
        .expect("Failed to parse env var NODE_ID into u64");
    // Raft config
    let config = Arc::new(Config::default().validate().unwrap());
    // Raft internal gRPC client
    let network = ClientFactory();
    // Create state machine
    let state_machine = StateMachine::new(storage_engine.clone());
    // Create log store
    let log_store = LogStorage::new(storage_engine.clone());
    // Create raft instance
    let raft = Raft::new(id, config, network, log_store, state_machine.clone())
        .await
        .expect("Failed to instantiate raft!");

    // Bring up Raft gRPC server
    let raft_server = RaftServer::new(raft.clone());
    let raft_server_jh = raft_server.start();

    // Bring up App gRPC server
    let platoon_server = PlatoonServer::new(raft.clone(), state_machine.clone());
    let platoon_server_jh = platoon_server.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Nodes (static cluster) initialized by node 1
    if id == 1 {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, BasicNode::new("node1:5001"));
        nodes.insert(2, BasicNode::new("node2:5001"));
        nodes.insert(3, BasicNode::new("node3:5001"));
        // Initialize raft instance
        let res = raft.initialize(nodes).await;
        match res {
            Ok(_) => tracing::info!("Raft cluster started!"),
            Err(err) => tracing::error!("Error starting raft cluster: {}", err.to_string()),
        }
    }

    // These tasks should never terminate, so we stop as soon as one stops with an error message
    select! {
        _ = raft_server_jh => tracing::error!("Raft gRPC server terminated"),
        _ = platoon_server_jh => tracing::error!("App gRPC server terminated"),
    }
}
