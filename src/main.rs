use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use openraft::{BasicNode, Config, Raft};
use raft::{
    network::{client::ClientFactory, server::RaftServer},
    storage::{engine::redb::RedbStorageEngine, log::LogStorage, state_machine::StateMachine},
};
use server::PlatoonServer;
use tokio::{select, sync::RwLock};
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

    // Environment
    // Node id
    let id: u64 = std::env::var("NODE_ID")
        .expect("Variable NODE_ID is not set")
        .parse()
        .expect("Failed to parse env var NODE_ID into u64");
    // Raft server port
    let raft_server_port: u16 = std::env::var("RAFT_SERVER_PORT")
        .expect("Variable RAFT_SERVER_PORT is not set")
        .parse()
        .expect("Failed to parse env var RAFT_SERVER_PORT into u16");
    // App server port
    let platoon_server_port: u16 = std::env::var("PLATOON_SERVER_PORT")
        .expect("Variable PLATOON_SERVER_PORT is not set")
        .parse()
        .expect("Failed to parse env var PLATOON_SERVER_PORT into u16");

    // Create raft instance
    // Create storage
    let storage_engine = Arc::new(RwLock::new(
        RedbStorageEngine::new(PathBuf::from("db.redb").as_path())
            .await
            .expect("Failed to instantiate redb storage engine!"),
    ));
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
    let raft_server = RaftServer::new(raft.clone(), raft_server_port);
    let raft_server_jh = raft_server.start();
    tracing::info!("Raft server started on port {raft_server_port}");

    // Bring up App gRPC server
    let platoon_server =
        PlatoonServer::new(raft.clone(), state_machine.clone(), platoon_server_port);
    let platoon_server_jh = platoon_server.start();
    tracing::info!("App server started on port {platoon_server_port}");

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
