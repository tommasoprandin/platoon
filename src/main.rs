use std::{collections::BTreeMap, future::poll_fn, path::PathBuf, sync::Arc, time::Duration};

use grpc::node::raft_service_server::RaftServiceServer;
use openraft::{BasicNode, Config, Raft};
use raft::{
    network::{client::ClientFactory, server::Server},
    storage::{engine::redb::RedbStorageEngine, log::LogStorage, state_machine::StateMachine},
    types::Request,
};
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub mod grpc;
mod raft;
mod types;

#[tokio::main]
async fn main() {
    // Tracing
    let _ = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
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
    let raft = Arc::new(RwLock::new(
        Raft::new(id, config, network, log_store, state_machine.clone())
            .await
            .expect("Failed to instantiate raft!"),
    ));

    // Bring up Raft internal gRPC service
    let raft_clone = raft.clone();
    tokio::spawn(async move {
        let raft = raft_clone;
        let raft_server = Server::new(raft.clone());
        let address = "0.0.0.0:5001";
        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(raft_server))
            .serve(
                address
                    .parse()
                    .expect(format!("Invalid address {address}").as_str()),
            )
            .await
            .expect(format!("Failed to bring up gRPC server at {address}").as_str());
    });

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // Nodes (static cluster) initialized by node 1
    if id == 1 {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, BasicNode::new("node1:5001"));
        nodes.insert(2, BasicNode::new("node2:5001"));
        nodes.insert(3, BasicNode::new("node3:5001"));
        // Initialize raft instance
        let _ = raft.write().await.initialize(nodes).await;
    }

    loop {
        if id == 2 {
            let raft_instance = raft.write().await;
            let vehicle = types::Vehicle {
                id: "1".to_owned(),
                position: types::VehiclePosition { x: 1.0, y: 2.0 },
                speed: types::VehicleSpeed {
                    speed: 1.0,
                    heading: 2.0,
                },
            };
            let res = raft_instance
                .client_write(Request::Set(vehicle.clone()))
                .await;
            tracing::info!("Updated vehicle {vehicle:?} with response {res:?}");
        }
        tokio::time::sleep(Duration::from_millis(2_000)).await;
        let raft_instance = raft.read().await;
        let _ = raft_instance.ensure_linearizable().await;
        info!("{:?}", state_machine);
    }
}
