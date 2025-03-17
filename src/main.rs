use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use client::PlatoonClient;
use openraft::{BasicNode, Config, Raft};
use raft::{
    network::{client::ClientFactory, server::RaftServer},
    storage::{engine::redb::RedbStorageEngine, log::LogStorage, state_machine::StateMachine},
};
use server::PlatoonServer;
use tokio::{select, sync::RwLock};
use tracing::instrument;
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter};

mod client;
pub mod grpc;
mod raft;
mod server;
mod types;

#[tokio::main]
#[instrument]
async fn main() {
    // Environment
    // Node id
    let id: u64 = std::env::var("NODE_ID")
        .expect("Variable NODE_ID is not set")
        .parse()
        .expect("Failed to parse env var NODE_ID into u64");
    // Cluster size
    let cluster_size: u64 = std::env::var("CLUSTER_SIZE")
        .expect("Variable CLUSTER_SIZE is not set")
        .parse()
        .expect("Failed to parse env var CLUSTER_SIZE into u32");
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
    // Seed for random operations
    let seed: u64 = std::env::var("RND_SEED")
        .expect("Variable RND_SEED is not set")
        .parse()
        .expect("Failed to parse env var RND_SEED into u64");
    // Client write period
    let write_period: u64 = std::env::var("WRITE_PERIOD")
        .expect("Variable WRITE_PERIOD is not set")
        .parse()
        .expect("Failed to parse env var WRITE_PERIOD into u64");
    // Client read period
    let read_period: u64 = std::env::var("READ_PERIOD")
        .expect("Variable READ_PERIOD is not set")
        .parse()
        .expect("Failed to parse env var READ_PERIOD into u64");

    // Tracing
    let (loki_layer, task) = tracing_loki::builder()
        .extra_field("node", format!("{}", id))
        .unwrap()
        .build_url(Url::parse("http://logger:3100").expect("Failed to parse Loki address"))
        .expect("Failed to instantiate Loki logger");

    // We need to register our layer with `tracing`.
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(loki_layer)
        // One could add more layers here, for example logging to stdout:
        .with(tracing_subscriber::fmt::Layer::new())
        .init();

    // The background task needs to be spawned so the logs actually get
    // delivered.
    tokio::spawn(task);

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
        for i in 1..=cluster_size {
            nodes.insert(i, BasicNode::new(format!("node{i}:5001")));
        }
        // Initialize raft instance
        let res = raft.initialize(nodes).await;
        match res {
            Ok(_) => tracing::info!("Raft cluster started!"),
            Err(err) => tracing::error!("Error starting raft cluster: {}", err.to_string()),
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now start client
    let client = PlatoonClient::new(
        id.to_string(),
        platoon_server_port,
        seed,
        Duration::from_millis(read_period),
        Duration::from_millis(write_period),
    );
    let (platoon_client_read_jh, platoon_client_write_jh) = client.start();

    // These tasks should never terminate, so we stop as soon as one stops with an error message
    select! {
        _ = raft_server_jh => tracing::error!("Raft gRPC server terminated"),
        _ = platoon_server_jh => tracing::error!("App gRPC server terminated"),
        _ = platoon_client_read_jh => tracing::error!("App read client terminated"),
        _ = platoon_client_write_jh => tracing::error!("App write client terminated")
    }
}
